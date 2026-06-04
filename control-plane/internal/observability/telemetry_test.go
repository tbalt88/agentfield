package observability

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Agent-Field/agentfield/control-plane/internal/config"
	"github.com/Agent-Field/agentfield/control-plane/internal/events"
	"github.com/Agent-Field/agentfield/control-plane/pkg/types"
)

func TestTelemetryServiceDisabled(t *testing.T) {
	disabled := false
	svc, err := NewTelemetryService(config.TelemetryConfig{
		Enabled:  &disabled,
		Endpoint: "https://agentfield.ai/api/oss/telemetry",
	}, t.TempDir(), "local", "test")
	if err != nil {
		t.Fatalf("NewTelemetryService error: %v", err)
	}
	if svc != nil {
		t.Fatal("expected nil service when telemetry disabled")
	}
}

func TestTelemetryServiceInstallIDPersistsAndHashes(t *testing.T) {
	dir := t.TempDir()
	cfg := config.TelemetryConfig{
		Endpoint:      "https://agentfield.ai/api/oss/telemetry",
		InstallIDPath: filepath.Join(dir, "install_id"),
		Timeout:       time.Millisecond,
	}

	first, err := NewTelemetryService(cfg, dir, "postgres", "test")
	if err != nil {
		t.Fatalf("first NewTelemetryService error: %v", err)
	}
	second, err := NewTelemetryService(cfg, dir, "postgres", "test")
	if err != nil {
		t.Fatalf("second NewTelemetryService error: %v", err)
	}
	if first.installHash != second.installHash {
		t.Fatalf("expected stable install hash, got %q and %q", first.installHash, second.installHash)
	}
	if len(first.installHash) != 64 {
		t.Fatalf("expected sha256 hex hash, got %q", first.installHash)
	}
}

func TestTelemetryServiceSanitizesEventData(t *testing.T) {
	svc := &TelemetryService{
		installHash: "a",
		runtimeName: "docker",
		version:     "test",
		storageMode: "postgres",
		queue:       make(chan TelemetryEvent, 10),
	}

	svc.handleExecutionEvent(events.ExecutionEvent{
		Type:   events.ExecutionFailed,
		Status: "failed",
		Data: map[string]interface{}{
			"target_type":       "reasoner",
			"execution_mode":    "async",
			"duration_ms":       int64(1200),
			"error_category":    "agent_restart_orphaned: raw execution text",
			"context":           map[string]interface{}{"prompt": "do not send"},
			"session_id":        "do-not-send",
			"actor_id":          "do-not-send",
			"raw_error_message": "do-not-send",
		},
	})

	event := <-svc.queue
	if event.EventName != "execution_failed" {
		t.Fatalf("unexpected event %q", event.EventName)
	}
	if _, ok := event.Properties["context"]; ok {
		t.Fatal("context leaked into telemetry properties")
	}
	if _, ok := event.Properties["session_id"]; ok {
		t.Fatal("session_id leaked into telemetry properties")
	}
	if got := event.Properties["error_category"]; got != "agent_restart_orphaned" {
		t.Fatalf("unexpected error category %v", got)
	}
	if got := event.Properties["duration_bucket_ms"]; got != "1000-4999" {
		t.Fatalf("unexpected duration bucket %v", got)
	}
}

func TestTelemetryServiceNodeRegistrationSDKMetadata(t *testing.T) {
	svc := &TelemetryService{
		installHash: "a",
		runtimeName: "docker",
		version:     "test",
		storageMode: "postgres",
		queue:       make(chan TelemetryEvent, 10),
	}

	svc.handleNodeEvent(events.NodeEvent{
		Type: events.NodeRegistered,
		Data: &types.AgentNode{
			Version:        "1.2.3",
			DeploymentType: "long_running",
			Reasoners:      []types.ReasonerDefinition{{ID: "one"}, {ID: "two"}},
			Metadata: types.AgentMetadata{
				Deployment: &types.DeploymentMetadata{
					Platform: "python",
					Tags:     map[string]string{"sdk_version": "0.1.82"},
				},
			},
		},
	})

	sdkEvent := <-svc.queue
	nodeEvent := <-svc.queue
	if sdkEvent.EventName != "sdk_used" {
		t.Fatalf("expected sdk_used first, got %q", sdkEvent.EventName)
	}
	if sdkEvent.Properties["sdk_language"] != "python" || sdkEvent.Properties["sdk_version"] != "0.1.82" {
		t.Fatalf("unexpected sdk properties: %#v", sdkEvent.Properties)
	}
	if nodeEvent.EventName != "node_registered" {
		t.Fatalf("expected node_registered second, got %q", nodeEvent.EventName)
	}
	if nodeEvent.Properties["reasoner_count_bucket"] != "2-5" {
		t.Fatalf("unexpected node properties: %#v", nodeEvent.Properties)
	}
}

func TestTelemetrySenderNonBlockingFailure(t *testing.T) {
	svc := &TelemetryService{
		cfg:         config.TelemetryConfig{Endpoint: "http://127.0.0.1:1"},
		installHash: "a",
		runtimeName: "binary",
		version:     "test",
		timeout:     time.Millisecond,
		sender: func(context.Context, string, time.Duration, TelemetryEvent) error {
			return context.Canceled
		},
		queue: make(chan TelemetryEvent, 1),
	}
	svc.ctx, svc.cancel = context.WithCancel(context.Background())
	svc.cancel()
	svc.Enqueue("control_plane_started", map[string]interface{}{"secret": "drop", "go_os": "linux"})
	event := <-svc.queue
	if _, ok := event.Properties["secret"]; ok {
		t.Fatal("secret property leaked")
	}
}
