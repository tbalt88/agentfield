package observability

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Agent-Field/agentfield/control-plane/internal/config"
	"github.com/Agent-Field/agentfield/control-plane/internal/events"
	"github.com/Agent-Field/agentfield/control-plane/internal/logger"
	"github.com/Agent-Field/agentfield/control-plane/pkg/types"
)

const (
	defaultTelemetryQueueSize = 256
	telemetrySubscriberID     = "anonymous-oss-telemetry"
)

// TelemetryEvent is the sanitized event payload sent to the hosted ingest API.
type TelemetryEvent struct {
	EventName              string                 `json:"event_name"`
	AnonymousInstallIDHash string                 `json:"anonymous_install_id_hash"`
	EventTime              string                 `json:"event_time"`
	Component              string                 `json:"component"`
	AgentFieldVersion      string                 `json:"agentfield_version,omitempty"`
	Runtime                string                 `json:"runtime"`
	StorageMode            string                 `json:"storage_mode,omitempty"`
	Properties             map[string]interface{} `json:"properties,omitempty"`
}

type telemetrySender func(context.Context, string, time.Duration, TelemetryEvent) error

// TelemetryService subscribes to internal event buses and forwards anonymous,
// low-cardinality usage events. It never forwards raw event payloads.
type TelemetryService struct {
	cfg         config.TelemetryConfig
	storageMode string
	installHash string
	runtimeName string
	version     string
	timeout     time.Duration
	sender      telemetrySender

	queue  chan TelemetryEvent
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewTelemetryService initializes anonymous telemetry, including install ID
// generation. If telemetry is disabled, it returns nil.
func NewTelemetryService(cfg config.TelemetryConfig, agentfieldHome, storageMode, version string) (*TelemetryService, error) {
	if !cfg.IsEnabled() {
		return nil, nil
	}
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, nil
	}
	if cfg.Mode == "" {
		cfg.Mode = "anonymous"
	}
	if cfg.Mode != "anonymous" {
		return nil, fmt.Errorf("unsupported telemetry mode %q", cfg.Mode)
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 800 * time.Millisecond
	}

	installID, err := resolveInstallID(cfg, agentfieldHome)
	if err != nil {
		return nil, err
	}

	return &TelemetryService{
		cfg:         cfg,
		storageMode: storageMode,
		installHash: hashInstallID(installID),
		runtimeName: detectRuntime(),
		version:     emptyTo(version, "unknown"),
		timeout:     timeout,
		sender:      sendTelemetryEvent,
		queue:       make(chan TelemetryEvent, defaultTelemetryQueueSize),
	}, nil
}

func resolveInstallID(cfg config.TelemetryConfig, agentfieldHome string) (string, error) {
	if strings.TrimSpace(cfg.InstallID) != "" {
		return strings.TrimSpace(cfg.InstallID), nil
	}

	path := cfg.InstallIDPath
	if path == "" {
		path = filepath.Join(agentfieldHome, "telemetry", "install_id")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(agentfieldHome, path)
	}

	if data, err := os.ReadFile(path); err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return id, nil
		}
	}

	id, err := randomHex(32)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		return "", err
	}
	return id, nil
}

func randomHex(bytesLen int) (string, error) {
	buf := make([]byte, bytesLen)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func hashInstallID(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

func detectRuntime() string {
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		return "kubernetes"
	}
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return "docker"
	}
	if os.Getenv("container") != "" {
		return "docker"
	}
	return "binary"
}

func detectUsageContext(runtimeName string) string {
	for _, key := range []string{"CI", "GITHUB_ACTIONS", "GITLAB_CI", "BUILDKITE", "CIRCLECI", "JENKINS_URL"} {
		if os.Getenv(key) != "" {
			return "ci"
		}
	}
	switch runtimeName {
	case "docker", "kubernetes":
		return "server"
	default:
		return "dev_or_local"
	}
}

// Start begins event subscriptions and worker processing.
func (s *TelemetryService) Start(ctx context.Context) {
	if s == nil {
		return
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.wg.Add(3)
	go s.worker()
	go s.subscribeNodeEvents()
	go s.subscribeExecutionEvents()

	s.Enqueue("control_plane_started", map[string]interface{}{
		"go_version":    runtime.Version(),
		"go_os":         runtime.GOOS,
		"go_arch":       runtime.GOARCH,
		"usage_context": detectUsageContext(s.runtimeName),
	})
	logger.Logger.Info().Msg("anonymous OSS telemetry enabled")
}

// Stop stops subscriptions. Queued events are best-effort and may be dropped.
func (s *TelemetryService) Stop() {
	if s == nil {
		return
	}
	s.Enqueue("control_plane_stopped", nil)
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

func (s *TelemetryService) Enqueue(eventName string, properties map[string]interface{}) {
	if s == nil {
		return
	}
	event := TelemetryEvent{
		EventName:              eventName,
		AnonymousInstallIDHash: s.installHash,
		EventTime:              time.Now().UTC().Format(time.RFC3339),
		Component:              "control-plane",
		AgentFieldVersion:      s.version,
		Runtime:                s.runtimeName,
		StorageMode:            s.storageMode,
		Properties:             sanitizeProperties(properties),
	}
	select {
	case s.queue <- event:
	default:
		logger.Logger.Debug().Str("event", eventName).Msg("anonymous telemetry queue full; dropping event")
	}
}

func (s *TelemetryService) worker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case event := <-s.queue:
			ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
			if err := s.sender(ctx, s.cfg.Endpoint, s.timeout, event); err != nil {
				logger.Logger.Debug().Err(err).Str("event", event.EventName).Msg("anonymous telemetry send failed")
			}
			cancel()
		}
	}
}

func (s *TelemetryService) subscribeNodeEvents() {
	defer s.wg.Done()
	ch := events.GlobalNodeEventBus.Subscribe(telemetrySubscriberID + "-nodes")
	defer events.GlobalNodeEventBus.Unsubscribe(telemetrySubscriberID + "-nodes")

	for {
		select {
		case <-s.ctx.Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			s.handleNodeEvent(event)
		}
	}
}

func (s *TelemetryService) subscribeExecutionEvents() {
	defer s.wg.Done()
	ch := events.GlobalExecutionEventBus.Subscribe(telemetrySubscriberID + "-executions")
	defer events.GlobalExecutionEventBus.Unsubscribe(telemetrySubscriberID + "-executions")

	for {
		select {
		case <-s.ctx.Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			s.handleExecutionEvent(event)
		}
	}
}

func (s *TelemetryService) handleNodeEvent(event events.NodeEvent) {
	if event.Type != events.NodeRegistered {
		return
	}

	props := map[string]interface{}{
		"reasoner_count_bucket": countBucket(0),
		"skill_count_bucket":    countBucket(0),
		"deployment_type":       "unknown",
	}
	if node, ok := event.Data.(*types.AgentNode); ok && node != nil {
		props["reasoner_count_bucket"] = countBucket(len(node.Reasoners))
		props["skill_count_bucket"] = countBucket(len(node.Skills))
		props["deployment_type"] = emptyTo(node.DeploymentType, "long_running")
		if node.Version != "" {
			props["agent_version"] = node.Version
		}
		if sdkLanguage, sdkVersion := extractSDKMetadata(node.Metadata); sdkLanguage != "" {
			props["sdk_language"] = sdkLanguage
			if sdkVersion != "" {
				props["sdk_version"] = sdkVersion
			}
			s.Enqueue("sdk_used", map[string]interface{}{
				"sdk_language": sdkLanguage,
				"sdk_version":  sdkVersion,
			})
		}
	}
	s.Enqueue("node_registered", props)
}

func (s *TelemetryService) handleExecutionEvent(event events.ExecutionEvent) {
	switch event.Type {
	case events.ExecutionCreated:
		s.Enqueue("execution_created", executionProperties(event))
	case events.ExecutionCompleted:
		props := executionProperties(event)
		props["status"] = "succeeded"
		s.Enqueue("execution_completed", props)
	case events.ExecutionFailed:
		props := executionProperties(event)
		props["status"] = "failed"
		s.Enqueue("execution_failed", props)
	}
}

func executionProperties(event events.ExecutionEvent) map[string]interface{} {
	props := map[string]interface{}{
		"execution_mode": "unknown",
		"target_type":    "unknown",
		"status":         emptyTo(event.Status, "unknown"),
	}
	if data, ok := event.Data.(map[string]interface{}); ok {
		if targetType, ok := stringProp(data, "target_type"); ok {
			props["target_type"] = targetType
		}
		if mode, ok := stringProp(data, "execution_mode"); ok {
			props["execution_mode"] = mode
		}
		if duration, ok := int64Prop(data, "duration_ms"); ok {
			props["duration_bucket_ms"] = durationBucket(duration)
		}
		if category, ok := stringProp(data, "error_category"); ok {
			props["error_category"] = errorCategory(category)
		}
	}
	return props
}

func extractSDKMetadata(metadata types.AgentMetadata) (string, string) {
	if metadata.Custom == nil {
		if metadata.Deployment != nil && metadata.Deployment.Platform != "" {
			return normalizeSDKLanguage(metadata.Deployment.Platform), metadata.Deployment.Tags["sdk_version"]
		}
		return "", ""
	}

	if sdk, ok := metadata.Custom["sdk"].(map[string]interface{}); ok {
		lang, _ := stringProp(sdk, "language")
		version, _ := stringProp(sdk, "version")
		return normalizeSDKLanguage(lang), version
	}
	if lang, ok := stringProp(metadata.Custom, "sdk_language"); ok {
		version, _ := stringProp(metadata.Custom, "sdk_version")
		return normalizeSDKLanguage(lang), version
	}
	if metadata.Deployment != nil && metadata.Deployment.Platform != "" {
		return normalizeSDKLanguage(metadata.Deployment.Platform), metadata.Deployment.Tags["sdk_version"]
	}
	return "", ""
}

func normalizeSDKLanguage(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "python", "py":
		return "python"
	case "typescript", "ts", "node", "nodejs", "javascript", "js":
		return "typescript"
	case "go", "golang":
		return "go"
	default:
		return ""
	}
}

func sendTelemetryEvent(ctx context.Context, endpoint string, timeout time.Duration, event TelemetryEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "agentfield-control-plane/telemetry")

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("telemetry endpoint returned %d", resp.StatusCode)
	}
	return nil
}

func sanitizeProperties(in map[string]interface{}) map[string]interface{} {
	if len(in) == 0 {
		return nil
	}
	allowed := map[string]struct{}{
		"go_version":            {},
		"go_os":                 {},
		"go_arch":               {},
		"storage_mode":          {},
		"deployment_type":       {},
		"agent_version":         {},
		"reasoner_count_bucket": {},
		"skill_count_bucket":    {},
		"sdk_language":          {},
		"sdk_version":           {},
		"execution_mode":        {},
		"target_type":           {},
		"status":                {},
		"duration_bucket_ms":    {},
		"error_category":        {},
		"usage_context":         {},
	}
	out := make(map[string]interface{}, len(in))
	for key, value := range in {
		if _, ok := allowed[key]; !ok {
			continue
		}
		switch v := value.(type) {
		case string:
			if trimmed := strings.TrimSpace(v); trimmed != "" {
				out[key] = trimmed
			}
		case int, int64, float64, bool:
			out[key] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func countBucket(n int) string {
	switch {
	case n <= 0:
		return "0"
	case n == 1:
		return "1"
	case n <= 5:
		return "2-5"
	case n <= 20:
		return "6-20"
	default:
		return "20+"
	}
}

func durationBucket(ms int64) string {
	switch {
	case ms < 100:
		return "<100"
	case ms < 1000:
		return "100-999"
	case ms < 5000:
		return "1000-4999"
	case ms < 30000:
		return "5000-29999"
	default:
		return "30000+"
	}
}

func errorCategory(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "unknown"
	}
	for _, allowed := range []string{"timeout", "cancelled", "permission_denied", "agent_unavailable", "agent_restart_orphaned", "validation", "unknown"} {
		if strings.Contains(value, allowed) {
			return allowed
		}
	}
	return "other"
}

func stringProp(data map[string]interface{}, key string) (string, bool) {
	value, ok := data[key]
	if !ok {
		return "", false
	}
	switch v := value.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return "", false
		}
		return strings.TrimSpace(v), true
	default:
		return "", false
	}
}

func int64Prop(data map[string]interface{}, key string) (int64, bool) {
	value, ok := data[key]
	if !ok {
		return 0, false
	}
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func emptyTo(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}
