package snowflake

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Agent-Field/agentfield/control-plane/internal/sources"
)

func TestMetadataAndSchema(t *testing.T) {
	s := &source{}
	if s.Name() != "snowflake" {
		t.Fatalf("Name() = %q, want snowflake", s.Name())
	}
	if s.Kind() != sources.KindLoop {
		t.Fatalf("Kind() = %v, want loop", s.Kind())
	}
	if !s.SecretRequired() {
		t.Fatal("snowflake should require a secret")
	}
	var schema map[string]any
	if err := json.Unmarshal(s.ConfigSchema(), &schema); err != nil {
		t.Fatalf("schema is not valid JSON: %v", err)
	}
}

func TestValidateEventTableConfig(t *testing.T) {
	valid := []byte(`{
		"account_url":"https://acct.snowflakecomputing.com",
		"database":"OBSERVABILITY",
		"schema":"AGENTFIELD",
		"table":"AGENTFIELD_EVENTS",
		"interval_seconds":5
	}`)
	if err := (&source{}).Validate(valid); err != nil {
		t.Fatalf("Validate(valid) = %v", err)
	}

	for _, raw := range []string{
		`{"account_url":"https://acct","database":"DB","schema":"S","table":"bad-name"}`,
		`{"account_url":"https://acct","mode":"unknown"}`,
		`{"account_url":"https://acct","database":"DB","schema":"S","table":"T","interval_seconds":1}`,
		`{"account_url":"https://acct","mode":"custom_query_poll","sql":"DELETE FROM T"}`,
		`{"account_url":"https://acct","mode":"custom_query_poll","sql":"SELECT 1; SELECT 2"}`,
	} {
		if err := (&source{}).Validate([]byte(raw)); err == nil {
			t.Fatalf("Validate(%s) expected error", raw)
		}
	}
}

func TestPollOnceCallsSnowflakeSQLAPIAndEmitsEvents(t *testing.T) {
	var gotAuth string
	var gotTokenType string
	var gotStatement string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotTokenType = r.Header.Get("X-Snowflake-Authorization-Token-Type")
		var req statementRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		gotStatement = req.Statement
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"statementHandle":"01b-123",
			"resultSetMetaData":{"rowType":[
				{"name":"EVENT_ID"},
				{"name":"EVENT_TYPE"},
				{"name":"PAYLOAD"},
				{"name":"OCCURRED_AT"}
			]},
			"data":[[
				"evt_1",
				"snowflake.alert.fired",
				"{\"severity\":\"high\",\"count\":7}",
				"2026-06-11 10:00:00.000 -0700"
			]]
		}`))
	}))
	defer server.Close()

	cfg, err := parseConfig([]byte(`{
		"account_url":"` + server.URL + `",
		"database":"OBSERVABILITY",
		"schema":"AGENTFIELD",
		"table":"AGENTFIELD_EVENTS",
		"warehouse":"COMPUTE_WH",
		"role":"AGENTFIELD_TRIGGER",
		"max_batch_size":10
	}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	var emitted []sources.Event
	next, err := (&source{}).pollOnce(context.Background(), &sqlAPIClient{httpClient: server.Client()}, cfg, "pat-secret", "", func(e sources.Event) {
		emitted = append(emitted, e)
	})
	if err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if gotAuth != "Bearer pat-secret" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotTokenType != "PROGRAMMATIC_ACCESS_TOKEN" {
		t.Fatalf("token type = %q", gotTokenType)
	}
	if !strings.Contains(gotStatement, `FROM "OBSERVABILITY"."AGENTFIELD"."AGENTFIELD_EVENTS"`) {
		t.Fatalf("statement did not target configured table: %s", gotStatement)
	}
	if !strings.Contains(gotStatement, `TO_VARCHAR("OCCURRED_AT", 'YYYY-MM-DD"T"HH24:MI:SS.FF9') AS OCCURRED_AT`) {
		t.Fatalf("statement did not normalize watermark column: %s", gotStatement)
	}
	if next == "" {
		t.Fatal("expected watermark")
	}
	if len(emitted) != 1 {
		t.Fatalf("emitted %d events, want 1", len(emitted))
	}
	if emitted[0].Type != "snowflake.alert.fired" || emitted[0].IdempotencyKey != "evt_1" {
		t.Fatalf("bad event metadata: %+v", emitted[0])
	}
	var normalized map[string]any
	if err := json.Unmarshal(emitted[0].Normalized, &normalized); err != nil {
		t.Fatalf("normalized JSON: %v", err)
	}
	payload := normalized["payload"].(map[string]any)
	if payload["severity"] != "high" {
		t.Fatalf("payload was not decoded: %#v", payload)
	}
}

func TestBuildPollStatementUsesTimestampWatermark(t *testing.T) {
	cfg := config{
		Mode:            modeEventTablePoll,
		Database:        "OBSERVABILITY",
		Schema:          "AGENTFIELD",
		Table:           "AGENTFIELD_EVENTS",
		EventIDColumn:   "EVENT_ID",
		EventTypeColumn: "EVENT_TYPE",
		PayloadColumn:   "PAYLOAD",
		WatermarkColumn: "OCCURRED_AT",
		MaxBatchSize:    100,
	}

	stmt := buildPollStatement(cfg, "2026-06-11T18:52:00.123456789")
	if !strings.Contains(stmt, `WHERE "OCCURRED_AT" > TO_TIMESTAMP_NTZ('2026-06-11T18:52:00.123456789')`) {
		t.Fatalf("statement did not use timestamp watermark predicate: %s", stmt)
	}
}

func TestSQLAPIClientPollsAcceptedStatement(t *testing.T) {
	var postSeen bool
	var getSeen bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			postSeen = true
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"statementHandle":"async-1","statementStatusUrl":"/api/v2/statements/async-1"}`))
		case http.MethodGet:
			getSeen = true
			if r.Header.Get("X-Snowflake-Authorization-Token-Type") != "PROGRAMMATIC_ACCESS_TOKEN" {
				t.Fatalf("missing PAT token type on poll")
			}
			_, _ = w.Write([]byte(`{
				"statementHandle":"async-1",
				"resultSetMetaData":{"rowType":[{"name":"EVENT_ID"},{"name":"EVENT_TYPE"},{"name":"PAYLOAD"},{"name":"OCCURRED_AT"}]},
				"data":[["evt_async","snowflake.async","{}", "2026-06-11 10:00:00.000 -0700"]]
			}`))
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := (&sqlAPIClient{httpClient: server.Client()}).Execute(ctx, config{
		AccountURL: server.URL, TimeoutSeconds: 2,
	}, "pat", "SELECT 1")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !postSeen || !getSeen {
		t.Fatalf("postSeen=%v getSeen=%v", postSeen, getSeen)
	}
	if res.StatementHandle != "async-1" || len(res.Data) != 1 {
		t.Fatalf("bad async result: %+v", res)
	}
}

func TestResultToEventsRequiresStandardColumns(t *testing.T) {
	_, _, err := resultToEvents(config{}, statementResponse{
		ResultSetMeta: resultSetMetadata{RowType: []columnMeta{{Name: "EVENT_ID"}}},
		Data:          [][]any{{"evt"}},
	})
	if err == nil || !strings.Contains(err.Error(), "missing EVENT_TYPE") {
		t.Fatalf("expected missing column error, got %v", err)
	}
}
