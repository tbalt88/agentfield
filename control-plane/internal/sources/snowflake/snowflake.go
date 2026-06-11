// Package snowflake implements a loop Source that polls Snowflake for trigger
// events and normalizes them into AgentField inbound events.
package snowflake

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Agent-Field/agentfield/control-plane/internal/sources"
)

const (
	modeEventTablePoll  = "event_table_poll"
	modeCustomQueryPoll = "custom_query_poll"

	defaultIntervalSeconds = 30
	defaultMaxBatchSize    = 100
	defaultEventIDColumn   = "EVENT_ID"
	defaultEventTypeColumn = "EVENT_TYPE"
	defaultPayloadColumn   = "PAYLOAD"
	defaultWatermarkColumn = "OCCURRED_AT"
)

var identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

type source struct {
	client *sqlAPIClient
}

func init() {
	sources.Register(&source{})
}

func (s *source) Name() string         { return "snowflake" }
func (s *source) Kind() sources.Kind   { return sources.KindLoop }
func (s *source) SecretRequired() bool { return true }

func (s *source) ConfigSchema() json.RawMessage {
	return json.RawMessage(`{
        "type":"object",
        "properties":{
          "mode":{"type":"string","enum":["event_table_poll","custom_query_poll"],"default":"event_table_poll"},
          "account_url":{"type":"string","description":"Snowflake account URL, e.g. https://abc123.snowflakecomputing.com"},
          "database":{"type":"string"},
          "schema":{"type":"string"},
          "table":{"type":"string"},
          "warehouse":{"type":"string"},
          "role":{"type":"string"},
          "event_id_column":{"type":"string","default":"EVENT_ID"},
          "event_type_column":{"type":"string","default":"EVENT_TYPE"},
          "payload_column":{"type":"string","default":"PAYLOAD"},
          "watermark_column":{"type":"string","default":"OCCURRED_AT"},
          "sql":{"type":"string","description":"Read-only query for custom_query_poll mode"},
          "interval_seconds":{"type":"integer","minimum":5,"default":30},
          "max_batch_size":{"type":"integer","minimum":1,"maximum":500,"default":100},
          "timeout_seconds":{"type":"integer","minimum":1,"maximum":60,"default":20}
        },
        "required":["account_url"],
        "additionalProperties": false
    }`)
}

type config struct {
	Mode            string `json:"mode"`
	AccountURL      string `json:"account_url"`
	Database        string `json:"database"`
	Schema          string `json:"schema"`
	Table           string `json:"table"`
	Warehouse       string `json:"warehouse"`
	Role            string `json:"role"`
	EventIDColumn   string `json:"event_id_column"`
	EventTypeColumn string `json:"event_type_column"`
	PayloadColumn   string `json:"payload_column"`
	WatermarkColumn string `json:"watermark_column"`
	SQL             string `json:"sql"`
	IntervalSeconds int    `json:"interval_seconds"`
	MaxBatchSize    int    `json:"max_batch_size"`
	TimeoutSeconds  int    `json:"timeout_seconds"`
}

func parseConfig(raw json.RawMessage) (config, error) {
	var c config
	if err := json.Unmarshal(raw, &c); err != nil {
		return c, fmt.Errorf("snowflake: invalid config: %w", err)
	}
	if strings.TrimSpace(c.Mode) == "" {
		c.Mode = modeEventTablePoll
	}
	c.Mode = strings.TrimSpace(c.Mode)
	c.AccountURL = strings.TrimRight(strings.TrimSpace(c.AccountURL), "/")
	c.Database = strings.TrimSpace(c.Database)
	c.Schema = strings.TrimSpace(c.Schema)
	c.Table = strings.TrimSpace(c.Table)
	c.Warehouse = strings.TrimSpace(c.Warehouse)
	c.Role = strings.TrimSpace(c.Role)
	c.EventIDColumn = defaultIfBlank(c.EventIDColumn, defaultEventIDColumn)
	c.EventTypeColumn = defaultIfBlank(c.EventTypeColumn, defaultEventTypeColumn)
	c.PayloadColumn = defaultIfBlank(c.PayloadColumn, defaultPayloadColumn)
	c.WatermarkColumn = defaultIfBlank(c.WatermarkColumn, defaultWatermarkColumn)
	if c.IntervalSeconds == 0 {
		c.IntervalSeconds = defaultIntervalSeconds
	}
	if c.MaxBatchSize == 0 {
		c.MaxBatchSize = defaultMaxBatchSize
	}
	if c.TimeoutSeconds == 0 {
		c.TimeoutSeconds = 20
	}
	return c, nil
}

func defaultIfBlank(v, fallback string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return fallback
	}
	return v
}

func (s *source) Validate(raw json.RawMessage) error {
	c, err := parseConfig(raw)
	if err != nil {
		return err
	}
	if c.AccountURL == "" || !(strings.HasPrefix(c.AccountURL, "https://") || strings.HasPrefix(c.AccountURL, "http://")) {
		return errors.New("snowflake: account_url must be an http(s) URL")
	}
	if c.IntervalSeconds < 5 {
		return errors.New("snowflake: interval_seconds must be at least 5")
	}
	if c.MaxBatchSize < 1 || c.MaxBatchSize > 500 {
		return errors.New("snowflake: max_batch_size must be between 1 and 500")
	}
	if c.TimeoutSeconds < 1 || c.TimeoutSeconds > 60 {
		return errors.New("snowflake: timeout_seconds must be between 1 and 60")
	}
	switch c.Mode {
	case modeEventTablePoll:
		for name, value := range map[string]string{
			"database": c.Database, "schema": c.Schema, "table": c.Table,
			"event_id_column": c.EventIDColumn, "event_type_column": c.EventTypeColumn,
			"payload_column": c.PayloadColumn, "watermark_column": c.WatermarkColumn,
		} {
			if err := validateIdentifier(name, value); err != nil {
				return err
			}
		}
	case modeCustomQueryPoll:
		if err := validateReadOnlySQL(c.SQL); err != nil {
			return err
		}
	default:
		return fmt.Errorf("snowflake: unsupported mode %q", c.Mode)
	}
	return nil
}

func validateIdentifier(name, value string) error {
	if !identifierRE.MatchString(value) {
		return fmt.Errorf("snowflake: %s must be an unquoted Snowflake identifier", name)
	}
	return nil
}

func validateReadOnlySQL(sql string) error {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return errors.New("snowflake: sql is required")
	}
	if strings.Contains(sql, ";") {
		return errors.New("snowflake: sql must contain exactly one statement")
	}
	first := strings.ToUpper(strings.Fields(sql)[0])
	switch first {
	case "SELECT", "WITH":
		return nil
	default:
		return fmt.Errorf("snowflake: custom query poll sql must start with SELECT or WITH, got %s", first)
	}
}

func (s *source) Run(ctx context.Context, raw json.RawMessage, secret string, emit func(sources.Event)) error {
	if strings.TrimSpace(secret) == "" {
		return errors.New("snowflake: secret PAT is required")
	}
	c, err := parseConfig(raw)
	if err != nil {
		return err
	}
	if err := s.Validate(raw); err != nil {
		return err
	}
	client := s.client
	if client == nil {
		client = &sqlAPIClient{httpClient: http.DefaultClient}
	}
	var watermark string
	for {
		next, err := s.pollOnce(ctx, client, c, secret, watermark, emit)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil && next != "" {
			watermark = next
		}
		timer := time.NewTimer(time.Duration(c.IntervalSeconds) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *source) pollOnce(ctx context.Context, client *sqlAPIClient, c config, secret, watermark string, emit func(sources.Event)) (string, error) {
	stmt := buildPollStatement(c, watermark)
	res, err := client.Execute(ctx, c, secret, stmt)
	if err != nil {
		return watermark, err
	}
	events, nextWatermark, err := resultToEvents(c, res)
	if err != nil {
		return watermark, err
	}
	for _, event := range events {
		emit(event)
	}
	return nextWatermark, nil
}

func buildPollStatement(c config, watermark string) string {
	if c.Mode == modeCustomQueryPoll {
		return fmt.Sprintf("SELECT * FROM (%s) LIMIT %d", strings.TrimSpace(c.SQL), c.MaxBatchSize)
	}
	table := fmt.Sprintf("%s.%s.%s", quoteIdent(c.Database), quoteIdent(c.Schema), quoteIdent(c.Table))
	columns := []string{
		fmt.Sprintf("%s AS EVENT_ID", quoteIdent(c.EventIDColumn)),
		fmt.Sprintf("%s AS EVENT_TYPE", quoteIdent(c.EventTypeColumn)),
		fmt.Sprintf("%s AS PAYLOAD", quoteIdent(c.PayloadColumn)),
		fmt.Sprintf("TO_VARCHAR(%s, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9') AS OCCURRED_AT", quoteIdent(c.WatermarkColumn)),
	}
	where := ""
	if watermark != "" {
		where = fmt.Sprintf(" WHERE %s > TO_TIMESTAMP_NTZ('%s')", quoteIdent(c.WatermarkColumn), strings.ReplaceAll(watermark, "'", "''"))
	}
	return fmt.Sprintf("SELECT %s FROM %s%s ORDER BY %s LIMIT %d", strings.Join(columns, ", "), table, where, quoteIdent(c.WatermarkColumn), c.MaxBatchSize)
}

func quoteIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}

type sqlAPIClient struct {
	httpClient *http.Client
}

type statementRequest struct {
	Statement     string `json:"statement"`
	Timeout       int    `json:"timeout,omitempty"`
	Database      string `json:"database,omitempty"`
	Schema        string `json:"schema,omitempty"`
	Warehouse     string `json:"warehouse,omitempty"`
	Role          string `json:"role,omitempty"`
	ResultSetMeta any    `json:"resultSetMetaData,omitempty"`
}

type statementResponse struct {
	StatementHandle    string            `json:"statementHandle"`
	StatementStatusURL string            `json:"statementStatusUrl"`
	Data               [][]any           `json:"data"`
	ResultSetMeta      resultSetMetadata `json:"resultSetMetaData"`
	Code               string            `json:"code"`
	Message            string            `json:"message"`
}

type resultSetMetadata struct {
	RowType []columnMeta `json:"rowType"`
}

type columnMeta struct {
	Name string `json:"name"`
}

func (c *sqlAPIClient) Execute(ctx context.Context, cfg config, token, statement string) (statementResponse, error) {
	var out statementResponse
	payload, err := json.Marshal(statementRequest{
		Statement: statement,
		Timeout:   cfg.TimeoutSeconds,
		Database:  cfg.Database,
		Schema:    cfg.Schema,
		Warehouse: cfg.Warehouse,
		Role:      cfg.Role,
	})
	if err != nil {
		return out, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.AccountURL+"/api/v2/statements", bytes.NewReader(payload))
	if err != nil {
		return out, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Snowflake-Authorization-Token-Type", "PROGRAMMATIC_ACCESS_TOKEN")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return out, fmt.Errorf("snowflake: SQL API status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return out, fmt.Errorf("snowflake: decode SQL API response: %w", err)
	}
	if resp.StatusCode == http.StatusAccepted {
		return c.pollStatement(ctx, cfg, token, out)
	}
	if out.Code != "" && out.Code != "090001" {
		return out, fmt.Errorf("snowflake: SQL API code %s: %s", out.Code, out.Message)
	}
	return out, nil
}

func (c *sqlAPIClient) pollStatement(ctx context.Context, cfg config, token string, status statementResponse) (statementResponse, error) {
	var out statementResponse
	if status.StatementHandle == "" {
		return out, errors.New("snowflake: async SQL response missing statementHandle")
	}
	statusURL := status.StatementStatusURL
	if statusURL == "" {
		statusURL = "/api/v2/statements/" + status.StatementHandle
	}
	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	deadline := time.Now().Add(time.Duration(cfg.TimeoutSeconds) * time.Second)
	for {
		if time.Now().After(deadline) {
			return out, fmt.Errorf("snowflake: statement %s did not complete before timeout", status.StatementHandle)
		}
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-time.After(time.Second):
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.AccountURL+statusURL, nil)
		if err != nil {
			return out, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Snowflake-Authorization-Token-Type", "PROGRAMMATIC_ACCESS_TOKEN")
		req.Header.Set("Accept", "application/json")
		resp, err := httpClient.Do(req)
		if err != nil {
			return out, err
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		resp.Body.Close()
		if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusTooManyRequests {
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return out, fmt.Errorf("snowflake: SQL API status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		if err := json.Unmarshal(body, &out); err != nil {
			return out, fmt.Errorf("snowflake: decode SQL API response: %w", err)
		}
		if out.Code != "" && out.Code != "090001" {
			return out, fmt.Errorf("snowflake: SQL API code %s: %s", out.Code, out.Message)
		}
		return out, nil
	}
}

func resultToEvents(c config, res statementResponse) ([]sources.Event, string, error) {
	index := columnIndexes(res.ResultSetMeta.RowType)
	required := []string{"EVENT_ID", "EVENT_TYPE", "PAYLOAD", "OCCURRED_AT"}
	for _, name := range required {
		if _, ok := index[name]; !ok {
			return nil, "", fmt.Errorf("snowflake: result missing %s column", name)
		}
	}
	events := make([]sources.Event, 0, len(res.Data))
	var nextWatermark string
	for _, row := range res.Data {
		eventID := valueString(row[index["EVENT_ID"]])
		eventType := valueString(row[index["EVENT_TYPE"]])
		occurredAt := valueString(row[index["OCCURRED_AT"]])
		payload := normalizePayload(row[index["PAYLOAD"]])
		normalized, err := json.Marshal(map[string]any{
			"event_id":    eventID,
			"event_type":  eventType,
			"occurred_at": occurredAt,
			"payload":     payload,
			"snowflake": map[string]any{
				"account_url": c.AccountURL,
				"database":    c.Database,
				"schema":      c.Schema,
				"table":       c.Table,
				"query_id":    res.StatementHandle,
			},
		})
		if err != nil {
			return nil, "", err
		}
		raw, _ := json.Marshal(map[string]any{
			"event_id": eventID,
			"payload":  payload,
		})
		events = append(events, sources.Event{
			Type:           eventType,
			IdempotencyKey: eventID,
			Raw:            raw,
			Normalized:     normalized,
		})
		if occurredAt != "" {
			nextWatermark = occurredAt
		}
	}
	return events, nextWatermark, nil
}

func columnIndexes(cols []columnMeta) map[string]int {
	out := make(map[string]int, len(cols))
	for i, col := range cols {
		out[strings.ToUpper(col.Name)] = i
	}
	return out
}

func valueString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case nil:
		return ""
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func normalizePayload(v any) any {
	if s, ok := v.(string); ok {
		var decoded any
		if json.Unmarshal([]byte(s), &decoded) == nil {
			return decoded
		}
		return s
	}
	return v
}
