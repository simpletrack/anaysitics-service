package collectapi

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-core/storage"
	"github.com/simpletrack/analytics-service/internal/controlplane"
	"github.com/valyala/fasthttp"
)

func TestCollectAcceptsValidWriteKeyAndOverridesClientScope(t *testing.T) {
	handler, bus := newTestHandler(t, testSourceConfig(), false)

	ctx := serve(handler, fasthttp.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin":     "https://docs.example.com",
		"User-Agent": "Mozilla/5.0",
		"Referer":    "https://docs.example.com/quickstart",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusAccepted {
		t.Fatalf("expected accepted response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if len(bus.published) != 1 {
		t.Fatalf("expected one published event, got %d", len(bus.published))
	}
	published := bus.published[0]
	if published.TenantID != "tenant_control" || published.ProjectID != "project_control" || published.SourceID != "source_control" {
		t.Fatalf("expected control-plane scope override, got %#v", published)
	}
	if published.SourceType != "web" {
		t.Fatalf("expected source type from control-plane config, got %q", published.SourceType)
	}
	if published.SessionID == "" {
		t.Fatalf("expected analytics-core session resolver to derive a session id")
	}
	if published.Properties["client.referrer"] != "https://docs.example.com/quickstart" {
		t.Fatalf("expected client enrichment property, got %#v", published.Properties)
	}
	if !strings.HasPrefix(published.Properties["client.ip_hash"].(string), "ip_") {
		t.Fatalf("expected hashed client IP property, got %#v", published.Properties)
	}
}

func TestCollectRejectsInvalidWriteKey(t *testing.T) {
	handler, bus := newTestHandler(t, testSourceConfig(), false)

	ctx := serve(handler, fasthttp.MethodPost, "/collect", validCollectBody("missing"), map[string]string{
		"Origin": "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusUnauthorized {
		t.Fatalf("expected unauthorized response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if len(bus.published) != 0 {
		t.Fatalf("invalid write key should not publish events")
	}
}

func TestCollectHidesInternalPublishErrors(t *testing.T) {
	handler := newTestHandlerWithBus(t, testSourceConfig(), false, &recordingBus{err: errors.New("redis password leaked")})

	ctx := serve(handler, fasthttp.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin": "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusInternalServerError {
		t.Fatalf("expected internal server error, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if strings.Contains(string(ctx.Response.Body()), "redis password leaked") {
		t.Fatalf("expected stable public error, got %s", ctx.Response.Body())
	}
}

func TestCollectRejectsBlockedOrigin(t *testing.T) {
	handler, bus := newTestHandler(t, testSourceConfig(), false)

	ctx := serve(handler, fasthttp.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin": "https://blocked.example.com",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusForbidden {
		t.Fatalf("expected forbidden response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if len(bus.published) != 0 {
		t.Fatalf("blocked origin should not publish events")
	}
}

func TestCollectPreflightReturnsCORSHeaders(t *testing.T) {
	handler, _ := newTestHandler(t, testSourceConfig(), false)

	ctx := serve(handler, fasthttp.MethodOptions, "/collect", "", map[string]string{
		"Origin": "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusNoContent {
		t.Fatalf("expected no-content preflight, got %d", ctx.Response.StatusCode())
	}
	if string(ctx.Response.Header.Peek("Access-Control-Allow-Origin")) != "https://docs.example.com" {
		t.Fatalf("expected reflected CORS origin, got %q", ctx.Response.Header.Peek("Access-Control-Allow-Origin"))
	}
	if !strings.Contains(string(ctx.Response.Header.Peek("Access-Control-Allow-Headers")), "X-SimpleTrack-Write-Key") {
		t.Fatalf("expected write-key header in CORS response")
	}
}

func TestCollectFiltersBotTraffic(t *testing.T) {
	handler, bus := newTestHandler(t, testSourceConfig(), false)

	ctx := serve(handler, fasthttp.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin":     "https://docs.example.com",
		"User-Agent": "Googlebot/2.1",
	})

	assertFiltered(t, ctx, bus)
}

func TestCollectFiltersInternalTraffic(t *testing.T) {
	source := testSourceConfig()
	source.InternalCIDRs = []string{"203.0.113.0/24"}
	handler, bus := newTestHandler(t, source, true)

	ctx := serve(handler, fasthttp.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin":          "https://docs.example.com",
		"X-Forwarded-For": "203.0.113.10",
	})

	assertFiltered(t, ctx, bus)
}

func TestTrackerRouteReturnsJavaScript(t *testing.T) {
	handler, _ := newTestHandler(t, testSourceConfig(), false)

	ctx := serve(handler, fasthttp.MethodGet, "/tracker.js", "", nil)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected tracker response, got %d", ctx.Response.StatusCode())
	}
	if !strings.Contains(string(ctx.Response.Body()), "window.simpletrack") {
		t.Fatalf("expected tracker script body, got %s", ctx.Response.Body())
	}
}

func TestHealthRouteReturnsOK(t *testing.T) {
	handler, _ := newTestHandler(t, testSourceConfig(), false)

	ctx := serve(handler, fasthttp.MethodGet, "/healthz", "", nil)

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected health response, got %d", ctx.Response.StatusCode())
	}
}

func TestRealtimeQueryReturnsRecords(t *testing.T) {
	reader := &recordingQueryReader{
		realtime: []storage.EventRecord{
			{
				ID:         "evt_recent",
				TenantID:   "tenant_control",
				ProjectID:  "project_control",
				SourceID:   "source_control",
				SourceType: "web",
				EventName:  "page_view",
				DistinctID: "visitor_1",
				SessionID:  "session_1",
				EventTime:  time.Date(2026, 5, 3, 9, 55, 0, 0, time.UTC),
				ReceivedAt: time.Date(2026, 5, 3, 9, 55, 1, 0, time.UTC),
				Properties: `{"path":"/"}`,
				Source:     "browser",
			},
		},
	}
	handler := newTestQueryHandler(t, testSourceConfig(), reader)

	ctx := serve(handler, fasthttp.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer query-token",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected realtime response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if reader.realtimeQuery.TenantID != "tenant_control" || reader.realtimeQuery.ProjectID != "project_control" || reader.realtimeQuery.SourceID != "source_control" {
		t.Fatalf("expected runtime source scope override, got %#v", reader.realtimeQuery)
	}
	expectedSince := time.Date(2026, 5, 3, 9, 30, 0, 0, time.UTC)
	if !reader.realtimeQuery.Since.Equal(expectedSince) {
		t.Fatalf("expected realtime default window since %s, got %s", expectedSince, reader.realtimeQuery.Since)
	}
	if reader.realtimeQuery.Limit != 50 {
		t.Fatalf("expected realtime default limit 50, got %d", reader.realtimeQuery.Limit)
	}

	var response queryRealtimeResponse
	if err := json.Unmarshal(ctx.Response.Body(), &response); err != nil {
		t.Fatalf("decode realtime response failed: %v", err)
	}
	if len(response.Items) != 1 {
		t.Fatalf("expected one realtime item, got %d", len(response.Items))
	}
	if got := string(response.Items[0].Properties); got != `{"path":"/"}` {
		t.Fatalf("expected realtime properties JSON, got %s", got)
	}
}

func TestEventsQueryReturnsRecords(t *testing.T) {
	reader := &recordingQueryReader{
		events: []storage.EventRecord{
			{
				ID:             "evt_custom",
				TenantID:       "tenant_control",
				ProjectID:      "project_control",
				SourceID:       "source_control",
				SourceType:     "web",
				EventName:      "signup_clicked",
				DistinctID:     "visitor_1",
				SessionID:      "session_1",
				EventTime:      time.Date(2026, 5, 3, 9, 20, 0, 0, time.UTC),
				ReceivedAt:     time.Date(2026, 5, 3, 9, 20, 1, 0, time.UTC),
				Properties:     `{"button":"hero"}`,
				UserProperties: `{"plan":"free"}`,
				Source:         "browser",
			},
		},
	}
	handler := newTestQueryHandler(t, testSourceConfig(), reader)

	ctx := serve(handler, fasthttp.MethodGet, "/v1/events?write_key=wk_live&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z&limit=25&offset=3&event_name=signup_clicked&distinct_id=visitor_1&sort_field=received_at&sort_direction=asc", "", map[string]string{
		"Authorization": "Bearer query-token",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected events response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if reader.eventsQuery.EventName != "signup_clicked" || reader.eventsQuery.DistinctID != "visitor_1" {
		t.Fatalf("expected request filters to reach analytics-core, got %#v", reader.eventsQuery)
	}
	if reader.eventsQuery.Limit != 25 || reader.eventsQuery.Offset != 3 {
		t.Fatalf("expected paging values to reach analytics-core, got limit=%d offset=%d", reader.eventsQuery.Limit, reader.eventsQuery.Offset)
	}
	if reader.eventsQuery.SortField != storage.EventSortByReceivedAt || reader.eventsQuery.SortDirection != storage.EventSortAscending {
		t.Fatalf("expected typed sort allowlist values, got %#v", reader.eventsQuery)
	}

	var response queryEventsResponse
	if err := json.Unmarshal(ctx.Response.Body(), &response); err != nil {
		t.Fatalf("decode events response failed: %v", err)
	}
	if response.From != "2026-05-03T09:00:00Z" || response.To != "2026-05-03T10:00:00Z" {
		t.Fatalf("expected preserved time range, got %#v", response)
	}
	if len(response.Items) != 1 {
		t.Fatalf("expected one events item, got %d", len(response.Items))
	}
	if got := string(response.Items[0].UserProperties); got != `{"plan":"free"}` {
		t.Fatalf("expected user properties JSON, got %s", got)
	}
}

func TestQueryRoutesRequireBearerToken(t *testing.T) {
	resolver := &countingResolver{source: testSourceConfig()}
	handler := newTestQueryHandlerWithResolver(t, resolver, &recordingQueryReader{})

	ctx := serve(handler, fasthttp.MethodGet, "/v1/events?write_key=wk_live&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z", "", map[string]string{
		"Origin": "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusUnauthorized {
		t.Fatalf("expected unauthorized response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if string(ctx.Response.Header.Peek("Access-Control-Allow-Origin")) != "https://docs.example.com" {
		t.Fatalf("expected unauthorized query response to include CORS, got %q", ctx.Response.Header.Peek("Access-Control-Allow-Origin"))
	}
	if resolver.calls != 0 {
		t.Fatalf("unauthorized query should not resolve source, got %d calls", resolver.calls)
	}
}

func TestQueryRoutesAcceptRotatedBearerToken(t *testing.T) {
	reader := &recordingQueryReader{}
	handler := newTestQueryHandlerWithTokens(t, testSourceConfig(), reader, []string{"current-token", "previous-token"})

	ctx := serve(handler, fasthttp.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer previous-token",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected rotated query token to be accepted, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if reader.realtimeQuery.SourceID != "source_control" {
		t.Fatalf("expected rotated token request to reach EventReader, got %#v", reader.realtimeQuery)
	}
}

func TestQueryRoutesRejectUnknownBearerDuringRotation(t *testing.T) {
	resolver := &countingResolver{source: testSourceConfig()}
	reader := &recordingQueryReader{}
	handler := newTestQueryHandlerWithResolverAndTokens(t, resolver, reader, []string{"current-token", "previous-token"})

	ctx := serve(handler, fasthttp.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer wrong-token",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusUnauthorized {
		t.Fatalf("expected unauthorized response for unknown rotated token, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if resolver.calls != 0 {
		t.Fatalf("unknown query token should not resolve source, got %d calls", resolver.calls)
	}
	if reader.realtimeQuery.SourceID != "" {
		t.Fatalf("unknown query token should not reach EventReader, got %#v", reader.realtimeQuery)
	}
}

func TestQueryRoutesReturnCORSForMissingWriteKey(t *testing.T) {
	handler := newTestQueryHandler(t, testSourceConfig(), &recordingQueryReader{})

	ctx := serve(handler, fasthttp.MethodGet, "/v1/events?from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z", "", map[string]string{
		"Authorization": "Bearer query-token",
		"Origin":        "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusBadRequest {
		t.Fatalf("expected bad-request response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if string(ctx.Response.Header.Peek("Access-Control-Allow-Origin")) != "https://docs.example.com" {
		t.Fatalf("expected bad-request query response to include CORS, got %q", ctx.Response.Header.Peek("Access-Control-Allow-Origin"))
	}
}

func TestQueryPreflightReturnsCORSHeaders(t *testing.T) {
	handler := newTestQueryHandler(t, testSourceConfig(), &recordingQueryReader{})

	ctx := serve(handler, fasthttp.MethodOptions, "/v1/events?write_key=wk_live", "", map[string]string{
		"Origin": "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusNoContent {
		t.Fatalf("expected no-content query preflight, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if string(ctx.Response.Header.Peek("Access-Control-Allow-Origin")) != "https://docs.example.com" {
		t.Fatalf("expected reflected query CORS origin, got %q", ctx.Response.Header.Peek("Access-Control-Allow-Origin"))
	}
	if got := string(ctx.Response.Header.Peek("Access-Control-Allow-Methods")); got != "GET, OPTIONS" {
		t.Fatalf("expected query methods, got %q", got)
	}
	allowHeaders := string(ctx.Response.Header.Peek("Access-Control-Allow-Headers"))
	for _, header := range []string{"Authorization", "X-SimpleTrack-Write-Key"} {
		if !strings.Contains(allowHeaders, header) {
			t.Fatalf("expected %s in query allow headers, got %q", header, allowHeaders)
		}
	}
}

func newTestHandler(t *testing.T, source controlplane.SourceConfig, trustForwarded bool) (fasthttp.RequestHandler, *recordingBus) {
	t.Helper()

	bus := &recordingBus{}
	handler := newTestHandlerWithBus(t, source, trustForwarded, bus)
	return handler, bus
}

func newTestHandlerWithBus(t *testing.T, source controlplane.SourceConfig, trustForwarded bool, bus *recordingBus) fasthttp.RequestHandler {
	t.Helper()

	resolver, err := controlplane.NewMemoryResolver([]controlplane.SourceConfig{source})
	if err != nil {
		t.Fatalf("new memory resolver failed: %v", err)
	}
	handler, err := NewHandler(Options{
		CollectPath:           "/collect",
		HealthPath:            "/healthz",
		TrackerPath:           "/tracker.js",
		TrustForwardedHeaders: trustForwarded,
		TrackerScript:         []byte("(function(window){ window.simpletrack = {}; })(window);"),
		Now: func() time.Time {
			return time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
		},
		Resolver: resolver,
		Bus:      bus,
	})
	if err != nil {
		t.Fatalf("new handler failed: %v", err)
	}
	return handler.ServeFastHTTP
}

func newTestQueryHandler(t *testing.T, source controlplane.SourceConfig, reader storage.EventReader) fasthttp.RequestHandler {
	t.Helper()

	resolver, err := controlplane.NewMemoryResolver([]controlplane.SourceConfig{source})
	if err != nil {
		t.Fatalf("new memory resolver failed: %v", err)
	}
	return newTestQueryHandlerWithResolver(t, resolver, reader)
}

func newTestQueryHandlerWithResolver(t *testing.T, resolver controlplane.Resolver, reader storage.EventReader) fasthttp.RequestHandler {
	t.Helper()

	return newTestQueryHandlerWithResolverAndTokens(t, resolver, reader, []string{"query-token"})
}

func newTestQueryHandlerWithTokens(t *testing.T, source controlplane.SourceConfig, reader storage.EventReader, tokens []string) fasthttp.RequestHandler {
	t.Helper()

	resolver, err := controlplane.NewMemoryResolver([]controlplane.SourceConfig{source})
	if err != nil {
		t.Fatalf("new memory resolver failed: %v", err)
	}
	return newTestQueryHandlerWithResolverAndTokens(t, resolver, reader, tokens)
}

func newTestQueryHandlerWithResolverAndTokens(t *testing.T, resolver controlplane.Resolver, reader storage.EventReader, tokens []string) fasthttp.RequestHandler {
	t.Helper()

	queryToken := ""
	if len(tokens) > 0 {
		queryToken = tokens[0]
	}
	queryTokens := []string(nil)
	if len(tokens) > 1 {
		queryTokens = tokens[1:]
	}

	handler, err := NewHandler(Options{
		CollectPath:           "/collect",
		HealthPath:            "/healthz",
		TrackerPath:           "/tracker.js",
		TrustForwardedHeaders: false,
		TrackerScript:         []byte("(function(window){ window.simpletrack = {}; })(window);"),
		Now: func() time.Time {
			return time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
		},
		Resolver:    resolver,
		Bus:         &recordingBus{},
		QueryReader: reader,
		QueryToken:  queryToken,
		QueryTokens: queryTokens,
	})
	if err != nil {
		t.Fatalf("new handler failed: %v", err)
	}
	return handler.ServeFastHTTP
}

func serve(handler fasthttp.RequestHandler, method string, path string, body string, headers map[string]string) *fasthttp.RequestCtx {
	var request fasthttp.Request
	request.Header.SetMethod(method)
	request.SetRequestURI(path)
	if body != "" {
		request.Header.SetContentType(contentTypeJSON)
		request.SetBodyString(body)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}

	var ctx fasthttp.RequestCtx
	ctx.Init(&request, &net.TCPAddr{IP: net.ParseIP("198.51.100.10"), Port: 443}, nil)
	handler(&ctx)
	return &ctx
}

func validCollectBody(writeKey string) string {
	return `{
		"write_key":"` + writeKey + `",
		"id":"evt_1",
		"tenant_id":"tenant_client",
		"project_id":"project_client",
		"source_id":"source_client",
		"source_type":"mobile",
		"event_name":"pageview",
		"distinct_id":"visitor_1",
		"properties":{"page.path":"/"}
	}`
}

func testSourceConfig() controlplane.SourceConfig {
	return controlplane.SourceConfig{
		WriteKey:                 "wk_live",
		Enabled:                  true,
		TenantID:                 "tenant_control",
		ProjectID:                "project_control",
		SourceID:                 "source_control",
		SourceType:               "web",
		AllowedOrigins:           []string{"https://docs.example.com"},
		SessionSalt:              "session-salt",
		ClientHashSalt:           "client-salt",
		IncludeClientFingerprint: true,
	}
}

func assertFiltered(t *testing.T, ctx *fasthttp.RequestCtx, bus *recordingBus) {
	t.Helper()

	if ctx.Response.StatusCode() != fasthttp.StatusAccepted {
		t.Fatalf("expected accepted filtered response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	var response AcceptedResponse
	if err := json.Unmarshal(ctx.Response.Body(), &response); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if !response.Filtered {
		t.Fatalf("expected filtered response, got %s", ctx.Response.Body())
	}
	if len(bus.published) != 0 {
		t.Fatalf("filtered traffic should not publish events")
	}
}

type recordingBus struct {
	err       error                     // err forces Publish to fail through the collect API
	published []contracts.EventEnvelope // published records events accepted by analytics-core collect handling
}

func (b *recordingBus) Publish(_ context.Context, envelope contracts.EventEnvelope) error {
	if b.err != nil {
		return b.err
	}
	b.published = append(b.published, envelope)
	return nil
}

func (b *recordingBus) Subscribe(context.Context, eventbus.ConsumerGroup, eventbus.Handler) error {
	return nil
}

type recordingQueryReader struct {
	eventsQuery   storage.EventListQuery
	realtimeQuery storage.RealtimeQuery
	events        []storage.EventRecord
	realtime      []storage.EventRecord
	err           error
}

type countingResolver struct {
	source controlplane.SourceConfig
	calls  int
}

func (r *countingResolver) ResolveSource(_ context.Context, _ string) (controlplane.SourceConfig, error) {
	r.calls++
	return r.source, nil
}

func (r *recordingQueryReader) ListEvents(_ context.Context, query storage.EventListQuery) ([]storage.EventRecord, error) {
	r.eventsQuery = query
	if r.err != nil {
		return nil, r.err
	}
	return append([]storage.EventRecord(nil), r.events...), nil
}

func (r *recordingQueryReader) ListRealtime(_ context.Context, query storage.RealtimeQuery) ([]storage.EventRecord, error) {
	r.realtimeQuery = query
	if r.err != nil {
		return nil, r.err
	}
	return append([]storage.EventRecord(nil), r.realtime...), nil
}
