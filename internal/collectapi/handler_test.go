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
