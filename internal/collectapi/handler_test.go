package collectapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
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

func TestCollectRejectsSourceDisabledAfterHTTPRevalidation(t *testing.T) {
	var requestCount int
	source := testSourceConfig().Normalize()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if got := r.Header.Get("Authorization"); got != "Bearer runtime-token" {
			t.Fatalf("expected bearer auth, got %q", got)
		}
		switch requestCount {
		case 1:
			w.Header().Set("ETag", `"runtime-source-v1"`)
			_ = json.NewEncoder(w).Encode(source)
		case 2:
			if got := r.Header.Get("If-None-Match"); got != `"runtime-source-v1"` {
				t.Fatalf("expected conditional revalidation, got %q", got)
			}
			w.WriteHeader(http.StatusGone)
		default:
			t.Fatalf("unexpected control-plane request %d", requestCount)
		}
	}))
	defer server.Close()

	bus := &recordingBus{}
	handler := newTestHandlerWithResolver(t, newTestControlPlaneHTTPResolver(t, server.URL), false, bus)

	first := serve(handler, fasthttp.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin": "https://docs.example.com",
	})
	if first.Response.StatusCode() != fasthttp.StatusAccepted {
		t.Fatalf("expected initial collect acceptance, got %d: %s", first.Response.StatusCode(), first.Response.Body())
	}
	if len(bus.published) != 1 {
		t.Fatalf("expected one accepted event before disable, got %d", len(bus.published))
	}

	second := serve(handler, fasthttp.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin": "https://docs.example.com",
	})
	if second.Response.StatusCode() != fasthttp.StatusForbidden {
		t.Fatalf("expected disabled source rejection, got %d: %s", second.Response.StatusCode(), second.Response.Body())
	}
	if string(second.Response.Body()) != `{"error":"source is disabled"}` {
		t.Fatalf("expected stable disabled error, got %s", second.Response.Body())
	}
	if len(bus.published) != 1 {
		t.Fatalf("disabled source should not publish another event, got %d", len(bus.published))
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
	eventTime := time.Date(2026, 5, 3, 9, 55, 0, 0, time.UTC)
	visitID := derivedVisitID("session_1", eventTime)
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
				VisitID:    visitID,
				EventTime:  eventTime,
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
	if got := response.Items[0].VisitID; got != visitID {
		t.Fatalf("expected realtime visit id %q, got %q", visitID, got)
	}
}

func TestRealtimeQueryRejectsDeletedSourceAfterHTTPRevalidation(t *testing.T) {
	var requestCount int
	source := testSourceConfig().Normalize()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		switch requestCount {
		case 1:
			w.Header().Set("ETag", `"runtime-source-v1"`)
			_ = json.NewEncoder(w).Encode(source)
		case 2:
			if got := r.Header.Get("If-None-Match"); got != `"runtime-source-v1"` {
				t.Fatalf("expected conditional revalidation, got %q", got)
			}
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Fatalf("unexpected control-plane request %d", requestCount)
		}
	}))
	defer server.Close()

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
				EventTime:  time.Date(2026, 5, 3, 9, 55, 0, 0, time.UTC),
				ReceivedAt: time.Date(2026, 5, 3, 9, 55, 1, 0, time.UTC),
			},
		},
	}
	handler := newTestQueryHandlerWithResolverAndCredentials(
		t,
		newTestControlPlaneHTTPResolver(t, server.URL),
		reader,
		[]QueryCredential{{Token: "query-token"}},
	)

	first := serve(handler, fasthttp.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer query-token",
	})
	if first.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected initial realtime response, got %d: %s", first.Response.StatusCode(), first.Response.Body())
	}
	if reader.realtimeCalls != 1 {
		t.Fatalf("expected one realtime read before delete, got %d", reader.realtimeCalls)
	}

	second := serve(handler, fasthttp.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer query-token",
	})
	if second.Response.StatusCode() != fasthttp.StatusUnauthorized {
		t.Fatalf("expected deleted source rejection, got %d: %s", second.Response.StatusCode(), second.Response.Body())
	}
	if string(second.Response.Body()) != `{"error":"invalid write key"}` {
		t.Fatalf("expected stable deleted-source error, got %s", second.Response.Body())
	}
	if reader.realtimeCalls != 1 {
		t.Fatalf("deleted source should not reach EventReader again, got %d calls", reader.realtimeCalls)
	}
}

func TestEventsQueryReturnsRecords(t *testing.T) {
	eventTime := time.Date(2026, 5, 3, 9, 20, 0, 0, time.UTC)
	visitID := derivedVisitID("session_1", eventTime)
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
				VisitID:        visitID,
				EventTime:      eventTime,
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
	if got := response.Items[0].VisitID; got != visitID {
		t.Fatalf("expected events visit id %q, got %q", visitID, got)
	}
}

func TestEventsQueryMapsAllowlistedPropertyFilters(t *testing.T) {
	source := testSourceConfig()
	source.AllowedPropertyFilters = []controlplane.AllowedPropertyFilter{
		{Scope: "event", Name: "button", ValueTypes: []string{"string"}},
		{Scope: "user", Name: "score", ValueTypes: []string{"number"}},
	}
	reader := &recordingQueryReader{}
	handler := newTestQueryHandler(t, source, reader)
	propertyFilter := url.QueryEscape(`{"scope":"event","name":"button","type":"string","op":"eq","value":"hero"}`)

	ctx := serve(handler, fasthttp.MethodGet, "/v1/events?write_key=wk_live&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z&property_filter="+propertyFilter, "", map[string]string{
		"Authorization": "Bearer query-token",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected events response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if len(reader.eventsQuery.PropertyFilters) != 1 {
		t.Fatalf("expected one property filter, got %#v", reader.eventsQuery.PropertyFilters)
	}
	got := reader.eventsQuery.PropertyFilters[0]
	if got.Scope != storage.PropertyScopeEvent || got.Name != "button" || got.ValueType != storage.PropertyValueString {
		t.Fatalf("unexpected property filter selector %#v", got)
	}
	if got.Operator != storage.EventFilterEquals || got.StringValue != "hero" {
		t.Fatalf("unexpected property filter predicate %#v", got)
	}
	if len(reader.eventsQuery.AllowedPropertySelectors) != 2 {
		t.Fatalf("expected source property selector allowlist to reach analytics-core, got %#v", reader.eventsQuery.AllowedPropertySelectors)
	}
}

func TestEventsQueryRejectsUnallowlistedPropertyFilters(t *testing.T) {
	source := testSourceConfig()
	source.AllowedPropertyFilters = []controlplane.AllowedPropertyFilter{
		{Scope: "event", Name: "button", ValueTypes: []string{"string"}},
	}
	resolver := &countingResolver{source: source.Normalize()}
	reader := &recordingQueryReader{}
	handler := newTestQueryHandlerWithResolver(t, resolver, reader)
	propertyFilter := url.QueryEscape(`{"scope":"event","name":"plan","type":"string","op":"eq","value":"pro"}`)

	ctx := serve(handler, fasthttp.MethodGet, "/v1/events?write_key=wk_live&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z&property_filter="+propertyFilter, "", map[string]string{
		"Authorization": "Bearer query-token",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusBadRequest {
		t.Fatalf("expected bad request for unallowlisted property filter, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if resolver.calls != 1 {
		t.Fatalf("expected source resolution before property whitelist check, got %d calls", resolver.calls)
	}
	if reader.eventsQuery.SourceID != "" {
		t.Fatalf("unallowlisted property filter should not reach EventReader, got %#v", reader.eventsQuery)
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

func TestQueryRoutesAcceptStructuredRotatedBearerToken(t *testing.T) {
	logs := captureLogs(t)
	reader := &recordingQueryReader{}
	handler := newTestQueryHandlerWithResolverAndCredentials(t, &countingResolver{source: testSourceConfig()}, reader, []QueryCredential{
		{
			ID:    "current",
			Token: "current-token",
		},
		{
			ID:        "previous",
			Token:     "previous-token",
			ExpiresAt: time.Date(2026, 5, 3, 10, 15, 0, 0, time.UTC),
		},
	})

	ctx := serve(handler, fasthttp.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer previous-token",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected structured rotated token to be accepted, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if reader.realtimeQuery.SourceID != "source_control" {
		t.Fatalf("expected structured rotated token to reach EventReader, got %#v", reader.realtimeQuery)
	}
	assertAuditLog(t, logs.String(), "token_id=previous", "/v1/realtime", "previous-token")
}

func TestQueryRoutesRejectExpiredBearerToken(t *testing.T) {
	logs := captureLogs(t)
	resolver := &countingResolver{source: testSourceConfig()}
	reader := &recordingQueryReader{}
	handler := newTestQueryHandlerWithResolverAndCredentials(t, resolver, reader, []QueryCredential{
		{
			ID:        "current",
			Token:     "expired-token",
			ExpiresAt: time.Date(2026, 5, 3, 9, 59, 0, 0, time.UTC),
		},
	})

	ctx := serve(handler, fasthttp.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer expired-token",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusUnauthorized {
		t.Fatalf("expected unauthorized response for expired token, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if resolver.calls != 0 {
		t.Fatalf("expired query token should not resolve source, got %d calls", resolver.calls)
	}
	if reader.realtimeQuery.SourceID != "" {
		t.Fatalf("expired query token should not reach EventReader, got %#v", reader.realtimeQuery)
	}
	assertAuditLog(t, logs.String(), "token_id=current", "/v1/realtime", "expired-token")
}

func TestQueryRoutesRejectNotYetValidBearerToken(t *testing.T) {
	logs := captureLogs(t)
	resolver := &countingResolver{source: testSourceConfig()}
	reader := &recordingQueryReader{}
	handler := newTestQueryHandlerWithResolverAndCredentials(t, resolver, reader, []QueryCredential{
		{
			ID:        "next",
			Token:     "future-token",
			NotBefore: time.Date(2026, 5, 3, 10, 5, 0, 0, time.UTC),
		},
	})

	ctx := serve(handler, fasthttp.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer future-token",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusUnauthorized {
		t.Fatalf("expected unauthorized response for not-yet-valid token, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if resolver.calls != 0 {
		t.Fatalf("not-yet-valid query token should not resolve source, got %d calls", resolver.calls)
	}
	if reader.realtimeQuery.SourceID != "" {
		t.Fatalf("not-yet-valid query token should not reach EventReader, got %#v", reader.realtimeQuery)
	}
	assertAuditLog(t, logs.String(), "token_id=next", "/v1/realtime", "future-token")
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
	return newTestHandlerWithResolver(t, resolver, trustForwarded, bus)
}

func newTestHandlerWithResolver(t *testing.T, resolver controlplane.Resolver, trustForwarded bool, bus *recordingBus) fasthttp.RequestHandler {
	t.Helper()

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

func newTestControlPlaneHTTPResolver(t *testing.T, endpoint string) controlplane.Resolver {
	t.Helper()

	resolver, err := controlplane.NewHTTPResolver(controlplane.HTTPResolverOptions{
		Endpoint:              endpoint,
		BearerToken:           "runtime-token",
		CacheTTL:              time.Minute,
		AllowInsecureLoopback: true,
	})
	if err != nil {
		t.Fatalf("new http resolver failed: %v", err)
	}
	return resolver
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

func newTestQueryHandlerWithResolverAndCredentials(t *testing.T, resolver controlplane.Resolver, reader storage.EventReader, credentials []QueryCredential) fasthttp.RequestHandler {
	t.Helper()

	handler, err := NewHandler(Options{
		CollectPath:           "/collect",
		HealthPath:            "/healthz",
		TrackerPath:           "/tracker.js",
		TrustForwardedHeaders: false,
		TrackerScript:         []byte("(function(window){ window.simpletrack = {}; })(window);"),
		Now: func() time.Time {
			return time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
		},
		Resolver:         resolver,
		Bus:              &recordingBus{},
		QueryReader:      reader,
		QueryCredentials: credentials,
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

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()

	buffer := &bytes.Buffer{}
	originalWriter := log.Writer()
	originalFlags := log.Flags()
	originalPrefix := log.Prefix()
	log.SetOutput(buffer)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(originalWriter)
		log.SetFlags(originalFlags)
		log.SetPrefix(originalPrefix)
	})
	return buffer
}

func assertAuditLog(t *testing.T, logs string, tokenID string, route string, rawToken string) {
	t.Helper()

	if !strings.Contains(logs, tokenID) {
		t.Fatalf("expected audit log to contain %q, got %q", tokenID, logs)
	}
	if !strings.Contains(logs, route) {
		t.Fatalf("expected audit log to contain route %q, got %q", route, logs)
	}
	if strings.Contains(logs, rawToken) {
		t.Fatalf("expected audit log to avoid raw token %q, got %q", rawToken, logs)
	}
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

func derivedVisitID(sessionID string, eventTime time.Time) string {
	if sessionID == "" || eventTime.IsZero() {
		return ""
	}
	bucket := eventTime.UTC().Truncate(30 * time.Minute).Unix()
	sum := sha256.Sum256([]byte(sessionID + ":" + strconv.FormatInt(bucket, 10)))
	return "vis_" + hex.EncodeToString(sum[:16])
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
	eventsCalls   int
	realtimeCalls int
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
	r.eventsCalls++
	r.eventsQuery = query
	if r.err != nil {
		return nil, r.err
	}
	return append([]storage.EventRecord(nil), r.events...), nil
}

func (r *recordingQueryReader) ListRealtime(_ context.Context, query storage.RealtimeQuery) ([]storage.EventRecord, error) {
	r.realtimeCalls++
	r.realtimeQuery = query
	if r.err != nil {
		return nil, r.err
	}
	return append([]storage.EventRecord(nil), r.realtime...), nil
}
