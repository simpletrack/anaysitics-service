package collectapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/simpletrack/analytics-core/collect"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-core/storage"
	"github.com/simpletrack/analytics-service/internal/controlplane"
)

func TestCollectAcceptsValidWriteKeyAndOverridesClientScope(t *testing.T) {
	handler, bus := newTestHandler(t, testSourceConfig(), false)

	ctx := serve(handler, fiber.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin":     "https://docs.example.com",
		"User-Agent": "Mozilla/5.0",
		"Referer":    "https://docs.example.com/quickstart",
	})

	if ctx.Response.StatusCode() != fiber.StatusAccepted {
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
	if published.VisitID == "" {
		t.Fatalf("expected analytics-core visit resolver to derive a visit id")
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

	ctx := serve(handler, fiber.MethodPost, "/collect", validCollectBody("missing"), map[string]string{
		"Origin": "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fiber.StatusUnauthorized {
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

	first := serve(handler, fiber.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin": "https://docs.example.com",
	})
	if first.Response.StatusCode() != fiber.StatusAccepted {
		t.Fatalf("expected initial collect acceptance, got %d: %s", first.Response.StatusCode(), first.Response.Body())
	}
	if len(bus.published) != 1 {
		t.Fatalf("expected one accepted event before disable, got %d", len(bus.published))
	}

	second := serve(handler, fiber.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin": "https://docs.example.com",
	})
	if second.Response.StatusCode() != fiber.StatusForbidden {
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

	ctx := serve(handler, fiber.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin": "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fiber.StatusInternalServerError {
		t.Fatalf("expected internal server error, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if strings.Contains(string(ctx.Response.Body()), "redis password leaked") {
		t.Fatalf("expected stable public error, got %s", ctx.Response.Body())
	}
}

func TestCollectPreservesExplicitVisitID(t *testing.T) {
	handler, bus := newTestHandler(t, testSourceConfig(), false)
	body := `{
		"write_key":"wk_live",
		"id":"evt_1",
		"event_name":"pageview",
		"distinct_id":"visitor_1",
		"visit_id":"sdk_visit_1"
	}`

	ctx := serve(handler, fiber.MethodPost, "/collect", body, map[string]string{
		"Origin": "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fiber.StatusAccepted {
		t.Fatalf("expected accepted response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if len(bus.published) != 1 {
		t.Fatalf("expected one published event, got %d", len(bus.published))
	}
	if bus.published[0].VisitID != "sdk_visit_1" {
		t.Fatalf("expected explicit visit id to be preserved, got %q", bus.published[0].VisitID)
	}
}

func TestCollectRejectsBlockedOrigin(t *testing.T) {
	handler, bus := newTestHandler(t, testSourceConfig(), false)

	ctx := serve(handler, fiber.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin": "https://blocked.example.com",
	})

	if ctx.Response.StatusCode() != fiber.StatusForbidden {
		t.Fatalf("expected forbidden response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if len(bus.published) != 0 {
		t.Fatalf("blocked origin should not publish events")
	}
}

func TestCollectPreflightReturnsCORSHeaders(t *testing.T) {
	handler, _ := newTestHandler(t, testSourceConfig(), false)

	ctx := serve(handler, fiber.MethodOptions, "/collect", "", map[string]string{
		"Origin": "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fiber.StatusNoContent {
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

	ctx := serve(handler, fiber.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin":     "https://docs.example.com",
		"User-Agent": "Googlebot/2.1",
	})

	assertFiltered(t, ctx, bus)
}

func TestCollectFiltersInternalTraffic(t *testing.T) {
	source := testSourceConfig()
	source.InternalCIDRs = []string{"203.0.113.0/24"}
	handler, bus := newTestHandler(t, source, true)

	ctx := serve(handler, fiber.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin":          "https://docs.example.com",
		"X-Forwarded-For": "203.0.113.10",
	})

	assertFiltered(t, ctx, bus)
}

func TestCollectAddsGeoPropertiesWhenResolverConfigured(t *testing.T) {
	handler, bus := newTestHandlerWithGeoResolver(t, testSourceConfig(), false, &stubCollectGeoResolver{})

	ctx := serve(handler, fiber.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin": "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fiber.StatusAccepted {
		t.Fatalf("expected accepted response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if len(bus.published) != 1 {
		t.Fatalf("expected one published event, got %d", len(bus.published))
	}
	published := bus.published[0]
	if published.Properties["geo.country"] != "United States" {
		t.Fatalf("expected geo country property, got %#v", published.Properties)
	}
	if published.Properties["geo.region"] != "California" {
		t.Fatalf("expected geo region property, got %#v", published.Properties)
	}
	if published.Properties["geo.city"] != "San Francisco" {
		t.Fatalf("expected geo city property, got %#v", published.Properties)
	}
}

func TestCollectFilteredAuditLogOmitsRawIP(t *testing.T) {
	logs := captureLogs(t)
	handler, bus := newTestHandler(t, testSourceConfig(), true)

	ctx := serve(handler, fiber.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin":          "https://docs.example.com",
		"User-Agent":      "Googlebot/2.1",
		"X-Forwarded-For": "203.0.113.10",
	})

	assertFiltered(t, ctx, bus)
	logText := logs.String()
	for _, want := range []string{
		"collect filtered:",
		"event_id=evt_1",
		"tenant_id=tenant_control",
		"project_id=project_control",
		"source_id=source_control",
		"reason=bot user agent",
	} {
		if !strings.Contains(logText, want) {
			t.Fatalf("expected audit log to contain %q, got %q", want, logText)
		}
	}
	if strings.Contains(logText, "203.0.113.10") {
		t.Fatalf("expected audit log to omit raw ip, got %q", logText)
	}
}

func TestCollectInternalTrafficAuditLogOmitsRawIP(t *testing.T) {
	logs := captureLogs(t)
	source := testSourceConfig()
	source.InternalIPs = []string{"203.0.113.10"}
	handler, bus := newTestHandler(t, source, true)

	// Exercise the trusted proxy path because internal traffic filters often
	// depend on load balancer headers in local and production deployments.
	ctx := serve(handler, fiber.MethodPost, "/collect", validCollectBody("wk_live"), map[string]string{
		"Origin":          "https://docs.example.com",
		"User-Agent":      "Mozilla/5.0",
		"X-Forwarded-For": "203.0.113.10",
	})

	assertFiltered(t, ctx, bus)
	logText := logs.String()
	for _, want := range []string{
		"collect filtered:",
		"event_id=evt_1",
		"tenant_id=tenant_control",
		"project_id=project_control",
		"source_id=source_control",
		"reason=internal ip",
	} {
		if !strings.Contains(logText, want) {
			t.Fatalf("expected audit log to contain %q, got %q", want, logText)
		}
	}
	if strings.Contains(logText, "203.0.113.10") {
		t.Fatalf("expected audit log to omit raw ip, got %q", logText)
	}
}

func TestTrackerRouteReturnsJavaScript(t *testing.T) {
	handler, _ := newTestHandler(t, testSourceConfig(), false)

	ctx := serve(handler, fiber.MethodGet, "/tracker.js", "", nil)

	if ctx.Response.StatusCode() != fiber.StatusOK {
		t.Fatalf("expected tracker response, got %d", ctx.Response.StatusCode())
	}
	if !strings.Contains(string(ctx.Response.Body()), "window.simpletrack") {
		t.Fatalf("expected tracker script body, got %s", ctx.Response.Body())
	}
}

func TestHealthRouteReturnsOK(t *testing.T) {
	handler, _ := newTestHandler(t, testSourceConfig(), false)

	ctx := serve(handler, fiber.MethodGet, "/healthz", "", nil)

	if ctx.Response.StatusCode() != fiber.StatusOK {
		t.Fatalf("expected health response, got %d", ctx.Response.StatusCode())
	}
}

func TestRealtimeQueryReturnsRecords(t *testing.T) {
	eventTime := time.Date(2026, 5, 3, 9, 55, 0, 0, time.UTC)
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
				VisitID:    "visit_1",
				EventTime:  eventTime,
				ReceivedAt: time.Date(2026, 5, 3, 9, 55, 1, 0, time.UTC),
				Properties: `{"path":"/"}`,
				Source:     "browser",
			},
		},
		realtimeEvidence: storage.EventQueryEvidence{
			Family:              storage.EventQueryFamilyRealtime,
			ReadPath:            storage.EventReadPathFactEvents,
			Optimization:        storage.EventQueryOptimizationDirectFactTable,
			EffectiveLimit:      50,
			Offset:              0,
			HasTimeLowerBound:   true,
			HasTimeUpperBound:   false,
			TimeWindowSeconds:   0,
			ScalarFilterCount:   1,
			PropertyFilterCount: 0,
			UsesPropertyTable:   false,
			SortField:           storage.EventSortByEventTime,
			SortDirection:       storage.EventSortDescending,
		},
	}
	handler := newTestQueryHandler(t, testSourceConfig(), reader)

	ctx := serve(handler, fiber.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer query-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusOK {
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
	if got := response.Items[0].VisitID; got != "visit_1" {
		t.Fatalf("expected realtime visit id %q, got %q", "visit_1", got)
	}
	if response.QueryEvidence == nil {
		t.Fatal("expected realtime query evidence")
	}
	if response.QueryEvidence.Pressure != "low" {
		t.Fatalf("expected realtime pressure low, got %#v", response.QueryEvidence)
	}
	if response.QueryEvidence.EffectiveLimit != 50 || response.QueryEvidence.Offset != 0 {
		t.Fatalf("expected realtime shape evidence limit/offset, got %#v", response.QueryEvidence)
	}
	if !response.QueryEvidence.HasTimeLowerBound || response.QueryEvidence.HasTimeUpperBound || response.QueryEvidence.TimeWindowSeconds != 0 {
		t.Fatalf("expected realtime open-ended time evidence, got %#v", response.QueryEvidence)
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

	first := serve(handler, fiber.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer query-token",
	})
	if first.Response.StatusCode() != fiber.StatusOK {
		t.Fatalf("expected initial realtime response, got %d: %s", first.Response.StatusCode(), first.Response.Body())
	}
	if reader.realtimeCalls != 1 {
		t.Fatalf("expected one realtime read before delete, got %d", reader.realtimeCalls)
	}

	second := serve(handler, fiber.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer query-token",
	})
	if second.Response.StatusCode() != fiber.StatusUnauthorized {
		t.Fatalf("expected deleted source rejection, got %d: %s", second.Response.StatusCode(), second.Response.Body())
	}
	if string(second.Response.Body()) != `{"error":"invalid write key"}` {
		t.Fatalf("expected stable deleted-source error, got %s", second.Response.Body())
	}
	if reader.realtimeCalls != 1 {
		t.Fatalf("deleted source should not reach EventReader again, got %d calls", reader.realtimeCalls)
	}
}

func TestPropertyCatalogQueryReturnsSourceScopedEntries(t *testing.T) {
	seenAt := time.Date(2026, 5, 3, 9, 0, 0, 0, time.UTC)
	catalog := &recordingPropertyCatalog{
		entries: []storage.PropertyCatalogEntry{
			{
				TenantID:    "tenant_control",
				ProjectID:   "project_control",
				SourceID:    "source_control",
				Scope:       storage.PropertyScopeEvent,
				Name:        "button",
				ValueType:   storage.PropertyValueString,
				FirstSeenAt: seenAt,
				LastSeenAt:  seenAt.Add(time.Hour),
			},
		},
	}
	handler := newTestPropertyCatalogHandler(t, testSourceConfig(), catalog)

	ctx := serve(handler, fiber.MethodGet, "/v1/properties?write_key=wk_live&scope=event&limit=25", "", map[string]string{
		"Authorization": "Bearer query-token",
		"Origin":        "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fiber.StatusOK {
		t.Fatalf("expected property catalog response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if catalog.query.TenantID != "tenant_control" || catalog.query.ProjectID != "project_control" || catalog.query.SourceID != "source_control" {
		t.Fatalf("expected source-scoped catalog query, got %#v", catalog.query)
	}
	if catalog.query.Scope != storage.PropertyScopeEvent || catalog.query.Limit != 25 {
		t.Fatalf("expected catalog scope/limit from query, got %#v", catalog.query)
	}

	var response propertyCatalogResponse
	if err := json.Unmarshal(ctx.Response.Body(), &response); err != nil {
		t.Fatalf("decode property catalog response failed: %v", err)
	}
	if len(response.Items) != 1 {
		t.Fatalf("expected one property catalog item, got %d", len(response.Items))
	}
	if response.Items[0].Scope != "event" || response.Items[0].Name != "button" || response.Items[0].ValueType != "string" {
		t.Fatalf("property catalog item mismatch: %#v", response.Items[0])
	}
	if response.Limit != 25 {
		t.Fatalf("expected response limit 25, got %d", response.Limit)
	}
}

func TestPropertyCatalogQueryRejectsInvalidScopeAndLimit(t *testing.T) {
	handler := newTestPropertyCatalogHandler(t, testSourceConfig(), &recordingPropertyCatalog{})

	badScope := serve(handler, fiber.MethodGet, "/v1/properties?write_key=wk_live&scope=account", "", map[string]string{
		"Authorization": "Bearer query-token",
		"Origin":        "https://docs.example.com",
	})
	if badScope.Response.StatusCode() != fiber.StatusBadRequest {
		t.Fatalf("expected invalid scope rejection, got %d: %s", badScope.Response.StatusCode(), badScope.Response.Body())
	}

	tooLarge := serve(handler, fiber.MethodGet, "/v1/properties?write_key=wk_live&limit=201", "", map[string]string{
		"Authorization": "Bearer query-token",
		"Origin":        "https://docs.example.com",
	})
	if tooLarge.Response.StatusCode() != fiber.StatusBadRequest {
		t.Fatalf("expected too-large limit rejection, got %d: %s", tooLarge.Response.StatusCode(), tooLarge.Response.Body())
	}
}

func TestEventsQueryReturnsRecords(t *testing.T) {
	eventTime := time.Date(2026, 5, 3, 9, 20, 0, 0, time.UTC)
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
				VisitID:        "visit_2",
				EventTime:      eventTime,
				ReceivedAt:     time.Date(2026, 5, 3, 9, 20, 1, 0, time.UTC),
				Properties:     `{"button":"hero"}`,
				UserProperties: `{"plan":"free"}`,
				Source:         "browser",
			},
		},
		eventsEvidence: storage.EventQueryEvidence{
			Family:              storage.EventQueryFamilyEvents,
			ReadPath:            storage.EventReadPathFactEvents,
			Optimization:        storage.EventQueryOptimizationDirectFactTable,
			EffectiveLimit:      25,
			Offset:              3,
			HasTimeLowerBound:   true,
			HasTimeUpperBound:   true,
			TimeWindowSeconds:   3600,
			ScalarFilterCount:   5,
			PropertyFilterCount: 2,
			UsesPropertyTable:   true,
			PropertyFilters: []storage.EventPropertyFilterEvidence{
				{
					Scope:     storage.PropertyScopeEvent,
					Name:      "button",
					ValueType: storage.PropertyValueString,
					Operator:  storage.EventFilterEquals,
				},
				{
					Scope:     storage.PropertyScopeUser,
					Name:      "plan",
					ValueType: storage.PropertyValueString,
					Operator:  storage.EventFilterNotEquals,
				},
			},
			SortField:     storage.EventSortByReceivedAt,
			SortDirection: storage.EventSortAscending,
		},
	}
	handler := newTestQueryHandler(t, testSourceConfig(), reader)

	ctx := serve(handler, fiber.MethodGet, "/v1/events?write_key=wk_live&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z&limit=25&offset=3&event_name=signup_clicked&distinct_id=visitor_1&visit_id=visit_2&sort_field=received_at&sort_direction=asc", "", map[string]string{
		"Authorization": "Bearer query-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusOK {
		t.Fatalf("expected events response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if reader.eventsQuery.EventName != "signup_clicked" || reader.eventsQuery.DistinctID != "visitor_1" {
		t.Fatalf("expected request filters to reach analytics-core, got %#v", reader.eventsQuery)
	}
	if len(reader.eventsQuery.Filters) != 1 ||
		reader.eventsQuery.Filters[0].Field != storage.EventFilterByVisitID ||
		reader.eventsQuery.Filters[0].Value != "visit_2" {
		t.Fatalf("expected visit id filter to reach analytics-core, got %#v", reader.eventsQuery.Filters)
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
	if got := response.Items[0].VisitID; got != "visit_2" {
		t.Fatalf("expected events visit id %q, got %q", "visit_2", got)
	}
	if response.QueryEvidence == nil {
		t.Fatal("expected events query evidence")
	}
	if response.QueryEvidence.Pressure != "high" {
		t.Fatalf("expected events pressure high, got %#v", response.QueryEvidence)
	}
	if response.QueryEvidence.EffectiveLimit != 25 || response.QueryEvidence.Offset != 3 {
		t.Fatalf("expected events shape evidence limit/offset, got %#v", response.QueryEvidence)
	}
	if !response.QueryEvidence.HasTimeLowerBound || !response.QueryEvidence.HasTimeUpperBound || response.QueryEvidence.TimeWindowSeconds != 3600 {
		t.Fatalf("expected one-hour events time evidence, got %#v", response.QueryEvidence)
	}
	if len(response.QueryEvidence.PropertyFilters) != 2 {
		t.Fatalf("expected property filter evidence shapes, got %#v", response.QueryEvidence.PropertyFilters)
	}
	if response.QueryEvidence.PropertyFilters[0].Name != "button" ||
		response.QueryEvidence.PropertyFilters[0].Scope != "event" ||
		response.QueryEvidence.PropertyFilters[0].ValueType != "string" ||
		response.QueryEvidence.PropertyFilters[0].Operator != "eq" {
		t.Fatalf("expected event button property evidence, got %#v", response.QueryEvidence.PropertyFilters[0])
	}
	if response.QueryEvidence.PropertyFilters[1].Name != "plan" ||
		response.QueryEvidence.PropertyFilters[1].Scope != "user" ||
		response.QueryEvidence.PropertyFilters[1].ValueType != "string" ||
		response.QueryEvidence.PropertyFilters[1].Operator != "neq" {
		t.Fatalf("expected user plan property evidence, got %#v", response.QueryEvidence.PropertyFilters[1])
	}
}

func TestGoalsQueryReturnsMatchingEventCount(t *testing.T) {
	reader := &recordingQueryReader{
		count: 3,
		countEvidence: storage.EventQueryEvidence{
			Family:            storage.EventQueryFamilyGoal,
			ReadPath:          storage.EventReadPathFactEvents,
			Optimization:      storage.EventQueryOptimizationDirectFactTable,
			HasTimeLowerBound: true,
			HasTimeUpperBound: true,
			TimeWindowSeconds: 3600,
			ScalarFilterCount: 4,
		},
	}
	handler := newTestQueryHandler(t, testSourceConfig(), reader)

	ctx := serve(handler, fiber.MethodGet, "/v1/goals?write_key=wk_live&event_name=signup_clicked&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z", "", map[string]string{
		"Authorization": "Bearer query-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusOK {
		t.Fatalf("expected goals response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if reader.countCalls != 1 {
		t.Fatalf("expected one count query, got %d", reader.countCalls)
	}
	if reader.countQuery.TenantID != "tenant_control" ||
		reader.countQuery.ProjectID != "project_control" ||
		reader.countQuery.SourceID != "source_control" ||
		reader.countQuery.EventName != "signup_clicked" {
		t.Fatalf("expected source-scoped goal query, got %#v", reader.countQuery)
	}
	if !reader.countQuery.From.Equal(time.Date(2026, 5, 3, 9, 0, 0, 0, time.UTC)) ||
		!reader.countQuery.To.Equal(time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected one-hour goal query range, got from=%s to=%s", reader.countQuery.From, reader.countQuery.To)
	}

	var response queryGoalResponse
	if err := json.Unmarshal(ctx.Response.Body(), &response); err != nil {
		t.Fatalf("decode goals response failed: %v", err)
	}
	if response.EventName != "signup_clicked" || response.MatchingEvents != 3 || !response.HasData {
		t.Fatalf("expected matching goal count, got %#v", response)
	}
	if response.QueryEvidence == nil || response.QueryEvidence.Family != "goal" || response.QueryEvidence.Pressure != "medium" {
		t.Fatalf("expected goal query evidence, got %#v", response.QueryEvidence)
	}
}

func TestGoalsQueryRequiresEventName(t *testing.T) {
	reader := &recordingQueryReader{count: 1}
	handler := newTestQueryHandler(t, testSourceConfig(), reader)

	ctx := serve(handler, fiber.MethodGet, "/v1/goals?write_key=wk_live&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z", "", map[string]string{
		"Authorization": "Bearer query-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusBadRequest {
		t.Fatalf("expected missing event_name rejection, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if reader.countCalls != 0 {
		t.Fatalf("invalid goal request should not query storage, got %d calls", reader.countCalls)
	}
}

func TestGoalsQueryRejectsUnsupportedEventName(t *testing.T) {
	reader := &recordingQueryReader{count: 1}
	handler := newTestQueryHandler(t, testSourceConfig(), reader)

	ctx := serve(handler, fiber.MethodGet, "/v1/goals?write_key=wk_live&event_name=%2Fsignup&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z", "", map[string]string{
		"Authorization": "Bearer query-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusBadRequest {
		t.Fatalf("expected unsupported event_name rejection, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if reader.countCalls != 0 {
		t.Fatalf("unsupported goal event name should not query storage, got %d calls", reader.countCalls)
	}
}

func TestQueryPressureBuckets(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		evidence storage.EventQueryEvidence
		want     string
	}{
		{
			name: "low",
			evidence: storage.EventQueryEvidence{
				ScalarFilterCount:   2,
				PropertyFilterCount: 0,
			},
			want: "low",
		},
		{
			name: "medium",
			evidence: storage.EventQueryEvidence{
				ScalarFilterCount:   4,
				PropertyFilterCount: 2,
			},
			want: "medium",
		},
		{
			name: "high",
			evidence: storage.EventQueryEvidence{
				ScalarFilterCount:   5,
				PropertyFilterCount: 3,
			},
			want: "high",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := queryPressure(tc.evidence); got != tc.want {
				t.Fatalf("expected %s pressure, got %s", tc.want, got)
			}
		})
	}
}

func TestEventsQuerySupportsReaderWithoutEvidence(t *testing.T) {
	reader := &legacyQueryReader{
		events: []storage.EventRecord{
			{
				ID:         "evt_legacy",
				TenantID:   "tenant_control",
				ProjectID:  "project_control",
				SourceID:   "source_control",
				SourceType: "web",
				EventName:  "page_view",
				DistinctID: "visitor_legacy",
				EventTime:  time.Date(2026, 5, 3, 9, 20, 0, 0, time.UTC),
				ReceivedAt: time.Date(2026, 5, 3, 9, 20, 1, 0, time.UTC),
			},
		},
	}
	handler := newTestQueryHandler(t, testSourceConfig(), reader)

	ctx := serve(handler, fiber.MethodGet, "/v1/events?write_key=wk_live&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z", "", map[string]string{
		"Authorization": "Bearer query-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusOK {
		t.Fatalf("expected events response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if reader.eventsCalls != 1 {
		t.Fatalf("expected legacy reader events call, got %d", reader.eventsCalls)
	}
	var response queryEventsResponse
	if err := json.Unmarshal(ctx.Response.Body(), &response); err != nil {
		t.Fatalf("decode events response failed: %v", err)
	}
	if len(response.Items) != 1 {
		t.Fatalf("expected one legacy item, got %d", len(response.Items))
	}
	if response.QueryEvidence != nil {
		t.Fatalf("legacy reader should omit query evidence, got %#v", response.QueryEvidence)
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

	ctx := serve(handler, fiber.MethodGet, "/v1/events?write_key=wk_live&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z&property_filter="+propertyFilter, "", map[string]string{
		"Authorization": "Bearer query-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusOK {
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

func TestEventsQueryMapsRepeatablePropertyFiltersInOrder(t *testing.T) {
	source := testSourceConfig()
	source.AllowedPropertyFilters = []controlplane.AllowedPropertyFilter{
		{Scope: "event", Name: "button", ValueTypes: []string{"string"}},
		{Scope: "user", Name: "score", ValueTypes: []string{"number"}},
	}
	reader := &recordingQueryReader{}
	handler := newTestQueryHandler(t, source, reader)
	firstFilter := url.QueryEscape(`{"scope":"event","name":"button","type":"string","op":"eq","value":"hero"}`)
	secondFilter := url.QueryEscape(`{"scope":"user","name":"score","type":"number","op":"neq","value":42}`)

	ctx := serve(handler, fiber.MethodGet, "/v1/events?write_key=wk_live&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z&property_filter="+firstFilter+"&property_filter="+secondFilter, "", map[string]string{
		"Authorization": "Bearer query-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusOK {
		t.Fatalf("expected events response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if len(reader.eventsQuery.PropertyFilters) != 2 {
		t.Fatalf("expected two property filters, got %#v", reader.eventsQuery.PropertyFilters)
	}
	first := reader.eventsQuery.PropertyFilters[0]
	if first.Scope != storage.PropertyScopeEvent || first.Name != "button" || first.ValueType != storage.PropertyValueString || first.Operator != storage.EventFilterEquals || first.StringValue != "hero" {
		t.Fatalf("unexpected first property filter %#v", first)
	}
	second := reader.eventsQuery.PropertyFilters[1]
	if second.Scope != storage.PropertyScopeUser || second.Name != "score" || second.ValueType != storage.PropertyValueNumber || second.Operator != storage.EventFilterNotEquals || second.NumberValue != 42 {
		t.Fatalf("unexpected second property filter %#v", second)
	}
}

func TestEventsQueryCombinesScalarSortAndRepeatablePropertyFilters(t *testing.T) {
	source := testSourceConfig()
	source.AllowedPropertyFilters = []controlplane.AllowedPropertyFilter{
		{Scope: "event", Name: "button", ValueTypes: []string{"string"}},
		{Scope: "user", Name: "score", ValueTypes: []string{"number"}},
	}
	reader := &recordingQueryReader{}
	handler := newTestQueryHandler(t, source, reader)
	firstFilter := url.QueryEscape(`{"scope":"event","name":"button","type":"string","op":"eq","value":"hero"}`)
	secondFilter := url.QueryEscape(`{"scope":"user","name":"score","type":"number","op":"neq","value":42}`)

	ctx := serve(handler, fiber.MethodGet, "/v1/events?write_key=wk_live&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z&limit=25&offset=5&event_name=signup_clicked&distinct_id=visitor_1&visit_id=visit_2&sort_field=event_name&sort_direction=asc&property_filter="+firstFilter+"&property_filter="+secondFilter, "", map[string]string{
		"Authorization": "Bearer query-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusOK {
		t.Fatalf("expected events response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if reader.eventsQuery.EventName != "signup_clicked" || reader.eventsQuery.DistinctID != "visitor_1" {
		t.Fatalf("expected scalar filters to reach analytics-core, got %#v", reader.eventsQuery)
	}
	if len(reader.eventsQuery.Filters) != 1 ||
		reader.eventsQuery.Filters[0].Field != storage.EventFilterByVisitID ||
		reader.eventsQuery.Filters[0].Value != "visit_2" {
		t.Fatalf("expected visit filter to reach analytics-core, got %#v", reader.eventsQuery.Filters)
	}
	if reader.eventsQuery.Limit != 25 || reader.eventsQuery.Offset != 5 {
		t.Fatalf("expected paging to reach analytics-core, got limit=%d offset=%d", reader.eventsQuery.Limit, reader.eventsQuery.Offset)
	}
	if reader.eventsQuery.SortField != storage.EventSortByEventName || reader.eventsQuery.SortDirection != storage.EventSortAscending {
		t.Fatalf("expected sort allowlist values, got %#v", reader.eventsQuery)
	}
	if len(reader.eventsQuery.PropertyFilters) != 2 {
		t.Fatalf("expected repeatable property filters to reach analytics-core, got %#v", reader.eventsQuery.PropertyFilters)
	}
	first := reader.eventsQuery.PropertyFilters[0]
	if first.Scope != storage.PropertyScopeEvent || first.Name != "button" || first.ValueType != storage.PropertyValueString || first.Operator != storage.EventFilterEquals || first.StringValue != "hero" {
		t.Fatalf("unexpected first property filter %#v", first)
	}
	second := reader.eventsQuery.PropertyFilters[1]
	if second.Scope != storage.PropertyScopeUser || second.Name != "score" || second.ValueType != storage.PropertyValueNumber || second.Operator != storage.EventFilterNotEquals || second.NumberValue != 42 {
		t.Fatalf("unexpected second property filter %#v", second)
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

	ctx := serve(handler, fiber.MethodGet, "/v1/events?write_key=wk_live&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z&property_filter="+propertyFilter, "", map[string]string{
		"Authorization": "Bearer query-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusBadRequest {
		t.Fatalf("expected bad request for unallowlisted property filter, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if resolver.calls != 1 {
		t.Fatalf("expected source resolution before property whitelist check, got %d calls", resolver.calls)
	}
	if reader.eventsQuery.SourceID != "" {
		t.Fatalf("unallowlisted property filter should not reach EventReader, got %#v", reader.eventsQuery)
	}
}

func TestQueryRoutesEnforceSourceReadbackPolicy(t *testing.T) {
	cases := []struct {
		name          string
		path          string
		route         controlplane.ReadbackRoute
		buildHandler  func(*testing.T, controlplane.SourceConfig, *recordingQueryReader, *recordingPropertyCatalog) *fiber.App
		assertNotRead func(*testing.T, *recordingQueryReader, *recordingPropertyCatalog)
		assertWasRead func(*testing.T, *recordingQueryReader, *recordingPropertyCatalog)
	}{
		{
			name:  "realtime",
			path:  "/v1/realtime?write_key=wk_live",
			route: controlplane.ReadbackRouteRealtime,
			buildHandler: func(t *testing.T, source controlplane.SourceConfig, reader *recordingQueryReader, _ *recordingPropertyCatalog) *fiber.App {
				return newTestQueryHandler(t, source, reader)
			},
			assertNotRead: func(t *testing.T, reader *recordingQueryReader, _ *recordingPropertyCatalog) {
				t.Helper()
				if reader.realtimeCalls != 0 {
					t.Fatalf("disabled realtime policy should not reach EventReader, got %d calls", reader.realtimeCalls)
				}
			},
			assertWasRead: func(t *testing.T, reader *recordingQueryReader, _ *recordingPropertyCatalog) {
				t.Helper()
				if reader.realtimeCalls != 1 {
					t.Fatalf("allowed realtime policy should reach EventReader once, got %d calls", reader.realtimeCalls)
				}
			},
		},
		{
			name:  "events",
			path:  "/v1/events?write_key=wk_live&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z",
			route: controlplane.ReadbackRouteEvents,
			buildHandler: func(t *testing.T, source controlplane.SourceConfig, reader *recordingQueryReader, _ *recordingPropertyCatalog) *fiber.App {
				return newTestQueryHandler(t, source, reader)
			},
			assertNotRead: func(t *testing.T, reader *recordingQueryReader, _ *recordingPropertyCatalog) {
				t.Helper()
				if reader.eventsCalls != 0 {
					t.Fatalf("disabled events policy should not reach EventReader, got %d calls", reader.eventsCalls)
				}
			},
			assertWasRead: func(t *testing.T, reader *recordingQueryReader, _ *recordingPropertyCatalog) {
				t.Helper()
				if reader.eventsCalls != 1 {
					t.Fatalf("allowed events policy should reach EventReader once, got %d calls", reader.eventsCalls)
				}
			},
		},
		{
			name:  "goals",
			path:  "/v1/goals?write_key=wk_live&event_name=signup&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z",
			route: controlplane.ReadbackRouteGoals,
			buildHandler: func(t *testing.T, source controlplane.SourceConfig, reader *recordingQueryReader, _ *recordingPropertyCatalog) *fiber.App {
				return newTestQueryHandler(t, source, reader)
			},
			assertNotRead: func(t *testing.T, reader *recordingQueryReader, _ *recordingPropertyCatalog) {
				t.Helper()
				if reader.countCalls != 0 {
					t.Fatalf("disabled goals policy should not reach EventReader, got %d calls", reader.countCalls)
				}
			},
			assertWasRead: func(t *testing.T, reader *recordingQueryReader, _ *recordingPropertyCatalog) {
				t.Helper()
				if reader.countCalls != 1 {
					t.Fatalf("allowed goals policy should reach EventReader once, got %d calls", reader.countCalls)
				}
			},
		},
		{
			name:  "properties",
			path:  "/v1/properties?write_key=wk_live&scope=event",
			route: controlplane.ReadbackRouteProperties,
			buildHandler: func(t *testing.T, source controlplane.SourceConfig, _ *recordingQueryReader, catalog *recordingPropertyCatalog) *fiber.App {
				return newTestPropertyCatalogHandler(t, source, catalog)
			},
			assertNotRead: func(t *testing.T, _ *recordingQueryReader, catalog *recordingPropertyCatalog) {
				t.Helper()
				if catalog.query.SourceID != "" {
					t.Fatalf("disabled properties policy should not reach catalog reader, got %#v", catalog.query)
				}
			},
			assertWasRead: func(t *testing.T, _ *recordingQueryReader, catalog *recordingPropertyCatalog) {
				t.Helper()
				if catalog.query.SourceID != "source_control" {
					t.Fatalf("allowed properties policy should reach catalog reader, got %#v", catalog.query)
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name+"/disabled", func(t *testing.T) {
			source := testSourceConfig()
			switch tc.route {
			case controlplane.ReadbackRouteRealtime:
				source.ReadbackPolicy.Realtime = false
			case controlplane.ReadbackRouteEvents:
				source.ReadbackPolicy.Events = false
			case controlplane.ReadbackRouteProperties:
				source.ReadbackPolicy.Properties = false
			case controlplane.ReadbackRouteGoals:
				source.ReadbackPolicy.Goals = false
			}
			reader := &recordingQueryReader{}
			catalog := &recordingPropertyCatalog{}
			handler := tc.buildHandler(t, source, reader, catalog)

			ctx := serve(handler, fiber.MethodGet, tc.path, "", map[string]string{
				"Authorization": "Bearer query-token",
			})

			if ctx.Response.StatusCode() != fiber.StatusForbidden {
				t.Fatalf("expected disabled readback policy response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
			}
			if string(ctx.Response.Body()) != `{"error":"readback is disabled"}` {
				t.Fatalf("expected stable readback-policy error, got %s", ctx.Response.Body())
			}
			tc.assertNotRead(t, reader, catalog)
		})

		t.Run(tc.name+"/allowed", func(t *testing.T) {
			reader := &recordingQueryReader{}
			catalog := &recordingPropertyCatalog{}
			handler := tc.buildHandler(t, testSourceConfig(), reader, catalog)

			ctx := serve(handler, fiber.MethodGet, tc.path, "", map[string]string{
				"Authorization": "Bearer query-token",
			})

			if ctx.Response.StatusCode() != fiber.StatusOK {
				t.Fatalf("expected allowed readback policy response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
			}
			tc.assertWasRead(t, reader, catalog)
		})
	}
}

func TestQueryRoutesRequireBearerToken(t *testing.T) {
	resolver := &countingResolver{source: testSourceConfig()}
	handler := newTestQueryHandlerWithResolver(t, resolver, &recordingQueryReader{})

	ctx := serve(handler, fiber.MethodGet, "/v1/events?write_key=wk_live&from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z", "", map[string]string{
		"Origin": "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fiber.StatusUnauthorized {
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

	ctx := serve(handler, fiber.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer previous-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusOK {
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

	ctx := serve(handler, fiber.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer wrong-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusUnauthorized {
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

	ctx := serve(handler, fiber.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer previous-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusOK {
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

	ctx := serve(handler, fiber.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer expired-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusUnauthorized {
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

	ctx := serve(handler, fiber.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer future-token",
	})

	if ctx.Response.StatusCode() != fiber.StatusUnauthorized {
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

	ctx := serve(handler, fiber.MethodGet, "/v1/events?from=2026-05-03T09:00:00Z&to=2026-05-03T10:00:00Z", "", map[string]string{
		"Authorization": "Bearer query-token",
		"Origin":        "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fiber.StatusBadRequest {
		t.Fatalf("expected bad-request response, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if string(ctx.Response.Header.Peek("Access-Control-Allow-Origin")) != "https://docs.example.com" {
		t.Fatalf("expected bad-request query response to include CORS, got %q", ctx.Response.Header.Peek("Access-Control-Allow-Origin"))
	}
}

func TestQueryPreflightReturnsCORSHeaders(t *testing.T) {
	handler := newTestQueryHandler(t, testSourceConfig(), &recordingQueryReader{})

	ctx := serve(handler, fiber.MethodOptions, "/v1/events?write_key=wk_live", "", map[string]string{
		"Origin": "https://docs.example.com",
	})

	if ctx.Response.StatusCode() != fiber.StatusNoContent {
		t.Fatalf("expected no-content query preflight, got %d: %s", ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if string(ctx.Response.Header.Peek("Access-Control-Allow-Origin")) != "https://docs.example.com" {
		t.Fatalf("expected reflected query CORS origin, got %q", ctx.Response.Header.Peek("Access-Control-Allow-Origin"))
	}
	if got := string(ctx.Response.Header.Peek("Access-Control-Allow-Methods")); got != "GET, POST, OPTIONS" {
		t.Fatalf("expected query methods, got %q", got)
	}
	allowHeaders := string(ctx.Response.Header.Peek("Access-Control-Allow-Headers"))
	for _, header := range []string{"Authorization", "X-SimpleTrack-Write-Key"} {
		if !strings.Contains(allowHeaders, header) {
			t.Fatalf("expected %s in query allow headers, got %q", header, allowHeaders)
		}
	}
}

func TestQueryRoutesUseConfiguredPaths(t *testing.T) {
	reader := &recordingQueryReader{}
	resolver, err := controlplane.NewMemoryResolver([]controlplane.SourceConfig{testSourceConfig()})
	if err != nil {
		t.Fatalf("new memory resolver failed: %v", err)
	}
	app, err := NewApp(Options{
		CollectPath:   "/collect",
		HealthPath:    "/healthz",
		TrackerPath:   "/tracker.js",
		EventsPath:    "/internal/events",
		RealtimePath:  "/internal/realtime",
		TrackerScript: []byte("(function(window){ window.simpletrack = {}; })(window);"),
		Now: func() time.Time {
			return time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
		},
		Resolver:       resolver,
		Bus:            &recordingBus{},
		QueryReader:    reader,
		QueryToken:     "query-token",
		QueryTokens:    nil,
		OpenAPIFile:    "",
		SwaggerEnabled: false,
	})
	if err != nil {
		t.Fatalf("new app failed: %v", err)
	}

	custom := serve(app, fiber.MethodGet, "/internal/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer query-token",
	})
	if custom.Response.StatusCode() != fiber.StatusOK {
		t.Fatalf("expected configured realtime route, got %d: %s", custom.Response.StatusCode(), custom.Response.Body())
	}
	defaultRoute := serve(app, fiber.MethodGet, "/v1/realtime?write_key=wk_live", "", map[string]string{
		"Authorization": "Bearer query-token",
	})
	if defaultRoute.Response.StatusCode() != fiber.StatusNotFound {
		t.Fatalf("expected default realtime route to be unregistered, got %d: %s", defaultRoute.Response.StatusCode(), defaultRoute.Response.Body())
	}
}

func TestSwaggerDisabledByDefault(t *testing.T) {
	app, _ := newTestHandler(t, testSourceConfig(), false)

	response := serve(app, fiber.MethodGet, "/swagger/docs", "", nil)

	if response.Response.StatusCode() != fiber.StatusNotFound {
		t.Fatalf("expected swagger to be disabled, got %d: %s", response.Response.StatusCode(), response.Response.Body())
	}
}

func TestSwaggerRoutesServeOpenAPIWhenEnabled(t *testing.T) {
	tempDir := t.TempDir()
	t.Chdir(tempDir)
	openAPI := filepath.Join(tempDir, "openapi.yaml")
	if err := os.WriteFile(openAPI, []byte("openapi: 3.0.3\ninfo:\n  title: Test API\n  version: 0.0.1\npaths: {}\n"), 0600); err != nil {
		t.Fatalf("write openapi file failed: %v", err)
	}
	resolver, err := controlplane.NewMemoryResolver([]controlplane.SourceConfig{testSourceConfig()})
	if err != nil {
		t.Fatalf("new memory resolver failed: %v", err)
	}
	app, err := NewApp(Options{
		CollectPath:   "/collect",
		HealthPath:    "/healthz",
		TrackerPath:   "/tracker.js",
		TrackerScript: []byte("(function(window){ window.simpletrack = {}; })(window);"),
		Now: func() time.Time {
			return time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
		},
		Resolver:       resolver,
		Bus:            &recordingBus{},
		SwaggerEnabled: true,
		SwaggerPath:    "/swagger",
		OpenAPIFile:    "openapi.yaml",
		QueryReader:    nil,
	})
	if err != nil {
		t.Fatalf("new app failed: %v", err)
	}

	spec := serve(app, fiber.MethodGet, "/swagger/openapi.yaml", "", nil)
	if spec.Response.StatusCode() != fiber.StatusOK {
		t.Fatalf("expected openapi spec response, got %d: %s", spec.Response.StatusCode(), spec.Response.Body())
	}
	if !strings.Contains(string(spec.Response.Body()), "Test API") {
		t.Fatalf("expected openapi body, got %s", spec.Response.Body())
	}
	ui := serve(app, fiber.MethodGet, "/swagger/docs", "", nil)
	if ui.Response.StatusCode() != fiber.StatusOK {
		t.Fatalf("expected swagger ui response, got %d: %s", ui.Response.StatusCode(), ui.Response.Body())
	}
	if !strings.Contains(string(ui.Response.Body()), "SimpleTrack Analytics Service API") {
		t.Fatalf("expected swagger ui body, got %s", ui.Response.Body())
	}
}

func newTestHandler(t *testing.T, source controlplane.SourceConfig, trustForwarded bool) (*fiber.App, *recordingBus) {
	t.Helper()

	bus := &recordingBus{}
	app := newTestHandlerWithBus(t, source, trustForwarded, bus)
	return app, bus
}

func newTestHandlerWithBus(t *testing.T, source controlplane.SourceConfig, trustForwarded bool, bus *recordingBus) *fiber.App {
	t.Helper()

	resolver, err := controlplane.NewMemoryResolver([]controlplane.SourceConfig{source})
	if err != nil {
		t.Fatalf("new memory resolver failed: %v", err)
	}
	return newTestHandlerWithResolver(t, resolver, trustForwarded, bus)
}

func newTestHandlerWithResolver(t *testing.T, resolver controlplane.Resolver, trustForwarded bool, bus *recordingBus) *fiber.App {
	t.Helper()

	app, err := NewApp(Options{
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
		t.Fatalf("new app failed: %v", err)
	}
	return app
}

func newTestHandlerWithGeoResolver(t *testing.T, source controlplane.SourceConfig, trustForwarded bool, geoResolver collect.GeoResolver) (*fiber.App, *recordingBus) {
	t.Helper()

	bus := &recordingBus{}
	resolver, err := controlplane.NewMemoryResolver([]controlplane.SourceConfig{source})
	if err != nil {
		t.Fatalf("new memory resolver failed: %v", err)
	}
	app, err := NewApp(Options{
		CollectPath:           "/collect",
		HealthPath:            "/healthz",
		TrackerPath:           "/tracker.js",
		TrustForwardedHeaders: trustForwarded,
		TrackerScript:         []byte("(function(window){ window.simpletrack = {}; })(window);"),
		Now: func() time.Time {
			return time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
		},
		Resolver:    resolver,
		Bus:         bus,
		GeoResolver: geoResolver,
	})
	if err != nil {
		t.Fatalf("new app failed: %v", err)
	}
	return app, bus
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

func newTestQueryHandler(t *testing.T, source controlplane.SourceConfig, reader storage.EventReader) *fiber.App {
	t.Helper()

	resolver, err := controlplane.NewMemoryResolver([]controlplane.SourceConfig{source})
	if err != nil {
		t.Fatalf("new memory resolver failed: %v", err)
	}
	return newTestQueryHandlerWithResolver(t, resolver, reader)
}

func newTestQueryHandlerWithResolver(t *testing.T, resolver controlplane.Resolver, reader storage.EventReader) *fiber.App {
	t.Helper()

	return newTestQueryHandlerWithResolverAndTokens(t, resolver, reader, []string{"query-token"})
}

func newTestQueryHandlerWithTokens(t *testing.T, source controlplane.SourceConfig, reader storage.EventReader, tokens []string) *fiber.App {
	t.Helper()

	resolver, err := controlplane.NewMemoryResolver([]controlplane.SourceConfig{source})
	if err != nil {
		t.Fatalf("new memory resolver failed: %v", err)
	}
	return newTestQueryHandlerWithResolverAndTokens(t, resolver, reader, tokens)
}

func newTestQueryHandlerWithResolverAndTokens(t *testing.T, resolver controlplane.Resolver, reader storage.EventReader, tokens []string) *fiber.App {
	t.Helper()

	queryToken := ""
	if len(tokens) > 0 {
		queryToken = tokens[0]
	}
	queryTokens := []string(nil)
	if len(tokens) > 1 {
		queryTokens = tokens[1:]
	}

	app, err := NewApp(Options{
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
		t.Fatalf("new app failed: %v", err)
	}
	return app
}

func newTestQueryHandlerWithResolverAndCredentials(t *testing.T, resolver controlplane.Resolver, reader storage.EventReader, credentials []QueryCredential) *fiber.App {
	t.Helper()

	app, err := NewApp(Options{
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
		t.Fatalf("new app failed: %v", err)
	}
	return app
}

func newTestPropertyCatalogHandler(t *testing.T, source controlplane.SourceConfig, catalog storage.PropertyCatalogReader) *fiber.App {
	t.Helper()

	resolver, err := controlplane.NewMemoryResolver([]controlplane.SourceConfig{source})
	if err != nil {
		t.Fatalf("new memory resolver failed: %v", err)
	}
	app, err := NewApp(Options{
		CollectPath:     "/collect",
		HealthPath:      "/healthz",
		TrackerPath:     "/tracker.js",
		PropertiesPath:  "/v1/properties",
		TrackerScript:   []byte("(function(window){ window.simpletrack = {}; })(window);"),
		Resolver:        resolver,
		Bus:             &recordingBus{},
		PropertyCatalog: catalog,
		QueryToken:      "query-token",
	})
	if err != nil {
		t.Fatalf("new app failed: %v", err)
	}
	return app
}

func serve(app *fiber.App, method string, path string, body string, headers map[string]string) *testCtx {
	request, err := http.NewRequest(method, path, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	request.RemoteAddr = "198.51.100.10:443"
	if body != "" {
		request.Header.Set("Content-Type", contentTypeJSON)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	if method == fiber.MethodOptions && request.Header.Get("Access-Control-Request-Method") == "" {
		if strings.HasPrefix(path, "/v1/") {
			request.Header.Set("Access-Control-Request-Method", fiber.MethodGet)
		} else {
			request.Header.Set("Access-Control-Request-Method", fiber.MethodPost)
		}
	}

	response, err := app.Test(request)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		panic(err)
	}
	return &testCtx{
		Response: testResponse{
			statusCode: response.StatusCode,
			body:       payload,
			Header:     testHeader{values: response.Header},
		},
	}
}

type testCtx struct {
	Response testResponse
}

type testResponse struct {
	statusCode int
	body       []byte
	Header     testHeader
}

func (r testResponse) StatusCode() int {
	return r.statusCode
}

func (r testResponse) Body() []byte {
	return r.body
}

type testHeader struct {
	values http.Header
}

func (h testHeader) Peek(key string) []byte {
	return []byte(h.values.Get(key))
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
		WriteKey:       "wk_live",
		Enabled:        true,
		TenantID:       "tenant_control",
		ProjectID:      "project_control",
		SourceID:       "source_control",
		SourceType:     "web",
		AllowedOrigins: []string{"https://docs.example.com"},
		ReadbackPolicy: controlplane.ReadbackPolicy{
			Realtime:   true,
			Events:     true,
			Properties: true,
			Goals:      true,
		},
		SessionSalt:              "session-salt",
		VisitSalt:                "visit-salt",
		ClientHashSalt:           "client-salt",
		IncludeClientFingerprint: true,
	}
}

func assertFiltered(t *testing.T, ctx *testCtx, bus *recordingBus) {
	t.Helper()

	if ctx.Response.StatusCode() != fiber.StatusAccepted {
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
	countQuery       storage.EventCountQuery
	eventsQuery      storage.EventListQuery
	realtimeQuery    storage.RealtimeQuery
	count            int64
	events           []storage.EventRecord
	realtime         []storage.EventRecord
	countEvidence    storage.EventQueryEvidence
	eventsEvidence   storage.EventQueryEvidence
	realtimeEvidence storage.EventQueryEvidence
	countCalls       int
	eventsCalls      int
	realtimeCalls    int
	err              error
}

type recordingPropertyCatalog struct {
	query   storage.PropertyCatalogQuery   // query records the last source-scoped catalog query
	entries []storage.PropertyCatalogEntry // entries are returned to the handler
	err     error                          // err forces catalog reads to fail
}

type countingResolver struct {
	source controlplane.SourceConfig
	calls  int
}

func (r *countingResolver) ResolveSource(_ context.Context, _ string) (controlplane.SourceConfig, error) {
	r.calls++
	return r.source, nil
}

func (c *recordingPropertyCatalog) ListPropertyCatalogEntries(_ context.Context, query storage.PropertyCatalogQuery) ([]storage.PropertyCatalogEntry, error) {
	c.query = query
	if c.err != nil {
		return nil, c.err
	}
	return append([]storage.PropertyCatalogEntry(nil), c.entries...), nil
}

func (r *recordingQueryReader) ListEvents(_ context.Context, query storage.EventListQuery) ([]storage.EventRecord, error) {
	r.eventsCalls++
	r.eventsQuery = query
	if r.err != nil {
		return nil, r.err
	}
	return append([]storage.EventRecord(nil), r.events...), nil
}

func (r *recordingQueryReader) ListEventsWithEvidence(ctx context.Context, query storage.EventListQuery) (storage.EventQueryResult, error) {
	records, err := r.ListEvents(ctx, query)
	if err != nil {
		return storage.EventQueryResult{}, err
	}
	return storage.EventQueryResult{Records: records, Evidence: r.eventsEvidence}, nil
}

func (r *recordingQueryReader) ListRealtime(_ context.Context, query storage.RealtimeQuery) ([]storage.EventRecord, error) {
	r.realtimeCalls++
	r.realtimeQuery = query
	if r.err != nil {
		return nil, r.err
	}
	return append([]storage.EventRecord(nil), r.realtime...), nil
}

func (r *recordingQueryReader) ListRealtimeWithEvidence(ctx context.Context, query storage.RealtimeQuery) (storage.EventQueryResult, error) {
	records, err := r.ListRealtime(ctx, query)
	if err != nil {
		return storage.EventQueryResult{}, err
	}
	return storage.EventQueryResult{Records: records, Evidence: r.realtimeEvidence}, nil
}

func (r *recordingQueryReader) CountEvents(_ context.Context, query storage.EventCountQuery) (int64, error) {
	r.countCalls++
	r.countQuery = query
	if r.err != nil {
		return 0, r.err
	}
	return r.count, nil
}

func (r *recordingQueryReader) CountEventsWithEvidence(ctx context.Context, query storage.EventCountQuery) (storage.EventCountResult, error) {
	count, err := r.CountEvents(ctx, query)
	if err != nil {
		return storage.EventCountResult{}, err
	}
	return storage.EventCountResult{Count: count, Evidence: r.countEvidence}, nil
}

type legacyQueryReader struct {
	countQuery    storage.EventCountQuery // countQuery records the last Goal count query
	eventsQuery   storage.EventListQuery  // eventsQuery records the last Events query
	realtimeQuery storage.RealtimeQuery   // realtimeQuery records the last Realtime query
	count         int64                   // count is returned from CountEvents
	events        []storage.EventRecord   // events are returned from ListEvents
	realtime      []storage.EventRecord   // realtime rows are returned from ListRealtime
	countCalls    int                     // countCalls counts legacy Goal reads
	eventsCalls   int                     // eventsCalls counts legacy Events reads
	realtimeCalls int                     // realtimeCalls counts legacy Realtime reads
	err           error                   // err forces legacy reads to fail
}

func (r *legacyQueryReader) ListEvents(_ context.Context, query storage.EventListQuery) ([]storage.EventRecord, error) {
	r.eventsCalls++
	r.eventsQuery = query
	if r.err != nil {
		return nil, r.err
	}
	return append([]storage.EventRecord(nil), r.events...), nil
}

func (r *legacyQueryReader) ListRealtime(_ context.Context, query storage.RealtimeQuery) ([]storage.EventRecord, error) {
	r.realtimeCalls++
	r.realtimeQuery = query
	if r.err != nil {
		return nil, r.err
	}
	return append([]storage.EventRecord(nil), r.realtime...), nil
}

func (r *legacyQueryReader) CountEvents(_ context.Context, query storage.EventCountQuery) (int64, error) {
	r.countCalls++
	r.countQuery = query
	if r.err != nil {
		return 0, r.err
	}
	return r.count, nil
}

type stubCollectGeoResolver struct{}

func (stubCollectGeoResolver) Resolve(string) (collect.GeoLocation, bool) {
	return collect.GeoLocation{
		Country: "United States",
		Region:  "California",
		City:    "San Francisco",
	}, true
}
