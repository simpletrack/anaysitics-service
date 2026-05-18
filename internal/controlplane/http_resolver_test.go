package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPResolverResolvesSourceWithAuth(t *testing.T) {
	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer runtime-token" {
			t.Fatalf("expected bearer auth, got %q", r.Header.Get("Authorization"))
		}
		var request resolveSourceRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request failed: %v", err)
		}
		if request.WriteKey != "wk_live" {
			t.Fatalf("expected write key wk_live, got %q", request.WriteKey)
		}
		_ = json.NewEncoder(w).Encode(testRuntimeSourceConfig())
	}))
	defer server.Close()

	resolver := newTestHTTPResolver(t, server.URL, time.Minute)
	source, err := resolver.ResolveSource(context.Background(), " wk_live ")
	if err != nil {
		t.Fatalf("resolve source failed: %v", err)
	}
	if source.TenantID != "tenant_1" || source.SourceType != "web" {
		t.Fatalf("unexpected source config: %#v", source)
	}
	if !source.AllowsReadback(ReadbackRouteRealtime) || !source.AllowsReadback(ReadbackRouteEvents) {
		t.Fatalf("expected runtime source readback policy to be preserved: %#v", source.ReadbackPolicy)
	}
	if requestCount != 1 {
		t.Fatalf("expected one control-plane request, got %d", requestCount)
	}
}

func TestHTTPResolverCachesSuccessfulSources(t *testing.T) {
	var requestCount int
	etag := `"runtime-source-v1"`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		switch requestCount {
		case 1:
			if got := r.Header.Get("If-None-Match"); got != "" {
				t.Fatalf("expected first resolve to skip conditional request, got %q", got)
			}
			w.Header().Set("ETag", etag)
			_ = json.NewEncoder(w).Encode(testRuntimeSourceConfig())
		case 2:
			if got := r.Header.Get("If-None-Match"); got != etag {
				t.Fatalf("expected conditional revalidation, got %q", got)
			}
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusNotModified)
		default:
			t.Fatalf("unexpected control-plane request %d", requestCount)
		}
	}))
	defer server.Close()

	now := time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)
	resolver, err := NewHTTPResolver(HTTPResolverOptions{
		Endpoint:              server.URL,
		BearerToken:           "runtime-token",
		CacheTTL:              time.Minute,
		AllowInsecureLoopback: true,
		Now: func() time.Time {
			return now
		},
	})
	if err != nil {
		t.Fatalf("new http resolver failed: %v", err)
	}
	if _, err := resolver.ResolveSource(context.Background(), "wk_live"); err != nil {
		t.Fatalf("first resolve failed: %v", err)
	}
	if _, err := resolver.ResolveSource(context.Background(), "wk_live"); err != nil {
		t.Fatalf("second resolve failed: %v", err)
	}
	if requestCount != 2 {
		t.Fatalf("expected conditional revalidation, got %d requests", requestCount)
	}
}

func TestHTTPResolverRevalidatesDisabledSources(t *testing.T) {
	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		switch requestCount {
		case 1:
			w.Header().Set("ETag", `"runtime-source-v1"`)
			_ = json.NewEncoder(w).Encode(testRuntimeSourceConfig())
		case 2:
			if got := r.Header.Get("If-None-Match"); got == "" {
				t.Fatalf("expected cached source to send conditional request")
			}
			w.WriteHeader(http.StatusGone)
		default:
			t.Fatalf("unexpected control-plane request %d", requestCount)
		}
	}))
	defer server.Close()

	resolver := newTestHTTPResolver(t, server.URL, time.Minute)
	if _, err := resolver.ResolveSource(context.Background(), "wk_live"); err != nil {
		t.Fatalf("first resolve failed: %v", err)
	}
	_, err := resolver.ResolveSource(context.Background(), "wk_live")
	if !errors.Is(err, ErrSourceDisabled) {
		t.Fatalf("expected disabled source after revalidation, got %v", err)
	}
}

func TestHTTPResolverRefreshesExpiredCacheEntries(t *testing.T) {
	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount++
		_ = json.NewEncoder(w).Encode(testRuntimeSourceConfig())
	}))
	defer server.Close()

	now := time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)
	resolver, err := NewHTTPResolver(HTTPResolverOptions{
		Endpoint:              server.URL,
		BearerToken:           "runtime-token",
		CacheTTL:              time.Minute,
		AllowInsecureLoopback: true,
		Now: func() time.Time {
			return now
		},
	})
	if err != nil {
		t.Fatalf("new http resolver failed: %v", err)
	}
	if _, err := resolver.ResolveSource(context.Background(), "wk_live"); err != nil {
		t.Fatalf("first resolve failed: %v", err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := resolver.ResolveSource(context.Background(), "wk_live"); err != nil {
		t.Fatalf("second resolve failed: %v", err)
	}
	if requestCount != 2 {
		t.Fatalf("expected expired cache to refresh, got %d requests", requestCount)
	}
}

func TestHTTPResolverMapsControlPlaneStatuses(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
		want       error
	}{
		{name: "missing", statusCode: http.StatusNotFound, want: ErrSourceNotFound},
		{name: "disabled", statusCode: http.StatusForbidden, want: ErrSourceDisabled},
		{name: "gone", statusCode: http.StatusGone, want: ErrSourceDisabled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.statusCode)
			}))
			defer server.Close()

			resolver := newTestHTTPResolver(t, server.URL, 0)
			_, err := resolver.ResolveSource(context.Background(), "wk_live")
			if !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, err)
			}
		})
	}
}

func TestHTTPResolverRejectsDisabledResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		source := testRuntimeSourceConfig()
		source.Enabled = false
		_ = json.NewEncoder(w).Encode(source)
	}))
	defer server.Close()

	resolver := newTestHTTPResolver(t, server.URL, 0)
	_, err := resolver.ResolveSource(context.Background(), "wk_live")
	if !errors.Is(err, ErrSourceDisabled) {
		t.Fatalf("expected disabled response body to fail, got %v", err)
	}
}

func TestHTTPResolverRequiresPrivateSaltsInResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		source := testRuntimeSourceConfig()
		source.SessionSalt = ""
		_ = json.NewEncoder(w).Encode(source)
	}))
	defer server.Close()

	resolver := newTestHTTPResolver(t, server.URL, 0)
	_, err := resolver.ResolveSource(context.Background(), "wk_live")
	if err == nil {
		t.Fatalf("expected missing private salt to fail")
	}
}

func TestHTTPResolverFailsClosedOnServerErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	resolver := newTestHTTPResolver(t, server.URL, 0)
	_, err := resolver.ResolveSource(context.Background(), "wk_live")
	if err == nil || errors.Is(err, ErrSourceNotFound) || errors.Is(err, ErrSourceDisabled) {
		t.Fatalf("expected generic fail-closed resolver error, got %v", err)
	}
}

func TestHTTPResolverRejectsMismatchedWriteKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		source := testRuntimeSourceConfig()
		source.WriteKey = "wk_other"
		_ = json.NewEncoder(w).Encode(source)
	}))
	defer server.Close()

	resolver := newTestHTTPResolver(t, server.URL, 0)
	_, err := resolver.ResolveSource(context.Background(), "wk_live")
	if err == nil {
		t.Fatalf("expected mismatched write key to fail")
	}
}

func TestNewHTTPResolverRequiresEndpointAndToken(t *testing.T) {
	if _, err := NewHTTPResolver(HTTPResolverOptions{BearerToken: "token"}); err == nil {
		t.Fatalf("expected missing endpoint to fail")
	}
	if _, err := NewHTTPResolver(HTTPResolverOptions{Endpoint: "https://control.example.com/runtime"}); err == nil {
		t.Fatalf("expected missing token to fail")
	}
}

func TestNewHTTPResolverRequiresHTTPSByDefault(t *testing.T) {
	if _, err := NewHTTPResolver(HTTPResolverOptions{
		Endpoint:    "http://control.example.com/runtime",
		BearerToken: "runtime-token",
	}); err == nil {
		t.Fatalf("expected insecure non-loopback endpoint to fail")
	}
	if _, err := NewHTTPResolver(HTTPResolverOptions{
		Endpoint:    "http://127.0.0.1:8080/runtime",
		BearerToken: "runtime-token",
	}); err == nil {
		t.Fatalf("expected loopback http endpoint without opt-in to fail")
	}
	if _, err := NewHTTPResolver(HTTPResolverOptions{
		Endpoint:              "http://127.0.0.1:8080/runtime",
		BearerToken:           "runtime-token",
		AllowInsecureLoopback: true,
	}); err != nil {
		t.Fatalf("expected explicit loopback http opt-in to pass: %v", err)
	}
	if _, err := NewHTTPResolver(HTTPResolverOptions{
		Endpoint:                           "http://saas:3000/runtime",
		BearerToken:                        "runtime-token",
		AllowInsecurePrivateNetwork: true,
	}); err != nil {
		t.Fatalf("expected explicit private-network http opt-in to pass: %v", err)
	}
}

func TestHTTPResolverDoesNotFollowRedirects(t *testing.T) {
	var redirected bool
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected = true
	}))
	defer redirectTarget.Close()

	controlPlane := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", redirectTarget.URL)
		w.WriteHeader(http.StatusFound)
	}))
	defer controlPlane.Close()

	resolver, err := NewHTTPResolver(HTTPResolverOptions{
		Endpoint:    controlPlane.URL,
		BearerToken: "runtime-token",
		Client:      controlPlane.Client(),
	})
	if err != nil {
		t.Fatalf("new http resolver failed: %v", err)
	}
	if _, err := resolver.ResolveSource(context.Background(), "wk_live"); err == nil {
		t.Fatalf("expected redirect response to fail closed")
	}
	if redirected {
		t.Fatalf("control-plane resolver must not follow redirects with bearer auth")
	}
}

func TestSchemaBoundResolverRejectsSourcesOutsideStartupSurface(t *testing.T) {
	startup := testRuntimeSourceConfig()
	remote := testRuntimeSourceConfig()
	remote.SourceID = "source_missing_schema"
	resolver := &staticResolver{source: remote}
	bound, err := NewSchemaBoundResolver(resolver, []SourceConfig{startup})
	if err != nil {
		t.Fatalf("new schema bound resolver failed: %v", err)
	}

	_, err = bound.ResolveSource(context.Background(), "wk_live")
	if !errors.Is(err, ErrSourceOutsideSchemaSurface) {
		t.Fatalf("expected schema surface error, got %v", err)
	}
}

func TestSchemaBoundResolverAllowsStartupSurfaceSources(t *testing.T) {
	startup := testRuntimeSourceConfig()
	resolver := &staticResolver{source: startup}
	bound, err := NewSchemaBoundResolver(resolver, []SourceConfig{startup})
	if err != nil {
		t.Fatalf("new schema bound resolver failed: %v", err)
	}

	source, err := bound.ResolveSource(context.Background(), "wk_live")
	if err != nil {
		t.Fatalf("expected source inside startup surface to pass: %v", err)
	}
	if source.SourceID != startup.SourceID {
		t.Fatalf("unexpected source %q", source.SourceID)
	}
}

func newTestHTTPResolver(t *testing.T, endpoint string, cacheTTL time.Duration) *HTTPResolver {
	t.Helper()

	resolver, err := NewHTTPResolver(HTTPResolverOptions{
		Endpoint:              endpoint,
		BearerToken:           "runtime-token",
		CacheTTL:              cacheTTL,
		AllowInsecureLoopback: true,
	})
	if err != nil {
		t.Fatalf("new http resolver failed: %v", err)
	}
	return resolver
}

type staticResolver struct {
	source SourceConfig // source is the runtime config returned for any non-empty write key
	err    error        // err forces ResolveSource to fail
}

func (r *staticResolver) ResolveSource(_ context.Context, _ string) (SourceConfig, error) {
	if r.err != nil {
		return SourceConfig{}, r.err
	}
	return r.source, nil
}

func testRuntimeSourceConfig() SourceConfig {
	return SourceConfig{
		WriteKey:   "wk_live",
		Enabled:    true,
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_web",
		SourceType: "web",
		ReadbackPolicy: ReadbackPolicy{
			Realtime:   true,
			Events:     true,
			Properties: true,
			Goals:      true,
		},
		SessionSalt:    "session-salt",
		VisitSalt:      "visit-salt",
		ClientHashSalt: "client-salt",
	}
}
