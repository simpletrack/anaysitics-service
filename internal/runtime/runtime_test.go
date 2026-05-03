package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/simpletrack/analytics-core/storage"
	"github.com/simpletrack/analytics-service/internal/config"
	"github.com/simpletrack/analytics-service/internal/controlplane"
)

func TestNewSourceResolverBindsHTTPResolverToIngestionSchemaSurface(t *testing.T) {
	// Return a valid control-plane source that intentionally differs from the
	// boot-time source list. The write key still matches so only schema-surface
	// binding can reject it.
	startup := testRuntimeSource()
	remote := testRuntimeSource()
	remote.SourceID = "source_missing_startup_schema"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(remote)
	}))
	defer server.Close()

	// Enable same-process ingestion with HTTP source resolution. This is the
	// runtime join point where dynamic control-plane config must be gated by the
	// startup schema surface before collect can publish.
	resolver, err := newSourceResolver(config.Config{
		SourceResolver:                    "http",
		ControlPlaneURL:                   server.URL,
		ControlPlaneToken:                 "runtime-token",
		ControlPlaneAllowInsecureLoopback: true,
		IngestionEnabled:                  true,
		Sources:                           []controlplane.SourceConfig{startup},
	})
	if err != nil {
		t.Fatalf("new source resolver failed: %v", err)
	}

	// Resolve through the assembled runtime resolver rather than the wrapper
	// directly, guarding against future refactors that forget the binding step.
	_, err = resolver.ResolveSource(context.Background(), startup.WriteKey)
	if !errors.Is(err, controlplane.ErrSourceOutsideSchemaSurface) {
		t.Fatalf("expected schema surface rejection, got %v", err)
	}
}

func TestAllowedPropertySelectorsUseEnabledStartupSources(t *testing.T) {
	enabled := testRuntimeSource()
	enabled.AllowedPropertyFilters = []controlplane.AllowedPropertyFilter{
		{Scope: "event", Name: "button", ValueTypes: []string{"string"}},
		{Scope: "user", Name: "plan", ValueTypes: []string{"string"}},
	}
	disabled := testRuntimeSource()
	disabled.WriteKey = "wk_disabled"
	disabled.SourceID = "source_disabled"
	disabled.Enabled = false
	disabled.AllowedPropertyFilters = []controlplane.AllowedPropertyFilter{
		{Scope: "event", Name: "hidden", ValueTypes: []string{"string"}},
	}

	selectors := allowedPropertySelectors([]controlplane.SourceConfig{enabled, disabled})

	if len(selectors) != 2 {
		t.Fatalf("expected enabled source selectors only, got %#v", selectors)
	}
	assertHasSelector(t, selectors, storage.PropertySelector{Scope: storage.PropertyScopeEvent, Name: "button"})
	assertHasSelector(t, selectors, storage.PropertySelector{Scope: storage.PropertyScopeUser, Name: "plan"})
}

func assertHasSelector(t *testing.T, selectors []storage.PropertySelector, want storage.PropertySelector) {
	t.Helper()

	for _, got := range selectors {
		if got == want {
			return
		}
	}
	t.Fatalf("expected selector %#v in %#v", want, selectors)
}
