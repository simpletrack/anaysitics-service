package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

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
