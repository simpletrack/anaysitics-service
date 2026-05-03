package controlplane_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/simpletrack/analytics-service/internal/controlplane"
)

func ExampleNewHTTPResolver() {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(controlplane.SourceConfig{
			WriteKey:       "wk_live",
			Enabled:        true,
			TenantID:       "tenant_1",
			ProjectID:      "project_1",
			SourceID:       "source_web",
			SourceType:     "web",
			SessionSalt:    "session-salt",
			ClientHashSalt: "client-salt",
		})
	}))
	defer server.Close()

	resolver, err := controlplane.NewHTTPResolver(controlplane.HTTPResolverOptions{
		Endpoint:              server.URL,
		BearerToken:           "runtime-token",
		AllowInsecureLoopback: true,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	source, err := resolver.ResolveSource(context.Background(), "wk_live")
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(source.SourceID)
	// Output: source_web
}
