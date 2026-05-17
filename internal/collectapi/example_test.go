package collectapi

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/gofiber/fiber/v3"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-service/internal/controlplane"
)

func ExampleNewApp() {
	resolver, _ := controlplane.NewMemoryResolver([]controlplane.SourceConfig{
		{
			WriteKey:       "wk_public_from_snippet",
			Enabled:        true,
			TenantID:       "tenant_1",
			ProjectID:      "project_1",
			SourceID:       "source_web",
			SessionSalt:    "server-only-session-salt",
			VisitSalt:      "server-only-visit-salt",
			ClientHashSalt: "server-only-client-salt",
			AllowedOrigins: []string{"https://example.com"},
		},
	})
	app, _ := NewApp(Options{
		Resolver:      resolver,
		Bus:           exampleEventBus{},
		TrackerScript: []byte("window.simpletrack = window.simpletrack || {};"),
	})
	_ = app
}

func ExampleNewApp_health() {
	resolver, _ := controlplane.NewMemoryResolver([]controlplane.SourceConfig{})
	app, _ := NewApp(Options{
		Resolver:      resolver,
		Bus:           exampleEventBus{},
		TrackerScript: []byte("window.simpletrack = window.simpletrack || {};"),
	})

	request, _ := http.NewRequest(http.MethodGet, "/healthz", nil)
	response, _ := app.Test(request)

	fmt.Println(response.StatusCode)

	// Output:
	// 200
}

func ExampleNewApp_kafkaDiagnostics() {
	resolver, _ := controlplane.NewMemoryResolver([]controlplane.SourceConfig{})
	app, _ := NewApp(Options{
		Resolver:      resolver,
		Bus:           exampleEventBus{},
		TrackerScript: []byte("window.simpletrack = window.simpletrack || {};"),
		QueryCredentials: []QueryCredential{
			{
				Token:  "diagnostics-token",
				Scopes: []controlplane.ReadbackRoute{controlplane.ReadbackRouteKafkaDiagnostics},
			},
		},
		KafkaDiagnostics: func() (KafkaDiagnosticsResponse, bool) {
			return sampleKafkaDiagnosticsResponse(), true
		},
	})

	request, _ := http.NewRequest(http.MethodGet, "/v1/kafka/diagnostics", nil)
	request.Header.Set("Authorization", "Bearer diagnostics-token")
	response, _ := app.Test(request)

	fmt.Println(response.StatusCode)

	// Output:
	// 200
}

func ExampleNewApp_kafkaMetrics() {
	resolver, _ := controlplane.NewMemoryResolver([]controlplane.SourceConfig{})
	app, _ := NewApp(Options{
		Resolver:      resolver,
		Bus:           exampleEventBus{},
		TrackerScript: []byte("window.simpletrack = window.simpletrack || {};"),
		QueryCredentials: []QueryCredential{
			{
				Token:  "metrics-token",
				Scopes: []controlplane.ReadbackRoute{controlplane.ReadbackRouteKafkaMetrics},
			},
		},
		KafkaMetrics: func() (KafkaDiagnosticsResponse, bool) {
			return sampleKafkaDiagnosticsResponse(), true
		},
	})

	request, _ := http.NewRequest(http.MethodGet, "/v1/kafka/metrics", nil)
	request.Header.Set("Authorization", "Bearer metrics-token")
	response, _ := app.Test(request)

	fmt.Println(response.StatusCode)

	// Output:
	// 200
}

func ExampleOptions_writeKeyPriority() {
	handler := &Handler{}
	app := fiber.New()
	app.Get("/collect", func(ctx fiber.Ctx) error {
		return ctx.SendString(handler.writeKey(ctx, "wk_body"))
	})

	request, _ := http.NewRequest(http.MethodGet, "/collect?write_key=wk_query", nil)
	request.Header.Set("X-SimpleTrack-Write-Key", " wk_header ")
	request.Header.Set("Authorization", "Bearer wk_bearer")
	response, _ := app.Test(request)
	defer response.Body.Close()
	first, _ := io.ReadAll(response.Body)
	fmt.Println(string(first))

	bodyOnly, _ := http.NewRequest(http.MethodGet, "/collect", nil)
	bodyResponse, _ := app.Test(bodyOnly)
	defer bodyResponse.Body.Close()
	second, _ := io.ReadAll(bodyResponse.Body)
	fmt.Println(string(second))

	// Output:
	// wk_header
	// wk_body
}

type exampleEventBus struct{}

func (exampleEventBus) Publish(context.Context, contracts.EventEnvelope) error {
	return nil
}

func (exampleEventBus) Subscribe(context.Context, eventbus.ConsumerGroup, eventbus.Handler) error {
	return nil
}
