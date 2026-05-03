package collectapi

import (
	"context"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-service/internal/controlplane"
)

func ExampleNewHandler() {
	resolver, _ := controlplane.NewMemoryResolver([]controlplane.SourceConfig{
		{
			WriteKey:       "wk_public_from_snippet",
			Enabled:        true,
			TenantID:       "tenant_1",
			ProjectID:      "project_1",
			SourceID:       "source_web",
			SessionSalt:    "server-only-session-salt",
			ClientHashSalt: "server-only-client-salt",
			AllowedOrigins: []string{"https://example.com"},
		},
	})
	handler, _ := NewHandler(Options{
		Resolver:      resolver,
		Bus:           exampleEventBus{},
		TrackerScript: []byte("window.simpletrack = window.simpletrack || {};"),
	})
	_ = handler
}

type exampleEventBus struct{}

func (exampleEventBus) Publish(context.Context, contracts.EventEnvelope) error {
	return nil
}

func (exampleEventBus) Subscribe(context.Context, eventbus.ConsumerGroup, eventbus.Handler) error {
	return nil
}
