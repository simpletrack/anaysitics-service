package runtime

import (
	"context"
	"sync"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
)

// memoryBus is an explicit non-durable event bus for local demos.
//
// Production must use Redis or another durable EventBus. This bus exists only
// after config.LoadFromEnv has required ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS.
type memoryBus struct {
	mu     sync.Mutex                // mu protects events while concurrent collect requests append
	events []contracts.EventEnvelope // events retains accepted events for the life of the process
}

func newMemoryBus() *memoryBus {
	return &memoryBus{}
}

// Publish stores one event in process memory.
func (b *memoryBus) Publish(_ context.Context, envelope contracts.EventEnvelope) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, envelope)
	return nil
}

// Subscribe registers no consumers because demo mode does not run ingestion.
func (b *memoryBus) Subscribe(ctx context.Context, _ eventbus.ConsumerGroup, _ eventbus.Handler) error {
	<-ctx.Done()
	return ctx.Err()
}
