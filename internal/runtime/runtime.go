package runtime

import (
	"context"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-core/eventbus/redisstream"
	"github.com/simpletrack/analytics-service/internal/collectapi"
	"github.com/simpletrack/analytics-service/internal/config"
	"github.com/simpletrack/analytics-service/internal/controlplane"
	"github.com/valyala/fasthttp"
)

// NewHandler builds the fasthttp handler used by the service process.
func NewHandler(cfg config.Config) (fasthttp.RequestHandler, error) {
	// Build the runtime-only view of SaaS source configuration. This resolver
	// reads config but does not own any CRUD lifecycle.
	resolver, err := controlplane.NewMemoryResolver(cfg.Sources)
	if err != nil {
		return nil, err
	}

	// Build the event bus before exposing HTTP routes. The default path is Redis
	// Stream so accepted /collect responses correspond to durable enqueueing.
	bus, err := newEventBus(cfg)
	if err != nil {
		return nil, err
	}

	// Load the tracker asset at startup so missing deployments fail closed
	// instead of returning 404s for a configured public SDK route.
	tracker, err := os.ReadFile(cfg.TrackerFile)
	if err != nil {
		return nil, err
	}

	// Wire HTTP to runtime enforcement and analytics-core collect handling. The
	// handler remains unaware of SaaS control-plane CRUD and storage adapters.
	handler, err := collectapi.NewHandler(collectapi.Options{
		CollectPath:           cfg.CollectPath,
		HealthPath:            cfg.HealthPath,
		TrackerPath:           cfg.TrackerPath,
		TrustForwardedHeaders: cfg.TrustForwardedHeaders,
		TrackerScript:         tracker,
		Resolver:              resolver,
		Bus:                   bus,
	})
	if err != nil {
		return nil, err
	}
	return handler.ServeFastHTTP, nil
}

func newEventBus(cfg config.Config) (eventbus.EventBus, error) {
	switch cfg.EventBus {
	case "redis":
		return newRedisBus(cfg)
	case "direct":
		return newMemoryBus(), nil
	default:
		return nil, configError("unsupported event bus")
	}
}

func newRedisBus(cfg config.Config) (eventbus.EventBus, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	// Ping during startup to fail fast when a deployment cannot durably enqueue
	// accepted analytics events.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}

	return redisstream.New(client, redisstream.Options{
		Stream:           cfg.RedisStream,
		Block:            cfg.RedisBlock,
		Count:            cfg.RedisReadCount,
		MaxAttempts:      cfg.RedisMaxAttempts,
		DeadLetterStream: cfg.RedisDeadLetterStream,
	})
}

type configError string

func (e configError) Error() string {
	return string(e)
}
