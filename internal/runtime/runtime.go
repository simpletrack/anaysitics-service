package runtime

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-core/eventbus/redisstream"
	"github.com/simpletrack/analytics-core/ingestion"
	"github.com/simpletrack/analytics-service/internal/collectapi"
	"github.com/simpletrack/analytics-service/internal/config"
	"github.com/simpletrack/analytics-service/internal/controlplane"
	"github.com/valyala/fasthttp"
)

// Runtime owns the assembled analytics data-plane dependencies.
type Runtime struct {
	handler   fasthttp.RequestHandler // handler serves health, tracker, preflight, and collect routes
	processor *ingestion.Processor    // processor consumes Redis Stream messages when ingestion is enabled
	closers   []io.Closer             // closers release network clients opened by the runtime assembly
	worker    chan error              // worker receives the optional ingestion worker terminal error
}

// New assembles the runtime without starting background workers.
func New(cfg config.Config) (*Runtime, error) {
	// Build the runtime-only view of SaaS source configuration. This resolver
	// reads config but does not own any CRUD lifecycle.
	resolver, err := newSourceResolver(cfg)
	if err != nil {
		return nil, err
	}

	// Build the event bus before exposing HTTP routes. The default path is Redis
	// Stream so accepted /collect responses correspond to durable enqueueing.
	bus, busClosers, err := newEventBus(cfg)
	if err != nil {
		return nil, err
	}
	closers := append([]io.Closer{}, busClosers...)

	// Load the tracker asset at startup so missing deployments fail closed
	// instead of returning 404s for a configured public SDK route.
	tracker, err := os.ReadFile(cfg.TrackerFile)
	if err != nil {
		_ = closeAll(closers)
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
		_ = closeAll(closers)
		return nil, err
	}

	var processor *ingestion.Processor
	if cfg.IngestionEnabled {
		// Ingestion is an explicit runtime mode because it opens storage
		// dependencies and turns accepted events into ClickHouse writes.
		startupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		processor, closers, err = newIngestionProcessor(startupCtx, cfg, bus, closers)
		if err != nil {
			_ = closeAll(closers)
			return nil, err
		}
	}

	return &Runtime{
		handler:   handler.ServeFastHTTP,
		processor: processor,
		closers:   closers,
	}, nil
}

// Handler returns the fasthttp handler owned by the runtime.
func (r *Runtime) Handler() fasthttp.RequestHandler {
	return r.handler
}

// Start launches optional background runtime workers.
func (r *Runtime) Start(ctx context.Context) {
	if r.processor == nil || r.worker != nil {
		return
	}
	r.worker = make(chan error, 1)
	go func() {
		// Processor.Run returns context cancellation when the service is
		// shutting down; callers decide whether that is expected.
		r.worker <- r.processor.Run(ctx)
	}()
}

// WorkerDone returns the optional ingestion worker terminal channel.
func (r *Runtime) WorkerDone() <-chan error {
	if r == nil {
		return nil
	}
	return r.worker
}

// Close releases network resources opened during runtime assembly.
func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	return closeAll(r.closers)
}

func newEventBus(cfg config.Config) (eventbus.EventBus, []io.Closer, error) {
	switch cfg.EventBus {
	case "redis":
		return newRedisBus(cfg)
	case "direct":
		return newMemoryBus(), nil, nil
	default:
		return nil, nil, configError("unsupported event bus")
	}
}

func newRedisBus(cfg config.Config) (eventbus.EventBus, []io.Closer, error) {
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
		return nil, nil, err
	}

	bus, err := redisstream.New(client, redisstream.Options{
		Stream:           cfg.RedisStream,
		Block:            cfg.RedisBlock,
		Count:            cfg.RedisReadCount,
		EnsureConsumer:   cfg.IngestionEnabled,
		MaxAttempts:      cfg.RedisMaxAttempts,
		DeadLetterStream: cfg.RedisDeadLetterStream,
	})
	if err != nil {
		_ = client.Close()
		return nil, nil, err
	}
	return bus, []io.Closer{client}, nil
}

func closeAll(closers []io.Closer) error {
	var err error
	for idx := len(closers) - 1; idx >= 0; idx-- {
		if closers[idx] == nil {
			continue
		}
		err = errors.Join(err, closers[idx].Close())
	}
	return err
}

func newSourceResolver(cfg config.Config) (controlplane.Resolver, error) {
	// Resolver selection is a deployment boundary: memory mode is local/static,
	// while http mode reads SaaS runtime config without taking over CRUD.
	var resolver controlplane.Resolver
	var err error
	switch cfg.SourceResolver {
	case "", "memory":
		resolver, err = controlplane.NewMemoryResolver(cfg.Sources)
	case "http":
		resolver, err = controlplane.NewHTTPResolver(controlplane.HTTPResolverOptions{
			Endpoint:              cfg.ControlPlaneURL,
			BearerToken:           cfg.ControlPlaneToken,
			Timeout:               cfg.ControlPlaneTimeout,
			CacheTTL:              cfg.ControlPlaneCacheTTL,
			AllowInsecureLoopback: cfg.ControlPlaneAllowInsecureLoopback,
		})
	default:
		return nil, configError("unsupported source resolver")
	}
	if err != nil {
		return nil, err
	}
	if cfg.IngestionEnabled && cfg.SourceResolver == "http" {
		// Same-process ingestion validates ClickHouse tables only for the
		// startup source list. Bind dynamic HTTP responses to that surface so
		// collect cannot accept a source whose tables were never checked.
		return controlplane.NewSchemaBoundResolver(resolver, cfg.Sources)
	}
	return resolver, nil
}

type configError string

func (e configError) Error() string {
	return string(e)
}
