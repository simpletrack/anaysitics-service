// Command simpletrack-anaysitics-service starts the SimpleTrack analytics data-plane runtime.
package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"

	"github.com/simpletrack/analytics-service/internal/config"
	"github.com/simpletrack/analytics-service/internal/runtime"
)

func main() {
	// Load process and runtime dependency settings before any sockets or queue
	// consumers are opened, so invalid deployments fail before accepting events.
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// Assemble HTTP, EventBus, and optional ingestion dependencies behind the
	// runtime package boundary; main only owns process lifecycle wiring.
	app, err := runtime.New(cfg)
	if err != nil {
		log.Fatalf("build runtime: %v", err)
	}
	defer func() {
		if err := app.Close(); err != nil {
			log.Printf("close runtime: %v", err)
		}
	}()

	// Use one cancellation context for both HTTP shutdown and worker shutdown so
	// Redis blocking reads and Fiber serving stop together on process signals.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	app.Start(ctx)

	// Start HTTP in the background so the process can also react to ingestion
	// worker failures instead of serving collect while storage is down.
	serverErr := make(chan error, 1)
	go func() {
		// Listen owns the public HTTP lifecycle while the optional
		// ingestion worker runs under the same process cancellation context.
		serverErr <- app.App().Listen(cfg.Addr)
	}()

	select {
	case err := <-serverErr:
		if err != nil {
			log.Fatalf("serve analytics service: %v", err)
		}
	case err := <-app.WorkerDone():
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("run ingestion worker: %v", err)
		}
		if err := app.App().Shutdown(); err != nil {
			log.Printf("shutdown server after worker stop: %v", err)
		}
	case <-ctx.Done():
		// Shutdown stops accepting HTTP requests; cancelling ctx also lets the
		// worker leave Redis blocking reads and return context.Canceled.
		if err := app.App().Shutdown(); err != nil {
			log.Printf("shutdown server: %v", err)
		}
		if err := <-serverErr; err != nil {
			log.Printf("serve analytics service: %v", err)
		}
	}
}
