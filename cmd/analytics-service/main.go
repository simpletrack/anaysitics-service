// Command analytics-service starts the SimpleTrack analytics data-plane runtime.
package main

import (
	"log"

	"github.com/simpletrack/analytics-service/internal/config"
	"github.com/simpletrack/analytics-service/internal/runtime"
	"github.com/valyala/fasthttp"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	handler, err := runtime.NewHandler(cfg)
	if err != nil {
		log.Fatalf("build runtime: %v", err)
	}
	if err := fasthttp.ListenAndServe(cfg.Addr, handler); err != nil {
		log.Fatalf("serve analytics service: %v", err)
	}
}
