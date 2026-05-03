package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/simpletrack/analytics-service/internal/config"
	"github.com/simpletrack/analytics-service/internal/controlplane"
)

func ExampleNew() {
	app, err := New(exampleRuntimeConfig())
	if err != nil {
		fmt.Println(err)
		return
	}
	defer app.Close()
	fmt.Println(app.Handler() != nil)
	// Output: true
}

func ExampleRuntime_Start() {
	app, err := New(exampleRuntimeConfig())
	if err != nil {
		fmt.Println(err)
		return
	}
	defer app.Close()

	ctx, cancel := context.WithCancel(context.Background())
	app.Start(ctx)
	cancel()

	fmt.Println(app.WorkerDone() == nil)
	// Output: true
}

func exampleRuntimeConfig() config.Config {
	trackerPath := filepath.Join(os.TempDir(), "simpletrack-example-tracker.js")
	if err := os.WriteFile(trackerPath, []byte("window.simpletrack = window.simpletrack || {};"), 0600); err != nil {
		panic(err)
	}

	return config.Config{
		EventBus:         "direct",
		AllowInMemoryBus: true,
		CollectPath:      "/collect",
		HealthPath:       "/healthz",
		TrackerPath:      "/tracker.js",
		TrackerFile:      trackerPath,
		Sources: []controlplane.SourceConfig{
			{
				WriteKey:       "wk_public_from_snippet",
				Enabled:        true,
				TenantID:       "tenant_1",
				ProjectID:      "project_1",
				SourceID:       "source_web",
				SessionSalt:    "server-only-session-salt",
				ClientHashSalt: "server-only-client-salt",
			},
		},
	}
}
