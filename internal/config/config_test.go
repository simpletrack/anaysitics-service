package config

import "testing"

func TestLoadFromEnvRequiresRedisAddressByDefault(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", `[{"write_key":"wk","enabled":true,"tenant_id":"ten","project_id":"prj","source_id":"src"}]`)

	if _, err := LoadFromEnv(); err == nil {
		t.Fatalf("expected missing redis address to fail default startup")
	}
}

func TestLoadFromEnvAllowsExplicitInMemoryMode(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "direct")
	t.Setenv("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", "true")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", `[{"write_key":"wk","enabled":true,"tenant_id":"ten","project_id":"prj","source_id":"src"}]`)

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected explicit in-memory mode to load: %v", err)
	}
	if cfg.EventBus != "direct" {
		t.Fatalf("expected direct event bus, got %q", cfg.EventBus)
	}
}
