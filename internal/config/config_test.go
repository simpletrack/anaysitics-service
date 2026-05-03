package config

import "testing"

func TestLoadFromEnvRequiresRedisAddressByDefault(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	if _, err := LoadFromEnv(); err == nil {
		t.Fatalf("expected missing redis address to fail default startup")
	}
}

func TestLoadFromEnvAllowsExplicitInMemoryMode(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "direct")
	t.Setenv("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", "true")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected explicit in-memory mode to load: %v", err)
	}
	if cfg.EventBus != "direct" {
		t.Fatalf("expected direct event bus, got %q", cfg.EventBus)
	}
}

func TestLoadFromEnvRequiresStorageWhenIngestionIsEnabled(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_REDIS_ADDR", "127.0.0.1:26379")
	t.Setenv("ANALYTICS_SERVICE_INGESTION_ENABLED", "true")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	if _, err := LoadFromEnv(); err == nil {
		t.Fatalf("expected ingestion startup without storage config to fail")
	}
}

func TestLoadFromEnvAcceptsIngestionStorageConfig(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_REDIS_ADDR", "127.0.0.1:26379")
	t.Setenv("ANALYTICS_SERVICE_INGESTION_ENABLED", "true")
	t.Setenv("ANALYTICS_SERVICE_MYSQL_DSN", "user:pass@tcp(127.0.0.1:3306)/analytics?parseTime=true")
	t.Setenv("ANALYTICS_SERVICE_CLICKHOUSE_ADDR", "127.0.0.1:9000")
	t.Setenv("ANALYTICS_SERVICE_WORKER_CONSUMER", "consumer-test")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected ingestion config to load: %v", err)
	}
	if !cfg.IngestionEnabled {
		t.Fatalf("expected ingestion to be enabled")
	}
	if cfg.WorkerGroup != "simpletrack-anaysistics-service" {
		t.Fatalf("unexpected worker group %q", cfg.WorkerGroup)
	}
	if cfg.ClickHouseTablePrefix != "events" {
		t.Fatalf("unexpected table prefix %q", cfg.ClickHouseTablePrefix)
	}
}

func testSourcesJSON() string {
	return `[{
		"write_key":"wk",
		"enabled":true,
		"tenant_id":"ten",
		"project_id":"prj",
		"source_id":"src",
		"session_salt":"server-only-session-salt",
		"client_hash_salt":"server-only-client-salt"
	}]`
}
