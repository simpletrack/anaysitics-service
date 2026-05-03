package config

import (
	"slices"
	"testing"
)

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
	if cfg.WorkerGroup != "simpletrack-anaysitics-service" {
		t.Fatalf("unexpected worker group %q", cfg.WorkerGroup)
	}
	if cfg.ClickHouseTablePrefix != "events" {
		t.Fatalf("unexpected table prefix %q", cfg.ClickHouseTablePrefix)
	}
}

func TestLoadFromEnvAcceptsClickHouseAutoMigrateFlag(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_REDIS_ADDR", "127.0.0.1:26379")
	t.Setenv("ANALYTICS_SERVICE_INGESTION_ENABLED", "true")
	t.Setenv("ANALYTICS_SERVICE_MYSQL_DSN", "user:pass@tcp(127.0.0.1:3306)/analytics?parseTime=true")
	t.Setenv("ANALYTICS_SERVICE_CLICKHOUSE_ADDR", "127.0.0.1:9000")
	t.Setenv("ANALYTICS_SERVICE_CLICKHOUSE_AUTO_MIGRATE", "true")
	t.Setenv("ANALYTICS_SERVICE_WORKER_CONSUMER", "consumer-test")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected ingestion config to load: %v", err)
	}
	if !cfg.ClickHouseAutoMigrate {
		t.Fatalf("expected ClickHouse auto migration to be enabled")
	}
}

func TestLoadFromEnvRequiresQueryTokenWhenQueryIsEnabled(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "direct")
	t.Setenv("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", "true")
	t.Setenv("ANALYTICS_SERVICE_QUERY_ENABLED", "true")
	t.Setenv("ANALYTICS_SERVICE_CLICKHOUSE_ADDR", "127.0.0.1:9000")

	if _, err := LoadFromEnv(); err == nil {
		t.Fatalf("expected enabled query mode without a token to fail")
	}
}

func TestLoadFromEnvAcceptsQueryModeConfig(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "direct")
	t.Setenv("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", "true")
	t.Setenv("ANALYTICS_SERVICE_QUERY_ENABLED", "true")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKEN", "query-token")
	t.Setenv("ANALYTICS_SERVICE_CLICKHOUSE_ADDR", "127.0.0.1:9000")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected query mode config to load: %v", err)
	}
	if !cfg.QueryEnabled {
		t.Fatalf("expected query mode to be enabled")
	}
	if cfg.QueryToken != "query-token" {
		t.Fatalf("unexpected query token %q", cfg.QueryToken)
	}
	if !slices.Equal(cfg.QueryTokens, []string{"query-token"}) {
		t.Fatalf("unexpected accepted query tokens %#v", cfg.QueryTokens)
	}
}

func TestLoadFromEnvAcceptsQueryTokenRotationAllowlist(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "direct")
	t.Setenv("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", "true")
	t.Setenv("ANALYTICS_SERVICE_QUERY_ENABLED", "true")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKEN", "current-token")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKENS_JSON", `["previous-token","current-token"," "]`)
	t.Setenv("ANALYTICS_SERVICE_CLICKHOUSE_ADDR", "127.0.0.1:9000")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected query rotation config to load: %v", err)
	}
	if !slices.Equal(cfg.QueryTokens, []string{"current-token", "previous-token"}) {
		t.Fatalf("unexpected accepted query tokens %#v", cfg.QueryTokens)
	}
}

func TestLoadFromEnvAllowsHTTPSourceResolverWithoutStaticSources(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "direct")
	t.Setenv("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", "true")
	t.Setenv("ANALYTICS_SERVICE_SOURCE_RESOLVER", "http")
	t.Setenv("ANALYTICS_SERVICE_CONTROL_PLANE_URL", "https://control.example.com/runtime/source")
	t.Setenv("ANALYTICS_SERVICE_CONTROL_PLANE_TOKEN", "runtime-token")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected http source resolver config to load: %v", err)
	}
	if cfg.SourceResolver != "http" {
		t.Fatalf("unexpected source resolver %q", cfg.SourceResolver)
	}
	if len(cfg.Sources) != 0 {
		t.Fatalf("expected http resolver to avoid static source config, got %#v", cfg.Sources)
	}
}

func TestLoadFromEnvAcceptsControlPlaneInsecureLoopbackFlag(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "direct")
	t.Setenv("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", "true")
	t.Setenv("ANALYTICS_SERVICE_SOURCE_RESOLVER", "http")
	t.Setenv("ANALYTICS_SERVICE_CONTROL_PLANE_URL", "http://127.0.0.1:8080/runtime-source")
	t.Setenv("ANALYTICS_SERVICE_CONTROL_PLANE_TOKEN", "runtime-token")
	t.Setenv("ANALYTICS_SERVICE_CONTROL_PLANE_ALLOW_INSECURE_LOOPBACK", "true")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected http source resolver config to load: %v", err)
	}
	if !cfg.ControlPlaneAllowInsecureLoopback {
		t.Fatalf("expected insecure loopback flag to be enabled")
	}
}

func TestLoadFromEnvRejectsMemoryResolverWithoutSources(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_SOURCE_RESOLVER", "memory")

	if _, err := LoadFromEnv(); err == nil {
		t.Fatalf("expected memory source resolver without static sources to fail")
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
