package config

import (
	"slices"
	"testing"
	"time"

	"github.com/simpletrack/analytics-service/internal/controlplane"
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

func TestLoadFromEnvRequiresKafkaBrokers(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "kafka")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	if _, err := LoadFromEnv(); err == nil {
		t.Fatalf("expected kafka event bus without brokers to fail")
	}
}

func TestLoadFromEnvReadsKafkaOptions(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "kafka")
	t.Setenv("ANALYTICS_SERVICE_KAFKA_BROKERS", "127.0.0.1:29092, 127.0.0.1:39092")
	t.Setenv("ANALYTICS_SERVICE_KAFKA_TOPIC", "custom.events")
	t.Setenv("ANALYTICS_SERVICE_KAFKA_DEAD_LETTER_TOPIC", "custom.events.dead")
	t.Setenv("ANALYTICS_SERVICE_KAFKA_CLIENT_ID", "analytics-service-test")
	t.Setenv("ANALYTICS_SERVICE_KAFKA_MAX_ATTEMPTS", "7")
	t.Setenv("ANALYTICS_SERVICE_KAFKA_RETRY_BACKOFF", "750ms")
	t.Setenv("ANALYTICS_SERVICE_KAFKA_WORKERS", "32")
	t.Setenv("ANALYTICS_SERVICE_KAFKA_QUEUE_SIZE", "96")
	t.Setenv("ANALYTICS_SERVICE_KAFKA_COMMIT_INTERVAL", "2s")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected kafka config to load: %v", err)
	}
	if !slices.Equal(cfg.KafkaBrokers, []string{"127.0.0.1:29092", "127.0.0.1:39092"}) {
		t.Fatalf("unexpected kafka brokers %#v", cfg.KafkaBrokers)
	}
	if cfg.KafkaTopic != "custom.events" || cfg.KafkaDeadLetterTopic != "custom.events.dead" {
		t.Fatalf("unexpected kafka topics %q / %q", cfg.KafkaTopic, cfg.KafkaDeadLetterTopic)
	}
	if cfg.KafkaClientID != "analytics-service-test" || cfg.KafkaMaxAttempts != 7 {
		t.Fatalf("unexpected kafka identity or attempts: %#v", cfg)
	}
	if cfg.KafkaRetryBackoff != 750*time.Millisecond || cfg.KafkaWorkers != 32 || cfg.KafkaQueueSize != 96 || cfg.KafkaCommitInterval != 2*time.Second {
		t.Fatalf("unexpected kafka runtime tuning: %#v", cfg)
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
	if !cfg.PropertyCataloging {
		t.Fatalf("expected property cataloging to be enabled by default")
	}
}

func TestLoadFromEnvAcceptsKafkaIngestionStorageConfig(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "kafka")
	t.Setenv("ANALYTICS_SERVICE_KAFKA_BROKERS", "127.0.0.1:29092")
	t.Setenv("ANALYTICS_SERVICE_INGESTION_ENABLED", "true")
	t.Setenv("ANALYTICS_SERVICE_MYSQL_DSN", "user:pass@tcp(127.0.0.1:3306)/analytics?parseTime=true")
	t.Setenv("ANALYTICS_SERVICE_CLICKHOUSE_ADDR", "127.0.0.1:9000")
	t.Setenv("ANALYTICS_SERVICE_WORKER_CONSUMER", "consumer-test")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected kafka ingestion config to load: %v", err)
	}
	if cfg.EventBus != "kafka" || !cfg.IngestionEnabled {
		t.Fatalf("expected kafka ingestion, got eventbus=%q ingestion=%v", cfg.EventBus, cfg.IngestionEnabled)
	}
	if cfg.KafkaTopic != "analytics.events" || cfg.KafkaDeadLetterTopic != "analytics.events.dead" {
		t.Fatalf("unexpected kafka default topics %q / %q", cfg.KafkaTopic, cfg.KafkaDeadLetterTopic)
	}
}

func TestLoadFromEnvRejectsDirectIngestion(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "direct")
	t.Setenv("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", "true")
	t.Setenv("ANALYTICS_SERVICE_INGESTION_ENABLED", "true")
	t.Setenv("ANALYTICS_SERVICE_MYSQL_DSN", "user:pass@tcp(127.0.0.1:3306)/analytics?parseTime=true")
	t.Setenv("ANALYTICS_SERVICE_CLICKHOUSE_ADDR", "127.0.0.1:9000")
	t.Setenv("ANALYTICS_SERVICE_WORKER_CONSUMER", "consumer-test")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	if _, err := LoadFromEnv(); err == nil {
		t.Fatalf("expected direct ingestion to fail")
	}
}

func TestLoadFromEnvCanDisablePropertyCataloging(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_REDIS_ADDR", "127.0.0.1:26379")
	t.Setenv("ANALYTICS_SERVICE_INGESTION_ENABLED", "true")
	t.Setenv("ANALYTICS_SERVICE_MYSQL_DSN", "user:pass@tcp(127.0.0.1:3306)/analytics?parseTime=true")
	t.Setenv("ANALYTICS_SERVICE_CLICKHOUSE_ADDR", "127.0.0.1:9000")
	t.Setenv("ANALYTICS_SERVICE_PROPERTY_CATALOGING", "false")
	t.Setenv("ANALYTICS_SERVICE_WORKER_CONSUMER", "consumer-test")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected ingestion config to load: %v", err)
	}
	if cfg.PropertyCataloging {
		t.Fatalf("expected property cataloging to be disabled")
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
	if cfg.PropertiesPath != "/v1/properties" {
		t.Fatalf("unexpected properties path %q", cfg.PropertiesPath)
	}
	if cfg.GoalsPath != "/v1/goals" {
		t.Fatalf("unexpected goals path %q", cfg.GoalsPath)
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

func TestLoadFromEnvAcceptsStructuredQueryTokenCredentials(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "direct")
	t.Setenv("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", "true")
	t.Setenv("ANALYTICS_SERVICE_QUERY_ENABLED", "true")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKEN", "current-token")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKEN_ID", "current")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKEN_EXPIRES_AT", "2026-05-03T12:00:00Z")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKEN_SCOPES", "realtime, events")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKEN_WRITE_KEYS", "wk_live, wk_docs")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKENS_JSON", `[
		{"id":"previous","token":"previous-token","expires_at":"2026-05-03T10:15:00Z","scopes":["properties"],"write_keys":["wk_docs","wk_docs"," "]},
		"legacy-token"
	]`)
	t.Setenv("ANALYTICS_SERVICE_CLICKHOUSE_ADDR", "127.0.0.1:9000")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected structured query credentials to load: %v", err)
	}
	if !slices.Equal(cfg.QueryTokens, []string{"current-token", "previous-token", "legacy-token"}) {
		t.Fatalf("unexpected accepted query tokens %#v", cfg.QueryTokens)
	}
	if len(cfg.QueryCredentials) != 3 {
		t.Fatalf("expected three query credentials, got %#v", cfg.QueryCredentials)
	}
	if len(cfg.QueryCredentials[0].Scopes) != 2 {
		t.Fatalf("expected primary credential scopes, got %#v", cfg.QueryCredentials[0])
	}
	if !slices.Equal(cfg.QueryCredentials[0].AllowedWriteKeys, []string{"wk_live", "wk_docs"}) {
		t.Fatalf("expected primary credential write-key allowlist, got %#v", cfg.QueryCredentials[0])
	}
	if cfg.QueryCredentials[1].ID != "previous" {
		t.Fatalf("expected previous token id, got %#v", cfg.QueryCredentials[1])
	}
	if cfg.QueryCredentials[1].ExpiresAt.Format(time.RFC3339) != "2026-05-03T10:15:00Z" {
		t.Fatalf("unexpected rotated token expiry %#v", cfg.QueryCredentials[1])
	}
	if len(cfg.QueryCredentials[1].Scopes) != 1 || cfg.QueryCredentials[1].Scopes[0] != controlplane.ReadbackRouteProperties {
		t.Fatalf("expected structured credential scopes, got %#v", cfg.QueryCredentials[1])
	}
	if !slices.Equal(cfg.QueryCredentials[1].AllowedWriteKeys, []string{"wk_docs"}) {
		t.Fatalf("expected structured credential write-key allowlist, got %#v", cfg.QueryCredentials[1])
	}
}

func TestLoadFromEnvRejectsInvalidStructuredQueryTokenWindow(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "direct")
	t.Setenv("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", "true")
	t.Setenv("ANALYTICS_SERVICE_QUERY_ENABLED", "true")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKEN", "current-token")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKENS_JSON", `[
		{"id":"broken","token":"previous-token","not_before":"2026-05-03T11:00:00Z","expires_at":"2026-05-03T10:00:00Z"}
	]`)
	t.Setenv("ANALYTICS_SERVICE_CLICKHOUSE_ADDR", "127.0.0.1:9000")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	if _, err := LoadFromEnv(); err == nil {
		t.Fatalf("expected invalid structured query token window to fail")
	}
}

func TestLoadFromEnvRejectsStructuredQueryTokenWithoutToken(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "direct")
	t.Setenv("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", "true")
	t.Setenv("ANALYTICS_SERVICE_QUERY_ENABLED", "true")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKEN", "current-token")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKENS_JSON", `[
		{"id":"broken","token":"   "}
	]`)
	t.Setenv("ANALYTICS_SERVICE_CLICKHOUSE_ADDR", "127.0.0.1:9000")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	if _, err := LoadFromEnv(); err == nil {
		t.Fatalf("expected structured query token without token to fail")
	}
}

func TestLoadFromEnvRejectsInvalidQueryTokenScope(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "direct")
	t.Setenv("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", "true")
	t.Setenv("ANALYTICS_SERVICE_QUERY_ENABLED", "true")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKEN", "current-token")
	t.Setenv("ANALYTICS_SERVICE_QUERY_TOKEN_SCOPES", "events, broken")
	t.Setenv("ANALYTICS_SERVICE_CLICKHOUSE_ADDR", "127.0.0.1:9000")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	if _, err := LoadFromEnv(); err == nil {
		t.Fatalf("expected invalid query token scope to fail")
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

func TestLoadFromEnvReadsGeoIPMMDBFile(t *testing.T) {
	t.Setenv("ANALYTICS_SERVICE_EVENTBUS", "direct")
	t.Setenv("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", "true")
	t.Setenv("ANALYTICS_SERVICE_GEOIP_MMDB_FILE", "C:/data/GeoLite2-City.mmdb")
	t.Setenv("ANALYTICS_SERVICE_SOURCES_JSON", testSourcesJSON())

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected geo config to load: %v", err)
	}
	if cfg.GeoIPMMDBFile != "C:/data/GeoLite2-City.mmdb" {
		t.Fatalf("unexpected geoip file %q", cfg.GeoIPMMDBFile)
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
