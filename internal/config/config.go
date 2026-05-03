package config

import (
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/simpletrack/analytics-service/internal/controlplane"
)

const (
	defaultAddr        = ":8080"
	defaultCollectPath = "/collect"
	defaultHealthPath  = "/healthz"
	defaultTrackerPath = "/tracker.js"
	defaultTrackerFile = "public/tracker.js"
	defaultEventBus    = "redis"
	defaultRedisStream = "analytics.events"
	defaultDeadLetters = "analytics.events.dead"
	defaultWorkerGroup = "simpletrack-anaysitics-service"
	defaultTablePrefix = "events"
)

// Config contains process-level runtime settings.
type Config struct {
	Addr                  string                      // Addr is the fasthttp listen address
	CollectPath           string                      // CollectPath is the event reporting route
	HealthPath            string                      // HealthPath is the health check route
	TrackerPath           string                      // TrackerPath is the browser tracker route
	TrackerFile           string                      // TrackerFile is the local JavaScript asset path
	TrustForwardedHeaders bool                        // TrustForwardedHeaders enables proxy-provided client IP headers
	EventBus              string                      // EventBus selects the runtime queue backend, usually redis
	AllowInMemoryBus      bool                        // AllowInMemoryBus explicitly permits non-durable demo mode
	RedisAddr             string                      // RedisAddr is the Redis server address used for durable event enqueueing
	RedisPassword         string                      // RedisPassword is the optional Redis password
	RedisDB               int                         // RedisDB is the Redis logical database index
	RedisStream           string                      // RedisStream is the stream receiving accepted analytics events
	RedisDeadLetterStream string                      // RedisDeadLetterStream receives exhausted queue messages
	RedisBlock            time.Duration               // RedisBlock is the blocking read duration for future workers
	RedisReadCount        int64                       // RedisReadCount is the maximum messages read per poll
	RedisMaxAttempts      int                         // RedisMaxAttempts is the dead-letter threshold for future workers
	IngestionEnabled      bool                        // IngestionEnabled starts the runtime Redis-to-storage worker
	WorkerGroup           string                      // WorkerGroup is the Redis Stream consumer group for ingestion
	WorkerConsumer        string                      // WorkerConsumer is the concrete consumer name for this process
	MySQLDSN              string                      // MySQLDSN stores ingestion idempotency checkpoints
	MySQLAutoMigrate      bool                        // MySQLAutoMigrate creates checkpoint tables at startup when enabled
	ClickHouseAddr        string                      // ClickHouseAddr is the native ClickHouse endpoint for event writes
	ClickHouseDatabase    string                      // ClickHouseDatabase is the ClickHouse database for analytics events
	ClickHouseUser        string                      // ClickHouseUser authenticates the ClickHouse native connection
	ClickHousePassword    string                      // ClickHousePassword authenticates the ClickHouse native connection
	ClickHouseTablePrefix string                      // ClickHouseTablePrefix is the safe prefix for routed event tables
	ClickHouseAutoMigrate bool                        // ClickHouseAutoMigrate creates routed ClickHouse event tables at startup
	PropertyIndexing      bool                        // PropertyIndexing writes typed property rows after primary events
	Sources               []controlplane.SourceConfig // Sources are runtime source configs loaded from the control plane substitute
}

// LoadFromEnv loads service config from environment variables.
func LoadFromEnv() (Config, error) {
	// Load route and queue defaults first so startup behavior is deterministic
	// before any control-plane source config is decoded.
	config := Config{
		Addr:        envString("ANALYTICS_SERVICE_ADDR", defaultAddr),
		CollectPath: envString("ANALYTICS_SERVICE_COLLECT_PATH", defaultCollectPath),
		HealthPath:  envString("ANALYTICS_SERVICE_HEALTH_PATH", defaultHealthPath),
		TrackerPath: envString("ANALYTICS_SERVICE_TRACKER_PATH", defaultTrackerPath),
		TrackerFile: envString("ANALYTICS_SERVICE_TRACKER_FILE", defaultTrackerFile),
		TrustForwardedHeaders: envBool(
			"ANALYTICS_SERVICE_TRUST_FORWARDED_HEADERS",
			false,
		),
		EventBus:              strings.ToLower(envString("ANALYTICS_SERVICE_EVENTBUS", defaultEventBus)),
		AllowInMemoryBus:      envBool("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", false),
		RedisAddr:             envString("ANALYTICS_SERVICE_REDIS_ADDR", ""),
		RedisPassword:         envString("ANALYTICS_SERVICE_REDIS_PASSWORD", ""),
		RedisDB:               envInt("ANALYTICS_SERVICE_REDIS_DB", 0),
		RedisStream:           envString("ANALYTICS_SERVICE_REDIS_STREAM", defaultRedisStream),
		RedisDeadLetterStream: envString("ANALYTICS_SERVICE_REDIS_DEAD_LETTER_STREAM", defaultDeadLetters),
		RedisBlock:            envDuration("ANALYTICS_SERVICE_REDIS_BLOCK", time.Second),
		RedisReadCount:        int64(envInt("ANALYTICS_SERVICE_REDIS_READ_COUNT", 10)),
		RedisMaxAttempts:      envInt("ANALYTICS_SERVICE_REDIS_MAX_ATTEMPTS", 5),
		IngestionEnabled:      envBool("ANALYTICS_SERVICE_INGESTION_ENABLED", false),
		WorkerGroup:           envString("ANALYTICS_SERVICE_WORKER_GROUP", defaultWorkerGroup),
		WorkerConsumer:        envString("ANALYTICS_SERVICE_WORKER_CONSUMER", defaultWorkerConsumer()),
		MySQLDSN:              envString("ANALYTICS_SERVICE_MYSQL_DSN", ""),
		MySQLAutoMigrate:      envBool("ANALYTICS_SERVICE_MYSQL_AUTO_MIGRATE", false),
		ClickHouseAddr:        envString("ANALYTICS_SERVICE_CLICKHOUSE_ADDR", ""),
		ClickHouseDatabase:    envString("ANALYTICS_SERVICE_CLICKHOUSE_DATABASE", "default"),
		ClickHouseUser:        envString("ANALYTICS_SERVICE_CLICKHOUSE_USER", "default"),
		ClickHousePassword:    envString("ANALYTICS_SERVICE_CLICKHOUSE_PASSWORD", ""),
		ClickHouseTablePrefix: envString("ANALYTICS_SERVICE_CLICKHOUSE_TABLE_PREFIX", defaultTablePrefix),
		ClickHouseAutoMigrate: envBool("ANALYTICS_SERVICE_CLICKHOUSE_AUTO_MIGRATE", false),
		PropertyIndexing:      envBool("ANALYTICS_SERVICE_PROPERTY_INDEXING", true),
	}

	// Refuse a startup mode that would acknowledge /collect without a durable
	// enqueue path. Local in-memory mode must be explicit in the environment.
	if config.EventBus == "redis" && config.RedisAddr == "" {
		return Config{}, errors.New("ANALYTICS_SERVICE_REDIS_ADDR is required when ANALYTICS_SERVICE_EVENTBUS=redis")
	}
	if config.EventBus == "direct" && !config.AllowInMemoryBus {
		return Config{}, errors.New("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS=true is required for direct event bus mode")
	}
	if config.EventBus != "redis" && config.EventBus != "direct" {
		return Config{}, errors.New("ANALYTICS_SERVICE_EVENTBUS must be redis or direct")
	}
	if config.IngestionEnabled {
		if config.EventBus != "redis" {
			return Config{}, errors.New("ANALYTICS_SERVICE_INGESTION_ENABLED requires ANALYTICS_SERVICE_EVENTBUS=redis")
		}
		if config.WorkerGroup == "" {
			return Config{}, errors.New("ANALYTICS_SERVICE_WORKER_GROUP is required when ingestion is enabled")
		}
		if config.WorkerConsumer == "" {
			return Config{}, errors.New("ANALYTICS_SERVICE_WORKER_CONSUMER is required when ingestion is enabled")
		}
		if config.MySQLDSN == "" {
			return Config{}, errors.New("ANALYTICS_SERVICE_MYSQL_DSN is required when ingestion is enabled")
		}
		if config.ClickHouseAddr == "" {
			return Config{}, errors.New("ANALYTICS_SERVICE_CLICKHOUSE_ADDR is required when ingestion is enabled")
		}
	}

	// Decode the runtime control-plane view last. These source configs are
	// read-only inputs for enforcement and are not owned by this service.
	rawSources := strings.TrimSpace(os.Getenv("ANALYTICS_SERVICE_SOURCES_JSON"))
	if rawSources == "" {
		return Config{}, errors.New("ANALYTICS_SERVICE_SOURCES_JSON is required")
	}
	if err := json.Unmarshal([]byte(rawSources), &config.Sources); err != nil {
		return Config{}, err
	}
	if len(config.Sources) == 0 {
		return Config{}, errors.New("at least one analytics source is required")
	}
	return config, nil
}

func envString(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func envBool(name string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes"
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func defaultWorkerConsumer() string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		return "consumer-1"
	}
	return strings.ToLower(strings.ReplaceAll(hostname, " ", "-"))
}
