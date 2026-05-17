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
	defaultAddr           = ":8080"
	defaultCollectPath    = "/collect"
	defaultHealthPath     = "/healthz"
	defaultTrackerPath    = "/tracker.js"
	defaultEventsPath     = "/v1/events"
	defaultGoalsPath      = "/v1/goals"
	defaultRealtimePath   = "/v1/realtime"
	defaultPropertiesPath = "/v1/properties"
	defaultKafkaDiagPath  = "/v1/kafka/diagnostics"
	defaultSwaggerPath    = "/swagger"
	defaultOpenAPIFile    = "api/openapi.yaml"
	defaultTrackerFile    = "public/tracker.js"
	defaultEventBus       = "redis"
	defaultRedisStream    = "analytics.events"
	defaultDeadLetters    = "analytics.events.dead"
	defaultKafkaTopic     = "analytics.events"
	defaultKafkaDeadTopic = "analytics.events.dead"
	defaultKafkaClientID  = "simpletrack-anaysitics-service"
	defaultWorkerGroup    = "simpletrack-anaysitics-service"
	defaultTablePrefix    = "events"
	defaultResolver       = "memory"
)

// QueryTokenCredential describes one internal readback bearer token and its lifecycle window.
type QueryTokenCredential struct {
	ID               string                       // ID is a non-secret alias used in operator audit logs
	Token            string                       // Token is the bearer secret accepted by the internal read API
	NotBefore        time.Time                    // NotBefore optionally delays token activation until this instant
	ExpiresAt        time.Time                    // ExpiresAt optionally rejects the token at or after this instant
	Scopes           []controlplane.ReadbackRoute // Scopes optionally narrows the readback route families this token may call
	AllowedWriteKeys []string                     // AllowedWriteKeys optionally narrows the token to specific runtime write keys
}

// Config contains process-level runtime settings.
type Config struct {
	Addr                              string                      // Addr is the Fiber listen address
	CollectPath                       string                      // CollectPath is the event reporting route
	HealthPath                        string                      // HealthPath is the health check route
	TrackerPath                       string                      // TrackerPath is the browser tracker route
	EventsPath                        string                      // EventsPath is the internal Events readback route
	GoalsPath                         string                      // GoalsPath is the internal Goal summary readback route
	RealtimePath                      string                      // RealtimePath is the internal Realtime readback route
	PropertiesPath                    string                      // PropertiesPath is the internal property catalog read route
	SwaggerEnabled                    bool                        // SwaggerEnabled exposes OpenAPI documentation routes
	SwaggerPath                       string                      // SwaggerPath is the Swagger UI route prefix
	OpenAPIFile                       string                      // OpenAPIFile is the local OpenAPI YAML or JSON file
	TrackerFile                       string                      // TrackerFile is the local JavaScript asset path
	TrustForwardedHeaders             bool                        // TrustForwardedHeaders enables proxy-provided client IP headers
	GeoIPMMDBFile                     string                      // GeoIPMMDBFile enables offline MaxMind-compatible geo enrichment when set
	EventBus                          string                      // EventBus selects redis, kafka, or explicitly allowed direct mode
	AllowInMemoryBus                  bool                        // AllowInMemoryBus explicitly permits non-durable demo mode
	RedisAddr                         string                      // RedisAddr is the Redis server address used for durable event enqueueing
	RedisPassword                     string                      // RedisPassword is the optional Redis password
	RedisDB                           int                         // RedisDB is the Redis logical database index
	RedisStream                       string                      // RedisStream is the stream receiving accepted analytics events
	RedisDeadLetterStream             string                      // RedisDeadLetterStream receives exhausted queue messages
	RedisBlock                        time.Duration               // RedisBlock is the blocking read duration for future workers
	RedisReadCount                    int64                       // RedisReadCount is the maximum messages read per poll
	RedisMaxAttempts                  int                         // RedisMaxAttempts is the dead-letter threshold for future workers
	KafkaBrokers                      []string                    // KafkaBrokers are bootstrap brokers for the production EventBus provider
	KafkaTopic                        string                      // KafkaTopic receives accepted analytics event envelopes
	KafkaDeadLetterTopic              string                      // KafkaDeadLetterTopic receives malformed or exhausted Kafka messages
	KafkaClientID                     string                      // KafkaClientID identifies this service instance family to Kafka
	KafkaMaxAttempts                  int                         // KafkaMaxAttempts is the handler attempt limit before Kafka DLQ
	KafkaRetryBackoff                 time.Duration               // KafkaRetryBackoff spaces Kafka handler and DLQ retries
	KafkaWorkers                      int                         // KafkaWorkers is the fixed Kafka handler worker count
	KafkaQueueSize                    int                         // KafkaQueueSize is the bounded Kafka handler work queue size
	KafkaCommitInterval               time.Duration               // KafkaCommitInterval controls Sarama offset commit cadence
	KafkaTLSEnabled                   bool                        // KafkaTLSEnabled turns on TLS for Kafka broker connections
	KafkaTLSServerName                string                      // KafkaTLSServerName overrides the broker certificate server name
	KafkaTLSCAFile                    string                      // KafkaTLSCAFile is an optional PEM CA bundle for broker trust
	KafkaTLSCertFile                  string                      // KafkaTLSCertFile is an optional client certificate PEM path
	KafkaTLSKeyFile                   string                      // KafkaTLSKeyFile is the private key path for KafkaTLSCertFile
	KafkaTLSInsecureSkipVerify        bool                        // KafkaTLSInsecureSkipVerify disables broker certificate verification for controlled tests
	KafkaSASLEnabled                  bool                        // KafkaSASLEnabled turns on Kafka broker authentication
	KafkaSASLMechanism                string                      // KafkaSASLMechanism selects the supported SASL mechanism
	KafkaSASLUsername                 string                      // KafkaSASLUsername is the broker authentication identity
	KafkaSASLPassword                 string                      // KafkaSASLPassword is the broker authentication secret
	KafkaSASLHandshake                bool                        // KafkaSASLHandshake controls the Kafka SASL pre-auth handshake
	KafkaDiagnosticsEnabled           bool                        // KafkaDiagnosticsEnabled exposes process-local Kafka provider diagnostics
	KafkaDiagnosticsPath              string                      // KafkaDiagnosticsPath is the internal Kafka diagnostics readback route
	IngestionEnabled                  bool                        // IngestionEnabled starts the runtime queue-to-storage worker
	WorkerGroup                       string                      // WorkerGroup is the durable queue consumer group for ingestion
	WorkerConsumer                    string                      // WorkerConsumer is the concrete consumer name for this process
	MySQLDSN                          string                      // MySQLDSN stores ingestion idempotency checkpoints
	MySQLAutoMigrate                  bool                        // MySQLAutoMigrate creates checkpoint tables at startup when enabled
	ClickHouseAddr                    string                      // ClickHouseAddr is the native ClickHouse endpoint for event writes
	ClickHouseDatabase                string                      // ClickHouseDatabase is the ClickHouse database for analytics events
	ClickHouseUser                    string                      // ClickHouseUser authenticates the ClickHouse native connection
	ClickHousePassword                string                      // ClickHousePassword authenticates the ClickHouse native connection
	ClickHouseTablePrefix             string                      // ClickHouseTablePrefix is the safe prefix for routed event tables
	ClickHouseAutoMigrate             bool                        // ClickHouseAutoMigrate creates routed ClickHouse event tables at startup
	PropertyIndexing                  bool                        // PropertyIndexing writes typed property rows after primary events
	PropertyCataloging                bool                        // PropertyCataloging records observed property selectors in MySQL metadata
	SourceResolver                    string                      // SourceResolver selects memory or http runtime source resolution
	ControlPlaneURL                   string                      // ControlPlaneURL is the SaaS runtime source resolver endpoint
	ControlPlaneToken                 string                      // ControlPlaneToken authenticates service-to-SaaS config reads
	ControlPlaneTimeout               time.Duration               // ControlPlaneTimeout bounds each SaaS resolver request
	ControlPlaneCacheTTL              time.Duration               // ControlPlaneCacheTTL caches resolved runtime source configs
	ControlPlaneAllowInsecureLoopback bool                        // ControlPlaneAllowInsecureLoopback allows http loopback control-plane URLs in local development
	QueryEnabled                      bool                        // QueryEnabled starts internal Events, Realtime, and property metadata read APIs
	QueryToken                        string                      // QueryToken authorizes internal readback requests
	QueryTokens                       []string                    // QueryTokens are accepted internal readback tokens during rotation windows
	QueryCredentials                  []QueryTokenCredential      // QueryCredentials are accepted internal read tokens with activation and expiry metadata
	Sources                           []controlplane.SourceConfig // Sources are runtime source configs loaded from the control plane substitute
}

// LoadFromEnv loads service config from environment variables.
func LoadFromEnv() (Config, error) {
	// Load route and queue defaults first so startup behavior is deterministic
	// before any control-plane source config is decoded.
	config := Config{
		Addr:           envString("ANALYTICS_SERVICE_ADDR", defaultAddr),
		CollectPath:    envString("ANALYTICS_SERVICE_COLLECT_PATH", defaultCollectPath),
		HealthPath:     envString("ANALYTICS_SERVICE_HEALTH_PATH", defaultHealthPath),
		TrackerPath:    envString("ANALYTICS_SERVICE_TRACKER_PATH", defaultTrackerPath),
		EventsPath:     envString("ANALYTICS_SERVICE_EVENTS_PATH", defaultEventsPath),
		GoalsPath:      envString("ANALYTICS_SERVICE_GOALS_PATH", defaultGoalsPath),
		RealtimePath:   envString("ANALYTICS_SERVICE_REALTIME_PATH", defaultRealtimePath),
		PropertiesPath: envString("ANALYTICS_SERVICE_PROPERTIES_PATH", defaultPropertiesPath),
		SwaggerEnabled: envBool("ANALYTICS_SERVICE_SWAGGER_ENABLED", false),
		SwaggerPath:    envString("ANALYTICS_SERVICE_SWAGGER_PATH", defaultSwaggerPath),
		OpenAPIFile:    envString("ANALYTICS_SERVICE_OPENAPI_FILE", defaultOpenAPIFile),
		TrackerFile:    envString("ANALYTICS_SERVICE_TRACKER_FILE", defaultTrackerFile),
		TrustForwardedHeaders: envBool(
			"ANALYTICS_SERVICE_TRUST_FORWARDED_HEADERS",
			false,
		),
		GeoIPMMDBFile:                     envString("ANALYTICS_SERVICE_GEOIP_MMDB_FILE", ""),
		EventBus:                          strings.ToLower(envString("ANALYTICS_SERVICE_EVENTBUS", defaultEventBus)),
		AllowInMemoryBus:                  envBool("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS", false),
		RedisAddr:                         envString("ANALYTICS_SERVICE_REDIS_ADDR", ""),
		RedisPassword:                     envString("ANALYTICS_SERVICE_REDIS_PASSWORD", ""),
		RedisDB:                           envInt("ANALYTICS_SERVICE_REDIS_DB", 0),
		RedisStream:                       envString("ANALYTICS_SERVICE_REDIS_STREAM", defaultRedisStream),
		RedisDeadLetterStream:             envString("ANALYTICS_SERVICE_REDIS_DEAD_LETTER_STREAM", defaultDeadLetters),
		RedisBlock:                        envDuration("ANALYTICS_SERVICE_REDIS_BLOCK", time.Second),
		RedisReadCount:                    int64(envInt("ANALYTICS_SERVICE_REDIS_READ_COUNT", 10)),
		RedisMaxAttempts:                  envInt("ANALYTICS_SERVICE_REDIS_MAX_ATTEMPTS", 5),
		KafkaBrokers:                      parseCSV(envString("ANALYTICS_SERVICE_KAFKA_BROKERS", "")),
		KafkaTopic:                        envString("ANALYTICS_SERVICE_KAFKA_TOPIC", defaultKafkaTopic),
		KafkaDeadLetterTopic:              envString("ANALYTICS_SERVICE_KAFKA_DEAD_LETTER_TOPIC", defaultKafkaDeadTopic),
		KafkaClientID:                     envString("ANALYTICS_SERVICE_KAFKA_CLIENT_ID", defaultKafkaClientID),
		KafkaMaxAttempts:                  envInt("ANALYTICS_SERVICE_KAFKA_MAX_ATTEMPTS", 5),
		KafkaRetryBackoff:                 envDuration("ANALYTICS_SERVICE_KAFKA_RETRY_BACKOFF", 250*time.Millisecond),
		KafkaWorkers:                      envInt("ANALYTICS_SERVICE_KAFKA_WORKERS", 100),
		KafkaQueueSize:                    envInt("ANALYTICS_SERVICE_KAFKA_QUEUE_SIZE", 200),
		KafkaCommitInterval:               envDuration("ANALYTICS_SERVICE_KAFKA_COMMIT_INTERVAL", time.Second),
		KafkaTLSEnabled:                   envBool("ANALYTICS_SERVICE_KAFKA_TLS_ENABLED", false),
		KafkaTLSServerName:                envString("ANALYTICS_SERVICE_KAFKA_TLS_SERVER_NAME", ""),
		KafkaTLSCAFile:                    envString("ANALYTICS_SERVICE_KAFKA_TLS_CA_FILE", ""),
		KafkaTLSCertFile:                  envString("ANALYTICS_SERVICE_KAFKA_TLS_CERT_FILE", ""),
		KafkaTLSKeyFile:                   envString("ANALYTICS_SERVICE_KAFKA_TLS_KEY_FILE", ""),
		KafkaTLSInsecureSkipVerify:        envBool("ANALYTICS_SERVICE_KAFKA_TLS_INSECURE_SKIP_VERIFY", false),
		KafkaSASLEnabled:                  envBool("ANALYTICS_SERVICE_KAFKA_SASL_ENABLED", false),
		KafkaSASLMechanism:                strings.ToLower(envString("ANALYTICS_SERVICE_KAFKA_SASL_MECHANISM", "plain")),
		KafkaSASLUsername:                 envString("ANALYTICS_SERVICE_KAFKA_SASL_USERNAME", ""),
		KafkaSASLPassword:                 envString("ANALYTICS_SERVICE_KAFKA_SASL_PASSWORD", ""),
		KafkaSASLHandshake:                envBool("ANALYTICS_SERVICE_KAFKA_SASL_HANDSHAKE", true),
		KafkaDiagnosticsEnabled:           envBool("ANALYTICS_SERVICE_KAFKA_DIAGNOSTICS_ENABLED", false),
		KafkaDiagnosticsPath:              envString("ANALYTICS_SERVICE_KAFKA_DIAGNOSTICS_PATH", defaultKafkaDiagPath),
		IngestionEnabled:                  envBool("ANALYTICS_SERVICE_INGESTION_ENABLED", false),
		WorkerGroup:                       envString("ANALYTICS_SERVICE_WORKER_GROUP", defaultWorkerGroup),
		WorkerConsumer:                    envString("ANALYTICS_SERVICE_WORKER_CONSUMER", defaultWorkerConsumer()),
		MySQLDSN:                          envString("ANALYTICS_SERVICE_MYSQL_DSN", ""),
		MySQLAutoMigrate:                  envBool("ANALYTICS_SERVICE_MYSQL_AUTO_MIGRATE", false),
		ClickHouseAddr:                    envString("ANALYTICS_SERVICE_CLICKHOUSE_ADDR", ""),
		ClickHouseDatabase:                envString("ANALYTICS_SERVICE_CLICKHOUSE_DATABASE", "default"),
		ClickHouseUser:                    envString("ANALYTICS_SERVICE_CLICKHOUSE_USER", "default"),
		ClickHousePassword:                envString("ANALYTICS_SERVICE_CLICKHOUSE_PASSWORD", ""),
		ClickHouseTablePrefix:             envString("ANALYTICS_SERVICE_CLICKHOUSE_TABLE_PREFIX", defaultTablePrefix),
		ClickHouseAutoMigrate:             envBool("ANALYTICS_SERVICE_CLICKHOUSE_AUTO_MIGRATE", false),
		PropertyIndexing:                  envBool("ANALYTICS_SERVICE_PROPERTY_INDEXING", true),
		PropertyCataloging:                envBool("ANALYTICS_SERVICE_PROPERTY_CATALOGING", true),
		SourceResolver:                    strings.ToLower(envString("ANALYTICS_SERVICE_SOURCE_RESOLVER", defaultResolver)),
		ControlPlaneURL:                   envString("ANALYTICS_SERVICE_CONTROL_PLANE_URL", ""),
		ControlPlaneToken:                 envString("ANALYTICS_SERVICE_CONTROL_PLANE_TOKEN", ""),
		ControlPlaneTimeout:               envDuration("ANALYTICS_SERVICE_CONTROL_PLANE_TIMEOUT", 3*time.Second),
		ControlPlaneCacheTTL:              envDuration("ANALYTICS_SERVICE_CONTROL_PLANE_CACHE_TTL", 5*time.Second),
		ControlPlaneAllowInsecureLoopback: envBool("ANALYTICS_SERVICE_CONTROL_PLANE_ALLOW_INSECURE_LOOPBACK", false),
		QueryEnabled:                      envBool("ANALYTICS_SERVICE_QUERY_ENABLED", false),
		QueryToken:                        envString("ANALYTICS_SERVICE_QUERY_TOKEN", ""),
	}
	queryCredentials, err := queryCredentialsFromEnv(config.QueryToken)
	if err != nil {
		return Config{}, err
	}
	config.QueryCredentials = queryCredentials
	config.QueryTokens = queryTokenValues(queryCredentials)

	// Refuse a startup mode that would acknowledge /collect without a durable
	// enqueue path. Local in-memory mode must be explicit in the environment.
	if config.EventBus == "redis" && config.RedisAddr == "" {
		return Config{}, errors.New("ANALYTICS_SERVICE_REDIS_ADDR is required when ANALYTICS_SERVICE_EVENTBUS=redis")
	}
	if config.EventBus == "kafka" && len(config.KafkaBrokers) == 0 {
		return Config{}, errors.New("ANALYTICS_SERVICE_KAFKA_BROKERS is required when ANALYTICS_SERVICE_EVENTBUS=kafka")
	}
	if config.EventBus == "kafka" {
		if err := validateKafkaSecurityConfig(config); err != nil {
			return Config{}, err
		}
	}
	if config.EventBus == "direct" && !config.AllowInMemoryBus {
		return Config{}, errors.New("ANALYTICS_SERVICE_ALLOW_IN_MEMORY_BUS=true is required for direct event bus mode")
	}
	if config.EventBus != "redis" && config.EventBus != "kafka" && config.EventBus != "direct" {
		return Config{}, errors.New("ANALYTICS_SERVICE_EVENTBUS must be redis, kafka, or direct")
	}
	if config.KafkaDiagnosticsEnabled {
		if config.EventBus != "kafka" {
			return Config{}, errors.New("ANALYTICS_SERVICE_KAFKA_DIAGNOSTICS_ENABLED requires ANALYTICS_SERVICE_EVENTBUS=kafka")
		}
		if len(config.QueryCredentials) == 0 {
			return Config{}, errors.New("ANALYTICS_SERVICE_QUERY_TOKEN or ANALYTICS_SERVICE_QUERY_TOKENS_JSON is required when ANALYTICS_SERVICE_KAFKA_DIAGNOSTICS_ENABLED=true")
		}
	}
	if config.SourceResolver != "memory" && config.SourceResolver != "http" {
		return Config{}, errors.New("ANALYTICS_SERVICE_SOURCE_RESOLVER must be memory or http")
	}
	if config.SourceResolver == "http" {
		if config.ControlPlaneURL == "" {
			return Config{}, errors.New("ANALYTICS_SERVICE_CONTROL_PLANE_URL is required when ANALYTICS_SERVICE_SOURCE_RESOLVER=http")
		}
		if config.ControlPlaneToken == "" {
			return Config{}, errors.New("ANALYTICS_SERVICE_CONTROL_PLANE_TOKEN is required when ANALYTICS_SERVICE_SOURCE_RESOLVER=http")
		}
	}
	if config.QueryEnabled {
		if len(config.QueryTokens) == 0 {
			return Config{}, errors.New("ANALYTICS_SERVICE_QUERY_TOKEN or ANALYTICS_SERVICE_QUERY_TOKENS_JSON is required when ANALYTICS_SERVICE_QUERY_ENABLED=true")
		}
		if config.ClickHouseAddr == "" {
			return Config{}, errors.New("ANALYTICS_SERVICE_CLICKHOUSE_ADDR is required when ANALYTICS_SERVICE_QUERY_ENABLED=true")
		}
	}
	if config.IngestionEnabled {
		if config.EventBus == "direct" {
			return Config{}, errors.New("ANALYTICS_SERVICE_INGESTION_ENABLED requires ANALYTICS_SERVICE_EVENTBUS=redis or kafka")
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
	if rawSources != "" {
		if err := json.Unmarshal([]byte(rawSources), &config.Sources); err != nil {
			return Config{}, err
		}
	}
	if config.SourceResolver == "memory" && len(config.Sources) == 0 {
		return Config{}, errors.New("ANALYTICS_SERVICE_SOURCES_JSON is required when ANALYTICS_SERVICE_SOURCE_RESOLVER=memory")
	}
	if config.IngestionEnabled && len(config.Sources) == 0 {
		return Config{}, errors.New("ANALYTICS_SERVICE_SOURCES_JSON is required when ingestion is enabled")
	}
	return config, nil
}

func validateKafkaSecurityConfig(config Config) error {
	// Validate deployment credentials before runtime assembly opens broker
	// connections, so operator mistakes produce env-focused startup errors.
	if !config.KafkaTLSEnabled && hasKafkaTLSMaterial(config) {
		return errors.New("ANALYTICS_SERVICE_KAFKA_TLS_ENABLED=true is required when Kafka TLS settings are provided")
	}
	if config.KafkaTLSCertFile != "" && config.KafkaTLSKeyFile == "" {
		return errors.New("ANALYTICS_SERVICE_KAFKA_TLS_KEY_FILE is required when ANALYTICS_SERVICE_KAFKA_TLS_CERT_FILE is set")
	}
	if config.KafkaTLSKeyFile != "" && config.KafkaTLSCertFile == "" {
		return errors.New("ANALYTICS_SERVICE_KAFKA_TLS_CERT_FILE is required when ANALYTICS_SERVICE_KAFKA_TLS_KEY_FILE is set")
	}
	if !config.KafkaSASLEnabled && hasKafkaSASLMaterial(config) {
		return errors.New("ANALYTICS_SERVICE_KAFKA_SASL_ENABLED=true is required when Kafka SASL settings are provided")
	}
	if config.KafkaSASLEnabled {
		if isUnsupportedKafkaSASLMechanism(config.KafkaSASLMechanism) {
			return errors.New("ANALYTICS_SERVICE_KAFKA_SASL_MECHANISM must be plain in this version")
		}
		if strings.TrimSpace(config.KafkaSASLUsername) == "" {
			return errors.New("ANALYTICS_SERVICE_KAFKA_SASL_USERNAME is required when Kafka SASL is enabled")
		}
		if config.KafkaSASLPassword == "" {
			return errors.New("ANALYTICS_SERVICE_KAFKA_SASL_PASSWORD is required when Kafka SASL is enabled")
		}
	}
	return nil
}

func hasKafkaTLSMaterial(config Config) bool {
	return strings.TrimSpace(config.KafkaTLSServerName) != "" ||
		strings.TrimSpace(config.KafkaTLSCAFile) != "" ||
		strings.TrimSpace(config.KafkaTLSCertFile) != "" ||
		strings.TrimSpace(config.KafkaTLSKeyFile) != "" ||
		config.KafkaTLSInsecureSkipVerify
}

func hasKafkaSASLMaterial(config Config) bool {
	return isUnsupportedKafkaSASLMechanism(config.KafkaSASLMechanism) ||
		strings.TrimSpace(config.KafkaSASLUsername) != "" ||
		config.KafkaSASLPassword != ""
}

func isUnsupportedKafkaSASLMechanism(mechanism string) bool {
	// Mechanism alias normalization is owned by analytics-core's Kafka provider;
	// the service only rejects values that are definitely outside the first
	// supported mechanism family before opening broker connections.
	switch strings.ToLower(strings.TrimSpace(mechanism)) {
	case "", "plain", "plaintext", "sasl_plaintext":
		return false
	default:
		return true
	}
}

func envString(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func queryCredentialsFromEnv(primary string) ([]QueryTokenCredential, error) {
	// Keep the single-token path as the default deployment shape, then merge in
	// an optional JSON rotation list that can also carry activation/expiry
	// metadata for bounded rollover windows.
	credentials := make([]QueryTokenCredential, 0, 2)
	seen := make(map[string]struct{})
	rotationIndex := 0
	appendCredential := func(credential QueryTokenCredential, defaultID string) error {
		credential.Token = strings.TrimSpace(credential.Token)
		if credential.Token == "" {
			return nil
		}
		credential.ID = strings.TrimSpace(credential.ID)
		if credential.ID == "" {
			credential.ID = defaultID
		}
		if !credential.NotBefore.IsZero() && !credential.ExpiresAt.IsZero() && !credential.NotBefore.Before(credential.ExpiresAt) {
			return errors.New("query token not_before must be earlier than expires_at")
		}
		if _, ok := seen[credential.Token]; ok {
			return nil
		}
		seen[credential.Token] = struct{}{}
		credentials = append(credentials, credential)
		return nil
	}

	primaryCredential, err := primaryQueryCredentialFromEnv(primary)
	if err != nil {
		return nil, err
	}
	if err := appendCredential(primaryCredential, "current"); err != nil {
		return nil, err
	}

	raw := strings.TrimSpace(os.Getenv("ANALYTICS_SERVICE_QUERY_TOKENS_JSON"))
	if raw == "" {
		return credentials, nil
	}
	var encoded []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &encoded); err != nil {
		return nil, err
	}
	for _, item := range encoded {
		rotationIndex++
		credential, err := decodeQueryRotationCredential(item)
		if err != nil {
			return nil, err
		}
		if err := appendCredential(credential, "rotation-"+strconv.Itoa(rotationIndex)); err != nil {
			return nil, err
		}
	}
	return credentials, nil
}

func primaryQueryCredentialFromEnv(primary string) (QueryTokenCredential, error) {
	allowedWriteKeys := parseCSV(envString("ANALYTICS_SERVICE_QUERY_TOKEN_WRITE_KEYS", ""))
	notBefore, err := envOptionalTime("ANALYTICS_SERVICE_QUERY_TOKEN_NOT_BEFORE")
	if err != nil {
		return QueryTokenCredential{}, err
	}
	expiresAt, err := envOptionalTime("ANALYTICS_SERVICE_QUERY_TOKEN_EXPIRES_AT")
	if err != nil {
		return QueryTokenCredential{}, err
	}
	scopes, err := parseQueryTokenScopesCSV(envString("ANALYTICS_SERVICE_QUERY_TOKEN_SCOPES", ""))
	if err != nil {
		return QueryTokenCredential{}, err
	}
	return QueryTokenCredential{
		ID:               envString("ANALYTICS_SERVICE_QUERY_TOKEN_ID", ""),
		Token:            primary,
		NotBefore:        notBefore,
		ExpiresAt:        expiresAt,
		Scopes:           scopes,
		AllowedWriteKeys: allowedWriteKeys,
	}, nil
}

func decodeQueryRotationCredential(item json.RawMessage) (QueryTokenCredential, error) {
	if len(item) == 0 {
		return QueryTokenCredential{}, nil
	}
	if item[0] == '"' {
		var token string
		if err := json.Unmarshal(item, &token); err != nil {
			return QueryTokenCredential{}, err
		}
		return QueryTokenCredential{Token: token}, nil
	}
	var encoded struct {
		ID        string   `json:"id"`
		Token     string   `json:"token"`
		NotBefore string   `json:"not_before"`
		ExpiresAt string   `json:"expires_at"`
		Scopes    []string `json:"scopes"`
		WriteKeys []string `json:"write_keys"`
	}
	if err := json.Unmarshal(item, &encoded); err != nil {
		return QueryTokenCredential{}, err
	}
	if strings.TrimSpace(encoded.Token) == "" {
		return QueryTokenCredential{}, errors.New("query token entry token is required")
	}
	notBefore, err := parseOptionalTime(encoded.NotBefore)
	if err != nil {
		return QueryTokenCredential{}, err
	}
	expiresAt, err := parseOptionalTime(encoded.ExpiresAt)
	if err != nil {
		return QueryTokenCredential{}, err
	}
	scopes, err := parseQueryTokenScopesList(encoded.Scopes)
	if err != nil {
		return QueryTokenCredential{}, err
	}
	return QueryTokenCredential{
		ID:               encoded.ID,
		Token:            encoded.Token,
		NotBefore:        notBefore,
		ExpiresAt:        expiresAt,
		Scopes:           scopes,
		AllowedWriteKeys: normalizeStringList(encoded.WriteKeys),
	}, nil
}

func parseQueryTokenScopesCSV(raw string) ([]controlplane.ReadbackRoute, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	return parseQueryTokenScopesList(strings.Split(raw, ","))
}

func parseQueryTokenScopesList(raw []string) ([]controlplane.ReadbackRoute, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	scopes := make([]controlplane.ReadbackRoute, 0, len(raw))
	seen := make(map[controlplane.ReadbackRoute]struct{}, len(raw))
	for _, value := range raw {
		scope, err := parseQueryTokenScope(value)
		if err != nil {
			return nil, err
		}
		if scope == "" {
			continue
		}
		if _, ok := seen[scope]; ok {
			continue
		}
		seen[scope] = struct{}{}
		scopes = append(scopes, scope)
	}
	if len(scopes) == 0 {
		return nil, nil
	}
	return scopes, nil
}

func parseQueryTokenScope(raw string) (controlplane.ReadbackRoute, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	switch controlplane.ReadbackRoute(value) {
	case "":
		return "", nil
	case controlplane.ReadbackRouteRealtime:
		return controlplane.ReadbackRouteRealtime, nil
	case controlplane.ReadbackRouteEvents:
		return controlplane.ReadbackRouteEvents, nil
	case controlplane.ReadbackRouteProperties:
		return controlplane.ReadbackRouteProperties, nil
	case controlplane.ReadbackRouteGoals:
		return controlplane.ReadbackRouteGoals, nil
	case controlplane.ReadbackRouteKafkaDiagnostics:
		return controlplane.ReadbackRouteKafkaDiagnostics, nil
	default:
		return "", errors.New("query token scope must be one of realtime, events, properties, goals, kafka_diagnostics")
	}
}

func queryTokenValues(credentials []QueryTokenCredential) []string {
	values := make([]string, 0, len(credentials))
	for _, credential := range credentials {
		credential.Token = strings.TrimSpace(credential.Token)
		if credential.Token == "" {
			continue
		}
		values = append(values, credential.Token)
	}
	return values
}

func parseCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return normalizeStringList(strings.Split(raw, ","))
}

func normalizeStringList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
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

func envOptionalTime(name string) (time.Time, error) {
	return parseOptionalTime(os.Getenv(name))
}

func parseOptionalTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func defaultWorkerConsumer() string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		return "consumer-1"
	}
	return strings.ToLower(strings.ReplaceAll(hostname, " ", "-"))
}
