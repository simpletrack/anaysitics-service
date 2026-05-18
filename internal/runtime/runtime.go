package runtime

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
	"github.com/simpletrack/analytics-core/eventbus"
	kafkaeventbus "github.com/simpletrack/analytics-core/eventbus/kafka"
	"github.com/simpletrack/analytics-core/eventbus/redisstream"
	"github.com/simpletrack/analytics-core/ingestion"
	"github.com/simpletrack/analytics-core/storage"
	"github.com/simpletrack/analytics-core/storage/clickhouse"
	"github.com/simpletrack/analytics-core/storage/mysql"
	"github.com/simpletrack/analytics-service/internal/collectapi"
	"github.com/simpletrack/analytics-service/internal/config"
	"github.com/simpletrack/analytics-service/internal/controlplane"
	"github.com/simpletrack/analytics-service/internal/geoip"
	gormclickhouse "gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

// Runtime owns the assembled analytics data-plane dependencies.
type Runtime struct {
	app       *fiber.App           // app serves health, tracker, collect, query, and documentation routes
	processor *ingestion.Processor // processor consumes configured queue messages when ingestion is enabled
	kafkaBus  *kafkaeventbus.Bus   // kafkaBus exposes provider diagnostics when Kafka is configured
	closers   []io.Closer          // closers release network clients opened by the runtime assembly
	worker    chan error           // worker receives the optional ingestion worker terminal error
}

// New assembles the runtime without starting background workers.
func New(cfg config.Config) (*Runtime, error) {
	closers := make([]io.Closer, 0, 4)

	// Build the runtime-only view of SaaS source configuration. This resolver
	// reads config but does not own any CRUD lifecycle.
	resolver, err := newSourceResolver(cfg)
	if err != nil {
		return nil, err
	}
	geoResolver, geoCloser, err := newGeoResolver(cfg)
	if err != nil {
		_ = closeAll(closers)
		return nil, err
	}
	if geoCloser != nil {
		closers = append(closers, geoCloser)
	}

	// Build the event bus before exposing HTTP routes. Redis remains the local
	// default, while Kafka is the production EventBus provider.
	bus, busClosers, err := newEventBus(cfg)
	if err != nil {
		_ = closeAll(closers)
		return nil, err
	}
	closers = append(closers, busClosers...)
	kafkaBus := kafkaBusFromEventBus(bus)
	kafkaDiagnostics, err := newKafkaDiagnosticsSource(cfg, kafkaBus)
	if err != nil {
		_ = closeAll(closers)
		return nil, err
	}
	kafkaMetrics, err := newKafkaMetricsSource(cfg, kafkaBus)
	if err != nil {
		_ = closeAll(closers)
		return nil, err
	}

	// Load the tracker asset at startup so missing deployments fail closed
	// instead of returning 404s for a configured public SDK route.
	tracker, err := os.ReadFile(cfg.TrackerFile)
	if err != nil {
		_ = closeAll(closers)
		return nil, err
	}

	// Build the internal query reader only when trusted server-side readback is
	// configured to serve Events / Realtime queries over ClickHouse.
	queryReader, queryClosers, err := newQueryReader(cfg)
	if err != nil {
		_ = closeAll(closers)
		return nil, err
	}
	closers = append(closers, queryClosers...)

	// Build the optional property catalog reader from MySQL. This powers
	// source-scoped filter suggestions without exposing MySQL row models to the
	// HTTP layer.
	propertyCatalog, catalogClosers, err := newPropertyCatalogReader(cfg)
	if err != nil {
		_ = closeAll(closers)
		return nil, err
	}
	closers = append(closers, catalogClosers...)

	// Wire HTTP to runtime enforcement and analytics-core collect handling. The
	// app remains unaware of SaaS control-plane CRUD and storage adapters.
	app, err := collectapi.NewApp(collectapi.Options{
		CollectPath:           cfg.CollectPath,
		HealthPath:            cfg.HealthPath,
		TrackerPath:           cfg.TrackerPath,
		EventsPath:            cfg.EventsPath,
		RealtimePath:          cfg.RealtimePath,
		PropertiesPath:        cfg.PropertiesPath,
		KafkaDiagnosticsPath:  cfg.KafkaDiagnosticsPath,
		SwaggerEnabled:        cfg.SwaggerEnabled,
		SwaggerPath:           cfg.SwaggerPath,
		OpenAPIFile:           cfg.OpenAPIFile,
		TrustForwardedHeaders: cfg.TrustForwardedHeaders,
		TrackerScript:         tracker,
		Resolver:              resolver,
		Bus:                   bus,
		QueryReader:           queryReader,
		GoalsPath:             cfg.GoalsPath,
		PropertyCatalog:       propertyCatalog,
		QueryToken:            cfg.QueryToken,
		QueryTokens:           cfg.QueryTokens,
		QueryCredentials:      toQueryCredentials(cfg.QueryCredentials),
		KafkaDiagnostics:      kafkaDiagnostics,
		KafkaMetricsPath:      cfg.KafkaMetricsPath,
		KafkaMetrics:          kafkaMetrics,
		GeoResolver:           geoResolver,
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
		app:       app,
		processor: processor,
		kafkaBus:  kafkaBus,
		closers:   closers,
	}, nil
}

// newKafkaDiagnosticsSource adapts the Kafka provider diagnostic surface to collectapi.
func newKafkaDiagnosticsSource(cfg config.Config, kafkaBus *kafkaeventbus.Bus) (collectapi.KafkaDiagnosticsSource, error) {
	if !cfg.KafkaDiagnosticsEnabled {
		return nil, nil
	}
	if kafkaBus == nil {
		return nil, configError("kafka diagnostics require a Kafka event bus")
	}
	return func() (collectapi.KafkaDiagnosticsResponse, bool) {
		// Convert the provider snapshot at the runtime boundary so collectapi never
		// imports analytics-core's Kafka package or any Sarama-facing adapter type.
		return kafkaDiagnosticsResponseFromStats(kafkaBus.Stats()), true
	}, nil
}

// newKafkaMetricsSource adapts the Kafka provider stats surface to collectapi metrics.
func newKafkaMetricsSource(cfg config.Config, kafkaBus *kafkaeventbus.Bus) (collectapi.KafkaMetricsSource, error) {
	if !cfg.KafkaMetricsEnabled {
		return nil, nil
	}
	if kafkaBus == nil {
		return nil, configError("kafka metrics require a Kafka event bus")
	}
	return func() (collectapi.KafkaDiagnosticsResponse, bool) {
		// Reuse the same sanitized DTO as diagnostics so metrics export cannot gain
		// broker secrets, Sarama internals, or raw event payloads by bypassing HTTP.
		return kafkaDiagnosticsResponseFromStats(kafkaBus.Stats()), true
	}, nil
}

// kafkaDiagnosticsResponseFromStats maps provider stats to the HTTP response contract.
func kafkaDiagnosticsResponseFromStats(stats kafkaeventbus.Stats) collectapi.KafkaDiagnosticsResponse {
	commits := make([]collectapi.KafkaOrderedCommitDiagnostics, 0, len(stats.Commits))
	for _, commit := range stats.Commits {
		commits = append(commits, collectapi.KafkaOrderedCommitDiagnostics{
			Topic:               commit.Topic,
			Partition:           commit.Partition,
			Initialized:         commit.Initialized,
			NextOffset:          commit.NextOffset,
			HighWaterMarkOffset: commit.HighWaterMarkOffset,
			LagEstimate:         commit.Lag,
			PendingCount:        commit.PendingCount,
			DoneCount:           commit.DoneCount,
			OldestPendingOffset: commit.OldestPendingOffset,
			LargestPendingGap:   commit.LargestPendingGap,
		})
	}

	return collectapi.KafkaDiagnosticsResponse{
		Topic:           stats.Topic,
		DeadLetterTopic: stats.DeadLetterTopic,
		WorkerPool: collectapi.KafkaWorkerPoolDiagnostics{
			Name:            stats.WorkerPool.Name,
			GoroutinesTotal: stats.WorkerPool.GoroutinesTotal,
			Queued:          stats.WorkerPool.Queued,
			QueueCapacity:   stats.WorkerPool.QueueCapacity,
			QueueUsageRatio: stats.WorkerPool.QueueUsageRatio,
			Workers:         stats.WorkerPool.Workers,
			SubmittedTotal:  stats.WorkerPool.SubmittedTotal,
			CompletedTotal:  stats.WorkerPool.CompletedTotal,
			RejectedTotal:   stats.WorkerPool.RejectedTotal,
			Closed:          stats.WorkerPool.Closed,
		},
		CompletionGate: collectapi.KafkaCompletionGateDiagnostics{
			InFlightMessages:  stats.CompletionGate.InFlightMessages,
			WaitingTasks:      stats.CompletionGate.WaitingTasks,
			CompletedMessages: stats.CompletionGate.CompletedMessages,
		},
		Commits: commits,
		Paused:  clonePausedPartitions(stats.Paused),
		Metrics: collectapi.KafkaMetricsDiagnostics{
			ConsumedTotal:          stats.Metrics.ConsumedTotal,
			HandlerSuccessTotal:    stats.Metrics.HandlerSuccessTotal,
			HandlerFailureTotal:    stats.Metrics.HandlerFailureTotal,
			HandlerRetryTotal:      stats.Metrics.HandlerRetryTotal,
			MalformedTotal:         stats.Metrics.MalformedTotal,
			DeadLetterSuccessTotal: stats.Metrics.DeadLetterSuccessTotal,
			DeadLetterFailureTotal: stats.Metrics.DeadLetterFailureTotal,
			PausedPartitions:       stats.Metrics.PausedPartitions,
			PauseTransitionsTotal:  stats.Metrics.PauseTransitionsTotal,
			ResumeTransitionsTotal: stats.Metrics.ResumeTransitionsTotal,
		},
	}
}

// clonePausedPartitions copies provider-owned pause state before JSON response shaping.
func clonePausedPartitions(paused map[string][]int32) map[string][]int32 {
	out := make(map[string][]int32, len(paused))
	if len(paused) == 0 {
		return out
	}
	for topic, partitions := range paused {
		out[topic] = append([]int32(nil), partitions...)
	}
	return out
}

func newGeoResolver(cfg config.Config) (*geoip.Resolver, io.Closer, error) {
	if cfg.GeoIPMMDBFile == "" {
		return nil, nil, nil
	}
	resolver, err := geoip.NewResolver(cfg.GeoIPMMDBFile)
	if err != nil {
		return nil, nil, err
	}
	return resolver, resolver, nil
}

func newPropertyCatalogReader(cfg config.Config) (storage.PropertyCatalogReader, []io.Closer, error) {
	if !cfg.QueryEnabled || cfg.MySQLDSN == "" {
		return nil, nil, nil
	}

	// Property catalog readback is metadata, but it still uses the same MySQL
	// initialization guard as ingestion cataloging. Readback is intentionally
	// independent of the write-side PropertyCataloging flag so an existing
	// catalog remains queryable even when this process is not updating it.
	startupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mysqlDB, mysqlCloser, err := openMySQL(startupCtx, cfg.MySQLDSN)
	if err != nil {
		return nil, nil, err
	}
	catalog, err := mysql.NewPropertyCatalog(mysqlDB)
	if err != nil {
		_ = mysqlCloser.Close()
		return nil, nil, err
	}
	if cfg.MySQLAutoMigrate {
		if err := catalog.AutoMigrate(startupCtx); err != nil {
			_ = mysqlCloser.Close()
			return nil, nil, err
		}
	} else if err := requireMySQLTable(mysqlDB.WithContext(startupCtx).Migrator(), &mysql.PropertyCatalogEntry{}, "property_catalog"); err != nil {
		_ = mysqlCloser.Close()
		return nil, nil, err
	}
	return catalog, []io.Closer{mysqlCloser}, nil
}

// App returns the Fiber app owned by the runtime.
func (r *Runtime) App() *fiber.App {
	return r.app
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

// KafkaEventBusStats returns Kafka provider diagnostics when Kafka is configured.
func (r *Runtime) KafkaEventBusStats() (kafkaeventbus.Stats, bool) {
	if r == nil || r.kafkaBus == nil {
		return kafkaeventbus.Stats{}, false
	}
	return r.kafkaBus.Stats(), true
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
	case "kafka":
		return newKafkaBus(cfg)
	case "direct":
		return newMemoryBus(), nil, nil
	default:
		return nil, nil, configError("unsupported event bus")
	}
}

// kafkaBusFromEventBus narrows the durable bus to the Kafka diagnostic surface.
func kafkaBusFromEventBus(bus eventbus.EventBus) *kafkaeventbus.Bus {
	kafkaBus, _ := bus.(*kafkaeventbus.Bus)
	return kafkaBus
}

func newQueryReader(cfg config.Config) (storage.EventReader, []io.Closer, error) {
	if !cfg.QueryEnabled {
		return nil, nil, nil
	}

	// Open a ClickHouse read connection only when the runtime is expected to
	// serve trusted Events and Realtime readback.
	startupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	queryDB, queryCloser, err := openClickHouseQuery(startupCtx, cfg)
	if err != nil {
		return nil, nil, err
	}

	router, err := clickhouse.NewTableRouter(cfg.ClickHouseTablePrefix)
	if err != nil {
		_ = queryCloser.Close()
		return nil, nil, err
	}
	builderOptions := queryBuilderOptions(cfg)
	builder, err := clickhouse.NewEventQueryBuilder(router, builderOptions...)
	if err != nil {
		_ = queryCloser.Close()
		return nil, nil, err
	}
	reader, err := clickhouse.NewEventReader(queryDB, builder)
	if err != nil {
		_ = queryCloser.Close()
		return nil, nil, err
	}
	return reader, []io.Closer{queryCloser}, nil
}

func queryBuilderOptions(cfg config.Config) []clickhouse.EventQueryBuilderOption {
	selectors := allowedPropertySelectors(cfg.Sources)
	if len(selectors) == 0 {
		return nil
	}
	return []clickhouse.EventQueryBuilderOption{
		clickhouse.WithAllowedPropertyFilters(selectors...),
	}
}

func toQueryCredentials(credentials []config.QueryTokenCredential) []collectapi.QueryCredential {
	if len(credentials) == 0 {
		return nil
	}
	out := make([]collectapi.QueryCredential, 0, len(credentials))
	for _, credential := range credentials {
		out = append(out, collectapi.QueryCredential{
			ID:               credential.ID,
			Token:            credential.Token,
			NotBefore:        credential.NotBefore,
			ExpiresAt:        credential.ExpiresAt,
			Scopes:           append([]controlplane.ReadbackRoute(nil), credential.Scopes...),
			AllowedWriteKeys: append([]string(nil), credential.AllowedWriteKeys...),
		})
	}
	return out
}

func allowedPropertySelectors(sources []controlplane.SourceConfig) []storage.PropertySelector {
	selectors := make([]storage.PropertySelector, 0)
	seen := make(map[storage.PropertySelector]struct{})
	for _, source := range sources {
		// Source config is the SaaS-owned query whitelist. The runtime only
		// passes the selector surface to analytics-core for query-builder
		// enforcement; value-type checks stay source-scoped in the HTTP layer.
		source = source.Normalize()
		if !source.Enabled {
			continue
		}
		for _, filter := range source.AllowedPropertyFilters {
			selector := storage.PropertySelector{
				Scope: storage.PropertyScope(filter.Scope),
				Name:  filter.Name,
			}
			if _, ok := seen[selector]; ok {
				continue
			}
			seen[selector] = struct{}{}
			selectors = append(selectors, selector)
		}
	}
	return selectors
}

func openClickHouseQuery(ctx context.Context, cfg config.Config) (*gorm.DB, io.Closer, error) {
	db, err := gorm.Open(gormclickhouse.New(gormclickhouse.Config{
		DSN:                       clickHouseQueryDSN(cfg),
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		return nil, nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, nil, err
	}
	return db, sqlDB, nil
}

func clickHouseQueryDSN(cfg config.Config) string {
	return "clickhouse://" + cfg.ClickHouseUser + ":" + cfg.ClickHousePassword + "@" + cfg.ClickHouseAddr + "/" + cfg.ClickHouseDatabase + "?dial_timeout=10s&read_timeout=20s"
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

// newSourceResolver builds the write-key lookup boundary used by /collect.
//
// A public write key is only a lookup credential. The returned resolver turns it
// into a trusted SourceConfig owned by the control plane, including tenant,
// project, source, origin, filtering, and privacy-salt settings.
func newSourceResolver(cfg config.Config) (controlplane.Resolver, error) {
	// Select the backing source of truth before HTTP routes are exposed. Memory
	// mode indexes the startup JSON source list for tests and local demos; HTTP
	// mode calls the SaaS control plane at request time and keeps source CRUD
	// outside this runtime service.
	var resolver controlplane.Resolver
	var err error
	switch cfg.SourceResolver {
	case "", "memory":
		// Memory mode still validates and normalizes every SourceConfig up front,
		// so a bad tenant/project/source mapping fails startup instead of
		// accepting events under the wrong analytics boundary.
		resolver, err = controlplane.NewMemoryResolver(cfg.Sources)
	case "http":
		// HTTP mode sends only the presented write key to the control plane. The
		// trusted response supplies the scope and runtime policy used later by
		// handleCollect to override client-supplied scope fields.
		resolver, err = controlplane.NewHTTPResolver(controlplane.HTTPResolverOptions{
			Endpoint:              cfg.ControlPlaneURL,
			BearerToken:           cfg.ControlPlaneToken,
			Timeout:               cfg.ControlPlaneTimeout,
			CacheTTL:              cfg.ControlPlaneCacheTTL,
			AllowInsecureLoopback: cfg.ControlPlaneAllowInsecureLoopback,
			AllowInsecurePrivateNetwork: cfg.ControlPlaneAllowInsecurePrivateNetwork,
		})
	default:
		return nil, configError("unsupported source resolver")
	}
	if err != nil {
		return nil, err
	}
	if cfg.IngestionEnabled && cfg.SourceResolver == "http" && len(cfg.Sources) > 0 {
		// Same-process ingestion validates ClickHouse tables only for the startup
		// source list. Bind dynamic HTTP responses to that surface so a freshly
		// created control-plane source cannot be accepted until its routed tables
		// have been included in startup schema validation.
		return controlplane.NewSchemaBoundResolver(resolver, cfg.Sources)
	}
	return resolver, nil
}

type configError string

func (e configError) Error() string {
	return string(e)
}
