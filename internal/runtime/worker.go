package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-core/ingestion"
	"github.com/simpletrack/analytics-core/storage"
	"github.com/simpletrack/analytics-core/storage/clickhouse"
	"github.com/simpletrack/analytics-core/storage/mysql"
	"github.com/simpletrack/analytics-service/internal/config"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func newIngestionProcessor(ctx context.Context, cfg config.Config, bus eventbus.EventBus, closers []io.Closer) (*ingestion.Processor, []io.Closer, error) {
	// Open checkpoint storage first. MySQL guards are the durable duplicate
	// boundary between at-least-once Redis delivery and ClickHouse appends.
	mysqlDB, mysqlCloser, err := openMySQL(ctx, cfg.MySQLDSN)
	if err != nil {
		return nil, closers, err
	}
	closers = append(closers, mysqlCloser)

	// Open ClickHouse after MySQL is reachable so startup errors point to the
	// missing dependency before a worker can subscribe and receive messages.
	clickConn, err := openClickHouseNative(ctx, cfg)
	if err != nil {
		return nil, closers, err
	}
	closers = append(closers, clickConn)

	writer, err := newEventWriter(ctx, cfg, mysqlDB, clickConn)
	if err != nil {
		return nil, closers, err
	}

	// The worker group is configured at the service layer because it is a
	// deployment concern, while Processor keeps queue semantics inside core.
	processor, err := ingestion.NewProcessor(bus, eventbus.ConsumerGroup{
		Name:     cfg.WorkerGroup,
		Consumer: cfg.WorkerConsumer,
	}, writer)
	if err != nil {
		return nil, closers, err
	}
	return processor, closers, nil
}

func newEventWriter(ctx context.Context, cfg config.Config, mysqlDB *gorm.DB, clickConn driver.Conn) (storage.EventWriter, error) {
	router, err := clickhouse.NewTableRouter(cfg.ClickHouseTablePrefix)
	if err != nil {
		return nil, err
	}
	if err := ensureClickHouseTables(ctx, cfg, clickConn, router); err != nil {
		return nil, err
	}

	eventGuard, err := mysql.NewIngestionStatusGuard(mysqlDB)
	if err != nil {
		return nil, err
	}
	if cfg.MySQLAutoMigrate {
		// Status migrations are opt-in because production schema lifecycle stays
		// outside the hot runtime. Local deployments can enable this switch.
		if err := eventGuard.AutoMigrate(ctx); err != nil {
			return nil, err
		}
	}

	eventWriter, err := clickhouse.NewBatchWriter(clickConn, router, clickhouse.WithEventWriteGuard(eventGuard))
	if err != nil {
		return nil, err
	}
	if !cfg.PropertyIndexing {
		return eventWriter, nil
	}

	propertyGuard, err := mysql.NewPropertyIndexingStatusGuard(mysqlDB)
	if err != nil {
		return nil, err
	}
	if cfg.MySQLAutoMigrate {
		// Property checkpoints are kept separate from event checkpoints because
		// property indexing can fail after the primary event row exists.
		if err := propertyGuard.AutoMigrate(ctx); err != nil {
			return nil, err
		}
	}
	propertyWriter, err := clickhouse.NewPropertyBatchWriter(clickConn, router)
	if err != nil {
		return nil, err
	}
	return storage.NewPropertyIndexingEventWriter(eventWriter, propertyWriter, propertyGuard)
}

// ensureClickHouseTables applies optional local DDL before the fail-closed schema check.
func ensureClickHouseTables(ctx context.Context, cfg config.Config, conn clickHouseSchemaConn, router *clickhouse.TableRouter) error {
	if cfg.ClickHouseAutoMigrate {
		// Auto migration is intentionally limited to the routed data tables
		// required by enabled sources. Control-plane lifecycle and production
		// schema reviews remain outside this runtime service.
		if err := createClickHouseTables(ctx, cfg, conn, router); err != nil {
			return err
		}
	}
	// Always validate after optional creation so startup remains fail-closed
	// when DDL permissions, table names, or property-table creation drift.
	return validateClickHouseTables(ctx, cfg, conn, router)
}

// createClickHouseTables creates only the routed ClickHouse tables required by enabled sources.
func createClickHouseTables(ctx context.Context, cfg config.Config, conn clickHouseSchemaConn, router *clickhouse.TableRouter) error {
	for _, source := range cfg.Sources {
		source = source.Normalize()
		if !source.Enabled {
			continue
		}
		table, err := router.RouteKey(clickhouse.RoutingKey{
			TenantID:  source.TenantID,
			ProjectID: source.ProjectID,
			SourceID:  source.SourceID,
		})
		if err != nil {
			return err
		}

		// Create the primary event table before the optional property table so
		// startup fails at the earliest missing write surface.
		eventDDL, err := clickhouse.CreateEventTableStatement(table)
		if err != nil {
			return err
		}
		if err := conn.Exec(ctx, eventDDL); err != nil {
			return err
		}
		if cfg.PropertyIndexing {
			propertyDDL, err := clickhouse.CreatePropertyTableStatement(table)
			if err != nil {
				return err
			}
			if err := conn.Exec(ctx, propertyDDL); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateClickHouseTables(ctx context.Context, cfg config.Config, conn clickHouseTableQuerier, router *clickhouse.TableRouter) error {
	// Ingestion must fail closed when storage schema is missing. Otherwise
	// /collect can return 202 while Redis messages are repeatedly nacked and
	// eventually dead-lettered because ClickHouse tables were never created.
	for _, source := range cfg.Sources {
		source = source.Normalize()
		if !source.Enabled {
			continue
		}
		table, err := router.RouteKey(clickhouse.RoutingKey{
			TenantID:  source.TenantID,
			ProjectID: source.ProjectID,
			SourceID:  source.SourceID,
		})
		if err != nil {
			return err
		}
		if err := requireClickHouseTable(ctx, conn, table.Physical); err != nil {
			return err
		}
		if cfg.PropertyIndexing {
			propertyTable, err := clickhouse.PropertyTableFor(table)
			if err != nil {
				return err
			}
			if err := requireClickHouseTable(ctx, conn, propertyTable.Physical); err != nil {
				return err
			}
		}
	}
	return nil
}

// clickHouseSchemaConn is the narrow startup DDL and validation surface.
type clickHouseSchemaConn interface {
	clickHouseTableQuerier
	// Exec runs startup DDL when local ClickHouse auto migration is enabled.
	Exec(context.Context, string, ...any) error
}

type clickHouseTableQuerier interface {
	// QueryRow returns one ClickHouse row for startup schema checks.
	QueryRow(context.Context, string, ...any) driver.Row
}

func requireClickHouseTable(ctx context.Context, conn clickHouseTableQuerier, tableName string) error {
	var count uint64
	row := conn.QueryRow(ctx, `
SELECT count()
FROM system.tables
WHERE database = currentDatabase() AND name = ?
`, tableName)
	if err := row.Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("clickhouse table %q is required when ingestion is enabled", tableName)
	}
	return nil
}

func openMySQL(ctx context.Context, dsn string) (*gorm.DB, io.Closer, error) {
	// Probe with database/sql before building the GORM handle. This gives a
	// clear readiness failure and avoids partially assembled ingestion workers.
	probe, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, nil, err
	}
	if err := probe.PingContext(ctx); err != nil {
		_ = probe.Close()
		return nil, nil, err
	}
	_ = probe.Close()

	db, err := gorm.Open(gormmysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}
	return db, sqlDB, nil
}

func openClickHouseNative(ctx context.Context, cfg config.Config) (driver.Conn, error) {
	// Open the native protocol connection used by analytics-core batch writers.
	// Query-only GORM connections are intentionally not part of this writer path.
	conn, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{cfg.ClickHouseAddr},
		Auth: clickhousedriver.Auth{
			Database: cfg.ClickHouseDatabase,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePassword,
		},
	})
	if err != nil {
		return nil, err
	}
	// Ping before returning so missing credentials or half-ready ClickHouse
	// startup states fail before Redis consumers can receive events.
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}
