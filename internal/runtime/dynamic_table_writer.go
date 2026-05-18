package runtime

import (
	"context"
	"fmt"
	"sync"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/storage"
	"github.com/simpletrack/analytics-core/storage/clickhouse"
)

// DynamicTableEnsuringWriter ensures routed ClickHouse tables exist on first write.
//
// Same-process ingestion under HTTP source resolution cannot rely on a
// boot-time static source list. This wrapper moves the routed-table check to
// the first write per routing key so Docker/local deployments can start before
// the first website source exists in the control plane.
type DynamicTableEnsuringWriter struct {
	conn             clickHouseSchemaConn  // conn applies and validates ClickHouse DDL on demand
	inner            storage.EventWriter   // inner performs the actual event and property writes
	propertyIndexing bool                  // propertyIndexing mirrors the runtime writer shape
	router           *clickhouse.TableRouter // router maps tenant/project/source to the routed table family

	mu       sync.Mutex
	ensured  map[string]struct{}
}

// NewDynamicTableEnsuringWriter wraps inner with first-write routed-table checks.
func NewDynamicTableEnsuringWriter(
	conn clickHouseSchemaConn,
	router *clickhouse.TableRouter,
	inner storage.EventWriter,
	propertyIndexing bool,
) (*DynamicTableEnsuringWriter, error) {
	if conn == nil {
		return nil, fmt.Errorf("clickhouse schema connection is required")
	}
	if router == nil {
		return nil, fmt.Errorf("clickhouse table router is required")
	}
	if inner == nil {
		return nil, fmt.Errorf("event writer is required")
	}
	return &DynamicTableEnsuringWriter{
		conn:             conn,
		inner:            inner,
		propertyIndexing: propertyIndexing,
		router:           router,
		ensured:          make(map[string]struct{}),
	}, nil
}

// WriteEvent ensures the routed tables exist before delegating to inner.
func (w *DynamicTableEnsuringWriter) WriteEvent(ctx context.Context, envelope contracts.EventEnvelope) (storage.WriteResult, error) {
	if err := w.ensureTablesForEnvelope(ctx, envelope); err != nil {
		return storage.WriteResult{}, err
	}
	return w.inner.WriteEvent(ctx, envelope)
}

func (w *DynamicTableEnsuringWriter) ensureTablesForEnvelope(ctx context.Context, envelope contracts.EventEnvelope) error {
	table, err := w.router.Route(envelope)
	if err != nil {
		return err
	}

	if w.isEnsured(table.Physical) {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.ensured[table.Physical]; ok {
		return nil
	}

	eventDDL, err := clickhouse.CreateEventTableStatement(table)
	if err != nil {
		return err
	}
	if err := w.conn.Exec(ctx, eventDDL); err != nil {
		return err
	}
	if err := requireClickHouseTable(ctx, w.conn, table.Physical); err != nil {
		return err
	}

	if w.propertyIndexing {
		propertyDDL, err := clickhouse.CreatePropertyTableStatement(table)
		if err != nil {
			return err
		}
		if err := w.conn.Exec(ctx, propertyDDL); err != nil {
			return err
		}
		propertyTable, err := clickhouse.PropertyTableFor(table)
		if err != nil {
			return err
		}
		if err := requireClickHouseTable(ctx, w.conn, propertyTable.Physical); err != nil {
			return err
		}
	}

	w.ensured[table.Physical] = struct{}{}
	return nil
}

func (w *DynamicTableEnsuringWriter) isEnsured(tableName string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.ensured[tableName]
	return ok
}
