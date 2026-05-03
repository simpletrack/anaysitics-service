package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/simpletrack/analytics-core/storage/clickhouse"
	"github.com/simpletrack/analytics-service/internal/config"
	"github.com/simpletrack/analytics-service/internal/controlplane"
)

func TestValidateClickHouseTablesRequiresEventAndPropertyTables(t *testing.T) {
	router, err := clickhouse.NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	source := testRuntimeSource()
	table, err := router.RouteKey(clickhouse.RoutingKey{
		TenantID:  source.TenantID,
		ProjectID: source.ProjectID,
		SourceID:  source.SourceID,
	})
	if err != nil {
		t.Fatalf("route table failed: %v", err)
	}

	conn := &fakeClickHouseTableConn{tables: map[string]uint64{
		table.Physical:                 1,
		table.Physical + "_properties": 1,
	}}
	if err := validateClickHouseTables(context.Background(), config.Config{
		Sources:          []controlplane.SourceConfig{source},
		PropertyIndexing: true,
	}, conn, router); err != nil {
		t.Fatalf("expected table validation to pass: %v", err)
	}
}

func TestValidateClickHouseTablesFailsWhenPropertyTableMissing(t *testing.T) {
	router, err := clickhouse.NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	source := testRuntimeSource()
	table, err := router.RouteKey(clickhouse.RoutingKey{
		TenantID:  source.TenantID,
		ProjectID: source.ProjectID,
		SourceID:  source.SourceID,
	})
	if err != nil {
		t.Fatalf("route table failed: %v", err)
	}

	conn := &fakeClickHouseTableConn{tables: map[string]uint64{
		table.Physical: 1,
	}}
	if err := validateClickHouseTables(context.Background(), config.Config{
		Sources:          []controlplane.SourceConfig{source},
		PropertyIndexing: true,
	}, conn, router); err == nil {
		t.Fatalf("expected missing property table to fail")
	}
}

func TestValidateClickHouseTablesSkipsPropertiesWhenDisabled(t *testing.T) {
	router, err := clickhouse.NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	source := testRuntimeSource()
	table, err := router.RouteKey(clickhouse.RoutingKey{
		TenantID:  source.TenantID,
		ProjectID: source.ProjectID,
		SourceID:  source.SourceID,
	})
	if err != nil {
		t.Fatalf("route table failed: %v", err)
	}

	conn := &fakeClickHouseTableConn{tables: map[string]uint64{
		table.Physical: 1,
	}}
	if err := validateClickHouseTables(context.Background(), config.Config{
		Sources:          []controlplane.SourceConfig{source},
		PropertyIndexing: false,
	}, conn, router); err != nil {
		t.Fatalf("expected table validation to pass without property indexing: %v", err)
	}
}

func testRuntimeSource() controlplane.SourceConfig {
	return controlplane.SourceConfig{
		WriteKey:       "wk_live",
		Enabled:        true,
		TenantID:       "tenant_control",
		ProjectID:      "project_control",
		SourceID:       "source_control",
		SessionSalt:    "server-only-session-salt",
		ClientHashSalt: "server-only-client-salt",
	}
}

type fakeClickHouseTableConn struct {
	tables map[string]uint64 // tables maps physical ClickHouse table names to existence counts
}

func (c *fakeClickHouseTableConn) QueryRow(_ context.Context, _ string, args ...any) driver.Row {
	tableName, ok := args[0].(string)
	if !ok {
		return fakeClickHouseRow{err: errors.New("table name argument is required")}
	}
	return fakeClickHouseRow{count: c.tables[tableName]}
}

type fakeClickHouseRow struct {
	count uint64 // count is the synthetic system.tables match count
	err   error  // err forces Scan to fail
}

func (r fakeClickHouseRow) Err() error {
	return r.err
}

func (r fakeClickHouseRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	value, ok := dest[0].(*uint64)
	if !ok {
		return errors.New("uint64 destination is required")
	}
	*value = r.count
	return nil
}

func (r fakeClickHouseRow) ScanStruct(any) error {
	return errors.New("scan struct is not implemented")
}
