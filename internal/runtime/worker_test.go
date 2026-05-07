package runtime

import (
	"context"
	"errors"
	"strings"
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

	propertyTable, err := clickhouse.PropertyTableFor(table)
	if err != nil {
		t.Fatalf("route property table failed: %v", err)
	}
	conn := &fakeClickHouseTableConn{tables: map[string]uint64{
		table.Physical:         1,
		propertyTable.Physical: 1,
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

func TestEnsureClickHouseTablesCreatesEventAndPropertyTablesWhenEnabled(t *testing.T) {
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

	conn := &fakeClickHouseTableConn{}
	if err := ensureClickHouseTables(context.Background(), config.Config{
		Sources:               []controlplane.SourceConfig{source},
		PropertyIndexing:      true,
		ClickHouseAutoMigrate: true,
	}, conn, router); err != nil {
		t.Fatalf("expected auto migration to create required tables: %v", err)
	}
	if conn.tables[table.Physical] != 1 {
		t.Fatalf("expected event table %q to be created; tables=%#v", table.Physical, conn.tables)
	}
	propertyTable, err := clickhouse.PropertyTableFor(table)
	if err != nil {
		t.Fatalf("route property table failed: %v", err)
	}
	if conn.tables[propertyTable.Physical] != 1 {
		t.Fatalf("expected property table %q to be created; tables=%#v", propertyTable.Physical, conn.tables)
	}
	if len(conn.execs) != 2 {
		t.Fatalf("expected 2 DDL statements, got %d: %#v", len(conn.execs), conn.execs)
	}
	if !strings.Contains(conn.execs[0], "ORDER BY (tenant_id, project_id, source_id, event_time, visit_id, event_id)") {
		t.Fatalf("event DDL did not include expected ordering: %s", conn.execs[0])
	}
}

func TestEnsureClickHouseTablesSkipsPropertyDDLWhenIndexingDisabled(t *testing.T) {
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

	conn := &fakeClickHouseTableConn{}
	if err := ensureClickHouseTables(context.Background(), config.Config{
		Sources:               []controlplane.SourceConfig{source},
		PropertyIndexing:      false,
		ClickHouseAutoMigrate: true,
	}, conn, router); err != nil {
		t.Fatalf("expected auto migration without property indexing to pass: %v", err)
	}
	if conn.tables[table.Physical] != 1 {
		t.Fatalf("expected event table %q to be created; tables=%#v", table.Physical, conn.tables)
	}
	propertyTable, err := clickhouse.PropertyTableFor(table)
	if err != nil {
		t.Fatalf("route property table failed: %v", err)
	}
	if _, ok := conn.tables[propertyTable.Physical]; ok {
		t.Fatalf("property table should not be created when property indexing is disabled")
	}
	if len(conn.execs) != 1 {
		t.Fatalf("expected 1 DDL statement, got %d: %#v", len(conn.execs), conn.execs)
	}
}

func TestEnsureClickHouseTablesFailsClosedWithoutAutoMigration(t *testing.T) {
	router, err := clickhouse.NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	source := testRuntimeSource()

	conn := &fakeClickHouseTableConn{}
	if err := ensureClickHouseTables(context.Background(), config.Config{
		Sources:               []controlplane.SourceConfig{source},
		PropertyIndexing:      false,
		ClickHouseAutoMigrate: false,
	}, conn, router); err == nil {
		t.Fatalf("expected missing event table to fail when auto migration is disabled")
	}
	if len(conn.execs) != 0 {
		t.Fatalf("auto migration disabled should not execute DDL: %#v", conn.execs)
	}
}

func TestEnsureClickHouseTablesCreatesAllEnabledRuntimeSources(t *testing.T) {
	router, err := clickhouse.NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	enabledA := testRuntimeSource()
	enabledB := testRuntimeSource()
	enabledB.SourceID = "source_control_two"
	disabled := testRuntimeSource()
	disabled.SourceID = "source_disabled"
	disabled.Enabled = false

	conn := &fakeClickHouseTableConn{}
	if err := ensureClickHouseTables(context.Background(), config.Config{
		Sources:               []controlplane.SourceConfig{enabledA, enabledB, disabled},
		PropertyIndexing:      false,
		ClickHouseAutoMigrate: true,
	}, conn, router); err != nil {
		t.Fatalf("expected auto migration to create all enabled source tables: %v", err)
	}

	for _, source := range []controlplane.SourceConfig{enabledA, enabledB} {
		table, err := router.RouteKey(clickhouse.RoutingKey{
			TenantID:  source.TenantID,
			ProjectID: source.ProjectID,
			SourceID:  source.SourceID,
		})
		if err != nil {
			t.Fatalf("route table failed: %v", err)
		}
		if conn.tables[table.Physical] != 1 {
			t.Fatalf("expected enabled source table %q to be created; tables=%#v", table.Physical, conn.tables)
		}
	}
	disabledTable, err := router.RouteKey(clickhouse.RoutingKey{
		TenantID:  disabled.TenantID,
		ProjectID: disabled.ProjectID,
		SourceID:  disabled.SourceID,
	})
	if err != nil {
		t.Fatalf("route disabled table failed: %v", err)
	}
	if _, ok := conn.tables[disabledTable.Physical]; ok {
		t.Fatalf("disabled source table %q should not be created; tables=%#v", disabledTable.Physical, conn.tables)
	}
	if len(conn.execs) != 2 {
		t.Fatalf("expected one DDL statement per enabled source, got %d: %#v", len(conn.execs), conn.execs)
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
		VisitSalt:      "server-only-visit-salt",
		ClientHashSalt: "server-only-client-salt",
	}
}

type fakeClickHouseTableConn struct {
	tables map[string]uint64 // tables maps physical ClickHouse table names to existence counts
	execs  []string          // execs records startup DDL executed by auto migration tests
}

func (c *fakeClickHouseTableConn) QueryRow(_ context.Context, _ string, args ...any) driver.Row {
	tableName, ok := args[0].(string)
	if !ok {
		return fakeClickHouseRow{err: errors.New("table name argument is required")}
	}
	return fakeClickHouseRow{count: c.tables[tableName]}
}

func (c *fakeClickHouseTableConn) Exec(_ context.Context, query string, _ ...any) error {
	if c.tables == nil {
		c.tables = make(map[string]uint64)
	}
	c.execs = append(c.execs, query)
	tableName, ok := extractCreatedTableName(query)
	if !ok {
		return errors.New("create table statement must contain a quoted table name")
	}
	c.tables[tableName] = 1
	return nil
}

func extractCreatedTableName(query string) (string, bool) {
	start := strings.Index(query, "`")
	if start < 0 {
		return "", false
	}
	end := strings.Index(query[start+1:], "`")
	if end < 0 {
		return "", false
	}
	return query[start+1 : start+1+end], true
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
