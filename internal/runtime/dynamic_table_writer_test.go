package runtime

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/storage"
	"github.com/simpletrack/analytics-core/storage/clickhouse"
)

func TestDynamicTableEnsuringWriterCreatesRoutedTablesOnce(t *testing.T) {
	router, err := clickhouse.NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}

	conn := &stubClickHouseSchemaConn{}
	inner := &recordingEventWriter{}
	writer, err := NewDynamicTableEnsuringWriter(conn, router, inner, true)
	if err != nil {
		t.Fatalf("new dynamic table writer failed: %v", err)
	}

	envelope := contracts.EventEnvelope{
		ID:        "evt_1",
		TenantID:  "tenant",
		ProjectID: "project",
		SourceID:  "source",
		EventName: "pageview",
	}

	if _, err := writer.WriteEvent(context.Background(), envelope); err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if _, err := writer.WriteEvent(context.Background(), envelope); err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	if inner.calls != 2 {
		t.Fatalf("expected wrapped writer to receive both writes, got %d", inner.calls)
	}
	if len(conn.execs) != 2 {
		t.Fatalf("expected one event ddl and one property ddl, got %#v", conn.execs)
	}
}

type recordingEventWriter struct {
	calls int
}

func (w *recordingEventWriter) WriteEvent(context.Context, contracts.EventEnvelope) (storage.WriteResult, error) {
	w.calls++
	return storage.WriteResult{Inserted: true}, nil
}

type stubClickHouseSchemaConn struct {
	execs []string
}

func (c *stubClickHouseSchemaConn) Exec(_ context.Context, query string, _ ...any) error {
	c.execs = append(c.execs, query)
	return nil
}

func (c *stubClickHouseSchemaConn) QueryRow(_ context.Context, _ string, _ ...any) driver.Row {
	return stubDriverRow{count: 1}
}

type stubDriverRow struct {
	count uint64
}

func (r stubDriverRow) Err() error {
	return nil
}

func (r stubDriverRow) Scan(dest ...any) error {
	if len(dest) != 1 {
		return fmt.Errorf("unexpected scan args %d", len(dest))
	}
	count, ok := dest[0].(*uint64)
	if !ok {
		return fmt.Errorf("unexpected scan target %T", dest[0])
	}
	*count = r.count
	return nil
}

func (r stubDriverRow) ScanStruct(dest any) error {
	count, ok := dest.(*struct {
		Count uint64 `ch:"count"`
	})
	if !ok {
		return fmt.Errorf("unexpected scan struct target %T", dest)
	}
	count.Count = r.count
	return nil
}
