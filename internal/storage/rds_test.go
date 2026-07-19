package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeDB is a minimal database/sql driver that records every Exec and
// serves canned Query results, so the RDS backend is testable without a
// PostgreSQL server.
type fakeDB struct {
	mu    sync.Mutex
	execs []recordedExec
	rows  [][]driver.Value // served to every Query
	cols  []string
}

type recordedExec struct {
	query string
	args  []driver.Value
}

func (f *fakeDB) recordedQueries() []recordedExec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedExec(nil), f.execs...)
}

type fakeConn struct{ db *fakeDB }

func (f *fakeDB) Open(string) (driver.Conn, error) { return &fakeConn{db: f}, nil }

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{conn: c, query: query}, nil
}
func (c *fakeConn) Close() error              { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("not supported") }

type fakeStmt struct {
	conn  *fakeConn
	query string
}

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 }

func (s *fakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	db := s.conn.db
	db.mu.Lock()
	db.execs = append(db.execs, recordedExec{query: s.query, args: args})
	db.mu.Unlock()
	return driver.RowsAffected(int64(len(args))), nil
}

func (s *fakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	db := s.conn.db
	db.mu.Lock()
	defer db.mu.Unlock()
	return &fakeRows{cols: db.cols, rows: append([][]driver.Value(nil), db.rows...)}, nil
}

type fakeRows struct {
	cols []string
	rows [][]driver.Value
	idx  int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

var (
	fakeRegistry   = map[string]*fakeDB{}
	fakeRegistryMu sync.Mutex
	fakeDriverOnce sync.Once
)

type fakeDriver struct{}

func (fakeDriver) Open(name string) (driver.Conn, error) {
	fakeRegistryMu.Lock()
	defer fakeRegistryMu.Unlock()
	db, ok := fakeRegistry[name]
	if !ok {
		return nil, fmt.Errorf("unknown fake db %q", name)
	}
	return &fakeConn{db: db}, nil
}

// newFakeSQLDB returns a *sql.DB backed by the recording fake.
func newFakeSQLDB(t *testing.T, fake *fakeDB) *sql.DB {
	t.Helper()
	fakeDriverOnce.Do(func() { sql.Register("kapturefake", fakeDriver{}) })
	name := fmt.Sprintf("fake-%s-%d", t.Name(), time.Now().UnixNano())
	fakeRegistryMu.Lock()
	fakeRegistry[name] = fake
	fakeRegistryMu.Unlock()
	db, err := sql.Open("kapturefake", name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestRDSWriterFactory_CreatesSchema(t *testing.T) {
	fake := &fakeDB{}
	factory, err := NewRDSWriterFactory(context.Background(), RDSConfig{DB: newFakeSQLDB(t, fake)})
	if err != nil {
		t.Fatalf("NewRDSWriterFactory: %v", err)
	}
	if factory.table != DefaultRDSTable {
		t.Errorf("table = %q", factory.table)
	}

	queries := fake.recordedQueries()
	if len(queries) != 3 {
		t.Fatalf("schema statements = %d, want 3", len(queries))
	}
	if !strings.Contains(queries[0].query, "CREATE TABLE IF NOT EXISTS captured_requests") {
		t.Errorf("first schema statement: %s", queries[0].query)
	}
	if !strings.Contains(queries[2].query, "captured_requests_manifests") {
		t.Errorf("manifest table statement: %s", queries[2].query)
	}
}

func TestRDSWriterFactory_RejectsBadTableName(t *testing.T) {
	_, err := NewRDSWriterFactory(context.Background(), RDSConfig{
		DB:    newFakeSQLDB(t, &fakeDB{}),
		Table: `captured"; DROP TABLE users; --`,
	})
	if err == nil {
		t.Fatal("SQL-unsafe table name accepted")
	}
}

func TestRDSWriter_BatchesAndFlushes(t *testing.T) {
	fake := &fakeDB{}
	factory, err := NewRDSWriterFactory(context.Background(), RDSConfig{
		DB:        newFakeSQLDB(t, fake),
		BatchSize: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	schemaStatements := len(fake.recordedQueries())

	w, err := factory.NewWriter(context.Background(), "shop/orders")
	if err != nil {
		t.Fatal(err)
	}

	req := func(id string) *CapturedRequest {
		return &CapturedRequest{
			ID:        id,
			Timestamp: time.Now(),
			Method:    "GET",
			Path:      "/x",
			Protocol:  "HTTP",
			Headers:   map[string][]string{"A": {"b"}},
			Body:      []byte("hello"),
		}
	}

	// Two writes hit the batch size → one insert.
	if err := w.Write(context.Background(), req("r1")); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(context.Background(), req("r2")); err != nil {
		t.Fatal(err)
	}
	queries := fake.recordedQueries()[schemaStatements:]
	if len(queries) != 1 {
		t.Fatalf("inserts after batch fill = %d, want 1", len(queries))
	}
	insert := queries[0]
	if !strings.Contains(insert.query, "ON CONFLICT (capture_id, id) DO NOTHING") {
		t.Errorf("insert not idempotent: %s", insert.query)
	}
	if len(insert.args) != 20 {
		t.Errorf("insert args = %d, want 20 (2 rows x 10 columns)", len(insert.args))
	}
	if insert.args[0] != "shop/orders" || insert.args[1] != "r1" || insert.args[11] != "r2" {
		t.Errorf("insert args mismatch: %v", insert.args[:2])
	}

	// Third write buffered; Close flushes it.
	if err := w.Write(context.Background(), req("r3")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	queries = fake.recordedQueries()[schemaStatements:]
	if len(queries) != 2 {
		t.Fatalf("inserts after close = %d, want 2", len(queries))
	}
	if err := w.Write(context.Background(), req("late")); err != ErrWriterClosed {
		t.Errorf("write after close = %v, want ErrWriterClosed", err)
	}
}

func TestRDSWriterFactory_PutManifest(t *testing.T) {
	fake := &fakeDB{}
	factory, err := NewRDSWriterFactory(context.Background(), RDSConfig{DB: newFakeSQLDB(t, fake)})
	if err != nil {
		t.Fatal(err)
	}
	before := len(fake.recordedQueries())
	if err := factory.PutManifest(context.Background(), "shop/orders/shards/0-of-2", []byte(`{"formatVersion":1}`)); err != nil {
		t.Fatal(err)
	}
	queries := fake.recordedQueries()[before:]
	if len(queries) != 1 || !strings.Contains(queries[0].query, "ON CONFLICT (capture_id) DO UPDATE") {
		t.Fatalf("manifest upsert queries: %+v", queries)
	}

	// The factory must satisfy the manifest capability used by preshard.
	var _ ManifestWriter = factory
}
