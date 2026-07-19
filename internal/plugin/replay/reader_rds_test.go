package replay

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// rdsFake is a minimal database/sql driver serving canned rows and
// recording queries, so RDSReader is testable without PostgreSQL.
type rdsFake struct {
	mu      sync.Mutex
	queries []string
	rows    [][]driver.Value
}

type rdsFakeConn struct{ db *rdsFake }
type rdsFakeStmt struct {
	conn  *rdsFakeConn
	query string
}

func (c *rdsFakeConn) Prepare(query string) (driver.Stmt, error) {
	return &rdsFakeStmt{conn: c, query: query}, nil
}
func (c *rdsFakeConn) Close() error              { return nil }
func (c *rdsFakeConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("not supported") }

func (s *rdsFakeStmt) Close() error  { return nil }
func (s *rdsFakeStmt) NumInput() int { return -1 }
func (s *rdsFakeStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}
func (s *rdsFakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	db := s.conn.db
	db.mu.Lock()
	defer db.mu.Unlock()
	db.queries = append(db.queries, s.query)
	return &rdsFakeRows{
		cols: []string{"id", "ts", "method", "path", "protocol", "headers", "metadata", "body", "content_length"},
		rows: append([][]driver.Value(nil), db.rows...),
	}, nil
}

type rdsFakeRows struct {
	cols []string
	rows [][]driver.Value
	idx  int
}

func (r *rdsFakeRows) Columns() []string { return r.cols }
func (r *rdsFakeRows) Close() error      { return nil }
func (r *rdsFakeRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

var (
	rdsFakeRegistry   = map[string]*rdsFake{}
	rdsFakeRegistryMu sync.Mutex
	rdsFakeDriverOnce sync.Once
)

type rdsFakeDriver struct{}

func (rdsFakeDriver) Open(name string) (driver.Conn, error) {
	rdsFakeRegistryMu.Lock()
	defer rdsFakeRegistryMu.Unlock()
	db, ok := rdsFakeRegistry[name]
	if !ok {
		return nil, fmt.Errorf("unknown fake db %q", name)
	}
	return &rdsFakeConn{db: db}, nil
}

func newRDSFakeDB(t *testing.T, fake *rdsFake) *sql.DB {
	t.Helper()
	rdsFakeDriverOnce.Do(func() { sql.Register("kapturereplayfake", rdsFakeDriver{}) })
	name := fmt.Sprintf("fake-%s-%d", t.Name(), time.Now().UnixNano())
	rdsFakeRegistryMu.Lock()
	rdsFakeRegistry[name] = fake
	rdsFakeRegistryMu.Unlock()
	db, err := sql.Open("kapturereplayfake", name)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func rdsRow(id, method, path string, ts time.Time) []driver.Value {
	return []driver.Value{
		id, ts, method, path, "HTTP",
		[]byte(`{"A":["b"]}`), []byte(`{"host":"api"}`), []byte("body"), int64(4),
	}
}

func TestRDSReader_StreamsRowsWithFilters(t *testing.T) {
	base := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	fake := &rdsFake{rows: [][]driver.Value{
		rdsRow("r1", "GET", "/api/a", base),
		rdsRow("r2", "POST", "/api/b", base.Add(time.Second)),
		rdsRow("r3", "GET", "/other", base.Add(2*time.Second)),
	}}

	reader, err := NewRDSReader(context.Background(), RDSReaderConfig{DB: newRDSFakeDB(t, fake)})
	if err != nil {
		t.Fatalf("NewRDSReader: %v", err)
	}
	defer reader.Close()

	opts := ReadOptions{
		CaptureID:  "shop/orders",
		PathPrefix: "/api",
		StartTime:  base.Add(-time.Hour),
	}
	if err := reader.Open(context.Background(), opts); err != nil {
		t.Fatalf("Open: %v", err)
	}

	var ids []string
	for {
		req, err := reader.Next(context.Background())
		if errors.Is(err, ErrReaderDone) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		ids = append(ids, req.ID)
		if req.ID == "r1" {
			if req.Headers["A"][0] != "b" || req.Metadata["host"] != "api" {
				t.Errorf("row r1 decoded badly: %+v", req)
			}
			if string(req.Body) != "body" || req.ContentLength != 4 {
				t.Errorf("row r1 body/length: %q %d", req.Body, req.ContentLength)
			}
		}
	}
	// /other filtered client-side.
	if len(ids) != 2 || ids[0] != "r1" || ids[1] != "r2" {
		t.Errorf("ids = %v, want [r1 r2]", ids)
	}
	if reader.TotalRead() != 2 {
		t.Errorf("totalRead = %d", reader.TotalRead())
	}

	// The time bound must be pushed into SQL.
	fake.mu.Lock()
	query := fake.queries[0]
	fake.mu.Unlock()
	if !strings.Contains(query, "ts >= $2") || !strings.Contains(query, "ORDER BY ts, id") {
		t.Errorf("query missing pushdown/order: %s", query)
	}
}

func TestRDSReader_RejectsBadTable(t *testing.T) {
	_, err := NewRDSReader(context.Background(), RDSReaderConfig{
		DB:    newRDSFakeDB(t, &rdsFake{}),
		Table: "bad;table",
	})
	if err == nil {
		t.Fatal("SQL-unsafe table name accepted")
	}
}
