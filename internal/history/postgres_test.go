package history

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

func TestArtifactHistoryAcceptsDestinationsWithoutBuckets(t *testing.T) {
	for _, test := range []struct {
		name, backend, bucket string
		wantBackend           string
		wantError             bool
	}{
		{name: "HTTP adapter", backend: "http", wantBackend: "http"},
		{name: "filesystem adapter", backend: "filesystem", wantBackend: "filesystem"},
		{name: "custom queue", backend: "queue", wantBackend: "queue"},
		{name: "legacy S3 default", bucket: "captures", wantBackend: "s3"},
		{name: "GCS", backend: "gcs", bucket: "captures", wantBackend: "gcs"},
		{name: "legacy S3 needs bucket", wantError: true},
		{name: "explicit S3 needs bucket", backend: "s3", wantError: true},
		{name: "GCS needs bucket", backend: "gcs", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls int
			var values []driver.NamedValue
			db := sql.OpenDB(artifactConnector{record: func(query string, args []driver.NamedValue) error {
				if !strings.Contains(query, "INSERT INTO capture_artifacts") {
					return fmt.Errorf("unexpected history query")
				}
				calls++
				values = append([]driver.NamedValue(nil), args...)
				return nil
			}})
			defer db.Close()
			repository := &PostgresRepository{db: db}
			artifact := storage.ArtifactRecord{CaptureID: "app/capture", StorageBackend: test.backend, Bucket: test.bucket, Key: "opaque/location", ContentType: "application/x-pcapng", SHA256: "checksum", SizeBytes: 512}
			err := repository.RecordArtifact(context.Background(), artifact)
			if test.wantError {
				if err == nil || calls != 0 {
					t.Fatalf("invalid location reached SQL: calls=%d err=%v", calls, err)
				}
				return
			}
			if err != nil || calls != 1 || len(values) != 10 {
				t.Fatalf("artifact metadata not indexed: calls=%d values=%d err=%v", calls, len(values), err)
			}
			if values[0].Value != artifact.CaptureID || values[1].Value != test.wantBackend || values[2].Value != test.bucket || values[3].Value != artifact.Key || values[7].Value != artifact.SizeBytes || values[8].Value != artifact.SHA256 {
				t.Fatalf("artifact location or checksum changed: %+v", values)
			}
			if values[9].Value.(time.Time).IsZero() {
				t.Fatal("artifact creation time absent")
			}
		})
	}
}

// Exercise database/sql argument conversion without requiring a live database.
// The production SQL statement and its historical columns remain unchanged.
type artifactConnector struct {
	record func(string, []driver.NamedValue) error
}

func (c artifactConnector) Connect(context.Context) (driver.Conn, error) {
	return artifactConnection{c}, nil
}
func (artifactConnector) Driver() driver.Driver { return artifactDriver{} }

type artifactDriver struct{}

func (artifactDriver) Open(string) (driver.Conn, error) { return nil, fmt.Errorf("use connector") }

type artifactConnection struct{ artifactConnector }

func (artifactConnection) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("unexpected prepare")
}
func (artifactConnection) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("unexpected transaction")
}
func (artifactConnection) Close() error { return nil }
func (c artifactConnection) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), c.record(query, args)
}
