package storage

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/kapture-io/kapture/pkg/backend"
	"github.com/kapture-io/kapture/pkg/backend/filesystem"
	"github.com/kapture-io/kapture/pkg/capture"
)

func TestHTTPDestinationAcknowledgesEvidenceAndRecordsOnlyMetadata(t *testing.T) {
	store, err := filesystem.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(backend.NewHandler(opaqueArtifactBackend{store}))
	defer server.Close()
	recorder := &fakeArtifactRecorder{}
	factory, err := NewHTTPWriterFactory(context.Background(), HTTPConfig{Endpoint: server.URL, HTTPClient: server.Client(), ArtifactRecorder: recorder, CaptureNamespace: "app", CaptureName: "session"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := factory.PutArtifact(context.Background(), "app/session", "exchange.json", []byte("opaque evidence"), "application/json")
	if err != nil || r.SHA256 == "" || recorder.calls != 1 || recorder.artifact.StorageBackend != "http" || recorder.artifact.Bucket != "" || recorder.artifact.CaptureNamespace != "app" || recorder.artifact.Key != r.Key {
		t.Fatalf("artifact metadata not preserved: %+v %v", r, err)
	}
	factory.config.ArtifactRecorder = failingEvidenceRecorder{}
	if _, err := factory.PutArtifact(context.Background(), "app/session", "exchange.json", []byte("opaque evidence"), "application/json"); err == nil {
		t.Fatal("uploaded artifact history failure was hidden")
	}
	writer, err := factory.NewWriter(context.Background(), "app/session")
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(context.Background(), &CapturedRequest{ID: "closed"}); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("closed writer accepted a record: %v", err)
	}
}

// A provider may return an opaque key without an underlying storage type.
// Its location must still be usable by the generic history recorder.
type opaqueArtifactBackend struct{ *filesystem.Store }

func (b opaqueArtifactBackend) PutArtifact(ctx context.Context, id, name string, data []byte, typ string) (capture.Artifact, error) {
	a, err := b.Store.PutArtifact(ctx, id, name, data, typ)
	a.StorageBackend = ""
	return a, err
}
