package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestArtifactPathsStayOutsideReplayDataset(t *testing.T) {
	key, err := artifactKey("captures", "app/session", "epoch/exchange.pcapng")
	if err != nil || key != "captures/evidence/app/session/epoch/exchange.pcapng" {
		t.Fatalf("unexpected key %q: %v", key, err)
	}
	for _, bad := range []string{"", ".", "..", "../escape", "/absolute", "a/../../escape", "a\\escape", "a//b"} {
		for _, pair := range [][2]string{{bad, "safe.json"}, {"session", bad}} {
			if _, err := artifactKey("", pair[0], pair[1]); err == nil {
				t.Errorf("accepted artifact path %q", bad)
			}
		}
	}
}

func TestFilesystemArtifactIsPrivateAtomicAndHasChecksum(t *testing.T) {
	root := t.TempDir()
	factory, err := NewEFSWriterFactory(EFSConfig{MountPath: root})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte{0, 1, 255, 0}
	r, err := factory.PutArtifact(context.Background(), "app/session", "epoch/exchange.pcapng", data, "application/x-pcapng")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if r.SizeBytes != int64(len(data)) || r.SHA256 != hex.EncodeToString(sum[:]) || !strings.HasPrefix(r.Key, filepath.Join(root, "evidence")) {
		t.Fatalf("incorrect artifact metadata: %+v", r)
	}
	got, err := os.ReadFile(r.Key)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("artifact differs: %x %v", got, err)
	}
	info, err := os.Stat(r.Key)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("artifact mode: %v %v", info, err)
	}
	entries, _ := os.ReadDir(filepath.Dir(r.Key))
	if len(entries) != 1 {
		t.Fatal("temporary artifact was left behind")
	}
}

func TestS3EvidenceRecordsOnlyMetadataAndSurfacesHistoryFailure(t *testing.T) {
	client, recorder := &fakeS3Client{}, &fakeArtifactRecorder{}
	factory, err := NewS3WriterFactory(context.Background(), S3Config{Bucket: "captures", Prefix: "prefix", Client: client, ArtifactRecorder: recorder})
	if err != nil {
		t.Fatal(err)
	}
	r, err := factory.PutArtifact(context.Background(), "app/session", "exchange.json", []byte("payload"), "application/json")
	if err != nil || client.key != "prefix/evidence/app/session/exchange.json" || recorder.calls != 1 || recorder.artifact.SHA256 != r.SHA256 {
		t.Fatalf("evidence upload/metadata failed: %+v %v", r, err)
	}
	factory.config.ArtifactRecorder = failingEvidenceRecorder{}
	if _, err := factory.PutArtifact(context.Background(), "app/session", "next.json", []byte("payload"), "application/json"); err == nil || !strings.Contains(err.Error(), "record uploaded artifact") {
		t.Fatalf("history failure hidden: %v", err)
	}
}

type failingEvidenceRecorder struct{}

func (failingEvidenceRecorder) RecordArtifact(context.Context, ArtifactRecord) error {
	return errors.New("metadata store unavailable")
}

func TestGCSEvidencePreservesBinaryAndContentType(t *testing.T) {
	w := &fakeGCSWriter{}
	var key string
	factory, err := NewGCSWriterFactory(context.Background(), GCSConfig{Bucket: "captures", Prefix: "prefix", OpenWriter: func(_ context.Context, _, object string) (gcsObjectWriter, error) {
		key = object
		return w, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte{0, 255, 1}
	if _, err := factory.PutArtifact(context.Background(), "session", "exchange.pcapng", data, "application/x-pcapng"); err != nil {
		t.Fatal(err)
	}
	if key != "prefix/evidence/session/exchange.pcapng" || !bytes.Equal(w.payload, data) || !w.closed || w.contentType != "application/x-pcapng" || w.contentEncoding != "" {
		t.Fatal("binary evidence changed during upload")
	}
}
