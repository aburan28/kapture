package l7

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kapture-io/kapture/internal/storage"
)

func TestExportRedactsEveryArtifactAndKeepsReplayDatasetSeparate(t *testing.T) {
	dir := t.TempDir()
	factory, err := storage.NewEFSWriterFactory(storage.EFSConfig{MountPath: dir, AgentPod: "test"})
	if err != nil {
		t.Fatal(err)
	}
	writer, _ := factory.NewWriter(context.Background(), "ns/capture")
	defer writer.Close()
	exporter, err := NewExporter(ExportConfig{CaptureID: "ns/capture", Writer: writer, Artifacts: factory, IncludeHeaders: true, IncludeBody: true, PCAP: true})
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, Config{})
	f.feed("write", []byte("GET /path?query=preserved HTTP/1.1\r\nAuthorization: canary-secret\r\n\r\n"))
	f.feed("read", []byte("HTTP/1.1 204 No Content\r\nSet-Cookie: canary-secret\r\n\r\n"))
	x := f.out[0]
	x.HeaderProofs = []HeaderProof{{Before: HeaderState{MaxBytes: 4096, Entries: []HeaderField{{"authorization", "canary-secret"}}}, Block: []byte("canary-secret")}}
	if err = exporter.Accept(context.Background(), x); err != nil {
		t.Fatal(err)
	}
	if err = writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	count := 0
	err = filepath.WalkDir(filepath.Join(dir, "evidence"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(b, []byte("canary-secret")) {
			t.Fatalf("credential leaked to %s", path)
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 || exporter.Requests != 1 {
		t.Fatalf("wrong exported artifacts/requests: %d/%d", count, exporter.Requests)
	}
	if x.Request.Headers.Get("Authorization") != "canary-secret" {
		t.Fatal("export mutated source observation")
	}
	files, _ := filepath.Glob(filepath.Join(dir, "ns/capture", "*", "*.jsonl.gz"))
	if len(files) != 1 {
		t.Fatal("replay projection not in ordinary dataset layout")
	}
}

type artifactFailure struct{}

func (artifactFailure) PutArtifact(context.Context, string, string, []byte, string) (storage.ArtifactRecord, error) {
	return storage.ArtifactRecord{}, errors.New("upload failed")
}
func TestRetentionTriggerPreservesHistoryAndEnforcesBudget(t *testing.T) {
	dir := t.TempDir()
	factory, _ := storage.NewEFSWriterFactory(storage.EFSConfig{MountPath: dir})
	writer, _ := factory.NewWriter(context.Background(), "capture")
	defer writer.Close()
	exporter, _ := NewExporter(ExportConfig{CaptureID: "capture", Writer: writer, Artifacts: factory, IncludeHeaders: true, IncludeBody: true, TriggerStatus: 500, History: time.Minute, HistoryBytes: 4096})
	for i, status := range []int{200, 200, 500} {
		x := &Exchange{FormatVersion: 1, ID: stableID(string(rune('a' + i))), Epoch: "e", Flow: "f", Protocol: "HTTP/1.1", StartedAt: time.Now(), EndedAt: time.Now(), Request: Message{Method: "GET", Path: "/", Headers: http.Header{}, Complete: true}, Response: Message{Status: status, Complete: true}}
		if err := exporter.Accept(context.Background(), x); err != nil {
			t.Fatal(err)
		}
		if i < 2 && exporter.Exchanges != 0 {
			t.Fatal("non-triggered history persisted")
		}
	}
	if exporter.Exchanges != 3 || exporter.MemoryBytes() > 4096 {
		t.Fatal("history not pinned within budget")
	}
	broken, _ := NewExporter(ExportConfig{CaptureID: "broken", Writer: writer, Artifacts: artifactFailure{}})
	if err := broken.Accept(context.Background(), &Exchange{ID: "x"}); err == nil {
		t.Fatal("upload failure hidden")
	}
}

func TestHeaderSnapshotRoundTripAndMalformedLimits(t *testing.T) {
	state := HeaderState{MaxBytes: 4096, Entries: []HeaderField{{"x-b", "two"}, {"x-a", "one"}}}
	d, err := RestoreHeaderState(state)
	if err != nil {
		t.Fatal(err)
	}
	fields, err := d.DecodeFull([]byte{0xbe, 0xbf})
	if err != nil || len(fields) != 2 || fields[0].Name != "x-b" || fields[1].Name != "x-a" {
		t.Fatalf("wrong restored indices: %+v %v", fields, err)
	}
	state.MaxBytes = 1
	if _, err = RestoreHeaderState(state); err == nil {
		t.Fatal("oversized snapshot accepted")
	}
	var round HeaderState
	b, _ := json.Marshal(state)
	if err = json.Unmarshal(b, &round); err != nil {
		t.Fatal(err)
	}
}
