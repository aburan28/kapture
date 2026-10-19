// Package filesystem is a small reference destination for the backend protocol.
// Its private layout is deliberately independent of Kapture's JSONL.gz layout.
// Payloads are read one record at a time; directory listings are held in memory.
package filesystem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kapture-io/kapture/pkg/backend"
	"github.com/kapture-io/kapture/pkg/capture"
)

type Store struct{ root string }

func New(root string) (*Store, error) {
	if root == "" {
		return nil, fmt.Errorf("backend data directory is required")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	return &Store{root: root}, nil
}

func (*Store) Describe(context.Context) (backend.Descriptor, error) {
	return backend.Descriptor{Version: backend.Version, Name: "filesystem-reference", Capabilities: []backend.Capability{backend.WriteRequests, backend.WriteArtifacts, backend.ReadRequests}}, nil
}

func encoded(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

func (s *Store) PutRequest(ctx context.Context, id string, r *capture.Request) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == "" || r == nil || r.ID == "" {
		return fmt.Errorf("capture and request identities are required")
	}
	data, err := json.Marshal(r)
	if err != nil || len(data) > backend.MaxRecordBytes {
		return fmt.Errorf("invalid or oversized request")
	}
	digest := sha256.Sum256([]byte(r.ID))
	name := r.Timestamp.UTC().Format("20060102T150405.000000000Z") + "-" + hex.EncodeToString(digest[:]) + ".json"
	return putImmutable(filepath.Join(s.root, encoded(id), "requests", name), data)
}

func (s *Store) PutArtifact(ctx context.Context, id, name string, data []byte, typ string) (capture.Artifact, error) {
	if err := ctx.Err(); err != nil {
		return capture.Artifact{}, err
	}
	if id == "" || name == "" || len(data) > backend.MaxArtifactBytes {
		return capture.Artifact{}, fmt.Errorf("invalid artifact identity or size")
	}
	target := filepath.Join(s.root, encoded(id), "evidence", encoded(name)+".blob")
	if err := putImmutable(target, data); err != nil {
		return capture.Artifact{}, err
	}
	sum := sha256.Sum256(data)
	return capture.Artifact{CaptureID: id, StorageBackend: "filesystem", Key: target, ContentType: typ, SizeBytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), CreatedAt: time.Now().UTC()}, nil
}

// A retry cannot overwrite an acknowledged object with different bytes. Linking
// a synced private tempfile publishes atomically and excludes concurrent writers.
func putImmutable(target string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".pending-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Link(tmp.Name(), target); err != nil {
		if !os.IsExist(err) {
			return err
		}
		old, err := os.ReadFile(target)
		if err != nil {
			return err
		}
		if !bytes.Equal(old, data) {
			return fmt.Errorf("immutable backend object already contains different bytes")
		}
	}
	dir, err := os.Open(filepath.Dir(target))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (s *Store) OpenReplay(ctx context.Context, opts capture.ReadOptions) (capture.Reader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.CaptureID == "" {
		return nil, fmt.Errorf("capture identity is required")
	}
	dir := filepath.Join(s.root, encoded(opts.CaptureID), "requests")
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return &reader{dir: dir, entries: entries}, nil
}

type reader struct {
	dir     string
	entries []os.DirEntry
	next    int
	closed  bool
}

func (r *reader) Next(ctx context.Context) (*capture.Request, error) {
	for !r.closed && r.next < len(r.entries) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry := r.entries[r.next]
		r.next++
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		file, err := os.Open(filepath.Join(r.dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(io.LimitReader(file, backend.MaxRecordBytes+1))
		file.Close()
		if err != nil || len(data) > backend.MaxRecordBytes {
			return nil, fmt.Errorf("cannot read bounded backend record")
		}
		var request capture.Request
		if err := json.Unmarshal(data, &request); err != nil {
			return nil, err
		}
		return &request, nil
	}
	return nil, io.EOF
}

func (r *reader) Close() error { r.closed = true; r.entries = nil; return nil }
