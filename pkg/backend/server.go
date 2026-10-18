package backend

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/kapture-io/kapture/pkg/capture"
)

// NewHandler serves the public v1 protocol. Deployments own TLS, authentication,
// authorization, and persistence. Wrap this handler with the chosen middleware.
func NewHandler(b Backend) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Kapture-Backend-Version", Version)
		if v := r.Header.Get("Kapture-Backend-Version"); v != "" && v != Version {
			http.Error(w, "unsupported backend version", http.StatusUpgradeRequired)
			return
		}
		d, err := b.Describe(r.Context())
		if err != nil || d.Version != Version || d.Name == "" {
			http.Error(w, "backend unavailable", http.StatusServiceUnavailable)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/capabilities" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(d)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 4 || parts[0] != "v1" || parts[1] != "captures" {
			http.NotFound(w, r)
			return
		}
		id, ok := decodePath(parts[2])
		if !ok {
			http.Error(w, "invalid capture identity", http.StatusBadRequest)
			return
		}
		switch {
		case len(parts) == 4 && parts[3] == "requests" && r.Method == http.MethodPost:
			sink, ok := b.(RequestSink)
			if !ok || !d.Supports(WriteRequests) {
				http.Error(w, "requests.write unavailable", http.StatusNotImplemented)
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, MaxRecordBytes)
			defer r.Body.Close()
			decoder := json.NewDecoder(r.Body)
			var request capture.Request
			if err := decoder.Decode(&request); err != nil || request.ID == "" {
				http.Error(w, "invalid or oversized request", http.StatusBadRequest)
				return
			}
			if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
				http.Error(w, "unexpected trailing request data", http.StatusBadRequest)
				return
			}
			if err := sink.PutRequest(r.Context(), id, &request); err != nil {
				http.Error(w, "request persistence failed", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case len(parts) == 5 && parts[3] == "artifacts" && r.Method == http.MethodPut:
			sink, ok := b.(ArtifactSink)
			if !ok || !d.Supports(WriteArtifacts) {
				http.Error(w, "artifacts.write unavailable", http.StatusNotImplemented)
				return
			}
			name, ok := decodePath(parts[4])
			if !ok {
				http.Error(w, "invalid artifact path", http.StatusBadRequest)
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, MaxArtifactBytes)
			defer r.Body.Close()
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "artifact exceeds limit or upload interrupted", http.StatusBadRequest)
				return
			}
			typ := r.Header.Get("Content-Type")
			a, err := sink.PutArtifact(r.Context(), id, name, data, typ)
			sum := sha256.Sum256(data)
			digest := hex.EncodeToString(sum[:])
			if err != nil || a.Key == "" || (a.CaptureID != "" && a.CaptureID != id) || (a.SHA256 != "" && a.SHA256 != digest) || (a.SizeBytes != 0 && a.SizeBytes != int64(len(data))) {
				http.Error(w, "artifact persistence failed", http.StatusInternalServerError)
				return
			}
			a.CaptureID, a.SHA256, a.SizeBytes, a.ContentType = id, digest, int64(len(data)), typ
			if a.CreatedAt.IsZero() {
				a.CreatedAt = time.Now().UTC()
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(a)
		case len(parts) == 4 && parts[3] == "requests" && r.Method == http.MethodGet:
			source, ok := b.(ReplaySource)
			if !ok || !d.Supports(ReadRequests) {
				http.Error(w, "requests.read unavailable", http.StatusNotImplemented)
				return
			}
			var opts capture.ReadOptions
			raw := r.URL.Query().Get("options")
			if len(raw) > 8192 || json.Unmarshal([]byte(raw), &opts) != nil || opts.CaptureID != id || opts.Limit < 0 || (!opts.StartTime.IsZero() && !opts.EndTime.IsZero() && !opts.StartTime.Before(opts.EndTime)) {
				http.Error(w, "invalid replay selection", http.StatusBadRequest)
				return
			}
			reader, err := source.OpenReplay(r.Context(), opts)
			if err != nil || reader == nil {
				http.Error(w, "cannot open replay source", http.StatusInternalServerError)
				return
			}
			defer reader.Close()
			w.Header().Set("Content-Type", "application/x-ndjson")
			write := func(f frame) bool {
				data, err := json.Marshal(f)
				if err != nil || len(data) > MaxRecordBytes {
					_, _ = io.WriteString(w, "{\"error\":\"record_encoding_failed\"}\n")
					return false
				}
				_, err = w.Write(append(data, '\n'))
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				return err == nil
			}
			var count int64
			for opts.Limit == 0 || count < opts.Limit {
				request, err := reader.Next(r.Context())
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil || request == nil || request.ID == "" {
					write(frame{Error: "backend_read_failed"})
					return
				}
				if !opts.Matches(request) {
					continue
				}
				if !write(frame{Request: request}) {
					return
				}
				count++
			}
			write(frame{End: true, Count: count})
		default:
			http.NotFound(w, r)
		}
	})
}

func decodePath(raw string) (string, bool) {
	b, err := base64.RawURLEncoding.Strict().DecodeString(raw)
	s := string(b)
	return s, err == nil && encode(s) == raw && validPath(s)
}
