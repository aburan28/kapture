package dataset

import (
	"encoding/json"
	"fmt"
	"time"
)

// ManifestFormatVersion is the dataset manifest schema version this build
// writes and the highest version it understands. Readers must reject
// manifests with a higher version rather than misinterpret them.
const ManifestFormatVersion = 1

// Manifest describes one stored capture dataset. Preshard writes a
// manifest per slice; single-writer pipelines can write one for whole
// captures. Raw captures written concurrently by multiple agents have no
// manifest — there is no atomic point at which any one writer knows the
// totals — so readers must treat a missing manifest as "unknown", not as
// an error.
type Manifest struct {
	// FormatVersion is the manifest schema version (ManifestFormatVersion).
	FormatVersion int `json:"formatVersion"`

	// CaptureID is the dataset's capture ID, including any shard-slice
	// suffix ("default/orders/shards/1-of-4").
	CaptureID string `json:"captureID"`

	// RecordCount is the exact number of captured requests in the dataset.
	RecordCount int64 `json:"recordCount"`

	// SHA256 is the hex digest of the concatenated uncompressed JSONL
	// lines in write order. Only single-writer pipelines can compute it;
	// empty means unknown.
	SHA256 string `json:"sha256,omitempty"`

	// ShardIndex/ShardCount identify a preshard slice. Both unset for
	// unsharded datasets.
	ShardIndex *int32 `json:"shardIndex,omitempty"`
	ShardCount *int32 `json:"shardCount,omitempty"`

	// SourceCaptureID names the capture a preshard slice was derived from.
	SourceCaptureID string `json:"sourceCaptureID,omitempty"`

	CreatedAt time.Time `json:"createdAt"`
}

// Validate checks the manifest is well-formed and its format version is
// understood by this build.
func (m *Manifest) Validate() error {
	if m.FormatVersion <= 0 {
		return fmt.Errorf("manifest has no formatVersion")
	}
	if m.FormatVersion > ManifestFormatVersion {
		return fmt.Errorf("manifest formatVersion %d is newer than supported version %d; upgrade the replay engine",
			m.FormatVersion, ManifestFormatVersion)
	}
	if m.RecordCount < 0 {
		return fmt.Errorf("manifest recordCount %d is negative", m.RecordCount)
	}
	return nil
}

// Marshal encodes the manifest as indented JSON.
func (m *Manifest) Marshal() ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

// ParseManifest decodes and validates manifest JSON.
func ParseManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &m, nil
}
