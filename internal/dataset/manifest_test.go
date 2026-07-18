package dataset

import (
	"strings"
	"testing"
	"time"
)

func TestManifest_RoundTrip(t *testing.T) {
	index, count := int32(2), int32(8)
	m := &Manifest{
		FormatVersion:   ManifestFormatVersion,
		CaptureID:       "prod/orders/shards/2-of-8",
		RecordCount:     1234,
		SHA256:          "abc123",
		ShardIndex:      &index,
		ShardCount:      &count,
		SourceCaptureID: "prod/orders",
		CreatedAt:       time.Now().UTC(),
	}
	data, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.RecordCount != m.RecordCount || got.CaptureID != m.CaptureID || got.SHA256 != m.SHA256 {
		t.Errorf("round trip mismatch: %+v", got)
	}
}

func TestParseManifest_RejectsNewerFormat(t *testing.T) {
	_, err := ParseManifest([]byte(`{"formatVersion": 99, "captureID": "c", "recordCount": 1}`))
	if err == nil || !strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("newer format accepted: %v", err)
	}
}

func TestParseManifest_RejectsInvalid(t *testing.T) {
	cases := map[string]string{
		"no version":     `{"captureID": "c", "recordCount": 1}`,
		"negative count": `{"formatVersion": 1, "captureID": "c", "recordCount": -5}`,
		"garbage":        `not json`,
	}
	for name, data := range cases {
		if _, err := ParseManifest([]byte(data)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
