package safety

import "testing"

func TestHostAllowed(t *testing.T) {
	tests := []struct {
		host    string
		allowed []string
		want    bool
	}{
		// No policy: everything allowed.
		{"prod-api.example.com", nil, true},
		{"prod-api.example.com", []string{}, true},

		// Exact match, case-insensitive, trailing-dot tolerant.
		{"staging.internal", []string{"staging.internal"}, true},
		{"STAGING.internal", []string{"staging.internal"}, true},
		{"staging.internal.", []string{"staging.internal"}, true},
		{"prod.internal", []string{"staging.internal"}, false},

		// Wildcard subdomains.
		{"api.staging.internal", []string{"*.staging.internal"}, true},
		{"a.b.staging.internal", []string{"*.staging.internal"}, true},
		{"staging.internal", []string{"*.staging.internal"}, false},
		{"notstaging.internal", []string{"*.staging.internal"}, false},

		// Multiple patterns: any match wins.
		{"sink.perf.svc", []string{"staging.internal", "*.perf.svc"}, true},
		{"prod.example.com", []string{"staging.internal", "*.perf.svc"}, false},

		// Blank patterns are ignored, not treated as match-all.
		{"prod.example.com", []string{""}, false},
	}
	for _, tt := range tests {
		if got := HostAllowed(tt.host, tt.allowed); got != tt.want {
			t.Errorf("HostAllowed(%q, %v) = %v, want %v", tt.host, tt.allowed, got, tt.want)
		}
	}
}
