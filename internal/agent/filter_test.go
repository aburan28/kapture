package agent

import (
	"context"
	"strings"
	"testing"
)

func filterTestRequest(path string, headers map[string][]string) *CapturedHTTPRequest {
	return &CapturedHTTPRequest{
		Method:   "GET",
		Path:     path,
		Headers:  headers,
		Body:     strings.NewReader("body"),
		Protocol: "HTTP",
	}
}

// ---------------------------------------------------------------------------
// RequestFilter.Matches
// ---------------------------------------------------------------------------

func TestRequestFilter_NilFilterMatchesEverything(t *testing.T) {
	var f *RequestFilter
	if !f.Matches(filterTestRequest("/anything", nil)) {
		t.Error("nil filter should match everything")
	}
}

func TestRequestFilter_EmptyConfigMatchesEverything(t *testing.T) {
	f := NewRequestFilter(RequestFilterConfig{Percentage: 100})
	if !f.Matches(filterTestRequest("/api/users", nil)) {
		t.Error("empty filter should match everything")
	}
}

func TestRequestFilter_PathPrefix(t *testing.T) {
	f := NewRequestFilter(RequestFilterConfig{PathPrefix: "/api/", Percentage: 100})

	tests := []struct {
		path string
		want bool
	}{
		{"/api/users", true},
		{"/api/", true},
		{"/api", false},
		{"/health", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := f.Matches(filterTestRequest(tt.path, nil)); got != tt.want {
			t.Errorf("Matches(path=%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestRequestFilter_HeaderMatch(t *testing.T) {
	f := NewRequestFilter(RequestFilterConfig{
		Headers:    []HeaderFilter{{Name: "X-Debug", Value: "true"}},
		Percentage: 100,
	})

	tests := []struct {
		name    string
		headers map[string][]string
		want    bool
	}{
		{"exact match", map[string][]string{"X-Debug": {"true"}}, true},
		{"case-insensitive name", map[string][]string{"x-debug": {"true"}}, true},
		{"value among multiple", map[string][]string{"X-Debug": {"false", "true"}}, true},
		{"wrong value", map[string][]string{"X-Debug": {"false"}}, false},
		{"case-sensitive value", map[string][]string{"X-Debug": {"TRUE"}}, false},
		{"header absent", map[string][]string{"X-Other": {"true"}}, false},
		{"nil headers", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := f.Matches(filterTestRequest("/p", tt.headers)); got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRequestFilter_AllHeadersMustMatch(t *testing.T) {
	f := NewRequestFilter(RequestFilterConfig{
		Headers: []HeaderFilter{
			{Name: "X-Env", Value: "prod"},
			{Name: "X-Team", Value: "core"},
		},
		Percentage: 100,
	})

	both := map[string][]string{"X-Env": {"prod"}, "X-Team": {"core"}}
	if !f.Matches(filterTestRequest("/p", both)) {
		t.Error("expected match when all headers present")
	}

	onlyOne := map[string][]string{"X-Env": {"prod"}}
	if f.Matches(filterTestRequest("/p", onlyOne)) {
		t.Error("expected no match when one header is missing")
	}
}

func TestRequestFilter_PercentageSampling(t *testing.T) {
	tests := []struct {
		name       string
		percentage int
		sample     int
		want       bool
	}{
		{"0 percent drops all", 0, 0, false},
		{"100 percent keeps all", 100, 99, true},
		{"sample below threshold kept", 50, 49, true},
		{"sample at threshold dropped", 50, 50, false},
		{"sample above threshold dropped", 50, 99, false},
		{"negative clamps to 0", -5, 0, false},
		{"over 100 clamps to 100", 250, 99, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewRequestFilter(RequestFilterConfig{
				Percentage: tt.percentage,
				SampleFn:   func() int { return tt.sample },
			})
			if got := f.Matches(filterTestRequest("/p", nil)); got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRequestFilter_DefaultSampleFnDistribution(t *testing.T) {
	f := NewRequestFilter(RequestFilterConfig{Percentage: 50})

	kept := 0
	const n = 10000
	for i := 0; i < n; i++ {
		if f.Matches(filterTestRequest("/p", nil)) {
			kept++
		}
	}
	// With 50% sampling over 10k requests, expect roughly half; allow wide margin.
	if kept < n/4 || kept > 3*n/4 {
		t.Errorf("kept %d of %d requests at 50%% sampling, expected roughly half", kept, n)
	}
}

func TestRequestFilter_CombinedFilters(t *testing.T) {
	f := NewRequestFilter(RequestFilterConfig{
		PathPrefix: "/api/",
		Headers:    []HeaderFilter{{Name: "X-Debug", Value: "true"}},
		Percentage: 100,
	})

	match := filterTestRequest("/api/users", map[string][]string{"X-Debug": {"true"}})
	if !f.Matches(match) {
		t.Error("expected match when both path and header match")
	}

	wrongPath := filterTestRequest("/health", map[string][]string{"X-Debug": {"true"}})
	if f.Matches(wrongPath) {
		t.Error("expected no match on wrong path even with matching header")
	}
}

// ---------------------------------------------------------------------------
// ParseHeaderFilters
// ---------------------------------------------------------------------------

func TestParseHeaderFilters(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantLen int
		wantErr bool
	}{
		{"empty string", "", 0, false},
		{"whitespace only", "   ", 0, false},
		{"single filter", `[{"name":"x-debug","value":"true"}]`, 1, false},
		{"multiple filters", `[{"name":"a","value":"1"},{"name":"b","value":"2"}]`, 2, false},
		{"invalid json", `{not json`, 0, true},
		{"missing name", `[{"value":"true"}]`, 0, true},
		{"blank name", `[{"name":"  ","value":"true"}]`, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filters, err := ParseHeaderFilters(tt.raw)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseHeaderFilters() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && len(filters) != tt.wantLen {
				t.Errorf("len(filters) = %d, want %d", len(filters), tt.wantLen)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CaptureHandler + filter integration
// ---------------------------------------------------------------------------

func TestHandle_FilteredRequestNotWritten(t *testing.T) {
	mw := &mockWriter{}
	h := NewCaptureHandler(CaptureHandlerConfig{
		Writer: mw,
		Filter: NewRequestFilter(RequestFilterConfig{PathPrefix: "/api/", Percentage: 100}),
		Logger: discardLogger(),
	})

	if err := h.Handle(context.Background(), filterTestRequest("/health", nil)); err != nil {
		t.Fatalf("Handle returned error for filtered request: %v", err)
	}
	if err := h.Handle(context.Background(), filterTestRequest("/api/users", nil)); err != nil {
		t.Fatalf("Handle returned error for matching request: %v", err)
	}

	writes := mw.getWrites()
	if len(writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(writes))
	}
	if writes[0].Path != "/api/users" {
		t.Errorf("written path = %q, want %q", writes[0].Path, "/api/users")
	}

	total, filtered, dropped, _ := h.Metrics()
	if total != 2 {
		t.Errorf("total = %d, want 2", total)
	}
	if filtered != 1 {
		t.Errorf("filtered = %d, want 1", filtered)
	}
	if dropped != 0 {
		t.Errorf("dropped = %d, want 0", dropped)
	}
}

func TestHandle_NoFilterCapturesAll(t *testing.T) {
	mw := &mockWriter{}
	h := NewCaptureHandler(CaptureHandlerConfig{Writer: mw, Logger: discardLogger()})

	for _, path := range []string{"/a", "/b", "/c"} {
		if err := h.Handle(context.Background(), filterTestRequest(path, nil)); err != nil {
			t.Fatalf("Handle(%s) failed: %v", path, err)
		}
	}

	if len(mw.getWrites()) != 3 {
		t.Fatalf("expected 3 writes, got %d", len(mw.getWrites()))
	}
	_, filtered, _, _ := h.Metrics()
	if filtered != 0 {
		t.Errorf("filtered = %d, want 0", filtered)
	}
}
