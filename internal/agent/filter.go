package agent

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
)

// HeaderFilter selects requests carrying an exact header name/value pair.
// Header names are matched case-insensitively; values must match exactly.
type HeaderFilter struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// RequestFilterConfig configures a RequestFilter.
type RequestFilterConfig struct {
	// PathPrefix keeps only requests whose path starts with this prefix.
	// Empty means all paths match.
	PathPrefix string
	// Headers keeps only requests that match every listed header pair.
	Headers []HeaderFilter
	// Percentage samples the given percent (0-100) of requests that pass
	// the path and header checks. Values outside the range are clamped.
	Percentage int
	// SampleFn returns a value in [0,100) used for sampling decisions.
	// Defaults to a uniform random source; override for deterministic tests.
	SampleFn func() int
}

// RequestFilter decides whether a mirrored request should be captured,
// enforcing the TrafficCapture spec.filters constraints inside the agent.
type RequestFilter struct {
	pathPrefix string
	headers    []HeaderFilter
	percentage int
	sampleFn   func() int
}

// NewRequestFilter creates a RequestFilter from the given configuration.
func NewRequestFilter(cfg RequestFilterConfig) *RequestFilter {
	percentage := cfg.Percentage
	if percentage > 100 {
		percentage = 100
	}
	if percentage < 0 {
		percentage = 0
	}
	sampleFn := cfg.SampleFn
	if sampleFn == nil {
		sampleFn = func() int { return rand.IntN(100) }
	}
	return &RequestFilter{
		pathPrefix: cfg.PathPrefix,
		headers:    cfg.Headers,
		percentage: percentage,
		sampleFn:   sampleFn,
	}
}

// Matches reports whether the request passes all configured filters.
// A nil filter matches everything.
func (f *RequestFilter) Matches(req *CapturedHTTPRequest) bool {
	if f == nil {
		return true
	}
	if f.pathPrefix != "" && !strings.HasPrefix(req.Path, f.pathPrefix) {
		return false
	}
	for _, hf := range f.headers {
		if !headerMatches(req.Headers, hf) {
			return false
		}
	}
	if f.percentage >= 100 {
		return true
	}
	if f.percentage <= 0 {
		return false
	}
	return f.sampleFn() < f.percentage
}

func headerMatches(headers map[string][]string, hf HeaderFilter) bool {
	for name, values := range headers {
		if !strings.EqualFold(name, hf.Name) {
			continue
		}
		for _, v := range values {
			if v == hf.Value {
				return true
			}
		}
	}
	return false
}

// ParseHeaderFilters parses header filters from a JSON array, e.g.
// [{"name":"x-debug","value":"true"}]. An empty string yields no filters.
func ParseHeaderFilters(raw string) ([]HeaderFilter, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var filters []HeaderFilter
	if err := json.Unmarshal([]byte(raw), &filters); err != nil {
		return nil, fmt.Errorf("parse header filters: %w", err)
	}
	for i, f := range filters {
		if strings.TrimSpace(f.Name) == "" {
			return nil, fmt.Errorf("header filter %d: name is required", i)
		}
	}
	return filters, nil
}
