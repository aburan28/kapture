package agent

import (
	"net/http"
	"strings"
)

// RedactedValue replaces credential header values in stored captures. It is
// a fixed sentinel rather than a hash so redacted captures can never be
// used to correlate or brute-force the original secret.
const RedactedValue = "[REDACTED]"

// DefaultRedactHeaders are the credential-bearing headers redacted by
// default before captured requests become durable data. Replayed traffic
// should inject fresh credentials via engine header overrides instead of
// resending recorded ones (see docs/multi-cell-load-testing.md).
var DefaultRedactHeaders = []string{
	"Authorization",
	"Proxy-Authorization",
	"Cookie",
	"Set-Cookie",
	"X-Api-Key",
	"X-Auth-Token",
}

// HeaderRedactor replaces the values of configured headers with
// RedactedValue. The header names are kept so replay tooling and analysis
// can still see that a credential was present.
type HeaderRedactor struct {
	names map[string]bool // canonical header names
}

// NewHeaderRedactor builds a redactor for the given header names. A nil or
// empty list produces a redactor that redacts nothing.
func NewHeaderRedactor(names []string) *HeaderRedactor {
	set := make(map[string]bool, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		set[http.CanonicalHeaderKey(name)] = true
	}
	return &HeaderRedactor{names: set}
}

// Redact replaces matching header values in place and reports how many
// headers were redacted.
func (r *HeaderRedactor) Redact(headers map[string][]string) int {
	if r == nil || len(r.names) == 0 || len(headers) == 0 {
		return 0
	}
	redacted := 0
	for name, values := range headers {
		if !r.names[http.CanonicalHeaderKey(name)] {
			continue
		}
		for i := range values {
			values[i] = RedactedValue
		}
		redacted++
	}
	return redacted
}
