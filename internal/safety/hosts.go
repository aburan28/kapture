// Package safety implements replay target safety policies, shared by the
// hub coordinator (which denies unsafe load tests before distribution) and
// the spoke replay controller (which denies unsafe TrafficReplays before
// creating worker Jobs).
package safety

import "strings"

// HostAllowed reports whether a replay target host satisfies an allowlist.
//
// Patterns:
//   - exact hostname match, case-insensitive ("staging-api.internal")
//   - wildcard subdomain match ("*.staging.internal" matches
//     "api.staging.internal" but not "staging.internal" itself)
//
// An empty allowlist means no policy is configured and every host is
// allowed — the field is opt-in. Once any pattern is present, the target
// must match one of them.
func HostAllowed(host string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))

	for _, pattern := range allowed {
		pattern = strings.ToLower(strings.TrimSpace(pattern))
		if pattern == "" {
			continue
		}
		if suffix, ok := strings.CutPrefix(pattern, "*."); ok {
			if strings.HasSuffix(host, "."+suffix) {
				return true
			}
			continue
		}
		if host == pattern {
			return true
		}
	}
	return false
}
