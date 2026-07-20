package spoke

import (
	"testing"
)

// TestBuildEnvVars_RedisStatsPassthrough verifies the spoke's Redis
// statistics configuration reaches the capture agent pods it deploys.
func TestBuildEnvVars_RedisStatsPassthrough(t *testing.T) {
	tc := newTestTrafficCapture("capture-1", "default")
	storage := newTestStorage()

	t.Run("disabled without REDIS_ADDR", func(t *testing.T) {
		envs := buildEnvVars(tc, storage)
		if _, ok := findEnvVar(envs, "REDIS_ADDR"); ok {
			t.Error("REDIS_ADDR set without spoke configuration")
		}
	})

	t.Run("addr and db pass through", func(t *testing.T) {
		t.Setenv("REDIS_ADDR", "redis.observability.svc:6379")
		t.Setenv("REDIS_DB", "2")

		envs := buildEnvVars(tc, storage)
		if got, _ := findEnvVar(envs, "REDIS_ADDR"); got != "redis.observability.svc:6379" {
			t.Errorf("REDIS_ADDR = %q", got)
		}
		if got, _ := findEnvVar(envs, "REDIS_DB"); got != "2" {
			t.Errorf("REDIS_DB = %q", got)
		}
		if _, ok := findEnvVar(envs, "REDIS_PASSWORD"); ok {
			t.Error("REDIS_PASSWORD set without a secret configured")
		}
	})

	t.Run("password comes from the secret ref", func(t *testing.T) {
		t.Setenv("REDIS_ADDR", "redis.observability.svc:6379")
		t.Setenv("REDIS_PASSWORD_SECRET_NAME", "redis-auth")
		t.Setenv("REDIS_PASSWORD_SECRET_KEY", "password")

		envs := buildEnvVars(tc, storage)
		for _, e := range envs {
			if e.Name != "REDIS_PASSWORD" {
				continue
			}
			if e.Value != "" {
				t.Error("REDIS_PASSWORD must come from a secret ref, not a literal value")
			}
			ref := e.ValueFrom.SecretKeyRef
			if ref == nil || ref.Name != "redis-auth" || ref.Key != "password" {
				t.Errorf("REDIS_PASSWORD secret ref = %+v", e.ValueFrom)
			}
			return
		}
		t.Error("REDIS_PASSWORD env var missing")
	})
}
