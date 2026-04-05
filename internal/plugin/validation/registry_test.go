package validation

import (
	"errors"
	"testing"
)

func TestRegistryRegister(t *testing.T) {
	t.Run("successful registration", func(t *testing.T) {
		r := NewRegistry()
		if err := r.Register("redact", &Constraints{}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !r.Has("redact") {
			t.Fatal("expected registry to have 'redact'")
		}
	})

	t.Run("nil constraints", func(t *testing.T) {
		r := NewRegistry()
		if err := r.Register("noop", nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !r.Has("noop") {
			t.Fatal("expected registry to have 'noop'")
		}
	})

	t.Run("duplicate registration", func(t *testing.T) {
		r := NewRegistry()
		_ = r.Register("redact", &Constraints{})
		err := r.Register("redact", &Constraints{})
		if err == nil {
			t.Fatal("expected error for duplicate registration")
		}
	})

	t.Run("empty name", func(t *testing.T) {
		r := NewRegistry()
		err := r.Register("", &Constraints{})
		if err == nil {
			t.Fatal("expected error for empty name")
		}
	})
}

func TestRegistryNames(t *testing.T) {
	r := NewRegistry()
	_ = r.Register("a", nil)
	_ = r.Register("b", nil)
	_ = r.Register("c", nil)

	names := r.Names()
	if len(names) != 3 {
		t.Fatalf("expected 3 names, got %d", len(names))
	}
}

func TestValidatePipeline_EmptyPipeline(t *testing.T) {
	r := NewRegistry()
	errs := r.ValidatePipeline(nil)
	if len(errs) != 0 {
		t.Fatalf("expected no errors for empty pipeline, got %d", len(errs))
	}
}

func TestValidatePipeline_UnknownPlugin(t *testing.T) {
	r := NewRegistry()
	errs := r.ValidatePipeline([]PluginRef{{Name: "missing"}})
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}
	if errs[0].Category != CategoryUnknown {
		t.Errorf("expected category %q, got %q", CategoryUnknown, errs[0].Category)
	}
}

func TestValidatePipeline_ConfigValidation(t *testing.T) {
	r := NewRegistry()
	_ = r.Register("strict", &Constraints{
		ConfigValidator: func(cfg map[string]any) error {
			if _, ok := cfg["required_field"]; !ok {
				return errors.New("required_field is missing")
			}
			return nil
		},
	})

	t.Run("invalid config", func(t *testing.T) {
		errs := r.ValidatePipeline([]PluginRef{
			{Name: "strict", Config: map[string]any{}},
		})
		if len(errs) != 1 {
			t.Fatalf("expected 1 error, got %d", len(errs))
		}
		if errs[0].Category != CategoryConfig {
			t.Errorf("expected category %q, got %q", CategoryConfig, errs[0].Category)
		}
	})

	t.Run("valid config", func(t *testing.T) {
		errs := r.ValidatePipeline([]PluginRef{
			{Name: "strict", Config: map[string]any{"required_field": true}},
		})
		if len(errs) != 0 {
			t.Fatalf("expected 0 errors, got %d", len(errs))
		}
	})
}

func TestValidatePipeline_ConflictDetection(t *testing.T) {
	r := NewRegistry()
	_ = r.Register("redact", &Constraints{
		ConflictsWith:   []string{"encrypt"},
		ConflictReasons: map[string]string{"encrypt": "both modify body in incompatible ways"},
	})
	_ = r.Register("encrypt", &Constraints{})
	_ = r.Register("metrics", &Constraints{})

	t.Run("conflicting plugins", func(t *testing.T) {
		errs := r.ValidatePipeline([]PluginRef{
			{Name: "redact"},
			{Name: "encrypt"},
		})
		if len(errs) != 1 {
			t.Fatalf("expected 1 error, got %d", len(errs))
		}
		if errs[0].Category != CategoryConflict {
			t.Errorf("expected category %q, got %q", CategoryConflict, errs[0].Category)
		}
		if errs[0].Message != "both modify body in incompatible ways" {
			t.Errorf("unexpected message: %s", errs[0].Message)
		}
	})

	t.Run("no conflict", func(t *testing.T) {
		errs := r.ValidatePipeline([]PluginRef{
			{Name: "redact"},
			{Name: "metrics"},
		})
		if len(errs) != 0 {
			t.Fatalf("expected 0 errors, got %d", len(errs))
		}
	})

	t.Run("conflict reported once not twice", func(t *testing.T) {
		// Even if we add bidirectional conflict declarations, only one error per pair.
		r2 := NewRegistry()
		_ = r2.Register("a", &Constraints{ConflictsWith: []string{"b"}})
		_ = r2.Register("b", &Constraints{ConflictsWith: []string{"a"}})

		errs := r2.ValidatePipeline([]PluginRef{
			{Name: "a"},
			{Name: "b"},
		})
		if len(errs) != 1 {
			t.Fatalf("expected exactly 1 conflict error, got %d", len(errs))
		}
	})
}

func TestValidatePipeline_OrderingConstraints(t *testing.T) {
	r := NewRegistry()
	_ = r.Register("auth", &Constraints{
		MustRunBefore: []string{"redact"},
	})
	_ = r.Register("redact", &Constraints{
		MustRunAfter: []string{"auth"},
	})
	_ = r.Register("metrics", &Constraints{})

	t.Run("correct order", func(t *testing.T) {
		errs := r.ValidatePipeline([]PluginRef{
			{Name: "auth"},
			{Name: "redact"},
			{Name: "metrics"},
		})
		if len(errs) != 0 {
			t.Fatalf("expected 0 errors, got %d: %v", len(errs), errs)
		}
	})

	t.Run("wrong order", func(t *testing.T) {
		errs := r.ValidatePipeline([]PluginRef{
			{Name: "redact"},
			{Name: "auth"},
		})
		// Both auth.MustRunBefore and redact.MustRunAfter should fire.
		if len(errs) != 2 {
			t.Fatalf("expected 2 ordering errors, got %d: %v", len(errs), errs)
		}
		for _, e := range errs {
			if e.Category != CategoryOrder {
				t.Errorf("expected category %q, got %q", CategoryOrder, e.Category)
			}
		}
	})

	t.Run("ordering constraint ignored for absent plugin", func(t *testing.T) {
		errs := r.ValidatePipeline([]PluginRef{
			{Name: "auth"},
			{Name: "metrics"},
		})
		if len(errs) != 0 {
			t.Fatalf("expected 0 errors (redact not in pipeline), got %d", len(errs))
		}
	})
}

func TestValidatePipeline_ResourceContention(t *testing.T) {
	r := NewRegistry()
	_ = r.Register("redact", &Constraints{
		Resources: []ResourceClaim{{Name: "body", Access: Exclusive}},
	})
	_ = r.Register("compress", &Constraints{
		Resources: []ResourceClaim{{Name: "body", Access: Exclusive}},
	})
	_ = r.Register("log-body", &Constraints{
		Resources: []ResourceClaim{{Name: "body", Access: Shared}},
	})
	_ = r.Register("log-headers", &Constraints{
		Resources: []ResourceClaim{{Name: "headers", Access: Shared}},
	})

	t.Run("two exclusive claims", func(t *testing.T) {
		errs := r.ValidatePipeline([]PluginRef{
			{Name: "redact"},
			{Name: "compress"},
		})
		if len(errs) != 1 {
			t.Fatalf("expected 1 resource error, got %d: %v", len(errs), errs)
		}
		if errs[0].Category != CategoryResource {
			t.Errorf("expected category %q, got %q", CategoryResource, errs[0].Category)
		}
	})

	t.Run("exclusive vs shared", func(t *testing.T) {
		errs := r.ValidatePipeline([]PluginRef{
			{Name: "redact"},
			{Name: "log-body"},
		})
		if len(errs) != 1 {
			t.Fatalf("expected 1 resource error, got %d: %v", len(errs), errs)
		}
	})

	t.Run("two shared claims are fine", func(t *testing.T) {
		errs := r.ValidatePipeline([]PluginRef{
			{Name: "log-body"},
			{Name: "log-headers"},
		})
		if len(errs) != 0 {
			t.Fatalf("expected 0 errors, got %d", len(errs))
		}
	})

	t.Run("different resources no conflict", func(t *testing.T) {
		errs := r.ValidatePipeline([]PluginRef{
			{Name: "redact"},
			{Name: "log-headers"},
		})
		if len(errs) != 0 {
			t.Fatalf("expected 0 errors, got %d", len(errs))
		}
	})
}

func TestValidatePipeline_MultipleErrorCategories(t *testing.T) {
	r := NewRegistry()
	_ = r.Register("a", &Constraints{
		ConfigValidator: func(cfg map[string]any) error { return errors.New("bad config") },
		ConflictsWith:   []string{"b"},
		MustRunBefore:   []string{"c"},
		Resources:       []ResourceClaim{{Name: "body", Access: Exclusive}},
	})
	_ = r.Register("b", &Constraints{})
	_ = r.Register("c", &Constraints{
		Resources: []ResourceClaim{{Name: "body", Access: Shared}},
	})

	// Pipeline: c, b, a — violates ordering (a must be before c), has conflict (a vs b),
	// has config error, and has resource contention (a exclusive body vs c shared body).
	errs := r.ValidatePipeline([]PluginRef{
		{Name: "c"},
		{Name: "b"},
		{Name: "a", Config: map[string]any{}},
	})

	categories := make(map[ErrorCategory]int)
	for _, e := range errs {
		categories[e.Category]++
	}

	if categories[CategoryConfig] < 1 {
		t.Error("expected at least 1 config error")
	}
	if categories[CategoryConflict] < 1 {
		t.Error("expected at least 1 conflict error")
	}
	if categories[CategoryOrder] < 1 {
		t.Error("expected at least 1 ordering error")
	}
	if categories[CategoryResource] < 1 {
		t.Error("expected at least 1 resource error")
	}
}

func TestValidateConfig_Standalone(t *testing.T) {
	r := NewRegistry()
	_ = r.Register("strict", &Constraints{
		ConfigValidator: func(cfg map[string]any) error {
			if cfg == nil {
				return errors.New("config is nil")
			}
			return nil
		},
	})

	if err := r.ValidateConfig("strict", map[string]any{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := r.ValidateConfig("strict", nil); err == nil {
		t.Fatal("expected error for nil config")
	}

	if err := r.ValidateConfig("unknown", nil); err == nil {
		t.Fatal("expected error for unknown plugin")
	}
}

func TestValidationError_String(t *testing.T) {
	t.Run("with related", func(t *testing.T) {
		e := &ValidationError{
			Plugin: "a", Related: "b",
			Category: CategoryConflict, Message: "incompatible",
		}
		got := e.Error()
		want := "[conflict] a <-> b: incompatible"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("without related", func(t *testing.T) {
		e := &ValidationError{
			Plugin: "a", Category: CategoryConfig, Message: "bad",
		}
		got := e.Error()
		want := "[config] a: bad"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})
}

func TestResourceAccess_String(t *testing.T) {
	if Shared.String() != "shared" {
		t.Errorf("got %q", Shared.String())
	}
	if Exclusive.String() != "exclusive" {
		t.Errorf("got %q", Exclusive.String())
	}
	if ResourceAccess(99).String() != "unknown" {
		t.Errorf("got %q", ResourceAccess(99).String())
	}
}
