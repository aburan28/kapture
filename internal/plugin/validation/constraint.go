// Package validation provides a cross-cutting validation framework for plugin
// pipelines. It enables plugins to declare constraints — not just about their
// own configuration, but about what they conflict with across plugin boundaries.
//
// # Two-Phase Design
//
// Validation runs in two phases:
//
//  1. Registration: Each plugin registers a [ConstraintProvider] that declares
//     its config validation rules, cross-plugin conflicts, ordering requirements,
//     and resource claims.
//
//  2. Evaluation: When a pipeline is assembled, the [Registry] evaluates all
//     declared constraints together, catching conflicts that no single plugin
//     could detect in isolation.
//
// # Constraint Types
//
// Four constraint categories address different cross-cutting concerns:
//
//   - Config: Validates a plugin's own configuration parameters
//   - Conflict: Declares incompatibility between specific plugin pairs
//   - Order: Requires a plugin to appear before or after another
//   - Resource: Claims shared or exclusive access to a named resource
//     (e.g., "body" or "headers"), detecting contention at assembly time
//
// # Usage
//
//	reg := validation.NewRegistry()
//	reg.Register("redact", &validation.Constraints{
//	    ConfigValidator: func(cfg map[string]any) error { ... },
//	    ConflictsWith:   []string{"encrypt"},
//	    Resources:       []validation.ResourceClaim{{Name: "body", Access: validation.Exclusive}},
//	})
//	errs := reg.ValidatePipeline([]validation.PluginRef{{Name: "redact", Config: ...}})
package validation

import "fmt"

// ResourceAccess defines how a plugin accesses a shared resource.
type ResourceAccess int

const (
	// Shared access allows multiple plugins to read a resource concurrently.
	Shared ResourceAccess = iota
	// Exclusive access requires that no other plugin (shared or exclusive)
	// claims the same resource.
	Exclusive
)

func (r ResourceAccess) String() string {
	switch r {
	case Shared:
		return "shared"
	case Exclusive:
		return "exclusive"
	default:
		return "unknown"
	}
}

// ResourceClaim declares that a plugin requires access to a named resource.
// Resources are abstract identifiers (e.g., "body", "headers", "metadata")
// that represent mutable parts of a captured request.
type ResourceClaim struct {
	// Name identifies the resource (e.g., "body", "headers").
	Name string

	// Access is how the plugin uses the resource.
	Access ResourceAccess
}

// Constraints declares a plugin's validation rules and cross-cutting concerns.
// All fields are optional — a plugin with no constraints is always valid.
type Constraints struct {
	// ConfigValidator validates the plugin's own configuration.
	// Return nil if the config is valid, or an error describing what's wrong.
	ConfigValidator func(config map[string]any) error

	// ConflictsWith lists plugin names that cannot coexist in the same pipeline.
	// The conflict is bidirectional: if "redact" conflicts with "encrypt",
	// then a pipeline containing both is invalid regardless of which registered
	// the conflict.
	ConflictsWith []string

	// ConflictReasons provides optional human-readable explanations for each
	// conflict. Keys are plugin names from ConflictsWith.
	ConflictReasons map[string]string

	// MustRunBefore lists plugin names that this plugin must precede in the
	// pipeline. If plugin A declares MustRunBefore: ["B"], then A must appear
	// at a lower index than B.
	MustRunBefore []string

	// MustRunAfter lists plugin names that this plugin must follow in the
	// pipeline. If plugin A declares MustRunAfter: ["B"], then A must appear
	// at a higher index than B.
	MustRunAfter []string

	// Resources declares resource claims for contention detection.
	// Two exclusive claims on the same resource, or an exclusive and shared
	// claim, are flagged as conflicts.
	Resources []ResourceClaim
}

// PluginRef identifies a plugin instance in a pipeline, pairing a registered
// name with its runtime configuration.
type PluginRef struct {
	// Name is the registered plugin name.
	Name string

	// Config is the plugin-specific configuration to validate.
	Config map[string]any
}

// ValidationError represents a single validation failure with enough context
// to produce a clear, actionable error message.
type ValidationError struct {
	// Plugin is the name of the plugin that caused or is involved in the error.
	Plugin string

	// Related is the other plugin involved (for conflict/ordering errors).
	Related string

	// Category classifies the error for programmatic handling.
	Category ErrorCategory

	// Message is a human-readable description.
	Message string
}

func (e *ValidationError) Error() string {
	if e.Related != "" {
		return fmt.Sprintf("[%s] %s <-> %s: %s", e.Category, e.Plugin, e.Related, e.Message)
	}
	return fmt.Sprintf("[%s] %s: %s", e.Category, e.Plugin, e.Message)
}

// ErrorCategory classifies validation errors.
type ErrorCategory string

const (
	// CategoryConfig is a plugin config validation failure.
	CategoryConfig ErrorCategory = "config"

	// CategoryConflict is a direct plugin-to-plugin incompatibility.
	CategoryConflict ErrorCategory = "conflict"

	// CategoryOrder is a pipeline ordering violation.
	CategoryOrder ErrorCategory = "order"

	// CategoryResource is a resource contention conflict.
	CategoryResource ErrorCategory = "resource"

	// CategoryUnknown is for unregistered plugins referenced in a pipeline.
	CategoryUnknown ErrorCategory = "unknown"
)
