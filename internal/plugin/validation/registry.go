package validation

import (
	"fmt"
	"sync"
)

// Registry collects plugin constraints and validates entire pipelines.
// It is safe for concurrent use.
type Registry struct {
	mu          sync.RWMutex
	constraints map[string]*Constraints
}

// NewRegistry creates an empty validation registry.
func NewRegistry() *Registry {
	return &Registry{constraints: make(map[string]*Constraints)}
}

// Register adds constraints for a named plugin. Returns an error if the
// plugin name is already registered.
func (r *Registry) Register(name string, c *Constraints) error {
	if name == "" {
		return fmt.Errorf("plugin name must not be empty")
	}
	if c == nil {
		c = &Constraints{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.constraints[name]; exists {
		return fmt.Errorf("validation constraints for plugin %q already registered", name)
	}
	r.constraints[name] = c
	return nil
}

// Has reports whether constraints are registered for the named plugin.
func (r *Registry) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.constraints[name]
	return ok
}

// Names returns all registered plugin names.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.constraints))
	for name := range r.constraints {
		names = append(names, name)
	}
	return names
}

// ValidatePipeline checks an ordered list of plugin references against all
// registered constraints. It returns all detected errors, not just the first.
// An empty return means the pipeline is valid.
func (r *Registry) ValidatePipeline(pipeline []PluginRef) []*ValidationError {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var errs []*ValidationError

	// Build index: plugin name -> position(s) in pipeline.
	positions := make(map[string][]int, len(pipeline))
	for i, ref := range pipeline {
		positions[ref.Name] = append(positions[ref.Name], i)
	}

	// Phase 1: Check that all plugins are registered.
	errs = append(errs, r.checkUnknown(pipeline)...)

	// Phase 2: Validate each plugin's config.
	errs = append(errs, r.checkConfigs(pipeline)...)

	// Phase 3: Check direct conflicts.
	errs = append(errs, r.checkConflicts(pipeline, positions)...)

	// Phase 4: Check ordering constraints.
	errs = append(errs, r.checkOrdering(pipeline, positions)...)

	// Phase 5: Check resource contention.
	errs = append(errs, r.checkResources(pipeline)...)

	return errs
}

// ValidateConfig validates a single plugin's configuration in isolation.
func (r *Registry) ValidateConfig(name string, config map[string]any) error {
	r.mu.RLock()
	c, ok := r.constraints[name]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("unknown plugin %q", name)
	}
	if c.ConfigValidator == nil {
		return nil
	}
	return c.ConfigValidator(config)
}

func (r *Registry) checkUnknown(pipeline []PluginRef) []*ValidationError {
	var errs []*ValidationError
	seen := make(map[string]bool)
	for _, ref := range pipeline {
		if seen[ref.Name] {
			continue
		}
		seen[ref.Name] = true
		if _, ok := r.constraints[ref.Name]; !ok {
			errs = append(errs, &ValidationError{
				Plugin:   ref.Name,
				Category: CategoryUnknown,
				Message:  fmt.Sprintf("plugin %q is not registered", ref.Name),
			})
		}
	}
	return errs
}

func (r *Registry) checkConfigs(pipeline []PluginRef) []*ValidationError {
	var errs []*ValidationError
	for _, ref := range pipeline {
		c, ok := r.constraints[ref.Name]
		if !ok || c.ConfigValidator == nil {
			continue
		}
		if err := c.ConfigValidator(ref.Config); err != nil {
			errs = append(errs, &ValidationError{
				Plugin:   ref.Name,
				Category: CategoryConfig,
				Message:  err.Error(),
			})
		}
	}
	return errs
}

func (r *Registry) checkConflicts(pipeline []PluginRef, positions map[string][]int) []*ValidationError {
	var errs []*ValidationError
	reported := make(map[[2]string]bool)

	for _, ref := range pipeline {
		c, ok := r.constraints[ref.Name]
		if !ok {
			continue
		}
		for _, conflictName := range c.ConflictsWith {
			if _, present := positions[conflictName]; !present {
				continue
			}
			// Normalize pair to avoid duplicate reports.
			pair := normalizePair(ref.Name, conflictName)
			if reported[pair] {
				continue
			}
			reported[pair] = true

			reason := "plugins are incompatible"
			if c.ConflictReasons != nil {
				if r, ok := c.ConflictReasons[conflictName]; ok {
					reason = r
				}
			}
			errs = append(errs, &ValidationError{
				Plugin:   ref.Name,
				Related:  conflictName,
				Category: CategoryConflict,
				Message:  reason,
			})
		}
	}
	return errs
}

func (r *Registry) checkOrdering(pipeline []PluginRef, positions map[string][]int) []*ValidationError {
	var errs []*ValidationError

	for i, ref := range pipeline {
		c, ok := r.constraints[ref.Name]
		if !ok {
			continue
		}

		// MustRunBefore: this plugin must appear before each listed plugin.
		for _, before := range c.MustRunBefore {
			for _, otherPos := range positions[before] {
				if i >= otherPos {
					errs = append(errs, &ValidationError{
						Plugin:   ref.Name,
						Related:  before,
						Category: CategoryOrder,
						Message: fmt.Sprintf(
							"%q must appear before %q (at index %d, but %q is at index %d)",
							ref.Name, before, i, before, otherPos),
					})
				}
			}
		}

		// MustRunAfter: this plugin must appear after each listed plugin.
		for _, after := range c.MustRunAfter {
			for _, otherPos := range positions[after] {
				if i <= otherPos {
					errs = append(errs, &ValidationError{
						Plugin:   ref.Name,
						Related:  after,
						Category: CategoryOrder,
						Message: fmt.Sprintf(
							"%q must appear after %q (at index %d, but %q is at index %d)",
							ref.Name, after, i, after, otherPos),
					})
				}
			}
		}
	}
	return errs
}

func (r *Registry) checkResources(pipeline []PluginRef) []*ValidationError {
	var errs []*ValidationError

	// Collect all claims per resource.
	type claim struct {
		plugin string
		access ResourceAccess
	}
	resourceClaims := make(map[string][]claim)

	for _, ref := range pipeline {
		c, ok := r.constraints[ref.Name]
		if !ok {
			continue
		}
		for _, rc := range c.Resources {
			resourceClaims[rc.Name] = append(resourceClaims[rc.Name], claim{
				plugin: ref.Name,
				access: rc.Access,
			})
		}
	}

	// Detect contention: exclusive + anything else = conflict.
	reported := make(map[[2]string]bool)
	for resource, claims := range resourceClaims {
		for i, a := range claims {
			if a.access != Exclusive {
				continue
			}
			for j, b := range claims {
				if i == j || a.plugin == b.plugin {
					continue
				}
				pair := normalizePair(a.plugin, b.plugin)
				if reported[pair] {
					continue
				}
				reported[pair] = true
				errs = append(errs, &ValidationError{
					Plugin:   a.plugin,
					Related:  b.plugin,
					Category: CategoryResource,
					Message: fmt.Sprintf(
						"resource %q contention: %q claims exclusive access but %q also claims %s access",
						resource, a.plugin, b.plugin, b.access),
				})
			}
		}
	}
	return errs
}

func normalizePair(a, b string) [2]string {
	if a > b {
		return [2]string{b, a}
	}
	return [2]string{a, b}
}
