package replay

import (
	"fmt"
	"plugin"
)

const pluginSymbolName = "NewReplayPlugin"

// LoadPlugin loads a ReplayPlugin from a Go plugin shared object (.so) file.
// The .so file must export a symbol named "NewReplayPlugin" matching the
// [PluginConstructor] signature.
func LoadPlugin(path string) (ReplayPlugin, error) {
	p, err := plugin.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open plugin %q: %w", path, err)
	}

	sym, err := p.Lookup(pluginSymbolName)
	if err != nil {
		return nil, fmt.Errorf("lookup symbol %q in plugin %q: %w", pluginSymbolName, path, err)
	}

	constructor, ok := sym.(func() (ReplayPlugin, error))
	if !ok {
		return nil, fmt.Errorf("symbol %q in plugin %q has wrong type %T, expected func() (ReplayPlugin, error)", pluginSymbolName, path, sym)
	}

	rp, err := constructor()
	if err != nil {
		return nil, fmt.Errorf("construct plugin from %q: %w", path, err)
	}

	return rp, nil
}

// LoadPlugins loads multiple plugins from .so file paths.
// Returns an error if any plugin fails to load. Plugin names must be unique.
func LoadPlugins(paths []string) ([]ReplayPlugin, error) {
	plugins := make([]ReplayPlugin, 0, len(paths))
	names := make(map[string]string) // name -> path

	for _, path := range paths {
		p, err := LoadPlugin(path)
		if err != nil {
			// Clean up already-loaded plugins.
			for _, loaded := range plugins {
				_ = loaded.Close()
			}
			return nil, err
		}

		name := p.Name()
		if existingPath, exists := names[name]; exists {
			_ = p.Close()
			for _, loaded := range plugins {
				_ = loaded.Close()
			}
			return nil, fmt.Errorf("duplicate plugin name %q: %q and %q", name, existingPath, path)
		}

		names[name] = path
		plugins = append(plugins, p)
	}

	return plugins, nil
}
