package storage

import (
	"context"
	"fmt"
	goplugin "plugin"
)

const defaultPluginSymbol = "NewWriterFactory"

type PluginConfig struct {
	Path   string
	Symbol string
	Config map[string]any
}

type PluginFactoryBuilder func(context.Context, map[string]any) (WriterFactory, error)

func ValidatePluginConfig(cfg PluginConfig) error {
	_, err := loadPluginBuilder(cfg)
	return err
}

func NewPluginWriterFactory(ctx context.Context, cfg PluginConfig) (WriterFactory, error) {
	builder, err := loadPluginBuilder(cfg)
	if err != nil {
		return nil, err
	}
	if builder == nil {
		return nil, fmt.Errorf("plugin factory builder cannot be nil")
	}
	factory, err := builder(ctx, cfg.Config)
	if err != nil {
		return nil, fmt.Errorf("invoke plugin factory builder: %w", err)
	}
	if factory == nil {
		return nil, fmt.Errorf("plugin factory builder returned nil factory")
	}
	return factory, nil
}

func loadPluginBuilder(cfg PluginConfig) (PluginFactoryBuilder, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("plugin path is required")
	}
	symbolName := cfg.Symbol
	if symbolName == "" {
		symbolName = defaultPluginSymbol
	}

	pl, err := goplugin.Open(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("open plugin %q: %w", cfg.Path, err)
	}
	sym, err := pl.Lookup(symbolName)
	if err != nil {
		return nil, fmt.Errorf("lookup plugin symbol %q: %w", symbolName, err)
	}

	builder, ok := sym.(func(context.Context, map[string]any) (WriterFactory, error))
	if !ok {
		return nil, fmt.Errorf("plugin symbol %q has incompatible type %T", symbolName, sym)
	}
	return builder, nil
}
