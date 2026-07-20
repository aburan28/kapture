/**
 * Plugin registry: build-time registration, render-time lookup.
 * Registration happens in `src/plugins/index.ts`; the analytics page
 * imports that module for its side effects and then reads the registry.
 */

import type { KapturePlugin, PanelDefinition } from "./types";

/** A panel joined with the plugin that contributed it. */
export interface RegisteredPanel {
  /** Stable grid key: `pluginId:panelId`. */
  key: string;
  plugin: Pick<KapturePlugin, "id" | "name" | "version">;
  panel: PanelDefinition;
}

const plugins = new Map<string, KapturePlugin>();

/**
 * Registers a plugin. Re-registering the same id replaces it (keeps
 * hot reload idempotent); an id collision between different plugins is
 * a programming error surfaced loudly.
 */
export function registerPlugin(plugin: KapturePlugin): void {
  if (!plugin.id || !/^[a-z0-9][a-z0-9-]*$/.test(plugin.id)) {
    throw new Error(`plugin id ${JSON.stringify(plugin.id)} must be kebab-case`);
  }
  const panelIds = new Set<string>();
  for (const panel of plugin.panels) {
    if (panelIds.has(panel.id)) {
      throw new Error(`plugin ${plugin.id}: duplicate panel id ${panel.id}`);
    }
    panelIds.add(panel.id);
  }
  plugins.set(plugin.id, plugin);
}

/** Removes a plugin (tests). */
export function unregisterPlugin(id: string): void {
  plugins.delete(id);
}

/** All registered plugins, registration order. */
export function listPlugins(): KapturePlugin[] {
  return [...plugins.values()];
}

/** All panels across plugins, sorted by order (then registration). */
export function listPanels(): RegisteredPanel[] {
  const out: RegisteredPanel[] = [];
  for (const plugin of plugins.values()) {
    for (const panel of plugin.panels) {
      out.push({
        key: `${plugin.id}:${panel.id}`,
        plugin: { id: plugin.id, name: plugin.name, version: plugin.version },
        panel,
      });
    }
  }
  return out.sort((a, b) => (a.panel.order ?? 100) - (b.panel.order ?? 100));
}
