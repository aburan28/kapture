import { describe, it, expect, afterEach } from "vitest";
import {
  registerPlugin,
  unregisterPlugin,
  listPanels,
  listPlugins,
} from "../registry";
import type { KapturePlugin } from "../types";

function testPlugin(id: string, panelIds: string[], order?: number): KapturePlugin {
  return {
    id,
    name: id,
    panels: panelIds.map((pid) => ({
      id: pid,
      title: pid,
      order,
      component: () => null,
    })),
  };
}

const registered: string[] = [];
function register(p: KapturePlugin) {
  registerPlugin(p);
  registered.push(p.id);
}

afterEach(() => {
  while (registered.length) {
    unregisterPlugin(registered.pop()!);
  }
});

describe("plugin registry", () => {
  it("registers plugins and lists their panels with stable keys", () => {
    register(testPlugin("test-alpha", ["one", "two"]));
    const keys = listPanels().map((p) => p.key);
    expect(keys).toContain("test-alpha:one");
    expect(keys).toContain("test-alpha:two");
    expect(listPlugins().some((p) => p.id === "test-alpha")).toBe(true);
  });

  it("orders panels by their order hint", () => {
    register(testPlugin("test-late", ["p"], 500));
    register(testPlugin("test-early", ["p"], 1));
    const keys = listPanels().map((p) => p.key);
    expect(keys.indexOf("test-early:p")).toBeLessThan(
      keys.indexOf("test-late:p")
    );
  });

  it("rejects non-kebab-case plugin ids", () => {
    expect(() => registerPlugin(testPlugin("Bad Id", ["p"]))).toThrow();
    expect(() => registerPlugin(testPlugin("", ["p"]))).toThrow();
  });

  it("rejects duplicate panel ids within a plugin", () => {
    expect(() => registerPlugin(testPlugin("test-dup", ["p", "p"]))).toThrow();
  });

  it("re-registering the same plugin id replaces it", () => {
    register(testPlugin("test-replace", ["old"]));
    register(testPlugin("test-replace", ["new"]));
    const keys = listPanels().map((p) => p.key);
    expect(keys).toContain("test-replace:new");
    expect(keys).not.toContain("test-replace:old");
  });
});
