import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { PanelGrid } from "../panel-grid";
import { registerPlugin, unregisterPlugin } from "@/lib/plugins/registry";
import type { PanelProps } from "@/lib/plugins/types";
import { mockStatsSnapshot } from "@/lib/mock-data";

const context = {
  captureId: "shop/orders-capture",
  generatedAt: new Date().toISOString(),
  mock: true,
};

describe("PanelGrid", () => {
  it("renders the built-in statistics panels from the registry", () => {
    render(<PanelGrid snapshot={mockStatsSnapshot} context={context} />);

    // One representative panel per built-in plugin.
    expect(screen.getByText("Cardinality")).toBeInTheDocument();
    expect(screen.getByText("Requests per window")).toBeInTheDocument();
    expect(screen.getByText("Top paths")).toBeInTheDocument();
    expect(screen.getByText("Body size distribution")).toBeInTheDocument();

    // Data flows into panels: hottest path from the snapshot is shown.
    expect(screen.getByText("/api/v1/orders")).toBeInTheDocument();
    // Identity is never color-alone: method legend labels are present.
    expect(screen.getByText("GET")).toBeInTheDocument();
  });

  it("renders custom plugin panels with data and context", () => {
    function CustomPanel({ snapshot, context }: PanelProps) {
      return (
        <div>
          custom sees {snapshot.uniqueClientIPs} clients of {context.captureId}
        </div>
      );
    }
    registerPlugin({
      id: "test-custom",
      name: "Test Custom",
      panels: [{ id: "panel", title: "My Custom Panel", component: CustomPanel }],
    });
    try {
      render(<PanelGrid snapshot={mockStatsSnapshot} context={context} />);
      expect(screen.getByText("My Custom Panel")).toBeInTheDocument();
      expect(
        screen.getByText(
          `custom sees ${mockStatsSnapshot.uniqueClientIPs} clients of shop/orders-capture`
        )
      ).toBeInTheDocument();
    } finally {
      unregisterPlugin("test-custom");
    }
  });
});
