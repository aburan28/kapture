"use client";

/**
 * Built-in plugin: cardinality — HyperLogLog estimates of unique client
 * IPs and 5-tuples, plus flow-filter saturation.
 */

import type { KapturePlugin, PanelProps } from "@/lib/plugins/types";
import { StatTile, formatNumber } from "@/components/viz";

function CardinalityPanel({ snapshot }: PanelProps) {
  const fill = snapshot.flowFilterFill;
  return (
    <div className="grid grid-cols-3 gap-6">
      <StatTile
        label="Unique client IPs"
        value={formatNumber(snapshot.uniqueClientIPs)}
        sub="HyperLogLog, ~1.6% error"
      />
      <StatTile
        label="Unique flows"
        value={formatNumber(snapshot.uniqueFlows)}
        sub="protocol · client · host"
      />
      <StatTile
        label="Flow filter fill"
        value={`${(fill * 100).toFixed(1)}%`}
        sub={
          fill > 0.5
            ? "saturating — new-flow counts degrade"
            : "membership filter healthy"
        }
      />
    </div>
  );
}

const plugin: KapturePlugin = {
  id: "cardinality",
  name: "Cardinality",
  version: "1.0.0",
  description: "Distinct-count estimates for clients and flows.",
  panels: [
    {
      id: "estimates",
      title: "Cardinality",
      description: "Sketch-based distinct counts",
      width: "full",
      order: 5,
      component: CardinalityPanel,
    },
  ],
};

export default plugin;
