"use client";

/**
 * Built-in plugin: quantiles — DDSketch distributions for request body
 * sizes and capture handling latency (1% relative-error guarantee).
 */

import type { KapturePlugin, PanelProps } from "@/lib/plugins/types";
import { QuantileBars, formatBytes, formatNumber } from "@/components/viz";

function formatMicros(us: number): string {
  if (us >= 1_000_000) return `${(us / 1_000_000).toFixed(2)} s`;
  if (us >= 1000) return `${(us / 1000).toFixed(1)} ms`;
  return `${Math.round(us)} µs`;
}

function BodySizes({ snapshot }: PanelProps) {
  const s = snapshot.bodySizeBytes;
  return (
    <div>
      <QuantileBars summary={s} valueFormatter={formatBytes} />
      <p className="mt-3 text-xs text-gray-500">
        {formatNumber(s.count)} observations · mean {formatBytes(s.mean)}
      </p>
    </div>
  );
}

function HandleLatency({ snapshot }: PanelProps) {
  const s = snapshot.handleLatencyMicros;
  return (
    <div>
      <QuantileBars summary={s} valueFormatter={formatMicros} />
      <p className="mt-3 text-xs text-gray-500">
        {formatNumber(s.count)} observations · mean {formatMicros(s.mean)}
      </p>
    </div>
  );
}

const plugin: KapturePlugin = {
  id: "quantiles",
  name: "Quantiles",
  version: "1.0.0",
  description: "DDSketch distributions: body sizes and handling latency.",
  panels: [
    {
      id: "body-sizes",
      title: "Body size distribution",
      description: "DDSketch, 1% relative error",
      width: "half",
      order: 30,
      component: BodySizes,
    },
    {
      id: "handle-latency",
      title: "Capture handling latency",
      description: "Filter → redact → enqueue",
      width: "half",
      order: 31,
      component: HandleLatency,
    },
  ],
};

export default plugin;
