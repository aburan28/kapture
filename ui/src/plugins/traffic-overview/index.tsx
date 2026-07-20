"use client";

/**
 * Built-in plugin: traffic overview — flow-window counters (requests,
 * bytes, new flows) and the method/protocol breakdown of the current
 * window.
 */

import type { KapturePlugin, PanelProps } from "@/lib/plugins/types";
import type { WindowCounts } from "@/lib/stats-types";
import {
  ColumnChart,
  SegmentBar,
  formatBytes,
  formatNumber,
  methodColor,
} from "@/components/viz";

function windowLabel(w: WindowCounts): string {
  const d = new Date(w.start);
  return `${String(d.getUTCHours()).padStart(2, "0")}:${String(d.getUTCMinutes()).padStart(2, "0")}`;
}

function allWindows(p: PanelProps): { w: WindowCounts; partial: boolean }[] {
  const out = (p.snapshot.completedWindows ?? []).map((w) => ({
    w,
    partial: false,
  }));
  if (p.snapshot.currentWindow) {
    out.push({ w: p.snapshot.currentWindow, partial: true });
  }
  return out;
}

function RequestsPerWindow(props: PanelProps) {
  return (
    <ColumnChart
      data={allWindows(props).map(({ w, partial }) => ({
        label: windowLabel(w),
        value: w.requests,
        partial,
      }))}
    />
  );
}

function BytesPerWindow(props: PanelProps) {
  return (
    <ColumnChart
      data={allWindows(props).map(({ w, partial }) => ({
        label: windowLabel(w),
        value: w.bytes,
        partial,
      }))}
      valueFormatter={formatBytes}
    />
  );
}

function MethodBreakdown(props: PanelProps) {
  const byMethod: Record<string, number> = {};
  for (const { w } of allWindows(props)) {
    for (const [method, count] of Object.entries(w.byMethod ?? {})) {
      byMethod[method] = (byMethod[method] ?? 0) + count;
    }
  }
  // Fixed slot order; everything beyond the assigned methods folds into
  // Other so hues are never cycled.
  const known = ["GET", "POST", "PUT", "DELETE"];
  const segments = known
    .filter((m) => byMethod[m])
    .map((m) => ({ label: m, value: byMethod[m], color: methodColor(m) }));
  const other = Object.entries(byMethod)
    .filter(([m]) => !known.includes(m))
    .reduce((sum, [, v]) => sum + v, 0);
  if (other > 0) {
    segments.push({ label: "Other", value: other, color: methodColor("other") });
  }
  return <SegmentBar segments={segments} />;
}

function NewFlowsPerWindow(props: PanelProps) {
  return (
    <ColumnChart
      data={allWindows(props).map(({ w, partial }) => ({
        label: windowLabel(w),
        value: w.newFlows,
        partial,
      }))}
      valueFormatter={formatNumber}
    />
  );
}

const plugin: KapturePlugin = {
  id: "traffic-overview",
  name: "Traffic Overview",
  version: "1.0.0",
  description: "Flow-window counters: request and byte rates, methods, new flows.",
  panels: [
    {
      id: "requests",
      title: "Requests per window",
      description: "Exact counts over tumbling flow windows",
      width: "half",
      order: 10,
      component: RequestsPerWindow,
    },
    {
      id: "bytes",
      title: "Bytes per window",
      width: "half",
      order: 11,
      component: BytesPerWindow,
    },
    {
      id: "methods",
      title: "Methods",
      description: "All retained windows",
      width: "half",
      order: 12,
      component: MethodBreakdown,
    },
    {
      id: "new-flows",
      title: "New flows per window",
      description: "First-seen 5-tuples (Bloom filter)",
      width: "half",
      order: 13,
      component: NewFlowsPerWindow,
    },
  ],
};

export default plugin;
