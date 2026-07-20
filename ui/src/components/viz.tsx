"use client";

/**
 * Chart primitives for analytics panels. Plain HTML/CSS marks with the
 * validated dark-surface categorical palette (see docs/ui-plugins.md);
 * text always wears text tokens, color carries identity/magnitude only.
 */

import { ReactNode } from "react";

/**
 * Categorical slots validated against the dashboard surface (#101828):
 * CVD ΔE 13.0, normal ΔE 19.3, all ≥3:1 contrast. Assigned in fixed
 * order by entity, never cycled.
 */
export const SERIES = ["#3987e5", "#008300", "#d55181", "#c98500"] as const;

/** Fixed method → slot assignment so colors follow the entity. */
const METHOD_SLOT: Record<string, number> = {
  GET: 0,
  POST: 1,
  PUT: 2,
  DELETE: 3,
};
const OTHER_COLOR = "#6b7280"; // gray-500: the "Other" fold, not a series

export function methodColor(method: string): string {
  const slot = METHOD_SLOT[method.toUpperCase()];
  return slot === undefined ? OTHER_COLOR : SERIES[slot];
}

export function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 10_000) return `${(n / 1000).toFixed(1)}k`;
  return n.toLocaleString("en-US");
}

export function formatBytes(n: number): string {
  if (n >= 1 << 30) return `${(n / (1 << 30)).toFixed(1)} GiB`;
  if (n >= 1 << 20) return `${(n / (1 << 20)).toFixed(1)} MiB`;
  if (n >= 1 << 10) return `${(n / (1 << 10)).toFixed(1)} KiB`;
  return `${Math.round(n)} B`;
}

// ---------------------------------------------------------------------------
// Column chart: one measure over time windows (single series).
// ---------------------------------------------------------------------------

export interface ColumnDatum {
  label: string;
  value: number;
  /** Marks the in-progress window (rendered hollow). */
  partial?: boolean;
}

export function ColumnChart({
  data,
  valueFormatter = formatNumber,
  height = 112,
}: {
  data: ColumnDatum[];
  valueFormatter?: (n: number) => string;
  height?: number;
}) {
  if (data.length === 0) {
    return <EmptyNote note="No completed windows yet." />;
  }
  const max = Math.max(...data.map((d) => d.value), 1);
  const maxIndex = data.findIndex((d) => d.value === max);

  return (
    <div>
      <div className="flex items-end gap-0.5" style={{ height }}>
        {data.map((d, i) => {
          const h = Math.max((d.value / max) * (height - 18), 2);
          return (
            <div
              key={`${d.label}-${i}`}
              className="group relative flex-1 flex flex-col items-center justify-end self-stretch"
            >
              {/* Selective direct label: the maximum only. */}
              {i === maxIndex && d.value > 0 && (
                <span className="text-[10px] text-gray-400 mb-0.5 tabular-nums">
                  {valueFormatter(d.value)}
                </span>
              )}
              <div
                className="w-full rounded-t"
                style={{
                  height: h,
                  background: d.partial ? "transparent" : SERIES[0],
                  border: d.partial ? `1.5px dashed ${SERIES[0]}` : "none",
                  minWidth: 3,
                }}
              />
              <div
                role="tooltip"
                className="pointer-events-none absolute bottom-full left-1/2 z-10 mb-1 hidden -translate-x-1/2 whitespace-nowrap rounded-md border border-gray-700 bg-gray-950 px-2 py-1 text-xs text-gray-200 group-hover:block"
              >
                {d.label}
                {d.partial ? " (in progress)" : ""} ·{" "}
                <span className="tabular-nums">{valueFormatter(d.value)}</span>
              </div>
            </div>
          );
        })}
      </div>
      <div className="mt-1 flex justify-between border-t border-gray-800 pt-1 text-[10px] text-gray-500">
        <span>{data[0].label}</span>
        <span>{data[data.length - 1].label}</span>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Bar list: ranked keys with magnitudes (table-with-bars hybrid).
// ---------------------------------------------------------------------------

export function BarList({
  items,
  color = SERIES[0],
  valueFormatter = formatNumber,
}: {
  items: { key: string; count: number }[];
  color?: string;
  valueFormatter?: (n: number) => string;
}) {
  if (items.length === 0) {
    return <EmptyNote note="Nothing observed yet." />;
  }
  const max = Math.max(...items.map((e) => e.count), 1);
  return (
    <ul className="space-y-2">
      {items.map((e) => (
        <li key={e.key}>
          <div className="mb-0.5 flex items-baseline justify-between gap-3 text-xs">
            <span className="truncate font-mono text-gray-300" title={e.key}>
              {e.key}
            </span>
            <span className="tabular-nums text-gray-400">
              {valueFormatter(e.count)}
            </span>
          </div>
          <div className="h-1.5 rounded-full bg-gray-800">
            <div
              className="h-full rounded-full"
              style={{ width: `${(e.count / max) * 100}%`, background: color }}
            />
          </div>
        </li>
      ))}
    </ul>
  );
}

// ---------------------------------------------------------------------------
// Segment bar: one categorical breakdown (methods, protocols).
// ---------------------------------------------------------------------------

export function SegmentBar({
  segments,
}: {
  segments: { label: string; value: number; color: string }[];
}) {
  const total = segments.reduce((sum, s) => sum + s.value, 0);
  if (total === 0) {
    return <EmptyNote note="Nothing observed yet." />;
  }
  return (
    <div>
      {/* 2px surface gaps between fills. */}
      <div className="flex h-3 gap-[2px] overflow-hidden rounded-full">
        {segments.map((s) => (
          <div
            key={s.label}
            title={`${s.label}: ${formatNumber(s.value)}`}
            style={{
              width: `${(s.value / total) * 100}%`,
              background: s.color,
              minWidth: s.value > 0 ? 3 : 0,
            }}
          />
        ))}
      </div>
      {/* Legend: identity never by color alone. */}
      <ul className="mt-3 space-y-1">
        {segments.map((s) => (
          <li key={s.label} className="flex items-center gap-2 text-xs">
            <span
              className="h-2.5 w-2.5 shrink-0 rounded-sm"
              style={{ background: s.color }}
            />
            <span className="text-gray-300">{s.label}</span>
            <span className="ml-auto tabular-nums text-gray-400">
              {formatNumber(s.value)}
              <span className="ml-1.5 text-gray-500">
                {((s.value / total) * 100).toFixed(1)}%
              </span>
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Quantile bars: p50/p90/p99/max of one measure, direct-labeled.
// ---------------------------------------------------------------------------

export function QuantileBars({
  summary,
  valueFormatter,
}: {
  summary: { p50: number; p90: number; p99: number; max: number };
  valueFormatter: (n: number) => string;
}) {
  const rows = [
    { label: "p50", value: summary.p50 },
    { label: "p90", value: summary.p90 },
    { label: "p99", value: summary.p99 },
    { label: "max", value: summary.max },
  ];
  const max = Math.max(...rows.map((r) => r.value), 1);
  return (
    <ul className="space-y-2">
      {rows.map((r) => (
        <li key={r.label} className="flex items-center gap-3 text-xs">
          <span className="w-8 text-gray-400">{r.label}</span>
          <div className="h-1.5 flex-1 rounded-full bg-gray-800">
            <div
              className="h-full rounded-full"
              style={{ width: `${(r.value / max) * 100}%`, background: SERIES[0] }}
            />
          </div>
          <span className="w-20 text-right tabular-nums text-gray-300">
            {valueFormatter(r.value)}
          </span>
        </li>
      ))}
    </ul>
  );
}

// ---------------------------------------------------------------------------
// Stat tile: a hero number (not a chart).
// ---------------------------------------------------------------------------

export function StatTile({
  label,
  value,
  sub,
}: {
  label: string;
  value: ReactNode;
  sub?: string;
}) {
  return (
    <div>
      <p className="mb-1 text-xs text-gray-400">{label}</p>
      <p className="text-3xl font-bold tabular-nums text-white">{value}</p>
      {sub && <p className="mt-1 text-xs text-gray-500">{sub}</p>}
    </div>
  );
}

function EmptyNote({ note }: { note: string }) {
  return <p className="py-6 text-center text-sm text-gray-500">{note}</p>;
}
