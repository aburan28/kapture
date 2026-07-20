# Dashboard Plugins

The `/analytics` page of the Kapture dashboard is assembled from
**plugins**: self-contained modules that contribute statistics panels
and traffic-analysis features. The built-in panels (traffic overview,
cardinality, heavy hitters, quantiles) are themselves plugins with no
special privileges — anything they do, yours can do.

## The model

- A plugin is a directory under `ui/src/plugins/` whose `index.tsx`
  default-exports a `KapturePlugin` object: metadata plus a list of
  panels.
- A panel is an ordinary React component receiving `PanelProps`: the
  capture's streaming-statistics `snapshot` and a `context`
  (`captureId`, `generatedAt`, `mock`).
- Plugins are registered at build time in `ui/src/plugins/index.ts` —
  one `registerPlugin(...)` line. The panel grid renders every
  registered panel, sorted by its `order` hint, sized by its `width`
  hint (`third` | `half` | `full`).

The data contract is `StatsSnapshot` (`ui/src/lib/stats-types.ts`),
mirroring the agent's `/stats` JSON:

| Field | Backing sketch |
|-------|----------------|
| `currentWindow`, `completedWindows` | exact tumbling-window counters (requests, bytes, per-method/protocol, new flows) |
| `uniqueClientIPs`, `uniqueFlows` | HyperLogLog cardinality estimates |
| `topPaths`, `topClients` | Count-Min sketch top-K heavy hitters |
| `bodySizeBytes`, `handleLatencyMicros` | DDSketch quantiles (p50/p90/p99/max, count, mean) |
| `flowFilterFill` | Bloom filter saturation |

## Writing a plugin

1. Create the plugin module:

```tsx
// ui/src/plugins/error-budget/index.tsx
"use client";

import type { KapturePlugin, PanelProps } from "@/lib/plugins/types";
import { StatTile, formatNumber } from "@/components/viz";

function ErrorBudgetPanel({ snapshot, context }: PanelProps) {
  const windows = snapshot.completedWindows ?? [];
  const requests = windows.reduce((sum, w) => sum + w.requests, 0);
  return (
    <StatTile
      label={`Requests across ${windows.length} windows`}
      value={formatNumber(requests)}
      sub={context.captureId}
    />
  );
}

const plugin: KapturePlugin = {
  id: "error-budget",
  name: "Error Budget",
  version: "1.0.0",
  panels: [
    {
      id: "burn",
      title: "Request volume",
      width: "third",
      order: 50,
      component: ErrorBudgetPanel,
    },
  ],
};

export default plugin;
```

2. Register it:

```ts
// ui/src/plugins/index.ts
import errorBudget from "./error-budget";
registerPlugin(errorBudget);
```

That's the whole integration. `npm run dev` and the panel is on
`/analytics`. Panels can fetch additional data of their own (they are
client components — call your own API routes), so traffic-analysis
features beyond the snapshot are possible; the snapshot is simply the
data every panel gets for free.

## Chart building blocks

`ui/src/components/viz.tsx` exports primitives the built-ins use, so
custom panels stay visually consistent:

- `ColumnChart` — one measure over time windows (hover tooltips,
  in-progress window rendered dashed)
- `BarList` — ranked keys with magnitude bars (heavy-hitter style)
- `SegmentBar` — one categorical breakdown with legend
- `QuantileBars` — p50/p90/p99/max, direct-labeled
- `StatTile` — a hero number
- `SERIES`, `methodColor`, `formatNumber`, `formatBytes`

The `SERIES` palette is validated for color-vision deficiency and
contrast against the dashboard surface; assign its hues to entities in
a fixed order (as `methodColor` does) rather than cycling them, and
never encode identity by color alone — the primitives handle legends
and labels for you.

## Data modes

Without configuration the dashboard serves realistic mock data
(`context.mock === true`), so plugins can be developed with no cluster.
Point `AGENT_STATS_URL` at a capture agent's stats endpoint (e.g. a
`kubectl port-forward` to `:8081/stats`) for live data. Statistics are
also published to Redis when the agent runs with `REDIS_ADDR` — see
[quickstart.md](quickstart.md).

## Rules

- Plugin `id` is kebab-case and unique; panel `id`s are unique within
  the plugin (the registry enforces both).
- Panels must tolerate empty data: a fresh capture has no completed
  windows and empty top-K lists.
- Keep panels presentation-only where possible; put reusable math in
  the plugin module so it can be unit-tested (see
  `ui/src/components/__tests__/panel-grid.test.tsx` for the pattern of
  testing a registered panel end to end).
