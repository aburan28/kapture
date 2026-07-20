"use client";

/**
 * Built-in plugin: heavy hitters — Count-Min top-K paths and client IPs.
 */

import type { KapturePlugin, PanelProps } from "@/lib/plugins/types";
import { BarList } from "@/components/viz";

function TopPaths({ snapshot }: PanelProps) {
  return <BarList items={snapshot.topPaths ?? []} />;
}

function TopClients({ snapshot }: PanelProps) {
  return <BarList items={snapshot.topClients ?? []} />;
}

const plugin: KapturePlugin = {
  id: "heavy-hitters",
  name: "Heavy Hitters",
  version: "1.0.0",
  description: "Count-Min sketch top-K: hottest paths and clients.",
  panels: [
    {
      id: "paths",
      title: "Top paths",
      description: "Count-Min estimates (never undercount)",
      width: "half",
      order: 20,
      component: TopPaths,
    },
    {
      id: "clients",
      title: "Top clients",
      width: "half",
      order: 21,
      component: TopClients,
    },
  ],
};

export default plugin;
