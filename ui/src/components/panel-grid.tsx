"use client";

/**
 * Renders every registered plugin panel into a responsive grid.
 * Importing the plugins module populates the registry before render.
 */

import "@/plugins";
import { listPanels } from "@/lib/plugins/registry";
import type { PanelContext, PanelWidth } from "@/lib/plugins/types";
import type { StatsSnapshot } from "@/lib/stats-types";

const widthClass: Record<PanelWidth, string> = {
  third: "md:col-span-2",
  half: "md:col-span-3",
  full: "md:col-span-6",
};

export function PanelGrid({
  snapshot,
  context,
}: {
  snapshot: StatsSnapshot;
  context: PanelContext;
}) {
  const panels = listPanels();
  if (panels.length === 0) {
    return (
      <p className="py-16 text-center text-gray-500">
        No panels registered. Add a plugin under ui/src/plugins/.
      </p>
    );
  }
  return (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-6">
      {panels.map(({ key, plugin, panel }) => {
        const Panel = panel.component;
        return (
          <section
            key={key}
            className={`rounded-xl border border-gray-800 bg-gray-900 p-5 ${widthClass[panel.width ?? "third"]}`}
          >
            <header className="mb-4 flex items-baseline justify-between gap-3">
              <div>
                <h2 className="text-sm font-semibold text-white">
                  {panel.title}
                </h2>
                {panel.description && (
                  <p className="text-xs text-gray-500">{panel.description}</p>
                )}
              </div>
              <span
                className="shrink-0 rounded-full border border-gray-700 px-2 py-0.5 text-[10px] uppercase tracking-wide text-gray-500"
                title={`Contributed by the ${plugin.name} plugin`}
              >
                {plugin.id}
              </span>
            </header>
            <Panel snapshot={snapshot} context={context} />
          </section>
        );
      })}
    </div>
  );
}
