import Link from "next/link";
import { PanelGrid } from "@/components/panel-grid";
import { getCaptureStats, listCaptures } from "@/lib/hub-client";

export const dynamic = "force-dynamic";

export default async function AnalyticsPage({
  searchParams,
}: {
  searchParams: Promise<{ capture?: string }>;
}) {
  const { capture } = await searchParams;
  const captures = await listCaptures();
  const captureId =
    capture ??
    (captures[0] ? `${captures[0].namespace}/${captures[0].name}` : "");

  const { snapshot, mock } = await getCaptureStats(captureId);

  return (
    <div className="p-8">
      <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-white">Traffic Analytics</h1>
          <p className="mt-1 text-sm text-gray-400">
            Streaming statistics for{" "}
            <span className="font-mono text-gray-300">{captureId || "—"}</span>
            {mock && (
              <span className="ml-2 rounded-full border border-amber-500/40 px-2 py-0.5 text-[10px] uppercase tracking-wide text-amber-400">
                mock data
              </span>
            )}
          </p>
        </div>
        {/* Capture selector: one filter row above the panels. */}
        <nav className="flex flex-wrap gap-1.5" aria-label="Capture">
          {captures.map((c) => {
            const id = `${c.namespace}/${c.name}`;
            const active = id === captureId;
            return (
              <Link
                key={id}
                href={`/analytics?capture=${encodeURIComponent(id)}`}
                className={`rounded-lg border px-3 py-1.5 text-xs font-medium transition-colors ${
                  active
                    ? "border-indigo-500/50 bg-indigo-600/15 text-indigo-300"
                    : "border-gray-800 text-gray-400 hover:bg-gray-800/60 hover:text-gray-200"
                }`}
              >
                {c.name}
              </Link>
            );
          })}
        </nav>
      </div>

      <PanelGrid
        snapshot={snapshot}
        context={{ captureId, generatedAt: snapshot.generatedAt, mock }}
      />

      <p className="mt-6 text-xs text-gray-600">
        Panels are contributed by plugins — see docs/ui-plugins.md to add
        custom statistics panels and traffic analysis features.
      </p>
    </div>
  );
}
