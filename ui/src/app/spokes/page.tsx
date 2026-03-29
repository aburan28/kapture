import { HealthBadge } from "@/components/status-badge";
import { StatCard } from "@/components/stat-card";
import { listSpokes, listCaptures } from "@/lib/hub-client";

function timeAgo(dateStr: string) {
  const diff = Date.now() - new Date(dateStr).getTime();
  const secs = Math.floor(diff / 1000);
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  return `${Math.floor(mins / 60)}h ago`;
}

export default async function SpokesPage() {
  const [spokes, captures] = await Promise.all([
    listSpokes(),
    listCaptures(),
  ]);

  const healthy = spokes.filter((s) => s.healthState === "Healthy").length;
  const degraded = spokes.filter((s) => s.healthState === "Degraded").length;
  const disconnected = spokes.filter(
    (s) => s.healthState === "Disconnected"
  ).length;

  return (
    <div className="p-8">
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-white">Spokes</h1>
        <p className="text-gray-400 text-sm mt-1">
          Connected Kubernetes clusters running Kapture spoke controllers
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        <StatCard label="Healthy" value={healthy} color="emerald" />
        <StatCard label="Degraded" value={degraded} color="amber" />
        <StatCard label="Disconnected" value={disconnected} color="red" />
      </div>

      <div className="grid grid-cols-1 gap-4">
        {spokes.map((spoke) => {
          const spokeCaptures = captures.filter(
            (c) => c.status.spokeId === spoke.id
          );
          const activeCount = spokeCaptures.filter(
            (c) => c.status.phase === "Active"
          ).length;
          const totalRequests = spokeCaptures.reduce(
            (sum, c) => sum + c.status.capturedRequests,
            0
          );

          return (
            <div
              key={spoke.id}
              className="bg-gray-900 border border-gray-800 rounded-xl p-6"
            >
              <div className="flex items-start justify-between mb-4">
                <div>
                  <h2 className="text-lg font-semibold text-white">
                    {spoke.clusterName}
                  </h2>
                  <p className="text-sm text-gray-500">{spoke.id}</p>
                </div>
                <HealthBadge state={spoke.healthState} />
              </div>

              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div>
                  <p className="text-xs text-gray-400 mb-1">
                    Active Captures
                  </p>
                  <p className="text-xl font-semibold text-white">
                    {activeCount}
                  </p>
                </div>
                <div>
                  <p className="text-xs text-gray-400 mb-1">
                    Total Requests
                  </p>
                  <p className="text-xl font-semibold text-white">
                    {totalRequests.toLocaleString()}
                  </p>
                </div>
                <div>
                  <p className="text-xs text-gray-400 mb-1">
                    Last Heartbeat
                  </p>
                  <p className="text-sm font-medium text-gray-300">
                    {timeAgo(spoke.lastHeartbeat)}
                  </p>
                </div>
                <div>
                  <p className="text-xs text-gray-400 mb-1">Version</p>
                  <p className="text-sm font-medium text-gray-300">
                    {spoke.version}
                  </p>
                </div>
              </div>

              {spokeCaptures.length > 0 && (
                <div className="mt-4 pt-4 border-t border-gray-800">
                  <p className="text-xs text-gray-400 mb-2 uppercase tracking-wider">
                    Captures on this spoke
                  </p>
                  <div className="flex flex-wrap gap-2">
                    {spokeCaptures.map((cap) => (
                      <span
                        key={cap.name}
                        className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded text-xs font-medium border ${
                          cap.status.phase === "Active"
                            ? "bg-emerald-500/10 text-emerald-400 border-emerald-500/20"
                            : cap.status.phase === "Completed"
                              ? "bg-blue-500/10 text-blue-400 border-blue-500/20"
                              : cap.status.phase === "Failed"
                                ? "bg-red-500/10 text-red-400 border-red-500/20"
                                : "bg-gray-800 text-gray-400 border-gray-700"
                        }`}
                      >
                        {cap.name}
                      </span>
                    ))}
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
