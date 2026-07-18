import Link from "next/link";
import { ReplayPhaseBadge } from "@/components/status-badge";
import { StatCard } from "@/components/stat-card";
import { listReplays } from "@/lib/hub-client";

function formatNumber(n: number) {
  return n.toLocaleString("en-US");
}

export default async function ReplaysPage() {
  const replays = await listReplays();

  const running = replays.filter((r) => r.status.phase === "Running").length;
  const completed = replays.filter(
    (r) => r.status.phase === "Completed"
  ).length;
  const failed = replays.filter((r) => r.status.phase === "Failed").length;

  return (
    <div className="p-8">
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-white">Replays</h1>
        <p className="text-gray-400 text-sm mt-1">
          Replay shards running across the fleet — load test workers and
          standalone TrafficReplays
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        <StatCard label="Running" value={running} color="emerald" />
        <StatCard label="Completed" value={completed} color="blue" />
        <StatCard label="Failed" value={failed} color="red" />
      </div>

      <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800 text-left text-xs text-gray-400 uppercase tracking-wider">
              <th className="px-6 py-3">Name</th>
              <th className="px-6 py-3">Phase</th>
              <th className="px-6 py-3">Load Test</th>
              <th className="px-6 py-3">Shard</th>
              <th className="px-6 py-3">Target</th>
              <th className="px-6 py-3">Sent / Failed</th>
              <th className="px-6 py-3">P95</th>
              <th className="px-6 py-3">RPS</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-800">
            {replays.map((replay) => (
              <tr
                key={`${replay.namespace}/${replay.name}`}
                className="hover:bg-gray-800/40"
              >
                <td className="px-6 py-4">
                  <span className="text-gray-200 font-medium">
                    {replay.name}
                  </span>
                  <p className="text-xs text-gray-500">{replay.namespace}</p>
                </td>
                <td className="px-6 py-4">
                  <ReplayPhaseBadge phase={replay.status.phase} />
                </td>
                <td className="px-6 py-4">
                  {replay.spec.loadTestRef ? (
                    <Link
                      href={`/loadtests/${replay.spec.loadTestRef.name}?namespace=${replay.namespace}`}
                      className="text-indigo-400 hover:text-indigo-300"
                    >
                      {replay.spec.loadTestRef.name}
                    </Link>
                  ) : (
                    <span className="text-gray-500">standalone</span>
                  )}
                </td>
                <td className="px-6 py-4 text-gray-300 font-mono text-xs">
                  {replay.spec.shard
                    ? `${replay.spec.shard.index}/${replay.spec.shard.count}`
                    : "—"}
                </td>
                <td className="px-6 py-4 text-gray-300 font-mono text-xs">
                  {replay.spec.target.host}
                </td>
                <td className="px-6 py-4 text-gray-300">
                  {formatNumber(replay.status.sentRequests)}
                  {replay.status.failedRequests > 0 && (
                    <span className="text-red-400/80 text-xs ml-2">
                      {formatNumber(replay.status.failedRequests)} failed
                    </span>
                  )}
                </td>
                <td className="px-6 py-4 text-gray-300">
                  {replay.status.p95Latency ?? "—"}
                </td>
                <td className="px-6 py-4 text-gray-300">
                  {replay.status.achievedRPS ?? "—"}
                </td>
              </tr>
            ))}
            {replays.length === 0 && (
              <tr>
                <td colSpan={8} className="px-6 py-12 text-center text-gray-500">
                  No replays reported by any spoke.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
