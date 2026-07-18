import Link from "next/link";
import { LoadTestPhaseBadge } from "@/components/status-badge";
import { StatCard } from "@/components/stat-card";
import { listLoadTests } from "@/lib/hub-client";

function formatNumber(n: number) {
  return n.toLocaleString("en-US");
}

function shardProgress(completed: number, total: number) {
  if (total === 0) return 0;
  return Math.round((completed / total) * 100);
}

export default async function LoadTestsPage() {
  const loadTests = await listLoadTests();

  const running = loadTests.filter((lt) =>
    ["Running", "Distributing"].includes(lt.status.phase)
  ).length;
  const completed = loadTests.filter(
    (lt) => lt.status.phase === "Completed"
  ).length;
  const problem = loadTests.filter((lt) =>
    ["Failed", "Aborted"].includes(lt.status.phase)
  ).length;
  const totalSent = loadTests.reduce(
    (sum, lt) => sum + lt.status.sentRequests,
    0
  );

  return (
    <div className="p-8">
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-white">Load Tests</h1>
        <p className="text-gray-400 text-sm mt-1">
          Distributed replays of captured traffic, fanned out across cells and
          spokes by the hub
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
        <StatCard label="Running" value={running} color="emerald" />
        <StatCard label="Completed" value={completed} color="blue" />
        <StatCard label="Failed / Aborted" value={problem} color="red" />
        <StatCard
          label="Requests Sent"
          value={formatNumber(totalSent)}
          color="indigo"
        />
      </div>

      <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800 text-left text-xs text-gray-400 uppercase tracking-wider">
              <th className="px-6 py-3">Name</th>
              <th className="px-6 py-3">Phase</th>
              <th className="px-6 py-3">Target</th>
              <th className="px-6 py-3">Engine</th>
              <th className="px-6 py-3">Shards</th>
              <th className="px-6 py-3">Sent / Failed</th>
              <th className="px-6 py-3">RPS</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-800">
            {loadTests.map((lt) => (
              <tr key={`${lt.namespace}/${lt.name}`} className="hover:bg-gray-800/40">
                <td className="px-6 py-4">
                  <Link
                    href={`/loadtests/${lt.name}?namespace=${lt.namespace}`}
                    className="text-indigo-400 hover:text-indigo-300 font-medium"
                  >
                    {lt.name}
                  </Link>
                  <p className="text-xs text-gray-500">{lt.namespace}</p>
                </td>
                <td className="px-6 py-4">
                  <LoadTestPhaseBadge phase={lt.status.phase} />
                </td>
                <td className="px-6 py-4 text-gray-300 font-mono text-xs">
                  {lt.spec.target.host}
                  {lt.spec.target.port ? `:${lt.spec.target.port}` : ""}
                </td>
                <td className="px-6 py-4 text-gray-300">
                  {lt.spec.engine?.name ?? "builtin"}
                </td>
                <td className="px-6 py-4">
                  <div className="flex items-center gap-2">
                    <div className="w-20 h-1.5 bg-gray-800 rounded-full overflow-hidden">
                      <div
                        className="h-full bg-indigo-500 rounded-full"
                        style={{
                          width: `${shardProgress(
                            lt.status.completedShards,
                            lt.status.totalShards
                          )}%`,
                        }}
                      />
                    </div>
                    <span className="text-gray-400 text-xs">
                      {lt.status.completedShards}/{lt.status.totalShards}
                    </span>
                  </div>
                </td>
                <td className="px-6 py-4 text-gray-300">
                  {formatNumber(lt.status.sentRequests)}
                  <span className="text-red-400/80 text-xs ml-2">
                    {lt.status.failedRequests > 0
                      ? `${formatNumber(lt.status.failedRequests)} failed`
                      : ""}
                  </span>
                </td>
                <td className="px-6 py-4 text-gray-300">
                  {lt.status.achievedRPS ?? "—"}
                </td>
              </tr>
            ))}
            {loadTests.length === 0 && (
              <tr>
                <td colSpan={7} className="px-6 py-12 text-center text-gray-500">
                  No load tests yet. Create a CaptureLoadTest resource on the
                  hub cluster to fan a captured dataset out across your spokes.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
