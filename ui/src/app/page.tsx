import Link from "next/link";
import { StatCard } from "@/components/stat-card";
import { PhaseBadge, HealthBadge } from "@/components/status-badge";
import {
  getDashboardSummary,
  listCaptures,
  listSpokes,
} from "@/lib/hub-client";

export default async function DashboardPage() {
  const [summary, captures, spokes] = await Promise.all([
    getDashboardSummary(),
    listCaptures(),
    listSpokes(),
  ]);

  const activeCaptures = captures.filter((c) => c.status.phase === "Active");

  return (
    <div className="p-8">
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-white">Dashboard</h1>
        <p className="text-gray-400 text-sm mt-1">
          Overview of your Kapture traffic capture environment
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <StatCard
          label="Total Captures"
          value={summary.totalCaptures}
          sub={`${summary.activeCaptures} active`}
          color="indigo"
        />
        <StatCard
          label="Captured Requests"
          value={summary.totalRequests.toLocaleString()}
          sub="Across all captures"
          color="emerald"
        />
        <StatCard
          label="Connected Spokes"
          value={`${summary.healthySpokes}/${summary.totalSpokes}`}
          sub={`${summary.totalSpokes - summary.healthySpokes} unhealthy`}
          color="blue"
        />
        <StatCard
          label="Storage Backends"
          value={summary.readyStorages}
          sub={`${summary.totalStorages} configured`}
          color="amber"
        />
      </div>

      {/* Active Captures */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl mb-8">
        <div className="flex items-center justify-between p-5 border-b border-gray-800">
          <h2 className="text-lg font-semibold text-white">
            Active Captures
          </h2>
          <Link
            href="/captures"
            className="text-sm text-indigo-400 hover:text-indigo-300"
          >
            View all
          </Link>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-800 text-left">
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Name
                </th>
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Target
                </th>
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Requests
                </th>
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Data
                </th>
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Agents
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-800/50">
              {activeCaptures.map((capture) => (
                <tr
                  key={capture.name}
                  className="hover:bg-gray-800/30 transition-colors"
                >
                  <td className="px-5 py-4">
                    <Link
                      href={`/captures/${capture.name}`}
                      className="text-sm font-medium text-indigo-400 hover:text-indigo-300"
                    >
                      {capture.name}
                    </Link>
                    <p className="text-xs text-gray-500">
                      {capture.namespace}
                    </p>
                  </td>
                  <td className="px-5 py-4">
                    <span className="text-sm text-gray-300">
                      {capture.spec.targetRef.kind}
                    </span>
                    <p className="text-xs text-gray-500">
                      {capture.spec.targetRef.name}
                    </p>
                  </td>
                  <td className="px-5 py-4">
                    <PhaseBadge phase={capture.status.phase} />
                  </td>
                  <td className="px-5 py-4 text-sm text-gray-300">
                    {capture.status.capturedRequests.toLocaleString()}
                  </td>
                  <td className="px-5 py-4 text-sm text-gray-300">
                    {capture.status.bytesWritten}
                  </td>
                  <td className="px-5 py-4 text-sm text-gray-300">
                    {capture.status.agentStatus
                      ? `${capture.status.agentStatus.readyReplicas}/${capture.status.agentStatus.desiredReplicas}`
                      : "-"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Spokes Overview */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl">
        <div className="flex items-center justify-between p-5 border-b border-gray-800">
          <h2 className="text-lg font-semibold text-white">Spokes</h2>
          <Link
            href="/spokes"
            className="text-sm text-indigo-400 hover:text-indigo-300"
          >
            View all
          </Link>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 p-5">
          {spokes.map((spoke) => (
            <div
              key={spoke.id}
              className="bg-gray-800/40 border border-gray-700/50 rounded-lg p-4"
            >
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium text-white">
                  {spoke.clusterName}
                </span>
                <HealthBadge state={spoke.healthState} />
              </div>
              <div className="flex items-center gap-4 text-xs text-gray-400">
                <span>{spoke.activeCaptures} captures</span>
                <span>{spoke.version}</span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
