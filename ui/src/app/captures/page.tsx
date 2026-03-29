import Link from "next/link";
import { PhaseBadge } from "@/components/status-badge";
import { listCaptures } from "@/lib/hub-client";

export default async function CapturesPage() {
  const captures = await listCaptures();

  const countByPhase = { Active: 0, Completed: 0, Pending: 0, Failed: 0 };
  for (const c of captures) {
    countByPhase[c.status.phase]++;
  }

  return (
    <div className="p-8">
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-white">Captures</h1>
        <p className="text-gray-400 text-sm mt-1">
          All traffic capture sessions across your clusters
        </p>
      </div>

      {/* Filter summary */}
      <div className="flex items-center gap-3 mb-6">
        {(["Active", "Completed", "Pending", "Failed"] as const).map(
          (phase) => (
            <div
              key={phase}
              className="bg-gray-900 border border-gray-800 rounded-lg px-4 py-2 text-sm"
            >
              <span className="text-gray-400">{phase}: </span>
              <span className="text-white font-medium">
                {countByPhase[phase]}
              </span>
            </div>
          )
        )}
      </div>

      <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-800 text-left">
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Capture
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
                  Data Written
                </th>
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Storage
                </th>
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Spoke
                </th>
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Filters
                </th>
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Agents
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-800/50">
              {captures.map((capture) => (
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
                    <div className="text-sm text-gray-300">
                      {capture.spec.targetRef.kind}
                    </div>
                    <div className="text-xs text-gray-500">
                      {capture.spec.targetRef.name}
                    </div>
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
                    {capture.spec.storageRef.name}
                  </td>
                  <td className="px-5 py-4 text-sm text-gray-400">
                    {capture.status.spokeId || "-"}
                  </td>
                  <td className="px-5 py-4">
                    <div className="flex flex-wrap gap-1">
                      {capture.spec.filters?.pathPrefix && (
                        <span className="inline-flex items-center px-2 py-0.5 rounded text-xs bg-gray-800 text-gray-300 border border-gray-700">
                          path: {capture.spec.filters.pathPrefix}
                        </span>
                      )}
                      {capture.spec.filters?.percentage != null && (
                        <span className="inline-flex items-center px-2 py-0.5 rounded text-xs bg-gray-800 text-gray-300 border border-gray-700">
                          {capture.spec.filters.percentage}%
                        </span>
                      )}
                      {capture.spec.filters?.headers?.map((h) => (
                        <span
                          key={h.name}
                          className="inline-flex items-center px-2 py-0.5 rounded text-xs bg-gray-800 text-gray-300 border border-gray-700"
                        >
                          {h.name}={h.value}
                        </span>
                      ))}
                      {!capture.spec.filters && (
                        <span className="text-xs text-gray-500">None</span>
                      )}
                    </div>
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
    </div>
  );
}
