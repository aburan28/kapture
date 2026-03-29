import Link from "next/link";
import { notFound } from "next/navigation";
import { PhaseBadge } from "@/components/status-badge";
import { getCaptureStatus, getCapturedRequests } from "@/lib/hub-client";

function timeAgo(dateStr: string) {
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

const methodColors: Record<string, string> = {
  GET: "text-emerald-400 bg-emerald-400/10",
  POST: "text-blue-400 bg-blue-400/10",
  PUT: "text-amber-400 bg-amber-400/10",
  PATCH: "text-orange-400 bg-orange-400/10",
  DELETE: "text-red-400 bg-red-400/10",
};

export default async function CaptureDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const capture = await getCaptureStatus(id, "kapture-system");
  if (!capture) notFound();

  const recentRequests = await getCapturedRequests(id);

  return (
    <div className="p-8">
      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center gap-2 text-sm text-gray-400 mb-2">
          <Link href="/captures" className="hover:text-gray-200">
            Captures
          </Link>
          <span>/</span>
          <span className="text-gray-200">{capture.name}</span>
        </div>
        <div className="flex items-center gap-4">
          <h1 className="text-2xl font-bold text-white">{capture.name}</h1>
          <PhaseBadge phase={capture.status.phase} />
        </div>
        <p className="text-gray-400 text-sm mt-1">
          {capture.namespace} &middot;{" "}
          {capture.spec.targetRef.kind}/{capture.spec.targetRef.name}
          {capture.status.spokeId && (
            <span> &middot; spoke: {capture.status.spokeId}</span>
          )}
        </p>
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <InfoCard
          label="Captured Requests"
          value={capture.status.capturedRequests.toLocaleString()}
        />
        <InfoCard label="Data Written" value={capture.status.bytesWritten} />
        <InfoCard
          label="Agents"
          value={
            capture.status.agentStatus
              ? `${capture.status.agentStatus.readyReplicas} / ${capture.status.agentStatus.desiredReplicas} ready`
              : "N/A"
          }
        />
        <InfoCard
          label="Started"
          value={
            capture.status.startTime
              ? timeAgo(capture.status.startTime)
              : "Not started"
          }
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
        {/* Capture Settings */}
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <h2 className="text-sm font-semibold text-white mb-4 uppercase tracking-wider">
            Capture Settings
          </h2>
          <dl className="space-y-3">
            <SettingRow
              label="Include Headers"
              value={capture.spec.capture.includeHeaders ? "Yes" : "No"}
            />
            <SettingRow
              label="Include Body"
              value={capture.spec.capture.includeBody ? "Yes" : "No"}
            />
            {capture.spec.capture.maxBodyBytes && (
              <SettingRow
                label="Max Body"
                value={`${(capture.spec.capture.maxBodyBytes / 1024).toFixed(0)} KB`}
              />
            )}
            {capture.spec.capture.duration && (
              <SettingRow
                label="Duration"
                value={capture.spec.capture.duration}
              />
            )}
          </dl>
        </div>

        {/* Filters */}
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <h2 className="text-sm font-semibold text-white mb-4 uppercase tracking-wider">
            Filters
          </h2>
          {capture.spec.filters ? (
            <dl className="space-y-3">
              {capture.spec.filters.pathPrefix && (
                <SettingRow
                  label="Path Prefix"
                  value={capture.spec.filters.pathPrefix}
                />
              )}
              {capture.spec.filters.percentage != null && (
                <SettingRow
                  label="Sampling"
                  value={`${capture.spec.filters.percentage}%`}
                />
              )}
              {capture.spec.filters.headers?.map((h) => (
                <SettingRow
                  key={h.name}
                  label={`Header: ${h.name}`}
                  value={h.value}
                />
              ))}
            </dl>
          ) : (
            <p className="text-sm text-gray-500">
              No filters configured - capturing all traffic.
            </p>
          )}
        </div>

        {/* Agent Scaling */}
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <h2 className="text-sm font-semibold text-white mb-4 uppercase tracking-wider">
            Agent Scaling
          </h2>
          <dl className="space-y-3">
            <SettingRow
              label="Replicas"
              value={String(capture.spec.agent?.replicas ?? 1)}
            />
            {capture.spec.agent?.minReplicas && (
              <SettingRow
                label="Min Replicas"
                value={String(capture.spec.agent.minReplicas)}
              />
            )}
            {capture.spec.agent?.maxReplicas && (
              <SettingRow
                label="Max Replicas"
                value={String(capture.spec.agent.maxReplicas)}
              />
            )}
            {capture.spec.agent?.targetCPU && (
              <SettingRow
                label="Target CPU"
                value={`${capture.spec.agent.targetCPU}%`}
              />
            )}
          </dl>
        </div>
      </div>

      {/* Captured Requests */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl">
        <div className="p-5 border-b border-gray-800">
          <h2 className="text-lg font-semibold text-white">
            Recent Captured Requests
          </h2>
          <p className="text-xs text-gray-500 mt-1">
            Sample of recently captured HTTP requests
          </p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-800 text-left">
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Method
                </th>
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Path
                </th>
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Size
                </th>
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Protocol
                </th>
                <th className="px-5 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">
                  Time
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-800/50">
              {recentRequests.map((req) => (
                <tr
                  key={req.id}
                  className="hover:bg-gray-800/30 transition-colors"
                >
                  <td className="px-5 py-3">
                    <span
                      className={`inline-flex px-2 py-0.5 rounded text-xs font-mono font-medium ${methodColors[req.method] || "text-gray-300 bg-gray-700"}`}
                    >
                      {req.method}
                    </span>
                  </td>
                  <td className="px-5 py-3 text-sm text-gray-300 font-mono max-w-md truncate">
                    {req.path}
                  </td>
                  <td className="px-5 py-3 text-sm">
                    {req.statusHint && (
                      <span
                        className={
                          req.statusHint < 300
                            ? "text-emerald-400"
                            : req.statusHint < 400
                              ? "text-blue-400"
                              : req.statusHint < 500
                                ? "text-amber-400"
                                : "text-red-400"
                        }
                      >
                        {req.statusHint}
                      </span>
                    )}
                  </td>
                  <td className="px-5 py-3 text-sm text-gray-400">
                    {req.contentLength > 0
                      ? `${(req.contentLength / 1024).toFixed(1)} KB`
                      : "-"}
                  </td>
                  <td className="px-5 py-3 text-sm text-gray-400">
                    {req.protocol}
                  </td>
                  <td className="px-5 py-3 text-xs text-gray-500">
                    {timeAgo(req.timestamp)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Request Headers Preview */}
      {recentRequests[0]?.headers && (
        <div className="bg-gray-900 border border-gray-800 rounded-xl mt-6">
          <div className="p-5 border-b border-gray-800">
            <h2 className="text-lg font-semibold text-white">
              Request Headers Preview
            </h2>
            <p className="text-xs text-gray-500 mt-1">
              Headers from the most recent captured request
            </p>
          </div>
          <div className="p-5">
            <div className="bg-gray-950 rounded-lg border border-gray-800 p-4 font-mono text-sm">
              {Object.entries(recentRequests[0].headers).map(
                ([key, values]) => (
                  <div key={key} className="flex gap-2 py-0.5">
                    <span className="text-indigo-400">{key}:</span>
                    <span className="text-gray-300">
                      {values.join(", ")}
                    </span>
                  </div>
                )
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function InfoCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
      <p className="text-xs text-gray-400 mb-1">{label}</p>
      <p className="text-lg font-semibold text-white">{value}</p>
    </div>
  );
}

function SettingRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between">
      <dt className="text-sm text-gray-400">{label}</dt>
      <dd className="text-sm text-gray-200 font-medium">{value}</dd>
    </div>
  );
}
