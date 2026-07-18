import Link from "next/link";
import { notFound } from "next/navigation";
import {
  LoadTestPhaseBadge,
  ReplayPhaseBadge,
} from "@/components/status-badge";
import { StatCard } from "@/components/stat-card";
import { getLoadTest, listReplays } from "@/lib/hub-client";

function formatNumber(n: number) {
  return n.toLocaleString("en-US");
}

export default async function LoadTestDetailPage({
  params,
  searchParams,
}: {
  params: Promise<{ name: string }>;
  searchParams: Promise<{ namespace?: string }>;
}) {
  const { name } = await params;
  const { namespace } = await searchParams;

  const loadTest = await getLoadTest(name, namespace);
  if (!loadTest) notFound();
  const shards = await listReplays({ loadTest: name });

  const spec = loadTest.spec;
  const status = loadTest.status;
  const errorPct =
    status.sentRequests > 0
      ? ((status.failedRequests / status.sentRequests) * 100).toFixed(2)
      : "0.00";

  return (
    <div className="p-8">
      <div className="mb-6">
        <Link
          href="/loadtests"
          className="text-sm text-gray-400 hover:text-gray-200"
        >
          ← Load Tests
        </Link>
      </div>

      <div className="flex items-start justify-between mb-8">
        <div>
          <h1 className="text-2xl font-bold text-white">{loadTest.name}</h1>
          <p className="text-gray-400 text-sm mt-1">
            {loadTest.namespace} · replays{" "}
            <span className="text-gray-300">{spec.sourceRef.name}</span> at{" "}
            <span className="font-mono text-gray-300">
              {spec.target.host}
              {spec.target.port ? `:${spec.target.port}` : ""}
            </span>{" "}
            with the{" "}
            <span className="text-gray-300">
              {spec.engine?.name ?? "builtin"}
            </span>{" "}
            engine
          </p>
        </div>
        <LoadTestPhaseBadge phase={status.phase} />
      </div>

      <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-8">
        <StatCard
          label="Shards"
          value={`${status.completedShards}/${status.totalShards}`}
          sub={`across ${status.assignedSpokes} spokes`}
        />
        <StatCard
          label="Sent"
          value={formatNumber(status.sentRequests)}
          color="emerald"
        />
        <StatCard
          label="Failed"
          value={formatNumber(status.failedRequests)}
          sub={`${errorPct}% of sent`}
          color={status.failedRequests > 0 ? "red" : "blue"}
        />
        <StatCard
          label="Aggregate RPS"
          value={status.achievedRPS ?? "—"}
          color="indigo"
        />
        <StatCard
          label="Rate"
          value={
            spec.rate?.mode === "Constant"
              ? `${spec.rate.requestsPerSecond} rps`
              : (spec.rate?.mode ?? "Unlimited")
          }
          color="blue"
        />
      </div>

      {status.cells && status.cells.length > 0 && (
        <div className="mb-8">
          <h2 className="text-lg font-semibold text-white mb-3">Cells</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {status.cells.map((cell) => (
              <div
                key={cell.name}
                className="bg-gray-900 border border-gray-800 rounded-xl p-5"
              >
                <p className="text-white font-medium">{cell.name}</p>
                <p className="text-xs text-gray-500 mb-3">
                  {cell.spokes} spokes · {cell.shards} shards
                </p>
                <div className="flex gap-6 text-sm">
                  <div>
                    <p className="text-xs text-gray-400">Sent</p>
                    <p className="text-gray-200">
                      {formatNumber(cell.sentRequests)}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-gray-400">Failed</p>
                    <p className="text-gray-200">
                      {formatNumber(cell.failedRequests)}
                    </p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <h2 className="text-lg font-semibold text-white mb-3">Shards</h2>
      <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800 text-left text-xs text-gray-400 uppercase tracking-wider">
              <th className="px-6 py-3">Shard</th>
              <th className="px-6 py-3">Phase</th>
              <th className="px-6 py-3">Spoke</th>
              <th className="px-6 py-3">Sent / Failed</th>
              <th className="px-6 py-3">P50</th>
              <th className="px-6 py-3">P95</th>
              <th className="px-6 py-3">P99</th>
              <th className="px-6 py-3">RPS</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-800">
            {shards.map((shard) => (
              <tr key={shard.name} className="hover:bg-gray-800/40">
                <td className="px-6 py-4 text-gray-200 font-mono text-xs">
                  {shard.spec.shard
                    ? `${shard.spec.shard.index} of ${shard.spec.shard.count}`
                    : "—"}
                  {shard.spec.shard?.presharded && (
                    <span className="ml-2 text-[10px] uppercase tracking-wide text-purple-400">
                      presharded
                    </span>
                  )}
                </td>
                <td className="px-6 py-4">
                  <ReplayPhaseBadge phase={shard.status.phase} />
                </td>
                <td className="px-6 py-4 text-gray-300">
                  {shard.status.spokeId ?? "—"}
                </td>
                <td className="px-6 py-4 text-gray-300">
                  {formatNumber(shard.status.sentRequests)}
                  {shard.status.failedRequests > 0 && (
                    <span className="text-red-400/80 text-xs ml-2">
                      {formatNumber(shard.status.failedRequests)} failed
                    </span>
                  )}
                </td>
                <td className="px-6 py-4 text-gray-300">
                  {shard.status.p50Latency ?? "—"}
                </td>
                <td className="px-6 py-4 text-gray-300">
                  {shard.status.p95Latency ?? "—"}
                </td>
                <td className="px-6 py-4 text-gray-300">
                  {shard.status.p99Latency ?? "—"}
                </td>
                <td className="px-6 py-4 text-gray-300">
                  {shard.status.achievedRPS ?? "—"}
                </td>
              </tr>
            ))}
            {shards.length === 0 && (
              <tr>
                <td colSpan={8} className="px-6 py-12 text-center text-gray-500">
                  No shards reported yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {status.assignments && status.assignments.length > 0 && (
        <div className="mt-8">
          <h2 className="text-lg font-semibold text-white mb-3">
            Shard Assignments
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {status.assignments.map((a) => (
              <div
                key={a.spokeID}
                className="bg-gray-900 border border-gray-800 rounded-xl p-5"
              >
                <p className="text-white font-medium">{a.spokeID}</p>
                <p className="text-xs text-gray-500 mb-2">
                  {a.cell ?? "no cell"}
                </p>
                <div className="flex flex-wrap gap-1.5">
                  {a.shardIndexes.map((i) => (
                    <span
                      key={i}
                      className="px-2 py-0.5 rounded bg-indigo-500/15 text-indigo-300 text-xs font-mono"
                    >
                      #{i}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
