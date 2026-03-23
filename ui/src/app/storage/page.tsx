import { StoragePhaseBadge } from "@/components/status-badge";
import { StatCard } from "@/components/stat-card";
import { mockStorages, mockCaptures } from "@/lib/mock-data";

export default function StoragePage() {
  const ready = mockStorages.filter((s) => s.phase === "Ready").length;
  const error = mockStorages.filter((s) => s.phase === "Error").length;

  const typeIcons: Record<string, string> = {
    S3: "S3",
    GCS: "GCS",
    EFS: "EFS",
    EBS: "EBS",
    Plugin: "PLG",
  };

  return (
    <div className="p-8">
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-white">Storage Backends</h1>
        <p className="text-gray-400 text-sm mt-1">
          Configured storage backends for capture data
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        <StatCard
          label="Total Backends"
          value={mockStorages.length}
          color="indigo"
        />
        <StatCard label="Ready" value={ready} color="emerald" />
        <StatCard label="Errors" value={error} color="red" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {mockStorages.map((storage) => {
          const usedBy = mockCaptures.filter(
            (c) => c.spec.storageRef.name === storage.name
          );

          return (
            <div
              key={`${storage.namespace}/${storage.name}`}
              className="bg-gray-900 border border-gray-800 rounded-xl p-6"
            >
              <div className="flex items-start justify-between mb-4">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 bg-gray-800 border border-gray-700 rounded-lg flex items-center justify-center text-xs font-bold text-indigo-400">
                    {typeIcons[storage.type]}
                  </div>
                  <div>
                    <h2 className="text-base font-semibold text-white">
                      {storage.name}
                    </h2>
                    <p className="text-xs text-gray-500">
                      {storage.namespace}
                    </p>
                  </div>
                </div>
                <StoragePhaseBadge phase={storage.phase} />
              </div>

              <div className="grid grid-cols-2 gap-4 mb-4">
                {storage.bucket && (
                  <div>
                    <p className="text-xs text-gray-400 mb-0.5">Bucket</p>
                    <p className="text-sm text-gray-200 font-mono">
                      {storage.bucket}
                    </p>
                  </div>
                )}
                {storage.region && (
                  <div>
                    <p className="text-xs text-gray-400 mb-0.5">Region</p>
                    <p className="text-sm text-gray-200">{storage.region}</p>
                  </div>
                )}
                {storage.prefix && (
                  <div>
                    <p className="text-xs text-gray-400 mb-0.5">Prefix</p>
                    <p className="text-sm text-gray-200 font-mono">
                      {storage.prefix}
                    </p>
                  </div>
                )}
                <div>
                  <p className="text-xs text-gray-400 mb-0.5">Type</p>
                  <p className="text-sm text-gray-200">{storage.type}</p>
                </div>
              </div>

              {storage.retention && (
                <div className="bg-gray-800/40 border border-gray-700/50 rounded-lg p-3 mb-4">
                  <p className="text-xs text-gray-400 mb-2 uppercase tracking-wider">
                    Retention Policy
                  </p>
                  <div className="flex gap-6">
                    {storage.retention.maxAge && (
                      <div>
                        <span className="text-xs text-gray-500">
                          Max Age:{" "}
                        </span>
                        <span className="text-sm text-gray-200">
                          {storage.retention.maxAge}
                        </span>
                      </div>
                    )}
                    {storage.retention.maxSize && (
                      <div>
                        <span className="text-xs text-gray-500">
                          Max Size:{" "}
                        </span>
                        <span className="text-sm text-gray-200">
                          {storage.retention.maxSize}
                        </span>
                      </div>
                    )}
                  </div>
                </div>
              )}

              <div>
                <p className="text-xs text-gray-400 mb-2">
                  Used by {usedBy.length} capture
                  {usedBy.length !== 1 ? "s" : ""}
                </p>
                {usedBy.length > 0 && (
                  <div className="flex flex-wrap gap-1.5">
                    {usedBy.map((cap) => (
                      <span
                        key={cap.name}
                        className="px-2 py-0.5 rounded text-xs bg-gray-800 text-gray-300 border border-gray-700"
                      >
                        {cap.name}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
