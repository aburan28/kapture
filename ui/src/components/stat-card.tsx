export function StatCard({
  label,
  value,
  sub,
  color = "indigo",
}: {
  label: string;
  value: string | number;
  sub?: string;
  color?: "indigo" | "emerald" | "amber" | "red" | "blue";
}) {
  const colorMap = {
    indigo: "border-indigo-500/30 from-indigo-500/10",
    emerald: "border-emerald-500/30 from-emerald-500/10",
    amber: "border-amber-500/30 from-amber-500/10",
    red: "border-red-500/30 from-red-500/10",
    blue: "border-blue-500/30 from-blue-500/10",
  };

  return (
    <div
      className={`bg-gradient-to-br ${colorMap[color]} to-transparent border rounded-xl p-5`}
    >
      <p className="text-sm text-gray-400 mb-1">{label}</p>
      <p className="text-3xl font-bold text-white">{value}</p>
      {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
    </div>
  );
}
