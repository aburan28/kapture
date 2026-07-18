import { CapturePhase, SpokeHealthState, LoadTestPhase, ReplayPhase } from "@/lib/types";

const phaseStyles: Record<CapturePhase, string> = {
  Active: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
  Completed: "bg-blue-500/15 text-blue-400 border-blue-500/30",
  Pending: "bg-amber-500/15 text-amber-400 border-amber-500/30",
  Failed: "bg-red-500/15 text-red-400 border-red-500/30",
};

const healthStyles: Record<SpokeHealthState, string> = {
  Healthy: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
  Degraded: "bg-amber-500/15 text-amber-400 border-amber-500/30",
  Disconnected: "bg-red-500/15 text-red-400 border-red-500/30",
};

const dotStyles: Record<string, string> = {
  Active: "bg-emerald-400",
  Running: "bg-emerald-400",
  Completed: "bg-blue-400",
  Pending: "bg-amber-400",
  Failed: "bg-red-400",
  Healthy: "bg-emerald-400",
  Degraded: "bg-amber-400",
  Disconnected: "bg-red-400",
  Ready: "bg-emerald-400",
  Error: "bg-red-400",
};

export function PhaseBadge({ phase }: { phase: CapturePhase }) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border ${phaseStyles[phase]}`}
    >
      <span
        className={`w-1.5 h-1.5 rounded-full ${dotStyles[phase]} ${phase === "Active" ? "animate-pulse" : ""}`}
      />
      {phase}
    </span>
  );
}

export function HealthBadge({ state }: { state: SpokeHealthState }) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border ${healthStyles[state]}`}
    >
      <span
        className={`w-1.5 h-1.5 rounded-full ${dotStyles[state]} ${state === "Healthy" ? "animate-pulse" : ""}`}
      />
      {state}
    </span>
  );
}

const loadTestStyles: Record<LoadTestPhase, string> = {
  Pending: "bg-amber-500/15 text-amber-400 border-amber-500/30",
  Distributing: "bg-purple-500/15 text-purple-400 border-purple-500/30",
  Running: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
  Completed: "bg-blue-500/15 text-blue-400 border-blue-500/30",
  Failed: "bg-red-500/15 text-red-400 border-red-500/30",
  Aborted: "bg-orange-500/15 text-orange-400 border-orange-500/30",
};

const loadTestDots: Record<LoadTestPhase, string> = {
  Pending: "bg-amber-400",
  Distributing: "bg-purple-400",
  Running: "bg-emerald-400",
  Completed: "bg-blue-400",
  Failed: "bg-red-400",
  Aborted: "bg-orange-400",
};

export function LoadTestPhaseBadge({ phase }: { phase: LoadTestPhase }) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border ${loadTestStyles[phase]}`}
    >
      <span
        className={`w-1.5 h-1.5 rounded-full ${loadTestDots[phase]} ${
          phase === "Running" || phase === "Distributing" ? "animate-pulse" : ""
        }`}
      />
      {phase}
    </span>
  );
}

export function ReplayPhaseBadge({ phase }: { phase: ReplayPhase }) {
  const styles: Record<ReplayPhase, string> = {
    Pending: "bg-amber-500/15 text-amber-400 border-amber-500/30",
    Running: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
    Completed: "bg-blue-500/15 text-blue-400 border-blue-500/30",
    Failed: "bg-red-500/15 text-red-400 border-red-500/30",
  };
  return (
    <span
      className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border ${styles[phase]}`}
    >
      <span
        className={`w-1.5 h-1.5 rounded-full ${dotStyles[phase]} ${phase === "Running" ? "animate-pulse" : ""}`}
      />
      {phase}
    </span>
  );
}

export function StoragePhaseBadge({
  phase,
}: {
  phase: "Ready" | "Error" | "Pending";
}) {
  const styles: Record<string, string> = {
    Ready: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
    Error: "bg-red-500/15 text-red-400 border-red-500/30",
    Pending: "bg-amber-500/15 text-amber-400 border-amber-500/30",
  };
  return (
    <span
      className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border ${styles[phase]}`}
    >
      <span className={`w-1.5 h-1.5 rounded-full ${dotStyles[phase]}`} />
      {phase}
    </span>
  );
}
