/**
 * Streaming-statistics types, mirroring the JSON the capture agent
 * serves at /stats (internal/stats.Snapshot in the Go backend).
 */

export interface WindowCounts {
  start: string;
  end?: string;
  requests: number;
  bytes: number;
  meanBytes: number;
  byMethod?: Record<string, number>;
  byProtocol?: Record<string, number>;
  newFlows: number;
}

export interface QuantileSummary {
  count: number;
  mean: number;
  p50: number;
  p90: number;
  p99: number;
  max: number;
}

export interface TopKEntry {
  key: string;
  count: number;
}

export interface StatsSnapshot {
  generatedAt: string;
  currentWindow?: WindowCounts;
  completedWindows?: WindowCounts[];
  uniqueClientIPs: number;
  uniqueFlows: number;
  topPaths?: TopKEntry[];
  topClients?: TopKEntry[];
  bodySizeBytes: QuantileSummary;
  handleLatencyMicros: QuantileSummary;
  flowFilterFill: number;
}
