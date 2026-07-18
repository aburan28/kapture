export type CapturePhase = "Pending" | "Active" | "Completed" | "Failed";
export type StorageType = "S3" | "GCS" | "EFS" | "EBS" | "Plugin";
export type SpokeHealthState = "Healthy" | "Degraded" | "Disconnected";

export interface TrafficCapture {
  name: string;
  namespace: string;
  createdAt: string;
  spec: {
    targetRef: {
      group: string;
      kind: string;
      name: string;
    };
    storageRef: {
      name: string;
      namespace?: string;
    };
    capture: {
      includeHeaders: boolean;
      includeBody: boolean;
      maxBodyBytes?: number;
      duration?: string;
    };
    filters?: {
      headers?: { name: string; value: string }[];
      pathPrefix?: string;
      percentage?: number;
    };
    agent?: {
      replicas?: number;
      minReplicas?: number;
      maxReplicas?: number;
      targetCPU?: number;
    };
  };
  status: {
    phase: CapturePhase;
    startTime?: string;
    capturedRequests: number;
    bytesWritten: string;
    agentStatus?: {
      readyReplicas: number;
      desiredReplicas: number;
    };
    spokeId?: string;
  };
}

export interface CaptureStorage {
  name: string;
  namespace: string;
  type: StorageType;
  phase: "Ready" | "Error" | "Pending";
  bucket?: string;
  region?: string;
  prefix?: string;
  retention?: {
    maxAge?: string;
    maxSize?: string;
  };
}

export interface Spoke {
  id: string;
  clusterName: string;
  healthState: SpokeHealthState;
  lastHeartbeat: string;
  activeCaptures: number;
  activeReplays?: number;
  cell?: string;
  version: string;
}

export interface Cell {
  name: string;
  connectedSpokes: number;
  totalSpokes: number;
  activeCaptures: number;
  activeReplays: number;
}

export type LoadTestPhase =
  | "Pending"
  | "Distributing"
  | "Running"
  | "Completed"
  | "Failed"
  | "Aborted";

export interface LoadTestAssignment {
  spokeID: string;
  cell?: string;
  shardIndexes: number[];
}

export interface LoadTestCellStatus {
  name: string;
  spokes: number;
  shards: number;
  sentRequests: number;
  failedRequests: number;
}

export interface CaptureLoadTest {
  name: string;
  namespace: string;
  createdAt: string;
  spec: {
    sourceRef: { name: string };
    storageRef: { name: string };
    target: { host: string; port?: number; tls?: boolean };
    rate?: { mode: string; requestsPerSecond?: number };
    distribution?: {
      cells?: string[];
      maxSpokes?: number;
      workersPerSpoke?: number;
      concurrencyPerWorker?: number;
      presharded?: boolean;
    };
    engine?: { name: string };
  };
  status: {
    phase: LoadTestPhase;
    startTime?: string;
    completionTime?: string;
    totalShards: number;
    assignedSpokes: number;
    completedShards: number;
    failedShards: number;
    totalRequests: number;
    sentRequests: number;
    failedRequests: number;
    achievedRPS?: string;
    assignments?: LoadTestAssignment[];
    cells?: LoadTestCellStatus[];
  };
}

export type ReplayPhase = "Pending" | "Running" | "Completed" | "Failed";

export interface TrafficReplay {
  name: string;
  namespace: string;
  createdAt: string;
  spec: {
    sourceRef: { name: string };
    target: { host: string; port?: number };
    shard?: { index: number; count: number; presharded?: boolean };
    loadTestRef?: { name: string };
    engine?: { name: string };
  };
  status: {
    phase: ReplayPhase;
    startTime?: string;
    completionTime?: string;
    totalRequests: number;
    sentRequests: number;
    failedRequests: number;
    p50Latency?: string;
    p95Latency?: string;
    p99Latency?: string;
    achievedRPS?: string;
    spokeId?: string;
  };
}

export interface CapturedRequest {
  id: string;
  timestamp: string;
  method: string;
  path: string;
  headers?: Record<string, string[]>;
  contentLength: number;
  protocol: string;
  statusHint?: number;
  metadata?: Record<string, string>;
}
