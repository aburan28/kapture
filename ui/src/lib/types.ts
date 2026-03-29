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
  version: string;
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
