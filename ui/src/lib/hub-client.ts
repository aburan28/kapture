/**
 * Hub gRPC client abstraction.
 *
 * The Kapture UI talks exclusively to the Hub controller, which aggregates
 * capture status from all connected spokes via its gRPC Query API:
 *
 *   - ListCaptures  (filter by spoke_id, namespace)
 *   - GetCaptureStatus  (by name + namespace, optionally scoped to spoke)
 *   - ListSpokes
 *
 * In production this module will use @grpc/grpc-js to call the hub's gRPC
 * server (default port 9443).  During development we fall back to mock data
 * so the UI can be iterated on without a running cluster.
 *
 * Architecture:
 *
 *   Browser ──HTTP/JSON──▶  Next.js API Routes  ──gRPC──▶  Hub (:9443)
 *                           (src/app/api/*)                     │
 *                                                               │ aggregates
 *                                                               ▼
 *                                                        Spoke 1 … Spoke N
 *
 * The hub address is configured via the HUB_GRPC_ADDRESS env var.  When it
 * is unset the client returns mock data, which makes local development and
 * testing possible without a live cluster.
 */

import {
  TrafficCapture,
  CaptureStorage,
  Spoke,
  CapturedRequest,
  CapturePhase,
} from "./types";
import {
  mockCaptures,
  mockStorages,
  mockSpokes,
  mockCapturedRequests,
} from "./mock-data";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const HUB_GRPC_ADDRESS = process.env.HUB_GRPC_ADDRESS; // e.g. "hub:9443"

function useMock(): boolean {
  return !HUB_GRPC_ADDRESS;
}

// ---------------------------------------------------------------------------
// Public API – called by Next.js API routes
// ---------------------------------------------------------------------------

export interface ListCapturesOpts {
  spokeId?: string;
  namespace?: string;
}

export async function listCaptures(
  opts: ListCapturesOpts = {}
): Promise<TrafficCapture[]> {
  if (useMock()) {
    let captures = mockCaptures;
    if (opts.spokeId) {
      captures = captures.filter((c) => c.status.spokeId === opts.spokeId);
    }
    if (opts.namespace) {
      captures = captures.filter((c) => c.namespace === opts.namespace);
    }
    return captures;
  }

  // TODO: gRPC call via @grpc/grpc-js
  // const client = getHubClient();
  // const resp = await client.listCaptures({ spokeId, namespace });
  // return resp.captures.map(fromProtoCaptureInfo);
  throw new Error("gRPC transport not yet implemented");
}

export async function getCaptureStatus(
  name: string,
  namespace: string
): Promise<TrafficCapture | null> {
  if (useMock()) {
    return (
      mockCaptures.find(
        (c) => c.name === name && c.namespace === namespace
      ) ??
      mockCaptures.find((c) => c.name === name) ??
      null
    );
  }

  throw new Error("gRPC transport not yet implemented");
}

export async function listSpokes(): Promise<Spoke[]> {
  if (useMock()) {
    return mockSpokes;
  }

  throw new Error("gRPC transport not yet implemented");
}

export async function listStorages(): Promise<CaptureStorage[]> {
  // Storage CRs are Kubernetes-native; the hub does not aggregate them today.
  // In production this would query the Kubernetes API or a future hub endpoint.
  if (useMock()) {
    return mockStorages;
  }

  throw new Error("gRPC transport not yet implemented");
}

export async function getCapturedRequests(
  _captureName: string
): Promise<CapturedRequest[]> {
  // Captured request payloads live in storage (S3/GCS/EFS).  In production
  // the UI would call a dedicated read-back API or pre-signed URL endpoint.
  if (useMock()) {
    return mockCapturedRequests;
  }

  throw new Error("Storage read-back not yet implemented");
}

// ---------------------------------------------------------------------------
// Dashboard helpers
// ---------------------------------------------------------------------------

export interface DashboardSummary {
  totalCaptures: number;
  activeCaptures: number;
  totalRequests: number;
  healthySpokes: number;
  totalSpokes: number;
  readyStorages: number;
  totalStorages: number;
  capturesByPhase: Record<CapturePhase, number>;
}

export async function getDashboardSummary(): Promise<DashboardSummary> {
  const [captures, spokes, storages] = await Promise.all([
    listCaptures(),
    listSpokes(),
    listStorages(),
  ]);

  const capturesByPhase = { Pending: 0, Active: 0, Completed: 0, Failed: 0 };
  for (const c of captures) {
    capturesByPhase[c.status.phase]++;
  }

  return {
    totalCaptures: captures.length,
    activeCaptures: capturesByPhase.Active,
    totalRequests: captures.reduce(
      (sum, c) => sum + c.status.capturedRequests,
      0
    ),
    healthySpokes: spokes.filter((s) => s.healthState === "Healthy").length,
    totalSpokes: spokes.length,
    readyStorages: storages.filter((s) => s.phase === "Ready").length,
    totalStorages: storages.length,
    capturesByPhase,
  };
}
