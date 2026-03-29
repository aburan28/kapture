import { describe, it, expect } from "vitest";
import { mockCaptures, mockSpokes, mockStorages } from "../mock-data";
import type {
  TrafficCapture,
  CaptureStorage,
  Spoke,
  CapturePhase,
} from "../types";

describe("mock data integrity", () => {
  describe("captures", () => {
    it("all captures have required fields", () => {
      for (const c of mockCaptures) {
        expect(c.name).toBeTruthy();
        expect(c.namespace).toBeTruthy();
        expect(c.createdAt).toBeTruthy();
        expect(c.spec.targetRef.kind).toMatch(/^(HTTPRoute|GRPCRoute)$/);
        expect(c.spec.storageRef.name).toBeTruthy();
        expect(typeof c.spec.capture.includeHeaders).toBe("boolean");
        expect(typeof c.spec.capture.includeBody).toBe("boolean");
      }
    });

    it("status phases are valid", () => {
      const validPhases: CapturePhase[] = [
        "Pending",
        "Active",
        "Completed",
        "Failed",
      ];
      for (const c of mockCaptures) {
        expect(validPhases).toContain(c.status.phase);
        expect(typeof c.status.capturedRequests).toBe("number");
        expect(c.status.capturedRequests).toBeGreaterThanOrEqual(0);
      }
    });

    it("active captures have start times", () => {
      const active = mockCaptures.filter((c) => c.status.phase === "Active");
      expect(active.length).toBeGreaterThan(0);
      for (const c of active) {
        expect(c.status.startTime).toBeTruthy();
        expect(new Date(c.status.startTime!).getTime()).not.toBeNaN();
      }
    });

    it("every capture references an existing storage", () => {
      const storageNames = new Set(mockStorages.map((s) => s.name));
      for (const c of mockCaptures) {
        expect(storageNames.has(c.spec.storageRef.name)).toBe(true);
      }
    });

    it("every capture references an existing spoke", () => {
      const spokeIds = new Set(mockSpokes.map((s) => s.id));
      for (const c of mockCaptures) {
        if (c.status.spokeId) {
          expect(spokeIds.has(c.status.spokeId)).toBe(true);
        }
      }
    });
  });

  describe("spokes", () => {
    it("all spokes have required fields", () => {
      for (const s of mockSpokes) {
        expect(s.id).toBeTruthy();
        expect(s.clusterName).toBeTruthy();
        expect(s.version).toMatch(/^v\d+\.\d+\.\d+$/);
        expect(new Date(s.lastHeartbeat).getTime()).not.toBeNaN();
      }
    });

    it("has at least one healthy spoke", () => {
      const healthy = mockSpokes.filter(
        (s) => s.healthState === "Healthy"
      );
      expect(healthy.length).toBeGreaterThan(0);
    });
  });

  describe("storages", () => {
    it("all storages have required fields", () => {
      for (const s of mockStorages) {
        expect(s.name).toBeTruthy();
        expect(s.namespace).toBeTruthy();
        expect(["S3", "GCS", "EFS", "EBS", "Plugin"]).toContain(s.type);
        expect(["Ready", "Error", "Pending"]).toContain(s.phase);
      }
    });

    it("S3 storages have bucket and region", () => {
      const s3 = mockStorages.filter((s) => s.type === "S3");
      for (const s of s3) {
        expect(s.bucket).toBeTruthy();
        expect(s.region).toBeTruthy();
      }
    });
  });
});
