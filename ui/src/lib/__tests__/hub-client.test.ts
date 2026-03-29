import { describe, it, expect } from "vitest";
import {
  listCaptures,
  getCaptureStatus,
  listSpokes,
  listStorages,
  getCapturedRequests,
  getDashboardSummary,
} from "../hub-client";

describe("hub-client (mock mode)", () => {
  describe("listCaptures", () => {
    it("returns all captures when no filters given", async () => {
      const captures = await listCaptures();
      expect(captures.length).toBeGreaterThan(0);
    });

    it("filters by spokeId", async () => {
      const captures = await listCaptures({ spokeId: "spoke-us-east-1" });
      expect(captures.length).toBeGreaterThan(0);
      for (const c of captures) {
        expect(c.status.spokeId).toBe("spoke-us-east-1");
      }
    });

    it("filters by namespace", async () => {
      const captures = await listCaptures({ namespace: "kapture-dev" });
      expect(captures.length).toBeGreaterThan(0);
      for (const c of captures) {
        expect(c.namespace).toBe("kapture-dev");
      }
    });

    it("filters by both spokeId and namespace", async () => {
      const captures = await listCaptures({
        spokeId: "spoke-us-east-1",
        namespace: "kapture-system",
      });
      for (const c of captures) {
        expect(c.status.spokeId).toBe("spoke-us-east-1");
        expect(c.namespace).toBe("kapture-system");
      }
    });

    it("returns empty when no matches", async () => {
      const captures = await listCaptures({ spokeId: "nonexistent-spoke" });
      expect(captures).toHaveLength(0);
    });
  });

  describe("getCaptureStatus", () => {
    it("returns a capture by name", async () => {
      const capture = await getCaptureStatus(
        "api-gateway-capture",
        "kapture-system"
      );
      expect(capture).not.toBeNull();
      expect(capture!.name).toBe("api-gateway-capture");
    });

    it("returns null for unknown capture", async () => {
      const capture = await getCaptureStatus("nonexistent", "default");
      expect(capture).toBeNull();
    });

    it("matches by name and namespace", async () => {
      const capture = await getCaptureStatus(
        "failed-capture-test",
        "kapture-dev"
      );
      expect(capture).not.toBeNull();
      expect(capture!.namespace).toBe("kapture-dev");
    });
  });

  describe("listSpokes", () => {
    it("returns all spokes", async () => {
      const spokes = await listSpokes();
      expect(spokes.length).toBeGreaterThan(0);
      for (const s of spokes) {
        expect(s.id).toBeTruthy();
        expect(s.clusterName).toBeTruthy();
        expect(["Healthy", "Degraded", "Disconnected"]).toContain(
          s.healthState
        );
      }
    });
  });

  describe("listStorages", () => {
    it("returns all storages", async () => {
      const storages = await listStorages();
      expect(storages.length).toBeGreaterThan(0);
      for (const s of storages) {
        expect(["S3", "GCS", "EFS", "EBS", "Plugin"]).toContain(s.type);
        expect(["Ready", "Error", "Pending"]).toContain(s.phase);
      }
    });
  });

  describe("getCapturedRequests", () => {
    it("returns sample requests", async () => {
      const requests = await getCapturedRequests("api-gateway-capture");
      expect(requests.length).toBeGreaterThan(0);
      for (const r of requests) {
        expect(r.id).toBeTruthy();
        expect(r.method).toBeTruthy();
        expect(r.path).toBeTruthy();
        expect(r.protocol).toBeTruthy();
      }
    });
  });

  describe("getDashboardSummary", () => {
    it("aggregates stats correctly", async () => {
      const summary = await getDashboardSummary();

      expect(summary.totalCaptures).toBeGreaterThan(0);
      expect(summary.activeCaptures).toBeGreaterThanOrEqual(0);
      expect(summary.totalRequests).toBeGreaterThan(0);
      expect(summary.totalSpokes).toBeGreaterThan(0);
      expect(summary.healthySpokes).toBeLessThanOrEqual(summary.totalSpokes);
      expect(summary.readyStorages).toBeLessThanOrEqual(
        summary.totalStorages
      );

      // Phase counts should sum to total
      const phaseSum = Object.values(summary.capturesByPhase).reduce(
        (a, b) => a + b,
        0
      );
      expect(phaseSum).toBe(summary.totalCaptures);
    });

    it("has all phase keys", async () => {
      const summary = await getDashboardSummary();
      expect(summary.capturesByPhase).toHaveProperty("Active");
      expect(summary.capturesByPhase).toHaveProperty("Completed");
      expect(summary.capturesByPhase).toHaveProperty("Pending");
      expect(summary.capturesByPhase).toHaveProperty("Failed");
    });
  });
});
