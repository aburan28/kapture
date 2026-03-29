import { describe, it, expect } from "vitest";
import { GET } from "../spokes/route";

describe("GET /api/spokes", () => {
  it("returns all spokes", async () => {
    const resp = await GET();
    expect(resp.status).toBe(200);

    const body = await resp.json();
    expect(body.spokes).toBeDefined();
    expect(Array.isArray(body.spokes)).toBe(true);
    expect(body.spokes.length).toBeGreaterThan(0);
  });

  it("spokes have expected shape", async () => {
    const resp = await GET();
    const body = await resp.json();

    for (const spoke of body.spokes) {
      expect(spoke.id).toBeTruthy();
      expect(spoke.clusterName).toBeTruthy();
      expect(["Healthy", "Degraded", "Disconnected"]).toContain(
        spoke.healthState
      );
      expect(typeof spoke.activeCaptures).toBe("number");
    }
  });
});
