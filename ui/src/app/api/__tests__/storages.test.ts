import { describe, it, expect } from "vitest";
import { GET } from "../storages/route";

describe("GET /api/storages", () => {
  it("returns all storages", async () => {
    const resp = await GET();
    expect(resp.status).toBe(200);

    const body = await resp.json();
    expect(body.storages).toBeDefined();
    expect(Array.isArray(body.storages)).toBe(true);
    expect(body.storages.length).toBeGreaterThan(0);
  });

  it("storages have expected shape", async () => {
    const resp = await GET();
    const body = await resp.json();

    for (const storage of body.storages) {
      expect(storage.name).toBeTruthy();
      expect(storage.namespace).toBeTruthy();
      expect(["S3", "GCS", "EFS", "EBS", "Plugin"]).toContain(storage.type);
      expect(["Ready", "Error", "Pending"]).toContain(storage.phase);
    }
  });
});
