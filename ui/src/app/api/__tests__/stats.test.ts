import { describe, it, expect } from "vitest";
import { NextRequest } from "next/server";
import { GET } from "../stats/route";

describe("GET /api/stats", () => {
  it("returns a well-formed statistics snapshot", async () => {
    const req = new NextRequest(
      "http://test/api/stats?capture=shop/orders-capture"
    );
    const resp = await GET(req);
    expect(resp.status).toBe(200);

    const body = await resp.json();
    expect(body.capture).toBe("shop/orders-capture");
    expect(body.mock).toBe(true);

    const s = body.snapshot;
    expect(typeof s.uniqueClientIPs).toBe("number");
    expect(typeof s.uniqueFlows).toBe("number");
    expect(Array.isArray(s.completedWindows)).toBe(true);
    expect(s.completedWindows.length).toBeGreaterThan(0);
    for (const w of s.completedWindows) {
      expect(typeof w.requests).toBe("number");
      expect(typeof w.bytes).toBe("number");
    }
    expect(s.topPaths.length).toBeGreaterThan(0);
    expect(s.bodySizeBytes.p50).toBeGreaterThan(0);
    expect(s.bodySizeBytes.p99).toBeGreaterThanOrEqual(s.bodySizeBytes.p90);
    expect(s.handleLatencyMicros.count).toBeGreaterThan(0);
  });
});
