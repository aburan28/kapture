import { describe, it, expect } from "vitest";
import { GET } from "../captures/route";
import { GET as GET_DETAIL } from "../captures/[name]/route";
import { NextRequest } from "next/server";

function makeRequest(url: string) {
  return new NextRequest(new URL(url, "http://localhost:3000"));
}

describe("GET /api/captures", () => {
  it("returns all captures", async () => {
    const resp = await GET(makeRequest("/api/captures"));
    expect(resp.status).toBe(200);

    const body = await resp.json();
    expect(body.captures).toBeDefined();
    expect(Array.isArray(body.captures)).toBe(true);
    expect(body.captures.length).toBeGreaterThan(0);
  });

  it("filters by spoke_id", async () => {
    const resp = await GET(
      makeRequest("/api/captures?spoke_id=spoke-us-east-1")
    );
    const body = await resp.json();
    for (const c of body.captures) {
      expect(c.status.spokeId).toBe("spoke-us-east-1");
    }
  });

  it("filters by namespace", async () => {
    const resp = await GET(
      makeRequest("/api/captures?namespace=kapture-dev")
    );
    const body = await resp.json();
    for (const c of body.captures) {
      expect(c.namespace).toBe("kapture-dev");
    }
  });

  it("returns empty list for unknown spoke", async () => {
    const resp = await GET(
      makeRequest("/api/captures?spoke_id=nonexistent")
    );
    const body = await resp.json();
    expect(body.captures).toHaveLength(0);
  });
});

describe("GET /api/captures/:name", () => {
  it("returns a capture by name", async () => {
    const resp = await GET_DETAIL(
      makeRequest("/api/captures/api-gateway-capture"),
      { params: Promise.resolve({ name: "api-gateway-capture" }) }
    );
    expect(resp.status).toBe(200);

    const body = await resp.json();
    expect(body.capture).toBeDefined();
    expect(body.capture.name).toBe("api-gateway-capture");
    expect(body.recentRequests).toBeDefined();
    expect(Array.isArray(body.recentRequests)).toBe(true);
  });

  it("returns 404 for unknown capture", async () => {
    const resp = await GET_DETAIL(
      makeRequest("/api/captures/nonexistent"),
      { params: Promise.resolve({ name: "nonexistent" }) }
    );
    expect(resp.status).toBe(404);

    const body = await resp.json();
    expect(body.error).toBe("Capture not found");
  });
});
