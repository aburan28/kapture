import { describe, it, expect } from "vitest";
import { NextRequest } from "next/server";
import { GET as listLoadTests } from "../loadtests/route";
import { GET as getLoadTest } from "../loadtests/[name]/route";
import { GET as listReplays } from "../replays/route";
import { GET as listCells } from "../cells/route";

describe("GET /api/loadtests", () => {
  it("returns load tests with aggregated status", async () => {
    const resp = await listLoadTests();
    expect(resp.status).toBe(200);

    const body = await resp.json();
    expect(Array.isArray(body.loadTests)).toBe(true);
    expect(body.loadTests.length).toBeGreaterThan(0);

    for (const lt of body.loadTests) {
      expect(lt.name).toBeTruthy();
      expect([
        "Pending",
        "Distributing",
        "Running",
        "Completed",
        "Failed",
        "Aborted",
      ]).toContain(lt.status.phase);
      expect(typeof lt.status.totalShards).toBe("number");
      expect(typeof lt.status.sentRequests).toBe("number");
      expect(lt.spec.target.host).toBeTruthy();
    }
  });
});

describe("GET /api/loadtests/:name", () => {
  it("returns the load test and its shards", async () => {
    const req = new NextRequest("http://test/api/loadtests/orders-peak-replay");
    const resp = await getLoadTest(req, {
      params: Promise.resolve({ name: "orders-peak-replay" }),
    });
    expect(resp.status).toBe(200);

    const body = await resp.json();
    expect(body.loadTest.name).toBe("orders-peak-replay");
    expect(Array.isArray(body.shards)).toBe(true);
    expect(body.shards.length).toBe(body.loadTest.status.totalShards);
    for (const shard of body.shards) {
      expect(shard.spec.loadTestRef.name).toBe("orders-peak-replay");
      expect(shard.spec.shard.count).toBe(body.loadTest.status.totalShards);
    }
  });

  it("404s on unknown load tests", async () => {
    const req = new NextRequest("http://test/api/loadtests/nope");
    const resp = await getLoadTest(req, {
      params: Promise.resolve({ name: "nope" }),
    });
    expect(resp.status).toBe(404);
  });
});

describe("GET /api/replays", () => {
  it("returns all replays", async () => {
    const req = new NextRequest("http://test/api/replays");
    const resp = await listReplays(req);
    const body = await resp.json();
    expect(body.replays.length).toBeGreaterThan(0);
  });

  it("filters by load test", async () => {
    const req = new NextRequest(
      "http://test/api/replays?loadTest=orders-peak-replay"
    );
    const resp = await listReplays(req);
    const body = await resp.json();
    expect(body.replays.length).toBeGreaterThan(0);
    for (const r of body.replays) {
      expect(r.spec.loadTestRef.name).toBe("orders-peak-replay");
    }
  });
});

describe("GET /api/cells", () => {
  it("returns cell topology", async () => {
    const resp = await listCells();
    const body = await resp.json();
    expect(body.cells.length).toBeGreaterThan(0);
    for (const cell of body.cells) {
      expect(cell.name).toBeTruthy();
      expect(typeof cell.connectedSpokes).toBe("number");
      expect(typeof cell.totalSpokes).toBe("number");
    }
  });
});
