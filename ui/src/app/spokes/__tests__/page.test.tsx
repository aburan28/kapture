import { vi, describe, it, expect, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";

vi.mock("next/link", () => ({
  default: ({ children, href, ...props }: any) => (
    <a href={href} {...props}>
      {children}
    </a>
  ),
}));

vi.mock("@/lib/hub-client", () => ({
  listSpokes: vi.fn(),
  listCaptures: vi.fn(),
}));

import SpokesPage from "../page";
import { listSpokes, listCaptures } from "@/lib/hub-client";

const mockSpokes = [
  {
    id: "spoke-east",
    clusterName: "production-east",
    healthState: "Healthy" as const,
    lastHeartbeat: new Date().toISOString(),
    activeCaptures: 3,
    version: "v0.1.0",
  },
  {
    id: "spoke-west",
    clusterName: "production-west",
    healthState: "Degraded" as const,
    lastHeartbeat: new Date().toISOString(),
    activeCaptures: 1,
    version: "v0.1.0",
  },
  {
    id: "spoke-staging",
    clusterName: "staging",
    healthState: "Disconnected" as const,
    lastHeartbeat: new Date(Date.now() - 3600000).toISOString(),
    activeCaptures: 0,
    version: "v0.0.9",
  },
];

const mockCaptures = [
  {
    name: "east-capture",
    namespace: "default",
    createdAt: new Date().toISOString(),
    spec: {
      targetRef: {
        group: "gateway.networking.k8s.io",
        kind: "HTTPRoute",
        name: "east-route",
      },
      storageRef: { name: "s3-backend" },
      capture: { includeHeaders: true, includeBody: false },
    },
    status: {
      phase: "Active" as const,
      capturedRequests: 200,
      bytesWritten: "5Mi",
      agentStatus: { readyReplicas: 1, desiredReplicas: 1 },
      spokeId: "spoke-east",
    },
  },
];

describe("SpokesPage", () => {
  beforeEach(() => {
    vi.mocked(listSpokes).mockResolvedValue(mockSpokes);
    vi.mocked(listCaptures).mockResolvedValue(mockCaptures);
  });

  it("renders Spokes heading", async () => {
    render(await SpokesPage());
    expect(
      screen.getByRole("heading", { name: "Spokes" })
    ).toBeInTheDocument();
  });

  it("shows healthy/degraded/disconnected stat cards", async () => {
    render(await SpokesPage());

    // StatCard labels and HealthBadge both render these texts, so use getAllByText
    expect(screen.getAllByText("Healthy").length).toBeGreaterThanOrEqual(2);
    expect(screen.getAllByText("Degraded").length).toBeGreaterThanOrEqual(2);
    expect(screen.getAllByText("Disconnected").length).toBeGreaterThanOrEqual(2);
  });

  it("renders spoke cards with cluster name", async () => {
    render(await SpokesPage());

    expect(screen.getByText("production-east")).toBeInTheDocument();
    expect(screen.getByText("production-west")).toBeInTheDocument();
    expect(screen.getByText("staging")).toBeInTheDocument();
  });
});
