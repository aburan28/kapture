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
  getDashboardSummary: vi.fn(),
  listCaptures: vi.fn(),
  listSpokes: vi.fn(),
}));

import DashboardPage from "../page";
import {
  getDashboardSummary,
  listCaptures,
  listSpokes,
} from "@/lib/hub-client";

const mockSummary = {
  totalCaptures: 5,
  activeCaptures: 2,
  totalRequests: 15000,
  healthySpokes: 3,
  totalSpokes: 4,
  readyStorages: 2,
  totalStorages: 3,
  capturesByPhase: { Pending: 1, Active: 2, Completed: 1, Failed: 1 },
};

const mockCapture = {
  name: "test-capture",
  namespace: "default",
  createdAt: new Date().toISOString(),
  spec: {
    targetRef: {
      group: "gateway.networking.k8s.io",
      kind: "HTTPRoute",
      name: "test-route",
    },
    storageRef: { name: "test-storage" },
    capture: { includeHeaders: true, includeBody: false },
  },
  status: {
    phase: "Active" as const,
    capturedRequests: 100,
    bytesWritten: "1Mi",
    agentStatus: { readyReplicas: 1, desiredReplicas: 1 },
    spokeId: "spoke-1",
  },
};

const mockSpoke = {
  id: "spoke-1",
  clusterName: "production-east",
  healthState: "Healthy" as const,
  lastHeartbeat: new Date().toISOString(),
  activeCaptures: 2,
  version: "v0.1.0",
};

describe("DashboardPage", () => {
  beforeEach(() => {
    vi.mocked(getDashboardSummary).mockResolvedValue(mockSummary);
    vi.mocked(listCaptures).mockResolvedValue([mockCapture]);
    vi.mocked(listSpokes).mockResolvedValue([mockSpoke]);
  });

  it("renders Dashboard heading", async () => {
    render(await DashboardPage());
    expect(
      screen.getByRole("heading", { name: "Dashboard" })
    ).toBeInTheDocument();
  });

  it("displays stat cards with summary data", async () => {
    render(await DashboardPage());

    expect(screen.getByText("Total Captures")).toBeInTheDocument();
    expect(screen.getByText("5")).toBeInTheDocument();
    expect(screen.getByText("2 active")).toBeInTheDocument();

    expect(screen.getByText("Captured Requests")).toBeInTheDocument();
    expect(screen.getByText("15,000")).toBeInTheDocument();

    expect(screen.getByText("Connected Spokes")).toBeInTheDocument();
    expect(screen.getByText("3/4")).toBeInTheDocument();

    expect(screen.getByText("Storage Backends")).toBeInTheDocument();
    expect(screen.getByText("2")).toBeInTheDocument();
  });

  it("shows active captures in table", async () => {
    render(await DashboardPage());

    expect(screen.getByText("Active Captures")).toBeInTheDocument();
    expect(screen.getByText("test-capture")).toBeInTheDocument();
    expect(screen.getByText("HTTPRoute")).toBeInTheDocument();
    expect(screen.getByText("test-route")).toBeInTheDocument();
    expect(screen.getByText("1/1")).toBeInTheDocument();
  });

  it("shows spokes overview section", async () => {
    render(await DashboardPage());

    expect(
      screen.getByRole("heading", { name: "Spokes" })
    ).toBeInTheDocument();
    expect(screen.getByText("production-east")).toBeInTheDocument();
    expect(screen.getByText("Healthy")).toBeInTheDocument();
  });
});
