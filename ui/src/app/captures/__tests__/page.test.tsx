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
  listCaptures: vi.fn(),
}));

import CapturesPage from "../page";
import { listCaptures } from "@/lib/hub-client";

const mockCaptures = [
  {
    name: "api-capture",
    namespace: "default",
    createdAt: new Date().toISOString(),
    spec: {
      targetRef: {
        group: "gateway.networking.k8s.io",
        kind: "HTTPRoute",
        name: "api-route",
      },
      storageRef: { name: "s3-backend" },
      capture: { includeHeaders: true, includeBody: false },
    },
    status: {
      phase: "Active" as const,
      capturedRequests: 500,
      bytesWritten: "10Mi",
      agentStatus: { readyReplicas: 2, desiredReplicas: 2 },
      spokeId: "spoke-1",
    },
  },
  {
    name: "grpc-capture",
    namespace: "production",
    createdAt: new Date().toISOString(),
    spec: {
      targetRef: {
        group: "gateway.networking.k8s.io",
        kind: "GRPCRoute",
        name: "grpc-route",
      },
      storageRef: { name: "gcs-backend" },
      capture: { includeHeaders: false, includeBody: true },
    },
    status: {
      phase: "Completed" as const,
      capturedRequests: 1200,
      bytesWritten: "50Mi",
      agentStatus: { readyReplicas: 0, desiredReplicas: 0 },
      spokeId: "spoke-2",
    },
  },
  {
    name: "failed-capture",
    namespace: "staging",
    createdAt: new Date().toISOString(),
    spec: {
      targetRef: {
        group: "gateway.networking.k8s.io",
        kind: "HTTPRoute",
        name: "staging-route",
      },
      storageRef: { name: "s3-backend" },
      capture: { includeHeaders: true, includeBody: true },
    },
    status: {
      phase: "Failed" as const,
      capturedRequests: 0,
      bytesWritten: "0",
    },
  },
];

describe("CapturesPage", () => {
  beforeEach(() => {
    vi.mocked(listCaptures).mockResolvedValue(mockCaptures);
  });

  it("renders Captures heading", async () => {
    render(await CapturesPage());
    expect(
      screen.getByRole("heading", { name: "Captures" })
    ).toBeInTheDocument();
  });

  it("shows phase filter counts", async () => {
    render(await CapturesPage());

    expect(screen.getByText("Active:")).toBeInTheDocument();
    expect(screen.getByText("Completed:")).toBeInTheDocument();
    expect(screen.getByText("Pending:")).toBeInTheDocument();
    expect(screen.getByText("Failed:")).toBeInTheDocument();
  });

  it("renders capture rows with name, target, and status", async () => {
    render(await CapturesPage());

    // Capture names
    expect(screen.getByText("api-capture")).toBeInTheDocument();
    expect(screen.getByText("grpc-capture")).toBeInTheDocument();
    expect(screen.getByText("failed-capture")).toBeInTheDocument();

    // Target kinds (two captures use HTTPRoute so use getAllByText)
    expect(screen.getAllByText("HTTPRoute").length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText("GRPCRoute")).toBeInTheDocument();

    // Statuses
    expect(screen.getByText("Active")).toBeInTheDocument();
    expect(screen.getByText("Completed")).toBeInTheDocument();
    expect(screen.getByText("Failed")).toBeInTheDocument();
  });
});
