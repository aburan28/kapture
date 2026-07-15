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
  listStorages: vi.fn(),
  listCaptures: vi.fn(),
}));

import StoragePage from "../page";
import { listStorages, listCaptures } from "@/lib/hub-client";

const mockStorages = [
  {
    name: "s3-backend",
    namespace: "default",
    type: "S3" as const,
    phase: "Ready" as const,
    bucket: "kapture-data",
    region: "us-east-1",
    prefix: "captures/",
  },
  {
    name: "gcs-backend",
    namespace: "production",
    type: "GCS" as const,
    phase: "Ready" as const,
    bucket: "kapture-prod",
    region: "us-central1",
  },
  {
    name: "broken-backend",
    namespace: "staging",
    type: "EBS" as const,
    phase: "Error" as const,
  },
];

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
      capturedRequests: 100,
      bytesWritten: "1Mi",
      agentStatus: { readyReplicas: 1, desiredReplicas: 1 },
      spokeId: "spoke-1",
    },
  },
];

describe("StoragePage", () => {
  beforeEach(() => {
    vi.mocked(listStorages).mockResolvedValue(mockStorages);
    vi.mocked(listCaptures).mockResolvedValue(mockCaptures);
  });

  it("renders Storage Backends heading", async () => {
    render(await StoragePage());
    expect(
      screen.getByRole("heading", { name: "Storage Backends" })
    ).toBeInTheDocument();
  });

  it("shows total/ready/error stat cards", async () => {
    render(await StoragePage());

    expect(screen.getByText("Total Backends")).toBeInTheDocument();
    // "3" appears as stat card value for total backends
    expect(screen.getAllByText("3").length).toBeGreaterThanOrEqual(1);

    // "Ready" appears both as a StatCard label and as StoragePhaseBadge text
    expect(screen.getAllByText("Ready").length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText("Errors")).toBeInTheDocument();
  });

  it("renders storage cards with name and type", async () => {
    render(await StoragePage());

    expect(screen.getByText("s3-backend")).toBeInTheDocument();
    expect(screen.getByText("gcs-backend")).toBeInTheDocument();
    expect(screen.getByText("broken-backend")).toBeInTheDocument();

    // Type labels appear both as icon abbreviation and type text in cards
    expect(screen.getAllByText("S3").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("GCS").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("EBS").length).toBeGreaterThanOrEqual(1);
  });
});
