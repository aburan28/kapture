import { vi, describe, it, expect, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";

const mockPathname = vi.fn();
vi.mock("next/navigation", () => ({
  usePathname: () => mockPathname(),
}));

vi.mock("next/link", () => ({
  default: ({ children, href, ...props }: any) => (
    <a href={href} {...props}>
      {children}
    </a>
  ),
}));

import { Sidebar } from "../sidebar";

describe("Sidebar", () => {
  beforeEach(() => {
    mockPathname.mockReset();
  });

  it("renders all 4 nav items", () => {
    mockPathname.mockReturnValue("/");
    render(<Sidebar />);

    expect(screen.getByText("Dashboard")).toBeInTheDocument();
    expect(screen.getByText("Captures")).toBeInTheDocument();
    expect(screen.getByText("Spokes")).toBeInTheDocument();
    expect(screen.getByText("Storage")).toBeInTheDocument();
  });

  it("marks Dashboard as active when pathname is /", () => {
    mockPathname.mockReturnValue("/");
    render(<Sidebar />);

    const dashboardLink = screen.getByText("Dashboard").closest("a");
    expect(dashboardLink?.className).toContain("bg-indigo-600/15");
    expect(dashboardLink?.className).toContain("text-indigo-400");
  });

  it("marks Captures as active when pathname is /captures", () => {
    mockPathname.mockReturnValue("/captures");
    render(<Sidebar />);

    const capturesLink = screen.getByText("Captures").closest("a");
    expect(capturesLink?.className).toContain("bg-indigo-600/15");
    expect(capturesLink?.className).toContain("text-indigo-400");
  });

  it("marks Captures as active for nested routes like /captures/some-id", () => {
    mockPathname.mockReturnValue("/captures/some-id");
    render(<Sidebar />);

    const capturesLink = screen.getByText("Captures").closest("a");
    expect(capturesLink?.className).toContain("bg-indigo-600/15");
    expect(capturesLink?.className).toContain("text-indigo-400");
  });

  it("does NOT mark Dashboard as active when pathname is /captures", () => {
    mockPathname.mockReturnValue("/captures");
    render(<Sidebar />);

    const dashboardLink = screen.getByText("Dashboard").closest("a");
    expect(dashboardLink?.className).not.toContain("bg-indigo-600/15");
    expect(dashboardLink?.className).not.toContain("text-indigo-400");
  });

  it("shows Kapture title and version", () => {
    mockPathname.mockReturnValue("/");
    render(<Sidebar />);

    expect(screen.getByText("Kapture")).toBeInTheDocument();
    expect(screen.getByText("Kapture UI v0.1.0")).toBeInTheDocument();
  });
});
