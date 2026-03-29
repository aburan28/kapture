import { describe, it, expect, afterEach } from "vitest";
import { render, screen, cleanup } from "@testing-library/react";
import {
  PhaseBadge,
  HealthBadge,
  StoragePhaseBadge,
} from "../status-badge";

afterEach(() => cleanup());

describe("PhaseBadge", () => {
  it.each(["Active", "Completed", "Pending", "Failed"] as const)(
    "renders %s phase",
    (phase) => {
      render(<PhaseBadge phase={phase} />);
      expect(screen.getByText(phase)).toBeInTheDocument();
    }
  );

  it("shows a pulsing dot for Active phase", () => {
    const { container } = render(<PhaseBadge phase="Active" />);
    const dot = container.querySelector(".animate-pulse");
    expect(dot).toBeInTheDocument();
  });

  it("does not pulse for Completed phase", () => {
    const { container } = render(<PhaseBadge phase="Completed" />);
    const dot = container.querySelector(".animate-pulse");
    expect(dot).not.toBeInTheDocument();
  });
});

describe("HealthBadge", () => {
  it.each(["Healthy", "Degraded", "Disconnected"] as const)(
    "renders %s state",
    (state) => {
      render(<HealthBadge state={state} />);
      expect(screen.getByText(state)).toBeInTheDocument();
    }
  );

  it("shows a pulsing dot for Healthy state", () => {
    const { container } = render(<HealthBadge state="Healthy" />);
    const dot = container.querySelector(".animate-pulse");
    expect(dot).toBeInTheDocument();
  });
});

describe("StoragePhaseBadge", () => {
  it.each(["Ready", "Error", "Pending"] as const)(
    "renders %s phase",
    (phase) => {
      render(<StoragePhaseBadge phase={phase} />);
      expect(screen.getByText(phase)).toBeInTheDocument();
    }
  );
});
