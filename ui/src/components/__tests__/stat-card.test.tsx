import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { StatCard } from "../stat-card";

describe("StatCard", () => {
  it("renders label and value", () => {
    render(<StatCard label="Total Captures" value={42} />);
    expect(screen.getByText("Total Captures")).toBeInTheDocument();
    expect(screen.getByText("42")).toBeInTheDocument();
  });

  it("renders string values", () => {
    render(<StatCard label="Ratio" value="3/4" />);
    expect(screen.getByText("3/4")).toBeInTheDocument();
  });

  it("renders sub text when provided", () => {
    render(<StatCard label="Active" value={5} sub="out of 10" />);
    expect(screen.getByText("out of 10")).toBeInTheDocument();
  });

  it("does not render sub when omitted", () => {
    const { container } = render(<StatCard label="Test" value={1} />);
    // The sub text element should not exist
    const paragraphs = container.querySelectorAll("p");
    // label + value = 2 elements
    expect(paragraphs.length).toBe(2);
  });
});
