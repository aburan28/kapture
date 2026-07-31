"""Analysis: anytime curves, normalized regret, rank aggregation.

Works off the JSONL rows written by runner.run(). Curves are best-so-far
values interpolated (step-wise, left-continuous) onto a common budget grid,
then aggregated across seeds with median and interquartile range.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from statistics import median, quantiles


def load_rows(paths: list[Path]) -> list[dict]:
    rows = []
    for p in paths:
        with p.open() as f:
            rows.extend(json.loads(line) for line in f)
    return rows


def best_so_far(rows: list[dict], budget_key: str = "trial") -> list[tuple[float, float]]:
    """(budget, best value so far) for one run, sorted by budget."""
    rows = sorted(rows, key=lambda r: r[budget_key])
    curve, best = [], float("inf")
    for r in rows:
        best = min(best, r["value"])
        curve.append((r[budget_key], best))
    return curve


def value_at(curve: list[tuple[float, float]], b: float) -> float:
    """Step-wise interpolation: value of the last point with budget <= b."""
    v = float("inf")
    for budget, value in curve:
        if budget > b:
            break
        v = value
    return v


def anytime_summary(
    paths: list[Path],
    grid: list[float],
    budget_key: str = "trial",
    optimum: float | None = None,
) -> dict[str, list[dict]]:
    """Per optimizer: median and IQR of best-so-far (or regret) on the grid."""
    by_run: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for r in load_rows(paths):
        by_run[(r["optimizer"], r["seed"])].append(r)

    curves: dict[str, list[list[tuple[float, float]]]] = defaultdict(list)
    for (opt, _seed), rows in by_run.items():
        curves[opt].append(best_so_far(rows, budget_key))

    out: dict[str, list[dict]] = {}
    for opt, runs in curves.items():
        points = []
        for b in grid:
            vals = [value_at(c, b) for c in runs]
            if optimum is not None:
                vals = [v - optimum for v in vals]  # simple regret
            vals = sorted(vals)
            q = quantiles(vals, n=4) if len(vals) >= 4 else [vals[0], median(vals), vals[-1]]
            points.append({"budget": b, "median": median(vals), "q25": q[0], "q75": q[-1]})
        out[opt] = points
    return out


def mean_ranks(summaries: dict[str, dict[str, list[dict]]], at_index: int = -1) -> dict[str, float]:
    """Average rank of each optimizer across problems at one budget point.

    `summaries` maps problem name -> output of anytime_summary. Lower is
    better. This is the input you'd feed a critical-difference diagram.
    """
    rank_sums: dict[str, float] = defaultdict(float)
    n_problems = 0
    for per_opt in summaries.values():
        finals = sorted(per_opt.items(), key=lambda kv: kv[1][at_index]["median"])
        for rank, (opt, _) in enumerate(finals, start=1):
            rank_sums[opt] += rank
        n_problems += 1
    return {opt: s / n_problems for opt, s in rank_sums.items()}
