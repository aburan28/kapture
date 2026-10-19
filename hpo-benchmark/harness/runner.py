"""The benchmark loop: budget accounting + JSONL recording.

One Run = (optimizer x problem x seed). Every observation is appended as a
JSONL row carrying all three cumulative budget counters, so analysis can
build anytime curves in any currency after the fact.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .optimizer import Optimizer
from .problem import Problem


@dataclass(frozen=True)
class Budget:
    max_trials: int | None = None
    max_cost: float | None = None       # in the problem's cost unit
    max_wall_s: float | None = None     # optimizer overhead + evaluation

    def exhausted(self, trials: int, cost: float, wall_s: float) -> bool:
        return (
            (self.max_trials is not None and trials >= self.max_trials)
            or (self.max_cost is not None and cost >= self.max_cost)
            or (self.max_wall_s is not None and wall_s >= self.max_wall_s)
        )


def run(
    make_optimizer: Callable[[Problem, int], Optimizer],
    problem: Problem,
    seed: int,
    budget: Budget,
    out_dir: Path,
) -> Path:
    opt = make_optimizer(problem, seed)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{opt.name}__{problem.name}__seed{seed}.jsonl"

    trials = 0
    cum_cost = 0.0
    cum_opt_wall = 0.0  # time spent inside the optimizer (sampler overhead)
    start = time.monotonic()

    with out_path.open("w") as f:
        while not budget.exhausted(trials, cum_cost, time.monotonic() - start):
            t0 = time.monotonic()
            trial = opt.ask()
            cum_opt_wall += time.monotonic() - t0

            obs = problem.evaluate(trial.config, trial.fidelity, seed)

            t0 = time.monotonic()
            opt.tell(trial, obs)
            cum_opt_wall += time.monotonic() - t0

            trials += 1
            cum_cost += obs.cost
            f.write(
                json.dumps(
                    {
                        "optimizer": opt.name,
                        "problem": problem.name,
                        "seed": seed,
                        "trial": trials,
                        "config": trial.config,
                        "value": obs.value,
                        "fidelity": obs.fidelity,
                        "cost": obs.cost,
                        "cum_cost": cum_cost,
                        "cum_opt_wall_s": cum_opt_wall,
                        "wall_s": time.monotonic() - start,
                    }
                )
                + "\n"
            )
    return out_path
