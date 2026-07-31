"""Example: Optuna TPE vs random search on the synthetic problem.

    pip install optuna numpy
    python run_example.py
"""

from __future__ import annotations

from pathlib import Path

import optuna

from harness.adapters.optuna_adapter import OptunaOptimizer
from harness.adapters.random_search import RandomSearch
from harness.analysis import anytime_summary
from harness.problem import NoisyBranin
from harness.runner import Budget, run

optuna.logging.set_verbosity(optuna.logging.WARNING)

SEEDS = range(10)
BUDGET = Budget(max_trials=100)
OUT = Path("results")

OPTIMIZERS = {
    "random": lambda problem, seed: RandomSearch(problem, seed),
    "tpe": lambda problem, seed: OptunaOptimizer(
        problem, seed, sampler=optuna.samplers.TPESampler(seed=seed), name="tpe"
    ),
    "tpe-multivariate": lambda problem, seed: OptunaOptimizer(
        problem,
        seed,
        sampler=optuna.samplers.TPESampler(seed=seed, multivariate=True),
        name="tpe-multivariate",
    ),
}


def main() -> None:
    problem = NoisyBranin()
    paths = [
        run(make, problem, seed, BUDGET, OUT)
        for make in OPTIMIZERS.values()
        for seed in SEEDS
    ]

    grid = [10, 25, 50, 100]
    summary = anytime_summary(paths, grid, budget_key="trial", optimum=problem.optimum)
    print(f"\nSimple regret on {problem.name} (median [q25, q75] over {len(SEEDS)} seeds)\n")
    header = "trials".ljust(8) + "".join(opt.ljust(28) for opt in summary)
    print(header)
    for i, b in enumerate(grid):
        row = f"{b:<8}"
        for points in summary.values():
            p = points[i]
            row += f"{p['median']:.3f} [{p['q25']:.3f}, {p['q75']:.3f}]".ljust(28)
        print(row)


if __name__ == "__main__":
    main()
