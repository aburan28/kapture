"""Ask/tell optimizer interface.

The harness drives the loop:

    trial = opt.ask()
    obs = problem.evaluate(trial.config, trial.fidelity, seed)
    # optionally, for iterative problems, report intermediate values and
    # honor opt.should_stop(trial, step, value) to support pruners
    opt.tell(trial, obs)

Keeping the loop in the harness — rather than calling each framework's
`optimize()` — is what makes budget accounting and logging uniform.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from .problem import Observation, Problem


@dataclass
class Trial:
    id: int
    config: dict[str, Any]
    fidelity: float
    native: Any = None  # adapter-private handle (e.g. an optuna.Trial)


class Optimizer(ABC):
    """One optimization run on one problem. Construct per (problem, seed)."""

    name: str

    def __init__(self, problem: Problem, seed: int):
        self.problem = problem
        self.seed = seed

    @abstractmethod
    def ask(self) -> Trial:
        """Propose the next configuration (and fidelity) to evaluate."""

    @abstractmethod
    def tell(self, trial: Trial, obs: Observation) -> None:
        """Report the final observation for a trial."""

    def report(self, trial: Trial, step: int, value: float) -> bool:
        """Report an intermediate value; return True if the trial should be
        stopped early (pruned). Default: never prune."""
        return False
