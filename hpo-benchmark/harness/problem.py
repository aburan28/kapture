"""Problem interface: the objective being optimized.

A Problem reports its own cost per evaluation (epochs, surrogate runtime,
...). For tabular/surrogate benchmarks this is the recorded training time,
which lets the harness simulate wall-clock without real training.
"""

from __future__ import annotations

import math
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from .space import Categorical, Float, SearchSpace


@dataclass(frozen=True)
class Observation:
    value: float          # objective value at this fidelity (lower is better)
    cost: float           # budget consumed by this evaluation (same unit problem-wide)
    fidelity: float       # fidelity it was evaluated at


class Problem(ABC):
    name: str
    space: SearchSpace
    minimize: bool = True
    max_fidelity: float = 1.0
    optimum: float | None = None  # known best value, enables regret plots

    @abstractmethod
    def evaluate(
        self, config: dict[str, Any], fidelity: float, seed: int
    ) -> Observation:
        """Evaluate config at the given fidelity. Must be deterministic in
        (config, fidelity, seed) so runs are reproducible."""


class NoisyBranin(Problem):
    """Cheap synthetic problem with a categorical twist and a fidelity knob.

    Branin on (x1, x2) plus a penalty depending on a categorical choice.
    Low fidelity adds bias + extra noise, mimicking partial training.
    Known optimum ~0.397887 (at full fidelity, noise-free).
    """

    name = "noisy_branin"
    minimize = True
    max_fidelity = 100.0  # "epochs"
    optimum = 0.397887

    space = SearchSpace(
        params=(
            Float("x1", -5.0, 10.0),
            Float("x2", 0.0, 15.0),
            Categorical("variant", ("a", "b", "c")),
        )
    )

    _variant_penalty = {"a": 0.0, "b": 0.5, "c": 2.0}

    def __init__(self, noise_sd: float = 0.1):
        self.noise_sd = noise_sd

    def evaluate(
        self, config: dict[str, Any], fidelity: float, seed: int
    ) -> Observation:
        x1, x2 = config["x1"], config["x2"]
        branin = (
            (x2 - 5.1 / (4 * math.pi**2) * x1**2 + 5 / math.pi * x1 - 6) ** 2
            + 10 * (1 - 1 / (8 * math.pi)) * math.cos(x1)
            + 10
        )
        value = branin + self._variant_penalty[config["variant"]]

        frac = max(fidelity, 1.0) / self.max_fidelity
        rng = random.Random((seed, repr(sorted(config.items())), fidelity).__hash__())
        bias = 2.0 * (1 - frac)              # low fidelity overestimates loss
        noise = rng.gauss(0, self.noise_sd * (1 + (1 - frac)))
        return Observation(
            value=value + bias + noise,
            cost=max(fidelity, 1.0),         # cost proportional to epochs run
            fidelity=fidelity,
        )
