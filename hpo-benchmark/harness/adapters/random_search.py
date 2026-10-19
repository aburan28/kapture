"""Random search baseline — the floor every method must clear."""

from __future__ import annotations

import random

from ..optimizer import Optimizer, Trial
from ..problem import Observation, Problem


class RandomSearch(Optimizer):
    name = "random"

    def __init__(self, problem: Problem, seed: int):
        super().__init__(problem, seed)
        self._rng = random.Random(seed)
        self._next_id = 0

    def ask(self) -> Trial:
        trial = Trial(
            id=self._next_id,
            config=self.problem.space.sample(self._rng),
            fidelity=self.problem.max_fidelity,
        )
        self._next_id += 1
        return trial

    def tell(self, trial: Trial, obs: Observation) -> None:
        pass  # uninformed: history is ignored by construction
