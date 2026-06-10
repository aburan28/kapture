"""Adapter wrapping any Optuna sampler (+ optional pruner) behind ask/tell.

Uses Optuna's study.ask()/study.tell() so the harness keeps control of the
loop. The neutral SearchSpace is replayed onto each optuna.Trial via the
suggest_* API, which preserves conditionals (Optuna is define-by-run).
"""

from __future__ import annotations

from typing import Any

import optuna

from ..optimizer import Optimizer, Trial
from ..problem import Observation, Problem
from ..space import Categorical, Float, Int


class OptunaOptimizer(Optimizer):
    def __init__(
        self,
        problem: Problem,
        seed: int,
        sampler: optuna.samplers.BaseSampler | None = None,
        pruner: optuna.pruners.BasePruner | None = None,
        name: str = "optuna-tpe",
    ):
        super().__init__(problem, seed)
        self.name = name
        self._study = optuna.create_study(
            direction="minimize" if problem.minimize else "maximize",
            sampler=sampler or optuna.samplers.TPESampler(seed=seed),
            pruner=pruner or optuna.pruners.NopPruner(),
        )
        self._next_id = 0

    def _suggest(self, otrial: optuna.Trial) -> dict[str, Any]:
        config: dict[str, Any] = {}
        for p in self.problem.space.params:
            if not self.problem.space.active(config, p):
                continue
            if isinstance(p, Float):
                config[p.name] = otrial.suggest_float(p.name, p.low, p.high, log=p.log)
            elif isinstance(p, Int):
                config[p.name] = otrial.suggest_int(p.name, p.low, p.high, log=p.log)
            elif isinstance(p, Categorical):
                config[p.name] = otrial.suggest_categorical(p.name, list(p.choices))
        return config

    def ask(self) -> Trial:
        otrial = self._study.ask()
        trial = Trial(
            id=self._next_id,
            config=self._suggest(otrial),
            fidelity=self.problem.max_fidelity,
            native=otrial,
        )
        self._next_id += 1
        return trial

    def report(self, trial: Trial, step: int, value: float) -> bool:
        trial.native.report(value, step)
        return trial.native.should_prune()

    def tell(self, trial: Trial, obs: Observation) -> None:
        if obs.fidelity < self.problem.max_fidelity:
            self._study.tell(trial.native, state=optuna.trial.TrialState.PRUNED)
        else:
            self._study.tell(trial.native, obs.value)
