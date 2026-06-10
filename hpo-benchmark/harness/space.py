"""Framework-neutral search-space definition.

Each optimizer adapter translates this into its framework's native space.
Conditionals are expressed as a parent (name, value) guard: the parameter
only exists when the parent parameter took one of the listed values.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import Any, Sequence


@dataclass(frozen=True)
class Float:
    name: str
    low: float
    high: float
    log: bool = False
    parent: tuple[str, tuple[Any, ...]] | None = None


@dataclass(frozen=True)
class Int:
    name: str
    low: int
    high: int
    log: bool = False
    parent: tuple[str, tuple[Any, ...]] | None = None


@dataclass(frozen=True)
class Categorical:
    name: str
    choices: tuple[Any, ...]
    parent: tuple[str, tuple[Any, ...]] | None = None


Param = Float | Int | Categorical


@dataclass(frozen=True)
class SearchSpace:
    params: tuple[Param, ...]

    def active(self, config: dict[str, Any], p: Param) -> bool:
        if p.parent is None:
            return True
        parent_name, allowed = p.parent
        return config.get(parent_name) in allowed

    def sample(self, rng: random.Random) -> dict[str, Any]:
        """Uniform sample respecting conditionals; the random-search baseline
        and useful for seeding/sanity checks."""
        config: dict[str, Any] = {}
        for p in self.params:  # params must be ordered parents-first
            if not self.active(config, p):
                continue
            if isinstance(p, Float):
                if p.log:
                    config[p.name] = math.exp(
                        rng.uniform(math.log(p.low), math.log(p.high))
                    )
                else:
                    config[p.name] = rng.uniform(p.low, p.high)
            elif isinstance(p, Int):
                if p.log:
                    config[p.name] = round(
                        math.exp(rng.uniform(math.log(p.low), math.log(p.high)))
                    )
                else:
                    config[p.name] = rng.randint(p.low, p.high)
            else:
                config[p.name] = rng.choice(list(p.choices))
        return config
