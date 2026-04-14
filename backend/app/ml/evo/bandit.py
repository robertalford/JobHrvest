"""Axis-level Thompson-sampling bandit for evolutionary focus selection."""

from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from pathlib import Path

_AXES = ("discovery", "quality_extraction", "volume_accuracy", "field_completeness")


@dataclass
class BetaPosterior:
    alpha: float = 1.0
    beta: float = 1.0

    @property
    def mean(self) -> float:
        return self.alpha / max(1e-9, self.alpha + self.beta)


class AxisBandit:
    def __init__(self, state: dict[str, dict[str, float]] | None = None) -> None:
        self.state = {
            axis: BetaPosterior(**(state or {}).get(axis, {}))
            for axis in _AXES
        }

    def sample(self, n: int) -> list[str]:
        draws = []
        for axis, posterior in self.state.items():
            draws.append((random.betavariate(posterior.alpha, posterior.beta), axis))
        draws.sort(reverse=True)
        return [axis for _, axis in draws[:n]]

    def update(self, axis: str, outcome: str) -> None:
        posterior = self.state.setdefault(axis, BetaPosterior())
        if outcome == "promoted":
            posterior.alpha += 1.0
        elif outcome == "fixture_close":
            posterior.alpha += 0.5
            posterior.beta += 0.5
        elif outcome == "fixture_regressed":
            posterior.beta += 1.0
        else:
            posterior.alpha += 0.1
            posterior.beta += 0.5

    def decay(self) -> None:
        for posterior in self.state.values():
            posterior.alpha = posterior.alpha * 0.8 + 1.0
            posterior.beta = posterior.beta * 0.8 + 1.0

    def posterior_means(self) -> dict[str, float]:
        return {axis: round(posterior.mean, 4) for axis, posterior in self.state.items()}

    def entropy_bits(self) -> float:
        means = list(self.posterior_means().values())
        total = sum(means) or 1.0
        probs = [value / total for value in means if value > 0]
        return round(-sum(p * math.log2(p) for p in probs), 4)

    def reset_if_collapsed(self, threshold_bits: float = 0.5) -> bool:
        if self.entropy_bits() >= threshold_bits:
            return False
        self.state = {axis: BetaPosterior() for axis in _AXES}
        return True

    def to_dict(self) -> dict[str, dict[str, float]]:
        return {
            axis: {"alpha": posterior.alpha, "beta": posterior.beta}
            for axis, posterior in self.state.items()
        }

    @classmethod
    def from_file(cls, path: str | Path) -> "AxisBandit":
        p = Path(path)
        if not p.exists():
            return cls()
        return cls(json.loads(p.read_text()))

    def save(self, path: str | Path) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self.to_dict(), indent=2))


__all__ = ["AxisBandit", "BetaPosterior"]
