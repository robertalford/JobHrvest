"""Population/island bookkeeping for evolutionary search."""

from __future__ import annotations

import json
import random
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass
class Individual:
    version_tag: str
    parent_tag: str | None
    island_id: int
    focus_axis: str
    behaviour_cell: str
    status: str
    fixture_composite: float | None
    ab_composite: float | None
    axes: dict
    loc: int | None
    file_path: str | None


class Island:
    def __init__(self, island_id: int, members: list[Individual] | None = None) -> None:
        self.island_id = island_id
        self.members = members or []

    def select_parent(self, p_explore: float = 0.3) -> Individual:
        if not self.members:
            raise ValueError(f"island {self.island_id} has no members")
        if len(self.members) == 1 or random.random() < p_explore:
            return random.choice(self.members)
        ranked = sorted(
            self.members,
            key=lambda item: (
                item.ab_composite if item.ab_composite is not None else -1,
                item.fixture_composite if item.fixture_composite is not None else -1,
            ),
            reverse=True,
        )
        weights = [1 / (idx + 1) for idx in range(len(ranked))]
        return random.choices(ranked, weights=weights, k=1)[0]


class PopulationStore:
    def __init__(self, archive_path: str | Path) -> None:
        self.archive_path = Path(archive_path)

    def load_ephemeral(self) -> dict:
        if not self.archive_path.exists():
            return {"islands": {}, "events": []}
        return json.loads(self.archive_path.read_text())

    def save_ephemeral(self, payload: dict) -> None:
        self.archive_path.parent.mkdir(parents=True, exist_ok=True)
        self.archive_path.write_text(json.dumps(payload, indent=2))

    def migrate_ring(self, islands: list[Island], top_n: int = 1) -> list[tuple[int, str]]:
        moved: list[tuple[int, str]] = []
        if not islands:
            return moved
        for idx, island in enumerate(islands):
            if not island.members:
                continue
            winners = sorted(
                island.members,
                key=lambda item: (
                    item.ab_composite if item.ab_composite is not None else -1,
                    item.fixture_composite if item.fixture_composite is not None else -1,
                ),
                reverse=True,
            )[:top_n]
            target = islands[(idx + 1) % len(islands)]
            for winner in winners:
                target.members.append(winner)
                moved.append((target.island_id, winner.version_tag))
        return moved

    @staticmethod
    def serialize_individuals(items: list[Individual]) -> list[dict]:
        return [asdict(item) for item in items]


__all__ = ["Individual", "Island", "PopulationStore"]
