"""MAP-Elites archive keyed by target axis and ATS cluster."""

from __future__ import annotations

from dataclasses import asdict

from .population import Individual

_AXES = ("discovery", "quality_extraction", "volume_accuracy", "field_completeness")
_ATS = ("workday", "oracle_cx", "greenhouse", "lever", "smartrecruiters", "jobvite", "applyflow", "wix", "squarespace", "wordpress", "other")


class MAPElitesArchive:
    def __init__(self) -> None:
        self.cells: dict[str, Individual] = {}

    def upsert(self, ind: Individual) -> bool:
        existing = self.cells.get(ind.behaviour_cell)
        new_score = ind.ab_composite if ind.ab_composite is not None else ind.fixture_composite or 0.0
        old_score = (
            existing.ab_composite if existing and existing.ab_composite is not None
            else existing.fixture_composite if existing else 0.0
        ) or 0.0
        if existing is None or new_score > old_score:
            self.cells[ind.behaviour_cell] = ind
            return True
        return False

    def sample_ancestors(self, target_cell: str, k: int = 3) -> list[Individual]:
        axis, _, ats = target_cell.partition("|")
        picks: list[Individual] = []
        direct = self.cells.get(target_cell)
        if direct:
            picks.append(direct)
        row = next((item for cell, item in self.cells.items() if cell.startswith(f"{axis}|") and item not in picks), None)
        if row:
            picks.append(row)
        col = next((item for cell, item in self.cells.items() if cell.endswith(f"|{ats}") and item not in picks), None)
        if col:
            picks.append(col)
        return picks[:k]

    def coverage(self) -> float:
        total = len(_AXES) * len(_ATS)
        return round(len(self.cells) / total, 4)

    def to_dict(self) -> dict:
        return {cell: asdict(ind) for cell, ind in self.cells.items()}


__all__ = ["MAPElitesArchive"]
