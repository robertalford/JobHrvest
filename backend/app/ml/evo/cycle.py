"""Async orchestration for an evolutionary-search cycle."""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from dataclasses import asdict
from pathlib import Path

from .archive import MAPElitesArchive
from .bandit import AxisBandit
from .population import Individual, Island, PopulationStore


async def run_cycle(*, model_id: str, working_dir: str, n_candidates: int = 3, islands_n: int = 1) -> dict:
    cycle_id = str(uuid.uuid4())
    root = Path(working_dir) / "storage" / "evo"
    root.mkdir(parents=True, exist_ok=True)

    bandit = AxisBandit.from_file(root / "bandit.json")
    focus_axes = bandit.sample(max(1, n_candidates))
    archive = MAPElitesArchive()
    store = PopulationStore(root / "archive.json")
    cycle_manifest = {
        "cycle_id": cycle_id,
        "model_id": model_id,
        "focus_axes": focus_axes,
        "candidates": [],
    }

    islands = [Island(island_id=idx) for idx in range(islands_n)]
    for idx in range(n_candidates):
        axis = focus_axes[idx % len(focus_axes)]
        island = islands[idx % len(islands)]
        tag = f"{_next_version_stub()}_i{idx}"
        individual = Individual(
            version_tag=tag,
            parent_tag=None,
            island_id=island.island_id,
            focus_axis=axis,
            behaviour_cell=f"{axis}|other",
            status="pending",
            fixture_composite=None,
            ab_composite=None,
            axes={},
            loc=None,
            file_path=None,
        )
        island.members.append(individual)
        archive.upsert(individual)
        cycle_manifest["candidates"].append(asdict(individual))

    bandit.save(root / "bandit.json")
    store.save_ephemeral({"archive": archive.to_dict(), "cycles": [cycle_manifest]})
    (root / "metrics.json").write_text(json.dumps({
        "promotion_rate_per_cycle": 0.0,
        "time_to_next_promotion_hours": None,
        "field_completeness": None,
        "champion_composite": None,
        "fixture_false_positive_rate": None,
        "codex_wall_time_median_min_per_iteration": None,
        "cycle_wall_time_median_min": None,
        "island_diversity": None,
        "archive_coverage": archive.coverage(),
        "bandit_entropy": bandit.entropy_bits(),
    }, indent=2))
    cycle_dir = root / f"cycle_{cycle_id}"
    cycle_dir.mkdir(parents=True, exist_ok=True)
    (cycle_dir / "manifest.json").write_text(json.dumps(cycle_manifest, indent=2))
    await asyncio.sleep(0)
    return cycle_manifest


def _next_version_stub() -> str:
    return os.environ.get("EVO_VERSION_TAG", "v_next")


__all__ = ["run_cycle"]
