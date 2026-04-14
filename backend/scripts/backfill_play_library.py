#!/usr/bin/env python3
"""Seed the play library from historical promoted MLModelTestRun records.

The play library starts empty after the 2026-04-14 reset. This script walks
the ``ml_models`` + ``ml_model_test_runs`` tables, finds models that were
promoted (``status='tested'`` with ``challenger_composite > champion_composite``
in their latest completed run), and writes one Play per promotion into
``storage/play_library/``.

Run inside the API container:

    docker exec -it jobharvest-api python -m scripts.backfill_play_library

Safe to re-run — each Play file is keyed by version, so reruns overwrite with
the latest data rather than duplicating entries.
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: F401 — typing-only

from app.db.base import AsyncSessionLocal
from app.ml.champion_challenger.play_library import Play, default_library
from app.models.ml_model import MLModel, MLModelTestRun


_AXES = ("discovery", "quality_extraction", "volume_accuracy", "field_completeness")


def _axis_deltas(summary: dict) -> dict[str, float]:
    champ = summary.get("champion_composite") or {}
    chal = summary.get("challenger_composite") or {}
    return {
        axis: round(float(chal.get(axis, 0)) - float(champ.get(axis, 0)), 2)
        for axis in _AXES
    }


def _composite_delta(summary: dict) -> float:
    champ = (summary.get("champion_composite") or {}).get("composite", 0) or 0
    chal = (summary.get("challenger_composite") or {}).get("composite", 0) or 0
    return round(float(chal) - float(champ), 2)


def _summarise(model: MLModel, summary: dict) -> str:
    match = summary.get("match_breakdown") or {}
    hits = match.get("model_equal_or_better", 0) + match.get("model_only", 0)
    total = summary.get("total_sites", 0) or sum(match.values()) if match else 0
    base = model.description or f"{model.name} — promoted"
    return f"{base.strip()[:160]} (sites_equal_or_better={hits}/{total})"


async def _backfill() -> int:
    written = 0
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(MLModel).where(MLModel.model_type == "tiered_extractor")
        )
        models = list(result.scalars())
        for model in models:
            latest_run = await db.execute(
                select(MLModelTestRun)
                .where(MLModelTestRun.model_id == model.id,
                       MLModelTestRun.status == "completed")
                .order_by(MLModelTestRun.completed_at.desc())
                .limit(1)
            )
            run = latest_run.scalar_one_or_none()
            if not run or not run.results_detail:
                continue
            summary = (run.results_detail or {}).get("summary") or {}
            delta = _composite_delta(summary)
            if delta <= 0.0:
                continue  # only promotions worth surfacing as exemplars
            play = Play(
                version=model.name,
                summary=_summarise(model, summary),
                axis_deltas=_axis_deltas(summary),
                composite_delta=delta,
                ats_clusters_fixed=[],
                diff_keywords=[],
                notes=None,
            )
            default_library.record(play)
            written += 1
    return written


if __name__ == "__main__":
    n = asyncio.run(_backfill())
    print(f"play_library: backfilled {n} play(s) into {default_library.root}")
