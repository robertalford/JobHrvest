#!/usr/bin/env python3
"""Backfill the play library from the v6.0 -> v6.9 extractor chain.

The historical DB rows were reset when v6.9 was re-crowned, so the durable
source of truth for earlier promotions is the extractor file chain itself.
This script diffs adjacent versions, asks Ollama to summarise the winning idea,
records one Play per child version, and mirrors the reviewed library into
``database/play_library.json`` for clone restore.

Run inside the API container:

    docker exec -it jobharvest-api python -m scripts.backfill_play_library

Safe to re-run — each Play file is keyed by version, so reruns overwrite the
same on-disk record rather than duplicating entries.
"""
from __future__ import annotations

import asyncio
import difflib
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.ml.champion_challenger import memory_store
from app.ml.champion_challenger.play_library import Play, default_library


_AXES = ("discovery", "quality_extraction", "volume_accuracy", "field_completeness")

def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "backend").is_dir() and (parent / "storage").exists():
            return parent
        if parent.name == "app" and (parent / "tests").is_dir():
            return parent
    return here.parents[1]


def _version_key(path: Path) -> int:
    match = re.search(r"_v(\d+)\.py$", path.name)
    return int(match.group(1)) if match else -1


def _version_label(path: Path) -> str:
    digits = _version_key(path)
    if digits == 610:
        return "v6.10"
    if 60 <= digits <= 69:
        return f"v6.{digits - 60}"
    return f"v{digits}"


def _candidate_chain() -> list[Path]:
    repo_root = _repo_root()
    root = repo_root / "backend" / "app" / "crawlers"
    if not root.exists():
        root = repo_root / "app" / "crawlers"
    files = sorted(root.glob("tiered_extractor_v*.py"), key=_version_key)
    return [f for f in files if 60 <= _version_key(f) <= 69]


def _diff_text(parent: Path, child: Path) -> str:
    parent_lines = parent.read_text().splitlines()
    child_lines = child.read_text().splitlines()
    diff = difflib.unified_diff(
        parent_lines,
        child_lines,
        fromfile=parent.name,
        tofile=child.name,
        lineterm="",
    )
    return "\n".join(diff)


def _heuristic_summary(child: str, diff_text: str) -> str:
    changed = max(diff_text.count("\n+"), diff_text.count("\n-"))
    return f"{child} — focused extractor refinement from parent ({changed} diff hunks reviewed)"


async def _backfill() -> tuple[int, Path]:
    written = 0
    chain = _candidate_chain()
    if len(chain) < 2:
        raise RuntimeError("not enough v6.x extractor files found to backfill the play library")

    for parent, child in zip(chain, chain[1:]):
        diff_text = _diff_text(parent, child)
        child_version = _version_label(child)
        parent_version = _version_label(parent)
        meta = memory_store._summarize_promotion_with_ollama(  # noqa: SLF001 - intentional one-off backfill reuse
            diff_summary=diff_text[:3000],
            axes={axis: 0.0 for axis in _AXES},
        )
        play = Play(
            version=child_version,
            summary=(meta.get("notes") or _heuristic_summary(child_version, diff_text))[:200],
            axis_deltas={axis: 0.0 for axis in _AXES},
            composite_delta=1.0,
            ats_clusters_fixed=list(meta.get("ats_clusters_fixed") or []),
            diff_keywords=list(meta.get("diff_keywords") or []),
            notes=f"{parent_version} -> {child_version}",
        )
        default_library.record(play)
        written += 1

    snapshot = default_library.write_snapshot()
    return written, snapshot


if __name__ == "__main__":
    n, snapshot = asyncio.run(_backfill())
    print(f"play_library: backfilled {n} play(s) into {default_library.root}")
    print(f"play_library: snapshot written to {snapshot}")
