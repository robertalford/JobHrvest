"""Lightweight read/write helper for ``storage/auto_improve_memory.json``.

The memory file (v2 schema, established 2026-04-14 reset) is the only persistent
context Codex sees between iterations besides the prompt itself. Keep it small —
the file is read directly into prompts and every byte is a token.

Schema fields (see file for current values):
    schema, reset_at, baseline, note,
    known_hard_patterns, what_works_well, what_doesnt_work,
    banned_approaches, recent_promotions, recent_rejections.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

def _resolve_default_path() -> Path:
    """Resolve the memory file path.

    Priority: env var → ``/storage`` bind mount (container) → repo-root storage (host).
    """
    explicit = os.environ.get("AUTO_IMPROVE_MEMORY_PATH")
    if explicit:
        return Path(explicit).resolve()
    if os.path.isdir("/storage"):
        return Path("/storage/auto_improve_memory.json")
    return Path(os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "..", "..", "..", "storage", "auto_improve_memory.json",
    )).resolve()


_DEFAULT_PATH = _resolve_default_path()

_MAX_PROMOTIONS = 10
_MAX_REJECTIONS = 20
_MAX_BANNED = 15
_BAN_EXPIRES_AFTER_ITERATIONS = 3


def _empty_memory() -> dict[str, Any]:
    return {
        "schema": "v2",
        "reset_at": "2026-04-14",
        "baseline": {"version": "v6.9", "composite": 85.4, "axes": {}},
        "banned_approaches": [],
        "recent_promotions": [],
        "recent_rejections": [],
    }


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat(timespec="seconds")


def load(path: Path | str | None = None) -> dict[str, Any]:
    p = Path(path) if path else _DEFAULT_PATH
    if not p.exists():
        return _empty_memory()
    try:
        with p.open() as f:
            data = json.load(f)
        if data.get("schema") != "v2":
            raise ValueError(f"unsupported memory schema: {data.get('schema')}")
        for key in ("banned_approaches", "recent_promotions", "recent_rejections"):
            data.setdefault(key, [])
        return data
    except (json.JSONDecodeError, ValueError) as e:
        # Refuse to silently overwrite a corrupt memory file. Caller must triage.
        raise RuntimeError(f"memory file at {p} is unreadable: {e}") from e


def save(data: dict[str, Any], path: Path | str | None = None) -> None:
    p = Path(path) if path else _DEFAULT_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as f:
        json.dump(data, f, indent=2)


def baseline_axes(data: dict[str, Any] | None = None) -> dict[str, float]:
    data = data or load()
    return data.get("baseline", {}).get("axes", {})


def append_promotion(
    version: str,
    composite: float,
    axes: dict[str, float],
    summary: str,
    *,
    test_run_id: str | None = None,
    diff_summary: str | None = None,
    path: Path | str | None = None,
) -> dict[str, Any]:
    data = load(path)
    entry = {
        "version": version,
        "composite": round(float(composite), 1),
        "axes": {k: round(float(v), 1) for k, v in axes.items()},
        "summary": summary[:200],
        "diff_summary": (diff_summary or "")[:240],
        "test_run_id": test_run_id,
        "promoted_at": _now(),
    }
    data["recent_promotions"].insert(0, entry)
    data["recent_promotions"] = data["recent_promotions"][:_MAX_PROMOTIONS]
    # Promotion supersedes any earlier ban on the same approach
    data["banned_approaches"] = [
        b for b in data["banned_approaches"]
        if b.get("summary", "").strip() != summary.strip()
    ]
    save(data, path)

    # Dual-write into the play library so Codex can retrieve this as an exemplar.
    # Play library lives alongside memory — same lifecycle, different indexing.
    # Failure here is non-fatal (the memory update already succeeded).
    try:
        from .play_library import Play, default_library  # local import to avoid cycle
        champ_axes = axes or {}
        composite_delta = round(float(composite) - data["baseline"].get("composite", 0.0), 2)
        default_library.record(Play(
            version=version,
            summary=summary[:200],
            axis_deltas={k: round(float(v), 2) for k, v in champ_axes.items()},
            composite_delta=composite_delta,
            ats_clusters_fixed=[],
            diff_keywords=[],
            notes=(diff_summary or None),
        ))
    except Exception as e:  # noqa: BLE001
        import logging
        logging.getLogger(__name__).warning("play_library.record failed for %s: %s", version, e)

    return entry


def append_rejection(
    version: str,
    reason: str,
    *,
    fixture_score: float | None = None,
    composite: float | None = None,
    path: Path | str | None = None,
) -> dict[str, Any]:
    data = load(path)
    entry = {
        "version": version,
        "reason": reason[:240],
        "fixture_score": fixture_score,
        "composite": composite,
        "rejected_at": _now(),
    }
    data["recent_rejections"].insert(0, entry)
    data["recent_rejections"] = data["recent_rejections"][:_MAX_REJECTIONS]
    save(data, path)
    return entry


def ban_approach(
    summary: str,
    *,
    reason: str = "",
    expires_after: int = _BAN_EXPIRES_AFTER_ITERATIONS,
    path: Path | str | None = None,
) -> dict[str, Any]:
    """Record an approach Codex should not retry for the next ``expires_after`` iterations.

    Each subsequent ``decay_bans`` call reduces ``ttl_iterations`` by 1; when it
    hits 0 the entry is removed.
    """
    data = load(path)
    summary = summary.strip()[:200]
    # Replace existing ban with the same summary (refresh TTL)
    data["banned_approaches"] = [
        b for b in data["banned_approaches"] if b.get("summary") != summary
    ]
    entry = {
        "summary": summary,
        "reason": reason[:200],
        "ttl_iterations": int(expires_after),
        "added_at": _now(),
    }
    data["banned_approaches"].insert(0, entry)
    data["banned_approaches"] = data["banned_approaches"][:_MAX_BANNED]
    save(data, path)
    return entry


def decay_bans(path: Path | str | None = None) -> int:
    """Tick every banned approach down by 1; drop expired entries. Returns the new count."""
    data = load(path)
    survivors = []
    for b in data.get("banned_approaches", []):
        ttl = int(b.get("ttl_iterations", 0)) - 1
        if ttl > 0:
            b["ttl_iterations"] = ttl
            survivors.append(b)
    data["banned_approaches"] = survivors
    save(data, path)
    return len(survivors)


def render_recent_changes_for_prompt(data: dict[str, Any] | None = None, max_items: int = 3) -> str:
    """Compact bullet list for the dynamic prompt head — costs ~300 tokens max."""
    data = data or load()
    promos = data.get("recent_promotions", [])[:max_items]
    if not promos:
        return "_no promoted iterations since reset_"
    lines = []
    for p in promos:
        axes = p.get("axes", {})
        delta = (
            f"d{axes.get('discovery', 0):.0f}/q{axes.get('quality_extraction', 0):.0f}"
            f"/v{axes.get('volume_accuracy', 0):.0f}/f{axes.get('field_completeness', 0):.0f}"
        )
        lines.append(
            f"- {p['version']} ({p['promoted_at'][:10]}, comp {p['composite']}) {delta}: {p.get('diff_summary') or p.get('summary')}"
        )
    return "\n".join(lines)


def render_banned_for_prompt(data: dict[str, Any] | None = None) -> str:
    data = data or load()
    bans = data.get("banned_approaches", [])
    if not bans:
        return "_none — fresh slate_"
    return "\n".join(
        f"- {b['summary']} (reason: {b.get('reason') or 'recently regressed'})"
        for b in bans
    )
