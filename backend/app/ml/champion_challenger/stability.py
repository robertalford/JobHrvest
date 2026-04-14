"""Per-site stability tracking for the auto-improve loop.

This module backs the oscillation-detection layer of the universality-first
redesign. Three responsibilities:

  1. `record_run_history(session, run_id, model_id, model_name, site_results)`
     — append one `site_result_history` row per tested site so the oscillation
     detector has data to compute flip counts against. Trims history to the
     last 20 observations per site so the table stays bounded.

  2. `upsert_ever_passed(session, run_id, model_name, site_results, per_site_composite)`
     — ratchet the `ever_passed_sites` monotonic set forward. If a site passes
     this run AND (wasn't in the set OR current version did better), update
     the row. Never downgrades.

  3. `compute_flip_counts(session, urls, lookback=5)`
     — return `{url: flip_count}` computed from the last N rows of history
     per URL. A flip is a pass→fail or fail→pass transition between
     consecutive observations. Used by the promotion gate to detect
     'unstable' sites.

All functions are async and take an AsyncSession so they run inside the
existing `_aggregate()` transaction in `backend/app/tasks/ml_tasks.py`.
"""

from __future__ import annotations

import logging
import uuid
from typing import Iterable, Optional

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.universality import EverPassedSite, SiteResultHistory

logger = logging.getLogger(__name__)


# A "pass" for ratcheting purposes: model extracted ≥ 90 % of the baseline OR
# (when the baseline is empty) the model found some real jobs on its own.
# Mirrors the existing match taxonomy in ml_tasks._run_site (model_equal_or_better
# / model_only).
_PASS_MATCHES = {"model_equal_or_better", "model_only"}

# When we replay the ever-passed set on a future run we allow a small drift
# before declaring regression — minor fluctuation in baseline volume should
# not count. Material loss does: e.g. 10 jobs → 4 jobs is a regression.
EVER_PASSED_REGRESSION_SLACK_PCT = 0.15  # 15 % of previous best jobs_quality

# History trimming: keep the last N observations per site. 20 is enough for a
# five-run oscillation window plus headroom for longer-lookback trend analysis.
HISTORY_KEEP_PER_SITE = 20


def _entry_pass(entry: dict) -> bool:
    """True if this site counts as 'passed' for ratcheting purposes."""
    return (entry.get("match") or "") in _PASS_MATCHES


def _composite_contribution(entry: dict, phase_key: str = "model") -> float:
    """Rough per-site composite contribution in points.

    This is deliberately simple — it's used for ranking and for trimming,
    not for promotion decisions. Promotion decisions use the stratified
    composite computed over full result sets in ml_models._composite_score_*.
    """
    phase = entry.get(phase_key) or {}
    jq = phase.get("jobs_quality", phase.get("jobs", 0) or 0)
    bj = entry.get("baseline", {}).get("jobs", 0) or 0
    if bj <= 0 and jq <= 0:
        return 0.0
    if bj <= 0:
        return 50.0  # model_only
    ratio = min(1.0, jq / bj)
    return round(100.0 * ratio, 2)


async def record_run_history(
    session: AsyncSession,
    *,
    run_id: uuid.UUID,
    model_id: Optional[uuid.UUID],
    model_name: Optional[str],
    site_results: list[dict],
) -> None:
    """Append one history row per site and trim older rows per-URL."""
    rows = []
    for entry in site_results or []:
        url = entry.get("url") or ""
        if not url or url == "missing":
            continue
        match = entry.get("match") or "unknown"
        passed = _entry_pass(entry)
        phase = entry.get("model") or {}
        rows.append({
            "url": url,
            "run_id": run_id,
            "model_id": model_id,
            "model_name": model_name,
            "ats_platform": entry.get("ats_platform"),
            "match": match,
            "passed": passed,
            "baseline_jobs": int(entry.get("baseline", {}).get("jobs") or 0),
            "model_jobs": int(phase.get("jobs") or 0),
            "jobs_quality": int(phase.get("jobs_quality") or phase.get("jobs") or 0),
            "composite_pts": _composite_contribution(entry),
        })

    if not rows:
        return

    await session.execute(
        SiteResultHistory.__table__.insert(),
        rows,
    )

    # Trim per-URL history to the last HISTORY_KEEP_PER_SITE rows. Done in one
    # window-function query so the transaction stays compact.
    urls = list({r["url"] for r in rows})
    await session.execute(
        text("""
            DELETE FROM site_result_history
            WHERE id IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (
                             PARTITION BY url ORDER BY observed_at DESC, id DESC
                           ) AS rn
                    FROM site_result_history
                    WHERE url = ANY(:urls)
                ) ranked
                WHERE rn > :keep
            )
        """),
        {"urls": urls, "keep": HISTORY_KEEP_PER_SITE},
    )


async def upsert_ever_passed(
    session: AsyncSession,
    *,
    run_id: uuid.UUID,
    model_name: str,
    site_results: list[dict],
) -> int:
    """Ratchet the ever-passed set forward. Returns count of rows upserted.

    A site is upserted when:
      - current run passed (model_equal_or_better / model_only), AND
      - (site not in set) OR (current jobs_quality > existing best_composite-era quality)

    We never delete from this set — regression is detected by the promotion
    gate, not by removing historical wins.
    """
    upserted = 0
    for entry in site_results or []:
        url = entry.get("url") or ""
        if not url or url == "missing":
            continue
        if not _entry_pass(entry):
            # Still update last_seen_at so we know the site was tested, even
            # if it didn't pass on this run.
            await session.execute(
                text("""
                    UPDATE ever_passed_sites
                    SET last_seen_at = NOW()
                    WHERE url = :url
                """),
                {"url": url},
            )
            continue

        phase = entry.get("model") or {}
        jq = int(phase.get("jobs_quality") or phase.get("jobs") or 0)
        bj = int(entry.get("baseline", {}).get("jobs") or 0)
        composite_pts = _composite_contribution(entry)

        stmt = pg_insert(EverPassedSite).values(
            url=url,
            company=entry.get("company"),
            ats_platform=entry.get("ats_platform"),
            best_composite=composite_pts,
            best_version_name=model_name or "unknown",
            best_run_id=run_id,
            jobs_quality=jq,
            baseline_jobs=bj,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[EverPassedSite.__table__.c.url],
            set_={
                "best_composite": stmt.excluded.best_composite,
                "best_version_name": stmt.excluded.best_version_name,
                "best_run_id": stmt.excluded.best_run_id,
                "jobs_quality": stmt.excluded.jobs_quality,
                "baseline_jobs": stmt.excluded.baseline_jobs,
                "ats_platform": stmt.excluded.ats_platform,
                "last_seen_at": text("NOW()"),
                "last_updated_at": text("NOW()"),
            },
            where=(EverPassedSite.__table__.c.best_composite
                   <= stmt.excluded.best_composite),
        )
        await session.execute(stmt)
        upserted += 1

    return upserted


async def fetch_ever_passed_regressions(
    session: AsyncSession,
    *,
    site_results: list[dict],
) -> list[dict]:
    """Return the list of sites the challenger has regressed vs ever-passed set.

    For each current site result, look up the ever-passed record (if any) and
    compare against it. A regression is: site previously passed with N jobs
    quality but challenger got <(1 - slack)·N, OR the challenger missed the
    site entirely (match not in pass set).
    """
    if not site_results:
        return []
    urls = list({(s.get("url") or "") for s in site_results if s.get("url")})
    if not urls:
        return []

    result = await session.execute(
        text("""
            SELECT url, ats_platform, jobs_quality, best_version_name, best_composite
            FROM ever_passed_sites
            WHERE url = ANY(:urls)
        """),
        {"urls": urls},
    )
    ever: dict[str, dict] = {
        row.url: {
            "ats_platform": row.ats_platform,
            "jobs_quality": int(row.jobs_quality or 0),
            "best_version_name": row.best_version_name,
            "best_composite": float(row.best_composite or 0),
        }
        for row in result
    }

    regressions: list[dict] = []
    for entry in site_results:
        url = entry.get("url") or ""
        prev = ever.get(url)
        if not prev:
            continue
        cur_pass = _entry_pass(entry)
        phase = entry.get("model") or {}
        cur_jq = int(phase.get("jobs_quality") or phase.get("jobs") or 0)
        prev_jq = prev["jobs_quality"]

        threshold = max(1, int(prev_jq * (1 - EVER_PASSED_REGRESSION_SLACK_PCT)))

        if not cur_pass or cur_jq < threshold:
            regressions.append({
                "url": url,
                "company": entry.get("company"),
                "ats_platform": entry.get("ats_platform") or prev["ats_platform"],
                "prev_jobs_quality": prev_jq,
                "cur_jobs_quality": cur_jq,
                "prev_best_version": prev["best_version_name"],
            })
    return regressions


async def compute_flip_counts(
    session: AsyncSession,
    *,
    urls: Iterable[str],
    lookback: int = 5,
) -> dict[str, int]:
    """Count pass/fail transitions in the last `lookback` observations per URL."""
    url_list = [u for u in (urls or []) if u]
    if not url_list:
        return {}

    result = await session.execute(
        text("""
            WITH windowed AS (
                SELECT url, passed,
                       ROW_NUMBER() OVER (
                         PARTITION BY url ORDER BY observed_at DESC, id DESC
                       ) AS rn
                FROM site_result_history
                WHERE url = ANY(:urls)
            )
            SELECT url, passed, rn
            FROM windowed
            WHERE rn <= :lookback
            ORDER BY url, rn
        """),
        {"urls": url_list, "lookback": lookback},
    )

    per_url: dict[str, list[bool]] = {}
    for row in result:
        per_url.setdefault(row.url, []).append(bool(row.passed))

    flips: dict[str, int] = {}
    for url, states in per_url.items():
        # states is in most-recent-first order; direction doesn't matter for
        # flip counting.
        count = 0
        for a, b in zip(states, states[1:]):
            if a != b:
                count += 1
        if count >= 1:
            flips[url] = count
    return flips


async def unstable_site_urls(
    session: AsyncSession,
    *,
    urls: Iterable[str],
    lookback: int = 5,
    min_flips: int = 2,
) -> set[str]:
    """Return the subset of `urls` that have flipped ≥ min_flips times recently."""
    flips = await compute_flip_counts(session, urls=urls, lookback=lookback)
    return {u for u, n in flips.items() if n >= min_flips}
