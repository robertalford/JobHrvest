"""Silver-label bootstrap: apply Jobstream wrappers to gold holdout snapshots.

Jobstream's hand-tuned selectors (in `fixed_test_sites.known_selectors` and
`site_wrapper_test_data.selectors`) are the strongest free source of job labels
we have. Running them against the frozen GOLD holdout snapshots yields
"silver" `gold_holdout_jobs` rows at near-zero marginal cost, which multiplies
the eval set ~10× vs manual labelling.

Silver labels are tagged accordingly:
  - verification_status='silver' when the baseline extraction looks sane
  - verification_status='suspect' when baseline count differs from the domain's
    expected_job_count by more than 2× (either direction)
  - source='baseline_wrapper' so the evaluator can weight gold > silver

Running the script is idempotent per (holdout_domain_id, source): re-running
deletes prior baseline_wrapper rows before writing new ones.

Usage (from inside a backend container):

    python -m scripts.build_silver_labels --name au_baseline_v1

If the --name set does not exist yet, run build_gold_holdout.py first.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Optional

from sqlalchemy import select, text, delete
from sqlalchemy.ext.asyncio import AsyncSession
from urllib.parse import urlparse

from app.crawlers.job_extractor import JobExtractor
from app.db.base import AsyncSessionLocal
from app.models.champion_challenger import (
    GoldHoldoutDomain,
    GoldHoldoutJob,
    GoldHoldoutSet,
    GoldHoldoutSnapshot,
)

logger = logging.getLogger("build_silver_labels")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


async def _load_holdout(session: AsyncSession, name: str) -> GoldHoldoutSet:
    row = await session.scalar(select(GoldHoldoutSet).where(GoldHoldoutSet.name == name))
    if row is None:
        raise SystemExit(f"Holdout set {name!r} not found. Run build_gold_holdout.py first.")
    return row


async def _load_wrappers_by_domain(session: AsyncSession) -> dict[str, dict]:
    """Build a lookup of {domain_host -> wrapper_selectors}.

    The primary source is `fixed_test_sites` — that's where Jobstream's
    hand-tuned wrappers live keyed by URL. We index by the bare hostname so
    multiple URL variants of the same company collapse to one entry.
    """
    result = await session.execute(
        text("SELECT url, known_selectors FROM fixed_test_sites WHERE known_selectors IS NOT NULL")
    )
    out: dict[str, dict] = {}
    for url, selectors in result.all():
        if isinstance(selectors, str):
            try:
                selectors = json.loads(selectors)
            except json.JSONDecodeError:
                continue
        if not isinstance(selectors, dict) or not selectors:
            continue
        host = urlparse(url).netloc.lstrip("www.").lower() if url else ""
        if not host:
            continue
        out.setdefault(host, selectors)
    return out


def _pick_wrapper_for_domain(
    domain: str, wrappers_by_host: dict[str, dict],
) -> Optional[dict]:
    """Match a domain to a Jobstream wrapper by host suffix.

    The lookup tries the exact host first, then the registered domain, then a
    progressively shorter suffix. This tolerates `careers.example.com` domains
    matching a wrapper registered against `example.com`.
    """
    d = (domain or "").lstrip("www.").lower()
    if not d:
        return None
    if d in wrappers_by_host:
        return wrappers_by_host[d]
    # Try registered domain (strip subdomain) and parent suffixes
    parts = d.split(".")
    for n in range(1, min(3, len(parts))):
        suffix = ".".join(parts[n:])
        if suffix in wrappers_by_host:
            return wrappers_by_host[suffix]
    # Last resort: search wrapper keys ending with our domain
    for host, sels in wrappers_by_host.items():
        if host.endswith(d) or d.endswith(host):
            return sels
    return None


async def _load_snapshot_html(snapshot_path: str) -> Optional[str]:
    p = Path(snapshot_path)
    if not p.exists():
        return None
    try:
        return p.read_bytes().decode("utf-8", errors="replace")
    except Exception as e:  # noqa: BLE001
        logger.warning("silver: could not read snapshot %s: %s", snapshot_path, e)
        return None


def _classify(extracted_count: int, expected: Optional[int]) -> str:
    """Decide verification_status for a silver-labelled domain.

    - 'silver'  when extraction count is within 2× of expected (or expected is unknown)
    - 'suspect' when the ratio is > 2× in either direction (baseline either
                over-fired or under-fired — don't trust it at gold strength)
    """
    if not expected or expected <= 0:
        return "silver" if extracted_count > 0 else "suspect"
    ratio = extracted_count / expected
    return "silver" if 0.5 <= ratio <= 2.0 else "suspect"


async def build_silver_for_set(
    session: AsyncSession,
    *,
    name: str,
    limit: Optional[int] = None,
) -> dict:
    holdout = await _load_holdout(session, name)
    domains = (await session.execute(
        select(GoldHoldoutDomain).where(GoldHoldoutDomain.holdout_set_id == holdout.id)
    )).scalars().all()

    wrappers = await _load_wrappers_by_domain(session)
    logger.info("silver: %d holdout domains, %d wrappers available", len(domains), len(wrappers))

    stats = {
        "domains_total": len(domains),
        "domains_with_wrapper": 0,
        "domains_extracted": 0,
        "jobs_silver": 0,
        "jobs_suspect": 0,
        "snapshots_missing": 0,
    }

    for i, domain in enumerate(domains):
        if limit is not None and i >= limit:
            break
        wrapper = _pick_wrapper_for_domain(domain.domain, wrappers)
        if not wrapper:
            continue
        stats["domains_with_wrapper"] += 1

        snapshot = await session.scalar(
            select(GoldHoldoutSnapshot)
            .where(GoldHoldoutSnapshot.holdout_domain_id == domain.id)
            .order_by(GoldHoldoutSnapshot.snapshotted_at.desc())
            .limit(1)
        )
        if not snapshot:
            stats["snapshots_missing"] += 1
            continue

        html = await _load_snapshot_html(snapshot.snapshot_path)
        if not html:
            stats["snapshots_missing"] += 1
            continue

        try:
            jobs = JobExtractor._static_extract_wrapper(html, snapshot.url, wrapper)
        except Exception as e:  # noqa: BLE001 — best effort across all domains
            logger.warning("silver: extraction failed for %s: %s", domain.domain, e)
            continue

        if not jobs:
            continue

        # Idempotency: remove any prior silver rows for this domain so reruns
        # don't accumulate duplicates.
        await session.execute(
            delete(GoldHoldoutJob)
            .where(GoldHoldoutJob.holdout_domain_id == domain.id)
            .where(GoldHoldoutJob.source == "baseline_wrapper")
        )

        status = _classify(len(jobs), domain.expected_job_count)
        for j in jobs:
            title = (j.get("title") or "").strip()
            if not title:
                continue
            desc = j.get("description") or ""
            session.add(GoldHoldoutJob(
                holdout_domain_id=domain.id,
                title=title[:500],
                location=(j.get("location_raw") or None),
                employment_type=(j.get("employment_type") or None),
                apply_url=(j.get("apply_url") or j.get("source_url")),
                source_url=(j.get("source_url") or None),
                description_length=len(desc) if desc else None,
                verification_status=status,
                source="baseline_wrapper",
                verified_by="silver-bootstrap",
                notes=f"wrapper_host={urlparse(snapshot.url).netloc}",
            ))
            if status == "silver":
                stats["jobs_silver"] += 1
            else:
                stats["jobs_suspect"] += 1

        stats["domains_extracted"] += 1
        # Periodic commit — keeps transaction sizes bounded on large holdouts
        if (i + 1) % 25 == 0:
            await session.commit()
            logger.info("silver: committed through domain %d", i + 1)

    await session.commit()
    return stats


async def main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap silver labels on a GOLD holdout")
    parser.add_argument("--name", required=True, help="Holdout set name (e.g. au_baseline_v1)")
    parser.add_argument("--limit", type=int, help="Limit to first N domains (for dry runs)")
    args = parser.parse_args()

    async with AsyncSessionLocal() as session:
        stats = await build_silver_for_set(session, name=args.name, limit=args.limit)

    logger.info("silver: done — %s", json.dumps(stats))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
