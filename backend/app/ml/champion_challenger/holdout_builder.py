"""Materialise a frozen GOLD holdout from lead_imports.

The lead_imports table contains advertiser → origin_domain mappings with an
expected_job_count signal — that's the closest thing JobHarvest has to an
external, rule-independent ground truth. This module turns those rows into:

  1. a gold_holdout_set     (the named, frozen evaluation set)
  2. gold_holdout_domains   (one row per advertiser/domain)
  3. gold_holdout_snapshots (raw HTML on disk + content_hash for reproducibility)
  4. gold_holdout_jobs      (left empty here — the verification step is
                             intentionally manual; this module won't fabricate
                             ground-truth job records)

The split between automated infrastructure and manual verification is the
point. Snapshots and the domain list can be built by a script. The actual
"ground truth" job records have to be entered/curated by a human, and we
won't pretend otherwise — verification_status=unverified makes that visible.

Usage (called from a CLI script or Celery task — see scripts/build_gold_holdout.py):

    builder = GoldHoldoutBuilder(
        snapshot_root=Path("/storage/gold_holdout"),
        http_client=client,
    )
    set_id = await builder.build(
        session,
        name="au_baseline_v1",
        market_id="AU",
        max_domains=100,
    )
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.champion_challenger import (
    GoldHoldoutSet,
    GoldHoldoutDomain,
    GoldHoldoutSnapshot,
)
from app.models.lead_import import LeadImport

logger = logging.getLogger(__name__)


class _HttpClient(Protocol):
    async def fetch(self, url: str) -> tuple[int, bytes, str]:
        """Return (status_code, body_bytes, content_type)."""
        ...


@dataclass
class HoldoutBuildReport:
    set_id: UUID
    name: str
    domains_attempted: int
    domains_added: int
    snapshots_saved: int
    snapshots_failed: int
    skipped_blocked: int
    skipped_duplicate: int


class GoldHoldoutBuilder:
    """Materialise a frozen GOLD holdout from lead_imports."""

    def __init__(self, *, snapshot_root: Path, http_client: _HttpClient):
        self.snapshot_root = Path(snapshot_root)
        self.snapshot_root.mkdir(parents=True, exist_ok=True)
        self.http = http_client

    async def build(
        self,
        session: AsyncSession,
        *,
        name: str,
        market_id: str = "AU",
        max_domains: int = 100,
        require_expected_count: bool = True,
        description: Optional[str] = None,
    ) -> HoldoutBuildReport:
        """Create a holdout set and populate it. Idempotent on `name`.

        If a set with the same name already exists, returns the existing
        set's id without modifying it. To create a new version of the
        holdout, supply a new `name` (e.g. au_baseline_v2).
        """
        existing = await session.scalar(select(GoldHoldoutSet).where(GoldHoldoutSet.name == name))
        if existing is not None:
            logger.info("Holdout set %r already exists (%s) — returning existing", name, existing.id)
            return HoldoutBuildReport(
                set_id=existing.id,
                name=name,
                domains_attempted=0, domains_added=0,
                snapshots_saved=0, snapshots_failed=0,
                skipped_blocked=0, skipped_duplicate=0,
            )

        holdout_set = GoldHoldoutSet(
            name=name,
            description=description or f"GOLD holdout materialised from lead_imports ({market_id})",
            source="lead_imports",
            market_id=market_id,
            is_frozen=False,
            is_active=True,
        )
        session.add(holdout_set)
        await session.flush()

        leads = await self._select_lead_seeds(
            session, market_id=market_id, max_domains=max_domains,
            require_expected_count=require_expected_count,
        )

        report = HoldoutBuildReport(
            set_id=holdout_set.id,
            name=name,
            domains_attempted=len(leads),
            domains_added=0,
            snapshots_saved=0,
            snapshots_failed=0,
            skipped_blocked=0,
            skipped_duplicate=0,
        )

        seen_domains: set[str] = set()
        for lead in leads:
            domain = (lead.origin_domain or "").strip().lower()
            if not domain or domain in seen_domains:
                report.skipped_duplicate += 1
                continue
            seen_domains.add(domain)

            holdout_domain = GoldHoldoutDomain(
                holdout_set_id=holdout_set.id,
                domain=domain,
                advertiser_name=lead.advertiser_name,
                expected_job_count=lead.expected_job_count,
                market_id=lead.country_id or market_id,
                source_lead_import_id=lead.id,
            )
            session.add(holdout_domain)
            await session.flush()
            report.domains_added += 1

            snapshot = await self._snapshot_domain(holdout_domain, lead)
            if snapshot is None:
                report.snapshots_failed += 1
                continue
            session.add(snapshot)
            report.snapshots_saved += 1

        # Freeze on the way out — once materialised, this set should not change
        # without explicit re-build.
        holdout_set.is_frozen = True
        holdout_set.frozen_at = datetime.now(timezone.utc)
        await session.commit()

        logger.info(
            "Built holdout %r: %d/%d domains, %d snapshots (%d failed)",
            name, report.domains_added, report.domains_attempted,
            report.snapshots_saved, report.snapshots_failed,
        )
        return report

    async def _select_lead_seeds(
        self,
        session: AsyncSession,
        *,
        market_id: str,
        max_domains: int,
        require_expected_count: bool,
    ) -> list[LeadImport]:
        stmt = select(LeadImport).where(LeadImport.country_id == market_id)
        if require_expected_count:
            stmt = stmt.where(LeadImport.expected_job_count.isnot(None))
            stmt = stmt.where(LeadImport.expected_job_count > 0)
        # Prefer leads that have already been successfully crawled — they are
        # most likely to give a clean snapshot.
        stmt = stmt.order_by(
            LeadImport.expected_job_count.desc().nullslast(),
            LeadImport.imported_at.desc(),
        ).limit(max_domains)
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def _snapshot_domain(
        self, domain_row: GoldHoldoutDomain, lead: LeadImport,
    ) -> Optional[GoldHoldoutSnapshot]:
        """Fetch the seed URL and persist the raw HTML to disk."""
        seed_url = lead.sample_linkout_url or f"https://{domain_row.domain}"
        try:
            status, body, content_type = await self.http.fetch(seed_url)
        except Exception as e:  # noqa: BLE001 — snapshotting is best-effort
            logger.warning("Snapshot fetch failed for %s: %s", seed_url, e)
            return None

        if status >= 400 or not body:
            logger.info("Snapshot skipped (HTTP %s) for %s", status, seed_url)
            return None

        content_hash = hashlib.sha256(body).hexdigest()
        relative = f"{domain_row.id}/{content_hash[:2]}/{content_hash}.html"
        path = self.snapshot_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)

        return GoldHoldoutSnapshot(
            holdout_domain_id=domain_row.id,
            url=seed_url,
            snapshot_path=str(path),
            content_hash=content_hash,
            content_type=content_type,
            http_status=status,
            byte_size=len(body),
        )
