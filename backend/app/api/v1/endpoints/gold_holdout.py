"""GOLD holdout endpoints — list/browse frozen champion/challenger evaluation sets."""

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import get_db
from app.models.champion_challenger import (
    GoldHoldoutDomain,
    GoldHoldoutJob,
    GoldHoldoutSet,
    GoldHoldoutSnapshot,
)

router = APIRouter()


@router.get("/sets")
async def list_sets(db: AsyncSession = Depends(get_db)):
    """List every holdout set with its size / verification rollup."""
    sets = list(await db.scalars(
        select(GoldHoldoutSet).order_by(GoldHoldoutSet.created_at.desc())
    ))
    if not sets:
        return {"sets": []}

    set_ids = [s.id for s in sets]

    domain_counts = dict((await db.execute(
        select(GoldHoldoutDomain.holdout_set_id, func.count(GoldHoldoutDomain.id))
        .where(GoldHoldoutDomain.holdout_set_id.in_(set_ids))
        .group_by(GoldHoldoutDomain.holdout_set_id)
    )).all())

    snapshot_counts = dict((await db.execute(
        select(GoldHoldoutDomain.holdout_set_id, func.count(GoldHoldoutSnapshot.id))
        .join(GoldHoldoutSnapshot, GoldHoldoutSnapshot.holdout_domain_id == GoldHoldoutDomain.id)
        .where(GoldHoldoutDomain.holdout_set_id.in_(set_ids))
        .group_by(GoldHoldoutDomain.holdout_set_id)
    )).all())

    verified_counts = dict((await db.execute(
        select(GoldHoldoutDomain.holdout_set_id, func.count(GoldHoldoutDomain.id))
        .where(
            GoldHoldoutDomain.holdout_set_id.in_(set_ids),
            GoldHoldoutDomain.verification_status == "verified",
        )
        .group_by(GoldHoldoutDomain.holdout_set_id)
    )).all())

    job_counts = dict((await db.execute(
        select(GoldHoldoutDomain.holdout_set_id, func.count(GoldHoldoutJob.id))
        .join(GoldHoldoutJob, GoldHoldoutJob.holdout_domain_id == GoldHoldoutDomain.id)
        .where(GoldHoldoutDomain.holdout_set_id.in_(set_ids))
        .group_by(GoldHoldoutDomain.holdout_set_id)
    )).all())

    return {
        "sets": [
            {
                "id": str(s.id),
                "name": s.name,
                "description": s.description,
                "source": s.source,
                "market_id": s.market_id,
                "is_frozen": s.is_frozen,
                "is_active": s.is_active,
                "frozen_at": s.frozen_at.isoformat() if s.frozen_at else None,
                "created_at": s.created_at.isoformat() if s.created_at else None,
                "stats": {
                    "domains": int(domain_counts.get(s.id, 0)),
                    "snapshots": int(snapshot_counts.get(s.id, 0)),
                    "verified_domains": int(verified_counts.get(s.id, 0)),
                    "ground_truth_jobs": int(job_counts.get(s.id, 0)),
                },
            }
            for s in sets
        ]
    }


@router.get("/sets/{set_id}/domains")
async def list_domains(
    set_id: UUID,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = None,
    verified_only: bool = False,
    db: AsyncSession = Depends(get_db),
):
    """Paginated domain list for a single holdout set."""
    holdout = await db.scalar(select(GoldHoldoutSet).where(GoldHoldoutSet.id == set_id))
    if not holdout:
        raise HTTPException(status_code=404, detail="Holdout set not found")

    q = select(GoldHoldoutDomain).where(GoldHoldoutDomain.holdout_set_id == set_id)
    c = (
        select(func.count())
        .select_from(GoldHoldoutDomain)
        .where(GoldHoldoutDomain.holdout_set_id == set_id)
    )
    if search:
        like = f"%{search.lower()}%"
        q = q.where(
            func.lower(GoldHoldoutDomain.domain).like(like)
            | func.lower(GoldHoldoutDomain.advertiser_name).like(like)
        )
        c = c.where(
            func.lower(GoldHoldoutDomain.domain).like(like)
            | func.lower(GoldHoldoutDomain.advertiser_name).like(like)
        )
    if verified_only:
        q = q.where(GoldHoldoutDomain.verification_status == "verified")
        c = c.where(GoldHoldoutDomain.verification_status == "verified")

    total = await db.scalar(c) or 0
    rows = list(
        await db.scalars(
            q.order_by(GoldHoldoutDomain.expected_job_count.desc().nullslast())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    )

    # Bulk-fetch snapshot + job counts so the UI can badge each domain without N+1 queries.
    domain_ids = [d.id for d in rows]
    snap_counts = (
        dict(
            (await db.execute(
                select(GoldHoldoutSnapshot.holdout_domain_id, func.count(GoldHoldoutSnapshot.id))
                .where(GoldHoldoutSnapshot.holdout_domain_id.in_(domain_ids))
                .group_by(GoldHoldoutSnapshot.holdout_domain_id)
            )).all()
        )
        if domain_ids
        else {}
    )
    job_counts = (
        dict(
            (await db.execute(
                select(GoldHoldoutJob.holdout_domain_id, func.count(GoldHoldoutJob.id))
                .where(GoldHoldoutJob.holdout_domain_id.in_(domain_ids))
                .group_by(GoldHoldoutJob.holdout_domain_id)
            )).all()
        )
        if domain_ids
        else {}
    )

    return {
        "set": {
            "id": str(holdout.id),
            "name": holdout.name,
            "description": holdout.description,
            "market_id": holdout.market_id,
            "is_frozen": holdout.is_frozen,
            "frozen_at": holdout.frozen_at.isoformat() if holdout.frozen_at else None,
        },
        "items": [
            {
                "id": str(d.id),
                "domain": d.domain,
                "advertiser_name": d.advertiser_name,
                "expected_job_count": d.expected_job_count,
                "market_id": d.market_id,
                "ats_platform": d.ats_platform,
                "verification_status": d.verification_status,
                "verified_at": d.verified_at.isoformat() if d.verified_at else None,
                "verified_by": d.verified_by,
                "snapshot_count": int(snap_counts.get(d.id, 0)),
                "ground_truth_job_count": int(job_counts.get(d.id, 0)),
            }
            for d in rows
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }
