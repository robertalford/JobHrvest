"""Excluded sites endpoints — unified list of domains that must not be crawled."""

import uuid
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import func, select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import get_db
from app.models.excluded_site import ExcludedSite

router = APIRouter()


@router.get("")
async def list_excluded_sites(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = None,
    country: Optional[str] = None,
    site_type: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    q = select(ExcludedSite)
    if search:
        q = q.where(or_(
            ExcludedSite.domain.ilike(f"%{search}%"),
            ExcludedSite.company_name.ilike(f"%{search}%"),
        ))
    if country:
        q = q.where(ExcludedSite.country_code == country.upper())
    if site_type:
        q = q.where(ExcludedSite.site_type == site_type)

    total = await db.scalar(select(func.count()).select_from(q.subquery()))
    rows = await db.scalars(q.order_by(ExcludedSite.created_at.desc()).offset((page - 1) * page_size).limit(page_size))

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [
            {
                "id": str(r.id),
                "domain": r.domain,
                "company_name": r.company_name,
                "site_url": r.site_url,
                "site_type": r.site_type,
                "country_code": r.country_code,
                "expected_job_count": r.expected_job_count,
                "reason": r.reason,
                "source_file": r.source_file,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ],
    }


@router.get("/stats")
async def excluded_site_stats(db: AsyncSession = Depends(get_db)):
    total = await db.scalar(select(func.count(ExcludedSite.id))) or 0
    by_type = await db.execute(
        select(ExcludedSite.site_type, func.count().label("n"))
        .group_by(ExcludedSite.site_type)
        .order_by(func.count().desc())
    )
    by_country = await db.execute(
        select(ExcludedSite.country_code, func.count().label("n"))
        .where(ExcludedSite.country_code != None)
        .group_by(ExcludedSite.country_code)
        .order_by(func.count().desc())
        .limit(20)
    )
    return {
        "total": total,
        "by_type": {r.site_type or "unknown": r.n for r in by_type},
        "by_country": {r.country_code: r.n for r in by_country},
    }


@router.post("", status_code=201)
async def add_excluded_site(
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Manually add a domain to the excluded/blocked list."""
    domain = (payload.get("domain") or "").strip().lower()
    if not domain:
        raise HTTPException(status_code=400, detail="domain is required")
    existing = await db.scalar(select(ExcludedSite).where(ExcludedSite.domain == domain))
    if existing:
        raise HTTPException(status_code=409, detail="Domain already in exclusion list")
    site = ExcludedSite(
        domain=domain,
        company_name=payload.get("company_name"),
        site_url=payload.get("site_url"),
        reason=payload.get("reason") or "manual",
    )
    db.add(site)
    await db.commit()
    await db.refresh(site)
    # Reload blocklist cache so the new domain takes effect immediately
    from app.crawlers.domain_blocklist import refresh_from_db_async
    await refresh_from_db_async()
    return {"id": str(site.id), "domain": site.domain}


@router.put("/{site_id}")
async def update_excluded_site(
    site_id: str,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    site = await db.get(ExcludedSite, uuid.UUID(site_id))
    if not site:
        raise HTTPException(status_code=404, detail="Not found")
    if "reason" in payload:
        site.reason = payload["reason"]
    if "company_name" in payload:
        site.company_name = payload["company_name"]
    await db.commit()
    return {"id": str(site.id), "domain": site.domain, "reason": site.reason}


@router.delete("/{site_id}", status_code=204)
async def remove_excluded_site(site_id: str, db: AsyncSession = Depends(get_db)):
    site = await db.get(ExcludedSite, uuid.UUID(site_id))
    if not site:
        raise HTTPException(status_code=404, detail="Not found")
    await db.delete(site)
    await db.commit()
    # Reload blocklist cache
    from app.crawlers.domain_blocklist import refresh_from_db_async
    await refresh_from_db_async()
