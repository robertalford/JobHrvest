"""Lead imports endpoints — status, analytics, and trigger."""

from fastapi import APIRouter, Depends, BackgroundTasks, Query
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from app.db.base import get_db
from app.models.lead_import import LeadImport

router = APIRouter()


@router.get("/summary")
async def lead_import_summary(db: AsyncSession = Depends(get_db)):
    """Overall import summary — totals by status."""
    total = await db.scalar(select(func.count(LeadImport.id)))
    if not total:
        return {"total": 0, "by_status": {}, "by_country": {}, "by_category": {}}

    # By status
    status_result = await db.execute(
        select(LeadImport.status, func.count().label("count"))
        .group_by(LeadImport.status)
    )
    by_status = {r.status: r.count for r in status_result}

    # By country
    country_result = await db.execute(
        select(
            LeadImport.country_id,
            LeadImport.status,
            func.count().label("count"),
            func.sum(LeadImport.jobs_extracted).label("total_jobs"),
            func.sum(LeadImport.career_pages_found).label("total_pages"),
        )
        .group_by(LeadImport.country_id, LeadImport.status)
        .order_by(LeadImport.country_id)
    )
    by_country: dict = {}
    for r in country_result:
        if r.country_id not in by_country:
            by_country[r.country_id] = {"total": 0, "by_status": {}, "jobs_extracted": 0, "pages_found": 0}
        by_country[r.country_id]["by_status"][r.status] = r.count
        by_country[r.country_id]["total"] += r.count
        by_country[r.country_id]["jobs_extracted"] += int(r.total_jobs or 0)
        by_country[r.country_id]["pages_found"] += int(r.total_pages or 0)

    # By category
    cat_result = await db.execute(
        select(
            LeadImport.ad_origin_category,
            LeadImport.status,
            func.count().label("count"),
        )
        .group_by(LeadImport.ad_origin_category, LeadImport.status)
        .order_by(LeadImport.ad_origin_category)
    )
    by_category: dict = {}
    for r in cat_result:
        cat = r.ad_origin_category or "unknown"
        if cat not in by_category:
            by_category[cat] = {"total": 0, "by_status": {}}
        by_category[cat]["by_status"][r.status] = r.count
        by_category[cat]["total"] += r.count

    return {
        "total": total,
        "by_status": by_status,
        "by_country": by_country,
        "by_category": by_category,
    }


@router.get("/")
async def list_lead_imports(
    db: AsyncSession = Depends(get_db),
    country: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    limit: int = Query(50, le=500),
    offset: int = Query(0),
):
    """List lead imports with optional filters."""
    q = select(LeadImport)
    if country:
        q = q.where(LeadImport.country_id == country.upper())
    if status:
        q = q.where(LeadImport.status == status)
    if category:
        q = q.where(LeadImport.ad_origin_category == category)
    q = q.order_by(LeadImport.imported_at.desc()).limit(limit).offset(offset)

    result = await db.execute(q)
    rows = result.scalars().all()

    total_q = select(func.count(LeadImport.id))
    if country:
        total_q = total_q.where(LeadImport.country_id == country.upper())
    if status:
        total_q = total_q.where(LeadImport.status == status)
    if category:
        total_q = total_q.where(LeadImport.ad_origin_category == category)
    total = await db.scalar(total_q)

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [
            {
                "id": str(r.id),
                "country_id": r.country_id,
                "advertiser_name": r.advertiser_name,
                "origin_domain": r.origin_domain,
                "sample_linkout_url": r.sample_linkout_url,
                "ad_origin_category": r.ad_origin_category,
                "expected_job_count": r.expected_job_count,
                "origin_rank": r.origin_rank,
                "status": r.status,
                "company_id": str(r.company_id) if r.company_id else None,
                "career_pages_found": r.career_pages_found,
                "jobs_extracted": r.jobs_extracted,
                "error_message": r.error_message,
                "skip_reason": r.skip_reason,
                "imported_at": r.imported_at.isoformat() if r.imported_at else None,
                "processed_at": r.processed_at.isoformat() if r.processed_at else None,
            }
            for r in rows
        ],
    }


@router.post("/trigger")
async def trigger_import(
    background_tasks: BackgroundTasks,
    csv_path: str = "/storage/ad_gap_data_all_markets.csv",
    limit: Optional[int] = None,
    country: Optional[str] = None,
):
    """Trigger a CSV lead import in the background."""
    import asyncio
    from scripts.import_leads import run_import

    async def _run():
        try:
            await run_import(csv_path, limit, country)
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"Lead import failed: {e}")

    background_tasks.add_task(asyncio.ensure_future, _run())
    return {"status": "started", "csv_path": csv_path, "limit": limit, "country": country}
