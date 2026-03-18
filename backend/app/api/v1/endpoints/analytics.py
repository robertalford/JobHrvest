"""Analytics endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import get_db
from app.models.job import Job
from app.models.company import Company
from app.models.crawl_log import CrawlLog

router = APIRouter()


@router.get("/extraction-accuracy")
async def extraction_accuracy(db: AsyncSession = Depends(get_db)):
    from app.models.extraction_comparison import ExtractionComparison
    result = await db.execute(
        select(
            ExtractionComparison.method_a,
            ExtractionComparison.method_b,
            func.avg(ExtractionComparison.agreement_score).label("avg_agreement"),
            func.count().label("count"),
        ).group_by(ExtractionComparison.method_a, ExtractionComparison.method_b)
    )
    return [dict(r._mapping) for r in result]


@router.get("/field-coverage")
async def field_coverage(db: AsyncSession = Depends(get_db)):
    total = await db.scalar(select(func.count(Job.id)).where(Job.is_active == True))
    if not total:
        return {"total": 0}

    async def pct(col):
        n = await db.scalar(select(func.count(Job.id)).where(col.isnot(None), Job.is_active == True))
        return round((n / total) * 100, 1) if total else 0

    return {
        "total_active_jobs": total,
        "title_pct": 100.0,
        "description_pct": await pct(Job.description),
        "location_pct": await pct(Job.location_raw),
        "salary_pct": await pct(Job.salary_raw),
        "employment_type_pct": await pct(Job.employment_type),
        "seniority_pct": await pct(Job.seniority_level),
        "requirements_pct": await pct(Job.requirements),
        "benefits_pct": await pct(Job.benefits),
    }


@router.get("/discovery-stats")
async def discovery_stats(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Company.discovered_via, func.count().label("count"))
        .group_by(Company.discovered_via)
    )
    return [dict(r._mapping) for r in result]


@router.get("/trends")
async def trends(db: AsyncSession = Depends(get_db)):
    # Jobs added per day for the last 30 days
    result = await db.execute(
        select(
            func.date_trunc("day", Job.first_seen_at).label("day"),
            func.count().label("count"),
        )
        .where(Job.first_seen_at >= func.now() - func.cast("30 days", type_=None))
        .group_by("day")
        .order_by("day")
    )
    return [{"day": str(r.day), "count": r.count} for r in result]
