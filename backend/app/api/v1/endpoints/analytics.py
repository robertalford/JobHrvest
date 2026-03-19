"""Analytics endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy import func, select, case
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


@router.get("/quality-distribution")
async def quality_distribution(db: AsyncSession = Depends(get_db)):
    """Quality score distribution across all scored jobs."""
    total_scored = await db.scalar(
        select(func.count(Job.id)).where(Job.quality_score.isnot(None), Job.is_active == True)
    )
    total_unscored = await db.scalar(
        select(func.count(Job.id)).where(Job.quality_score.is_(None), Job.is_active == True)
    )

    if not total_scored:
        return {"total_scored": 0, "total_unscored": total_unscored, "bands": {}}

    # Count by band
    bands = {}
    for band, low, high in [
        ("excellent", 80, 101),
        ("good", 60, 80),
        ("fair", 40, 60),
        ("poor", 20, 40),
        ("disqualified", 0, 20),
    ]:
        n = await db.scalar(
            select(func.count(Job.id)).where(
                Job.quality_score >= low,
                Job.quality_score < high,
                Job.quality_score.isnot(None),
                Job.is_active == True,
            )
        )
        bands[band] = {"count": n or 0, "pct": round((n or 0) / total_scored * 100, 1)}

    # Average and median
    avg = await db.scalar(
        select(func.avg(Job.quality_score)).where(Job.quality_score.isnot(None), Job.is_active == True)
    )

    # Flags breakdown
    scam_count = await db.scalar(
        select(func.count(Job.id))
        .where(Job.quality_flags["scam_detected"].as_boolean() == True, Job.is_active == True)
    )
    discrimination_count = await db.scalar(
        select(func.count(Job.id))
        .where(Job.quality_flags["discrimination_detected"].as_boolean() == True, Job.is_active == True)
    )
    bad_words_count = await db.scalar(
        select(func.count(Job.id))
        .where(Job.quality_flags["bad_words_detected"].as_boolean() == True, Job.is_active == True)
    )

    return {
        "total_scored": total_scored,
        "total_unscored": total_unscored,
        "average_score": round(float(avg or 0), 1),
        "bands": bands,
        "flags": {
            "scam_detected": scam_count or 0,
            "discrimination_detected": discrimination_count or 0,
            "bad_words_detected": bad_words_count or 0,
        },
    }


@router.get("/quality-by-site")
async def quality_by_site(db: AsyncSession = Depends(get_db)):
    """Top/bottom 20 companies by quality score."""
    result = await db.execute(
        select(
            Company.id,
            Company.name,
            Company.domain,
            Company.market_code,
            Company.quality_score,
            func.count(Job.id).label("job_count"),
        )
        .join(Job, Job.company_id == Company.id, isouter=True)
        .where(Company.quality_score.isnot(None))
        .group_by(Company.id)
        .order_by(Company.quality_score.desc())
        .limit(40)
    )
    rows = result.all()
    return [
        {
            "id": str(r.id),
            "name": r.name,
            "domain": r.domain,
            "market_code": r.market_code,
            "quality_score": round(r.quality_score, 1) if r.quality_score else None,
            "job_count": r.job_count,
        }
        for r in rows
    ]


@router.post("/trigger-quality-scoring")
async def trigger_quality_scoring():
    """Trigger a background quality scoring task."""
    from app.tasks.ml_tasks import score_jobs_batch
    score_jobs_batch.delay(limit=1000)
    return {"status": "queued"}
