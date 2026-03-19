"""Analytics endpoints — optimised with single-query aggregations and Redis caching."""

import asyncio
import json
import redis.asyncio as aioredis
from fastapi import APIRouter, Depends
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.base import get_db
from app.models.job import Job
from app.models.company import Company
from app.models.crawl_log import CrawlLog

router = APIRouter()

# ── Redis cache helpers ────────────────────────────────────────────────────────
_redis: aioredis.Redis | None = None


def _get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis


async def _cache_get(key: str):
    try:
        raw = await _get_redis().get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


async def _cache_set(key: str, value, ttl: int) -> None:
    try:
        await _get_redis().set(key, json.dumps(value, default=str), ex=ttl)
    except Exception:
        pass


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
    """Single query returning all coverage stats — cached 120s."""
    cached = await _cache_get("analytics:field_coverage")
    if cached is not None:
        return cached

    result = await db.execute(
        select(
            func.count(Job.id).label("total"),
            func.count(Job.description).label("desc_n"),
            func.count(Job.location_raw).label("loc_n"),
            func.count(Job.salary_raw).label("sal_n"),
            func.count(Job.employment_type).label("emp_n"),
            func.count(Job.seniority_level).label("sen_n"),
            func.count(Job.requirements).label("req_n"),
            func.count(Job.benefits).label("ben_n"),
        ).where(Job.is_active == True)
    )
    r = result.one()
    total = r.total or 1  # avoid division by zero

    def pct(n):
        return round((n / total) * 100, 1) if total else 0

    result = {
        "total_active_jobs": r.total,
        "company_name_pct": 100.0,  # company_id + company.name are both non-nullable
        "title_pct": 100.0,
        "description_pct": pct(r.desc_n),
        "location_pct": pct(r.loc_n),
        "salary_pct": pct(r.sal_n),
        "employment_type_pct": pct(r.emp_n),
        "seniority_pct": pct(r.sen_n),
        "requirements_pct": pct(r.req_n),
        "benefits_pct": pct(r.ben_n),
    }
    await _cache_set("analytics:field_coverage", result, ttl=120)
    return result


@router.get("/discovery-stats")
async def discovery_stats(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Company.discovered_via, func.count().label("count"))
        .group_by(Company.discovered_via)
    )
    return [dict(r._mapping) for r in result]


@router.get("/trends")
async def trends(db: AsyncSession = Depends(get_db)):
    """Jobs added per day for the last 30 days — cached 5 min."""
    cached = await _cache_get("analytics:trends")
    if cached is not None:
        return cached

    result = await db.execute(
        select(
            func.date_trunc("day", Job.first_seen_at).label("day"),
            func.count().label("count"),
        )
        .where(Job.first_seen_at >= func.now() - text("interval '30 days'"))
        .group_by("day")
        .order_by("day")
    )
    data = [{"day": str(r.day)[:10], "count": r.count} for r in result]
    await _cache_set("analytics:trends", data, ttl=300)
    return data


@router.get("/quality-distribution")
async def quality_distribution(db: AsyncSession = Depends(get_db)):
    """Single query with conditional aggregation — cached 120s."""
    cached = await _cache_get("analytics:quality_distribution")
    if cached is not None:
        return cached

    result = await db.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE quality_score IS NOT NULL AND is_active)     AS total_scored,
            COUNT(*) FILTER (WHERE quality_score IS NULL     AND is_active)     AS total_unscored,
            AVG(quality_score) FILTER (WHERE quality_score IS NOT NULL AND is_active) AS avg_score,
            COUNT(*) FILTER (WHERE quality_score >= 80  AND quality_score <= 100 AND is_active) AS excellent,
            COUNT(*) FILTER (WHERE quality_score >= 60  AND quality_score < 80   AND is_active) AS good,
            COUNT(*) FILTER (WHERE quality_score >= 40  AND quality_score < 60   AND is_active) AS fair,
            COUNT(*) FILTER (WHERE quality_score >= 20  AND quality_score < 40   AND is_active) AS poor,
            COUNT(*) FILTER (WHERE quality_score >= 0   AND quality_score < 20   AND is_active) AS disqualified,
            COUNT(*) FILTER (WHERE (quality_flags->>'scam_detected')::boolean IS TRUE AND is_active)          AS scam_count,
            COUNT(*) FILTER (WHERE (quality_flags->>'discrimination_detected')::boolean IS TRUE AND is_active) AS disc_count,
            COUNT(*) FILTER (WHERE (quality_flags->>'bad_words_detected')::boolean IS TRUE AND is_active)     AS bad_words_count
        FROM jobs
    """))
    r = result.one()

    total = r.total_scored or 0

    def pct(n):
        return round((n / total) * 100, 1) if total else 0

    if not total:
        result = {"total_scored": 0, "total_unscored": r.total_unscored, "bands": {}}
        await _cache_set("analytics:quality_distribution", result, ttl=120)
        return result

    result = {
        "total_scored": total,
        "total_unscored": r.total_unscored,
        "average_score": round(float(r.avg_score or 0), 1),
        "bands": {
            "excellent":    {"count": r.excellent,    "pct": pct(r.excellent)},
            "good":         {"count": r.good,          "pct": pct(r.good)},
            "fair":         {"count": r.fair,          "pct": pct(r.fair)},
            "poor":         {"count": r.poor,          "pct": pct(r.poor)},
            "disqualified": {"count": r.disqualified,  "pct": pct(r.disqualified)},
        },
        "flags": {
            "scam_detected":           r.scam_count or 0,
            "discrimination_detected": r.disc_count or 0,
            "bad_words_detected":      r.bad_words_count or 0,
        },
    }
    await _cache_set("analytics:quality_distribution", result, ttl=120)
    return result


@router.get("/quality-by-site")
async def quality_by_site(db: AsyncSession = Depends(get_db)):
    """Top/bottom 40 companies by quality score — cached 5 min."""
    cached = await _cache_get("analytics:quality_by_site")
    if cached is not None:
        return cached

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
    data = [
        {
            "id": str(r.id),
            "name": r.name,
            "domain": r.domain,
            "market_code": r.market_code,
            "quality_score": round(r.quality_score, 1) if r.quality_score else None,
            "job_count": r.job_count,
        }
        for r in result.all()
    ]
    await _cache_set("analytics:quality_by_site", data, ttl=300)
    return data


@router.post("/trigger-quality-scoring")
async def trigger_quality_scoring():
    """Trigger a background quality scoring task."""
    from app.tasks.ml_tasks import score_jobs_batch
    score_jobs_batch.delay(limit=5000)
    return {"status": "queued"}


@router.get("/market-breakdown")
async def market_breakdown(db: AsyncSession = Depends(get_db)):
    """Jobs and companies broken down by market — cached 2 min."""
    cached = await _cache_get("analytics:market_breakdown")
    if cached is not None:
        return cached

    result = await db.execute(
        select(
            Company.market_code,
            func.count(Job.id.distinct()).label("job_count"),
            func.count(Company.id.distinct()).label("company_count"),
            func.avg(Job.quality_score).label("avg_quality"),
        )
        .join(Job, Job.company_id == Company.id, isouter=True)
        .where(Job.is_active == True, Job.is_canonical == True)
        .group_by(Company.market_code)
        .order_by(func.count(Job.id.distinct()).desc())
    )
    data = [
        {
            "market": r.market_code or "Unknown",
            "jobs": r.job_count or 0,
            "companies": r.company_count or 0,
            "avg_quality": round(r.avg_quality, 1) if r.avg_quality else None,
        }
        for r in result.all()
    ]
    await _cache_set("analytics:market_breakdown", data, ttl=120)
    return data


@router.post("/rescue-locations")
async def trigger_rescue_locations(db: AsyncSession = Depends(get_db)):
    """
    Trigger the location rescue task: fetches individual job detail pages
    for all jobs missing location_raw, extracts location + employment_type,
    and updates the records in place.

    Also resets career pages that are generating structural-only jobs so they
    get re-processed with the improved extraction code on the next crawl cycle.
    """
    from app.tasks.crawl_tasks import rescue_job_locations

    # Count jobs needing rescue
    result = await db.execute(
        select(func.count(Job.id)).where(
            Job.is_active == True,
            Job.location_raw.is_(None),
        )
    )
    missing_count = result.scalar() or 0

    # Queue rescue task (processes up to 500 per run; repeat to clear backlog)
    task = rescue_job_locations.apply_async(kwargs={"limit": 500}, queue="default")

    # Reset career pages with structural-only jobs back to no_structure_new
    # (clears last_content_hash so next crawl re-runs extraction with improved code)
    reset_result = await db.execute(text("""
        UPDATE career_pages
        SET last_content_hash = NULL
        WHERE is_active = true
          AND id IN (
            SELECT DISTINCT career_page_id
            FROM jobs
            WHERE is_active = true AND location_raw IS NULL AND career_page_id IS NOT NULL
          )
        RETURNING id
    """))
    reset_count = len(reset_result.fetchall())

    await db.commit()

    return {
        "status": "queued",
        "jobs_missing_location": missing_count,
        "rescue_task_id": task.id,
        "career_pages_reset": reset_count,
    }


@router.get("/location-rescue-status")
async def location_rescue_status(db: AsyncSession = Depends(get_db)):
    """Summary of location coverage and what still needs rescuing."""
    result = await db.execute(
        select(
            Job.extraction_method,
            func.count(Job.id).label("total"),
            func.count(Job.location_raw).label("has_location"),
            func.count(Job.employment_type).label("has_emp_type"),
        )
        .where(Job.is_active == True)
        .group_by(Job.extraction_method)
        .order_by(func.count(Job.id).desc())
    )
    rows = result.all()
    total_jobs = sum(r.total for r in rows)
    total_with_loc = sum(r.has_location for r in rows)
    return {
        "total_active_jobs": total_jobs,
        "location_coverage_pct": round(100 * total_with_loc / total_jobs, 1) if total_jobs else 0,
        "by_method": [
            {
                "method": r.extraction_method or "unknown",
                "total": r.total,
                "has_location": r.has_location,
                "missing_location": r.total - r.has_location,
                "location_pct": round(100 * r.has_location / r.total, 1) if r.total else 0,
                "has_emp_type": r.has_emp_type,
                "emp_type_pct": round(100 * r.has_emp_type / r.total, 1) if r.total else 0,
            }
            for r in rows
        ],
    }


@router.get("/dashboard-snapshot")
async def dashboard_snapshot(db: AsyncSession = Depends(get_db)):
    """
    Single request that returns all dashboard metrics in parallel.
    Replaces 4-6 separate API calls from the frontend.
    """
    async def _field_coverage():
        result = await db.execute(
            select(
                func.count(Job.id).label("total"),
                func.count(Job.description).label("desc_n"),
                func.count(Job.location_raw).label("loc_n"),
                func.count(Job.salary_raw).label("sal_n"),
                func.count(Job.employment_type).label("emp_n"),
            ).where(Job.is_active == True)
        )
        r = result.one()
        total = r.total or 1
        return {
            "total_active_jobs": r.total,
            "description_pct": round((r.desc_n / total) * 100, 1),
            "location_pct": round((r.loc_n / total) * 100, 1),
            "salary_pct": round((r.sal_n / total) * 100, 1),
            "employment_type_pct": round((r.emp_n / total) * 100, 1),
        }

    async def _market_summary():
        result = await db.execute(
            select(
                Company.market_code,
                func.count(Job.id.distinct()).label("job_count"),
                func.count(Company.id.distinct()).label("company_count"),
            )
            .join(Job, Job.company_id == Company.id, isouter=True)
            .where(Job.is_active == True, Job.is_canonical == True)
            .group_by(Company.market_code)
            .order_by(func.count(Job.id.distinct()).desc())
        )
        return [
            {"market": r.market_code or "Unknown", "jobs": r.job_count or 0, "companies": r.company_count or 0}
            for r in result.all()
        ]

    async def _quality_summary():
        result = await db.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE quality_score IS NOT NULL AND is_active) AS total_scored,
                COUNT(*) FILTER (WHERE quality_score IS NULL AND is_active)     AS total_unscored,
                AVG(quality_score) FILTER (WHERE quality_score IS NOT NULL AND is_active) AS avg_score
            FROM jobs
        """))
        r = result.one()
        return {
            "total_scored": r.total_scored or 0,
            "total_unscored": r.total_unscored or 0,
            "average_score": round(float(r.avg_score or 0), 1),
        }

    coverage = await _field_coverage()
    markets = await _market_summary()
    quality = await _quality_summary()

    return {
        "coverage": coverage,
        "markets": markets,
        "quality": quality,
    }


@router.get("/overview")
async def overview():
    """
    Single endpoint returning everything the Overview page needs.

    Each sub-query gets its own AsyncSession so they can run truly in parallel
    via asyncio.gather without SQLAlchemy concurrent-access errors. Result is
    cached in Redis for 15 seconds — cold miss happens at most once per 15s.
    """
    cached = await _cache_get("analytics:overview")
    if cached is not None:
        return cached

    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    from app.db.base import AsyncSessionLocal

    async def _job_stats():
        async with AsyncSessionLocal() as s:
            AEST = ZoneInfo("Australia/Sydney")
            today_start = datetime.now(AEST).replace(hour=0, minute=0, second=0, microsecond=0)
            week_start = today_start - timedelta(days=7)
            r = (await s.execute(text("""
                SELECT
                    COUNT(*) FILTER (WHERE is_active)                              AS active,
                    COUNT(*) FILTER (WHERE is_active AND is_canonical)             AS unique_active,
                    COUNT(*) FILTER (WHERE is_active AND NOT is_canonical)         AS duplicates,
                    COUNT(*) FILTER (WHERE is_active AND is_canonical
                                     AND first_seen_at >= :today)                  AS new_today,
                    COUNT(*) FILTER (WHERE is_active AND is_canonical
                                     AND first_seen_at >= :week)                   AS new_this_week
                FROM jobs
            """), {"today": today_start, "week": week_start})).one()
            return {
                "active": r.active, "unique_active": r.unique_active,
                "duplicates": r.duplicates, "new_today": r.new_today,
                "new_this_week": r.new_this_week,
            }

    async def _market_breakdown():
        async with AsyncSessionLocal() as s:
            result = await s.execute(text("""
                SELECT c.market_code,
                       COUNT(DISTINCT j.id) AS job_count,
                       COUNT(DISTINCT c.id) AS company_count,
                       AVG(j.quality_score) AS avg_quality
                FROM companies c
                JOIN jobs j ON j.company_id = c.id
                WHERE j.is_active = true AND j.is_canonical = true
                GROUP BY c.market_code
                ORDER BY job_count DESC
            """))
            return [
                {"market": r.market_code or "Unknown", "jobs": r.job_count or 0,
                 "companies": r.company_count or 0,
                 "avg_quality": round(r.avg_quality, 1) if r.avg_quality else None}
                for r in result.all()
            ]

    async def _field_coverage():
        async with AsyncSessionLocal() as s:
            r = (await s.execute(select(
                func.count(Job.id).label("total"),
                func.count(Job.description).label("desc_n"),
                func.count(Job.location_raw).label("loc_n"),
                func.count(Job.salary_raw).label("sal_n"),
                func.count(Job.employment_type).label("emp_n"),
                func.count(Job.seniority_level).label("sen_n"),
                func.count(Job.requirements).label("req_n"),
                func.count(Job.benefits).label("ben_n"),
            ).where(Job.is_active == True))).one()
            total = r.total or 1
            pct = lambda n: round((n / total) * 100, 1)
            return {
                "total_active_jobs": r.total,
                "company_name_pct": 100.0, "title_pct": 100.0,
                "description_pct": pct(r.desc_n), "location_pct": pct(r.loc_n),
                "salary_pct": pct(r.sal_n), "employment_type_pct": pct(r.emp_n),
                "seniority_pct": pct(r.sen_n), "requirements_pct": pct(r.req_n),
                "benefits_pct": pct(r.ben_n),
            }

    async def _quality_distribution():
        async with AsyncSessionLocal() as s:
            r = (await s.execute(text("""
                SELECT
                    COUNT(*) FILTER (WHERE quality_score IS NOT NULL AND is_active) AS total_scored,
                    COUNT(*) FILTER (WHERE quality_score IS NULL     AND is_active) AS total_unscored,
                    AVG(quality_score) FILTER (WHERE quality_score IS NOT NULL AND is_active) AS avg_score,
                    COUNT(*) FILTER (WHERE quality_score >= 80  AND is_active) AS excellent,
                    COUNT(*) FILTER (WHERE quality_score >= 60  AND quality_score < 80 AND is_active) AS good,
                    COUNT(*) FILTER (WHERE quality_score >= 40  AND quality_score < 60 AND is_active) AS fair,
                    COUNT(*) FILTER (WHERE quality_score >= 20  AND quality_score < 40 AND is_active) AS poor,
                    COUNT(*) FILTER (WHERE quality_score >= 0   AND quality_score < 20 AND is_active) AS disqualified,
                    COUNT(*) FILTER (WHERE (quality_flags->>'scam_detected')::boolean IS TRUE AND is_active) AS scam_count,
                    COUNT(*) FILTER (WHERE (quality_flags->>'discrimination_detected')::boolean IS TRUE AND is_active) AS disc_count,
                    COUNT(*) FILTER (WHERE (quality_flags->>'bad_words_detected')::boolean IS TRUE AND is_active) AS bad_words_count
                FROM jobs
            """))).one()
            total = r.total_scored or 0
            pct = lambda n: round((n / total) * 100, 1) if total else 0
            return {
                "total_scored": total, "total_unscored": r.total_unscored,
                "average_score": round(float(r.avg_score or 0), 1),
                "bands": {
                    "excellent":    {"count": r.excellent,    "pct": pct(r.excellent)},
                    "good":         {"count": r.good,          "pct": pct(r.good)},
                    "fair":         {"count": r.fair,          "pct": pct(r.fair)},
                    "poor":         {"count": r.poor,          "pct": pct(r.poor)},
                    "disqualified": {"count": r.disqualified,  "pct": pct(r.disqualified)},
                },
                "flags": {
                    "scam_detected": r.scam_count or 0,
                    "discrimination_detected": r.disc_count or 0,
                    "bad_words_detected": r.bad_words_count or 0,
                },
            }

    async def _trends():
        async with AsyncSessionLocal() as s:
            result = await s.execute(select(
                func.date_trunc("day", Job.first_seen_at).label("day"),
                func.count().label("count"),
            ).where(Job.first_seen_at >= func.now() - text("interval '30 days'"))
             .group_by("day").order_by("day"))
            return [{"day": str(r.day)[:10], "count": r.count} for r in result]

    async def _quality_by_site():
        async with AsyncSessionLocal() as s:
            result = await s.execute(
                select(Company.id, Company.name, Company.domain, Company.market_code,
                       Company.quality_score, func.count(Job.id).label("job_count"))
                .join(Job, Job.company_id == Company.id, isouter=True)
                .where(Company.quality_score.isnot(None))
                .group_by(Company.id).order_by(Company.quality_score.desc()).limit(40)
            )
            return [
                {"id": str(r.id), "name": r.name, "domain": r.domain,
                 "market_code": r.market_code,
                 "quality_score": round(r.quality_score, 1) if r.quality_score else None,
                 "job_count": r.job_count}
                for r in result.all()
            ]

    async def _crawl_history():
        async with AsyncSessionLocal() as s:
            result = await s.execute(text("""
                SELECT cl.id, cl.crawl_type, cl.status, cl.jobs_found, cl.started_at,
                       c.name AS company_name
                FROM crawl_logs cl
                LEFT JOIN companies c ON c.id = cl.company_id
                ORDER BY cl.started_at DESC
                LIMIT 10
            """))
            return [
                {"id": str(r.id), "crawl_type": r.crawl_type, "status": r.status,
                 "jobs_found": r.jobs_found, "company_name": r.company_name,
                 "started_at": r.started_at.isoformat() if r.started_at else None}
                for r in result.all()
            ]

    # Each coroutine owns its own session → safe to run truly in parallel
    (job_stats, markets, coverage, quality, trends_data,
     quality_sites, crawl_history) = await asyncio.gather(
        _job_stats(), _market_breakdown(), _field_coverage(),
        _quality_distribution(), _trends(), _quality_by_site(), _crawl_history(),
    )

    result = {
        "job_stats": job_stats, "markets": markets, "coverage": coverage,
        "quality": quality, "trends": trends_data,
        "quality_sites": quality_sites, "crawl_history": crawl_history,
    }
    await _cache_set("analytics:overview", result, ttl=15)
    return result
