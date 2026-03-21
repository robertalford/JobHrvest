"""Job endpoints."""

import csv
import io
import json
from typing import Optional
from uuid import UUID

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy import func, select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.base import get_db
from app.models.job import Job

router = APIRouter()

_redis: aioredis.Redis | None = None


async def _get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis

_LIST_TTL = 30  # seconds


async def _cache_get(key: str) -> dict | None:
    try:
        r = await _get_redis()
        raw = await r.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


async def _cache_set(key: str, value: dict) -> None:
    try:
        r = await _get_redis()
        await r.set(key, json.dumps(value), ex=_LIST_TTL)
    except Exception:
        pass


@router.get("/stats")
async def job_stats(db: AsyncSession = Depends(get_db)):
    """Single-query job stats using conditional aggregation — cached 15s."""
    cached = await _cache_get("jobs:stats")
    if cached is not None:
        return cached

    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    from sqlalchemy import text

    AEST = ZoneInfo("Australia/Sydney")
    today_start = datetime.now(AEST).replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=7)

    sixty_days_ago = today_start - timedelta(days=60)

    result = await db.execute(text("""
        SELECT
            COUNT(*)                                                                    AS total,
            COUNT(*) FILTER (WHERE is_active)                                           AS active,
            COUNT(*) FILTER (WHERE is_active AND is_canonical)                          AS unique_active,
            COUNT(*) FILTER (WHERE is_active AND NOT is_canonical)                      AS duplicates,
            COUNT(*) FILTER (WHERE is_active AND is_canonical
                             AND first_seen_at >= :today_start)                         AS new_today,
            COUNT(*) FILTER (WHERE is_active AND is_canonical
                             AND first_seen_at >= :week_start)                          AS new_this_week,
            -- Live jobs: pass full quality gate (core fields + geocoded + no bad/scam + not expired)
            COUNT(*) FILTER (
                WHERE is_active
                  AND is_canonical
                  AND title IS NOT NULL AND title != ''
                  AND company_id IS NOT NULL
                  AND description IS NOT NULL AND length(description) >= 200
                  AND location_raw IS NOT NULL AND location_raw != ''
                  AND geo_resolved = true
                  AND (quality_flags IS NULL OR (quality_flags->>'scam_detected')::boolean IS NOT TRUE)
                  AND (quality_flags IS NULL OR (quality_flags->>'bad_words_detected')::boolean IS NOT TRUE)
                  AND quality_score IS NOT NULL AND quality_score > 0
                  AND (date_expires IS NULL OR date_expires >= CURRENT_DATE)
                  AND first_seen_at >= :sixty_days_ago
            )                                                                           AS live_jobs
        FROM jobs
    """), {"today_start": today_start, "week_start": week_start, "sixty_days_ago": sixty_days_ago})
    r = result.one()
    data = {
        "total": r.total,
        "active": r.active,
        "unique_active": r.unique_active,
        "duplicates": r.duplicates,
        "new_today": r.new_today,
        "new_this_week": r.new_this_week,
        "live_jobs": r.live_jobs,
    }
    await _cache_set("jobs:stats", data)
    return data


def _apply_job_filters(q, *, search, company_id, location_country, location_city,
                       remote_type, employment_type, seniority_level, is_active,
                       salary_min, salary_max, quality_min, quality_band, canonical_only):
    """Apply all list/export filters to a Job query — shared by list and export endpoints."""
    if search:
        q = q.where(or_(Job.title.ilike(f"%{search}%"), Job.description.ilike(f"%{search}%")))
    if company_id:
        q = q.where(Job.company_id == company_id)
    if location_country:
        q = q.where(Job.location_country.ilike(f"%{location_country}%"))
    if location_city:
        q = q.where(Job.location_city.ilike(f"%{location_city}%"))
    if remote_type:
        q = q.where(Job.remote_type == remote_type)
    if employment_type:
        q = q.where(Job.employment_type == employment_type)
    if seniority_level:
        q = q.where(Job.seniority_level == seniority_level)
    if is_active is not None:
        q = q.where(Job.is_active == is_active)
    if salary_min is not None:
        q = q.where(Job.salary_min >= salary_min)
    if salary_max is not None:
        q = q.where(Job.salary_max <= salary_max)
    if quality_min is not None:
        q = q.where(Job.quality_score >= quality_min)
    if quality_band:
        band_ranges = {
            "excellent": (80, 101), "good": (60, 80), "fair": (40, 60),
            "poor": (20, 40), "disqualified": (0, 20),
        }
        lo, hi = band_ranges[quality_band]
        q = q.where(Job.quality_score >= lo, Job.quality_score < hi)
    if canonical_only:
        q = q.where(Job.is_canonical == True)
    return q


@router.get("")
async def list_jobs(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = None,
    company_id: Optional[UUID] = None,
    location_country: Optional[str] = None,
    location_city: Optional[str] = None,
    remote_type: Optional[str] = None,
    employment_type: Optional[str] = None,
    seniority_level: Optional[str] = None,
    is_active: Optional[bool] = True,
    salary_min: Optional[float] = None,
    salary_max: Optional[float] = None,
    quality_min: Optional[float] = Query(None, ge=0, le=100),
    quality_band: Optional[str] = Query(None, pattern="^(excellent|good|fair|poor|disqualified)$"),
    canonical_only: Optional[bool] = Query(True, description="Show only canonical (deduplicated best) jobs"),
    db: AsyncSession = Depends(get_db),
):
    from sqlalchemy import text as sa_text
    from sqlalchemy.orm import joinedload

    cache_key = (f"jobs:list:{page}:{page_size}:{search}:{company_id}:{location_country}:"
                 f"{location_city}:{remote_type}:{employment_type}:{seniority_level}:"
                 f"{is_active}:{salary_min}:{salary_max}:{quality_min}:{quality_band}:{canonical_only}")
    cached = await _cache_get(cache_key)
    if cached is not None:
        return cached

    is_filtered = bool(search or company_id or location_country or location_city
                       or remote_type or employment_type or seniority_level
                       or salary_min is not None or salary_max is not None
                       or quality_min is not None or quality_band
                       or canonical_only is False)

    q = select(Job).options(joinedload(Job.company))
    q = _apply_job_filters(
        q, search=search, company_id=company_id, location_country=location_country,
        location_city=location_city, remote_type=remote_type, employment_type=employment_type,
        seniority_level=seniority_level, is_active=is_active, salary_min=salary_min,
        salary_max=salary_max, quality_min=quality_min, quality_band=quality_band,
        canonical_only=canonical_only,
    )

    if not is_filtered:
        # Fast path: Postgres statistics for unfiltered total (< 1ms vs full COUNT scan)
        total = (await db.execute(sa_text(
            "SELECT reltuples::bigint FROM pg_class WHERE relname = 'jobs'"
        ))).scalar() or 0
    else:
        total = await db.scalar(select(func.count()).select_from(
            _apply_job_filters(
                select(Job), search=search, company_id=company_id,
                location_country=location_country, location_city=location_city,
                remote_type=remote_type, employment_type=employment_type,
                seniority_level=seniority_level, is_active=is_active,
                salary_min=salary_min, salary_max=salary_max,
                quality_min=quality_min, quality_band=quality_band,
                canonical_only=canonical_only,
            ).subquery()
        ))
    jobs = list(await db.scalars(q.order_by(Job.first_seen_at.desc()).offset((page - 1) * page_size).limit(page_size)))

    items = [
        {
            "id": str(j.id),
            "title": j.title,
            "company_id": str(j.company_id),
            "company_name": j.company.name if j.company else None,
            "company_domain": j.company.domain if j.company else None,
            "source_url": j.source_url,
            "location_raw": j.location_raw,
            "location_city": j.location_city,
            "location_country": j.location_country,
            "remote_type": j.remote_type,
            "employment_type": j.employment_type,
            "seniority_level": j.seniority_level,
            "salary_raw": j.salary_raw,
            "salary_min": float(j.salary_min) if j.salary_min is not None else None,
            "salary_max": float(j.salary_max) if j.salary_max is not None else None,
            "salary_currency": j.salary_currency,
            "quality_score": j.quality_score,
            "extraction_method": j.extraction_method,
            "is_active": j.is_active,
            "is_canonical": j.is_canonical,
            "first_seen_at": j.first_seen_at.isoformat() if j.first_seen_at else None,
            "date_posted": j.date_posted.isoformat() if j.date_posted else None,
        }
        for j in jobs
    ]
    result = {"items": items, "total": total, "page": page, "page_size": page_size}
    await _cache_set(cache_key, result)
    return result


@router.get("/banned")
async def list_banned_jobs(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = None,
    location_country: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    quality_threshold: float = Query(40.0, ge=0, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Return jobs below quality threshold with company info and quality detail."""
    from datetime import datetime, timezone
    from sqlalchemy.orm import joinedload
    from app.models.company import Company

    q = (
        select(Job)
        .options(joinedload(Job.company))
        .where(Job.quality_score.isnot(None), Job.quality_score < quality_threshold)
    )

    if search:
        q = q.where(or_(
            Job.title.ilike(f"%{search}%"),
            Job.description.ilike(f"%{search}%"),
            Job.location_raw.ilike(f"%{search}%"),
        ))
    if location_country:
        q = q.where(Job.location_country.ilike(f"%{location_country}%"))
    if date_from:
        try:
            q = q.where(Job.first_seen_at >= datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc))
        except ValueError:
            pass
    if date_to:
        try:
            q = q.where(Job.first_seen_at <= datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc))
        except ValueError:
            pass

    total = await db.scalar(select(func.count()).select_from(q.subquery()))
    items = await db.scalars(q.order_by(Job.quality_score.asc(), Job.first_seen_at.desc()).offset((page - 1) * page_size).limit(page_size))

    result = []
    for j in items:
        # Determine primary reason from quality_issues or quality_flags
        primary_reason = "Low quality score"
        snippet = None
        if j.quality_flags:
            flags = j.quality_flags
            if flags.get("contains_profanity"):
                primary_reason = "Contains profanity/inappropriate language"
            elif flags.get("likely_scam"):
                primary_reason = "Likely scam or fraudulent listing"
            elif flags.get("discriminatory_content"):
                primary_reason = "Contains discriminatory content"
        if j.quality_issues and isinstance(j.quality_issues, list) and j.quality_issues:
            issue = j.quality_issues[0]
            if isinstance(issue, dict):
                primary_reason = issue.get("reason", primary_reason)
                snippet = issue.get("snippet")
            elif isinstance(issue, str):
                primary_reason = issue

        result.append({
            "id": str(j.id),
            "title": j.title,
            "source_url": j.source_url,
            "company_name": j.company.name if j.company else None,
            "company_domain": j.company.domain if j.company else None,
            "location_country": j.location_country,
            "quality_score": j.quality_score,
            "primary_reason": primary_reason,
            "snippet": snippet or (j.description[:150] if j.description else None),
            "first_seen_at": j.first_seen_at.isoformat() if j.first_seen_at else None,
        })

    return {"items": result, "total": total, "page": page, "page_size": page_size}


@router.get("/crawl-breakdown")
async def job_crawl_breakdown(
    from_dt: Optional[str] = Query(None),
    to_dt: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Breakdown of job extraction: pass/fail gate + quality richness for passing jobs."""
    from datetime import datetime, timedelta
    from sqlalchemy import text
    from zoneinfo import ZoneInfo

    if not from_dt:
        f = datetime.now(ZoneInfo("UTC")) - timedelta(hours=24)
    else:
        f = datetime.fromisoformat(from_dt)
    t = datetime.fromisoformat(to_dt) if to_dt else datetime.now(ZoneInfo("UTC"))

    # Pass/fail gate criteria (must match enforce_quality_gate exactly)
    PASS_GATE = """
        title IS NOT NULL AND title != ''
        AND company_id IS NOT NULL
        AND description IS NOT NULL AND length(description) >= 200
        AND location_raw IS NOT NULL AND location_raw != ''
        AND geo_resolved = true
        AND is_canonical = true
        AND (quality_flags IS NULL OR (quality_flags->>'bad_words_detected')::boolean IS NOT TRUE)
        AND (quality_flags IS NULL OR (quality_flags->>'scam_detected')::boolean IS NOT TRUE)
        AND (date_expires IS NULL OR date_expires >= CURRENT_DATE)
        AND first_seen_at >= NOW() - INTERVAL '60 days'
    """

    result = await db.execute(text(f"""
        SELECT
            COUNT(*) AS total_extracted,
            COUNT(*) FILTER (WHERE
                (title IS NULL OR title = '') OR
                company_id IS NULL OR
                (description IS NULL OR length(description) < 200) OR
                (location_raw IS NULL OR location_raw = '' OR geo_resolved IS NOT TRUE)
            ) AS failed_core_fields,
            COUNT(*) FILTER (WHERE (quality_flags->>'bad_words_detected')::boolean IS TRUE) AS failed_bad_words,
            COUNT(*) FILTER (WHERE is_active AND NOT is_canonical) AS failed_duplicates,
            COUNT(*) FILTER (WHERE
                (date_expires IS NOT NULL AND date_expires < CURRENT_DATE) OR
                (first_seen_at < NOW() - INTERVAL '60 days')
            ) AS failed_expired,
            COUNT(*) FILTER (WHERE (quality_flags->>'scam_detected')::boolean IS TRUE) AS failed_scam,
            COUNT(*) FILTER (WHERE is_active AND {PASS_GATE}) AS live_jobs
        FROM jobs
        WHERE created_at BETWEEN :from_dt AND :to_dt
    """), {"from_dt": f, "to_dt": t})
    r = result.one()

    # Quality breakdown of live (passing) jobs — richness bands
    quality_result = await db.execute(text(f"""
        SELECT
            COUNT(*) FILTER (WHERE quality_score >= 0.80) AS quality_a,
            COUNT(*) FILTER (WHERE quality_score >= 0.60 AND quality_score < 0.80) AS quality_b,
            COUNT(*) FILTER (WHERE quality_score >= 0.40 AND quality_score < 0.60) AS quality_c,
            COUNT(*) FILTER (WHERE quality_score > 0 AND quality_score < 0.40) AS quality_d,
            COUNT(*) AS total_live
        FROM jobs
        WHERE is_active AND {PASS_GATE}
    """))
    q = quality_result.one()

    return {
        "total_extracted": r.total_extracted,
        "failed_core_fields": r.failed_core_fields,
        "failed_bad_words": r.failed_bad_words,
        "failed_duplicates": r.failed_duplicates,
        "failed_expired": r.failed_expired,
        "failed_scam": r.failed_scam,
        "live_jobs": r.live_jobs,
        "quality_breakdown": {
            "A_complete": {"count": q.quality_a, "pct": round(100 * q.quality_a / max(q.total_live, 1), 1)},
            "B_good": {"count": q.quality_b, "pct": round(100 * q.quality_b / max(q.total_live, 1), 1)},
            "C_fair": {"count": q.quality_c, "pct": round(100 * q.quality_c / max(q.total_live, 1), 1)},
            "D_poor": {"count": q.quality_d, "pct": round(100 * q.quality_d / max(q.total_live, 1), 1)},
            "total": q.total_live,
        }
    }

@router.get("/description-audit")
async def description_audit(db: AsyncSession = Depends(get_db)):
    """
    Audit description extraction quality.

    Returns:
    - Breakdown of banned jobs (description_very_short) by extraction method
    - Description length distribution buckets
    - 5 sample jobs per bucket with source URLs for manual spot-checking
    - Count of jobs pending enrichment
    """
    from sqlalchemy import text, func as sqlfunc

    # Stats by extraction method for jobs flagged as description_very_short
    method_breakdown = await db.execute(text("""
        SELECT
            COALESCE(extraction_method, 'unknown')             AS method,
            COUNT(*)                                           AS total,
            COUNT(*) FILTER (WHERE description IS NULL)        AS no_description,
            COUNT(*) FILTER (WHERE description = '')           AS empty_description,
            ROUND(AVG(COALESCE(LENGTH(description), 0)))       AS avg_desc_len,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY COALESCE(LENGTH(description), 0)
            ))                                                 AS median_desc_len
        FROM jobs
        WHERE is_active = true
          AND quality_issues::text LIKE '%description_very_short%'
        GROUP BY extraction_method
        ORDER BY total DESC
    """))

    # Overall description length buckets across ALL active canonical jobs
    length_buckets = await db.execute(text("""
        SELECT
            CASE
                WHEN description IS NULL OR description = ''  THEN 'empty'
                WHEN LENGTH(description) < 50                 THEN '<50'
                WHEN LENGTH(description) < 200                THEN '50-199'
                WHEN LENGTH(description) < 500                THEN '200-499'
                WHEN LENGTH(description) < 1000               THEN '500-999'
                ELSE '1000+'
            END AS bucket,
            COUNT(*) AS count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()) AS pct
        FROM jobs
        WHERE is_active = true AND is_canonical = true
        GROUP BY 1
        ORDER BY MIN(COALESCE(LENGTH(description), 0))
    """))

    # Sample jobs with very short descriptions for spot-checking
    # Grouped by bucket so we can see concrete examples
    samples_raw = await db.execute(text("""
        SELECT
            id, title, source_url, extraction_method,
            COALESCE(LENGTH(description), 0)  AS desc_len,
            LEFT(description, 200)            AS desc_snippet,
            quality_score,
            description_enriched_at
        FROM jobs
        WHERE is_active = true
          AND (description IS NULL OR LENGTH(description) < 200)
        ORDER BY COALESCE(LENGTH(description), 0) ASC, first_seen_at DESC
        LIMIT 30
    """))

    # Count of jobs pending enrichment (not yet attempted)
    pending_enrichment = await db.scalar(text("""
        SELECT COUNT(*) FROM jobs
        WHERE is_active = true
          AND description_enriched_at IS NULL
          AND (description IS NULL OR LENGTH(description) < 200)
    """))

    # Count already enriched
    enriched_count = await db.scalar(text("""
        SELECT COUNT(*) FROM jobs
        WHERE is_active = true AND description_enriched_at IS NOT NULL
    """))

    return {
        "by_extraction_method": [
            {
                "method": r.method,
                "total_flagged": r.total,
                "no_description": r.no_description,
                "empty_description": r.empty_description,
                "avg_desc_len": int(r.avg_desc_len or 0),
                "median_desc_len": int(r.median_desc_len or 0),
            }
            for r in method_breakdown
        ],
        "length_distribution": [
            {"bucket": r.bucket, "count": r.count, "pct": float(r.pct or 0)}
            for r in length_buckets
        ],
        "samples_short_description": [
            {
                "id": str(r.id),
                "title": r.title,
                "source_url": r.source_url,
                "extraction_method": r.extraction_method,
                "desc_len": r.desc_len,
                "desc_snippet": r.desc_snippet,
                "quality_score": r.quality_score,
                "already_enriched": r.description_enriched_at is not None,
            }
            for r in samples_raw
        ],
        "pending_enrichment": pending_enrichment or 0,
        "already_enriched": enriched_count or 0,
    }


@router.post("/trigger-enrichment", status_code=202)
async def trigger_description_enrichment(limit: int = Query(150, ge=1, le=1000)):
    """Manually trigger a description enrichment batch."""
    from app.tasks.ml_tasks import enrich_job_descriptions
    task = enrich_job_descriptions.apply_async(kwargs={"limit": limit}, queue="ml")
    return {"task_id": task.id, "status": "queued", "limit": limit}


@router.post("/trigger-reprocess", status_code=202)
async def trigger_reprocess(
    limit: int = Query(50, ge=1, le=500, description="Max companies to reprocess"),
    quality_threshold: float = Query(40.0, ge=0, le=100, description="Quality score threshold"),
    db: AsyncSession = Depends(get_db),
):
    """
    Trigger bulk reprocessing of low-quality and duplicate jobs.

    For each targeted company:
    1. Re-enrich descriptions using the 7-layer DescriptionExtractor
    2. Re-run deduplication with improved data (fixed null-null scoring)
    3. Re-score quality with enriched fields

    Returns immediately; reprocessing runs in the ML worker queue.
    """
    from app.tasks.ml_tasks import batch_reprocess
    task = batch_reprocess.apply_async(
        kwargs={"limit": limit, "quality_threshold": quality_threshold},
        queue="ml",
    )
    return {"task_id": task.id, "status": "queued", "limit": limit, "quality_threshold": quality_threshold}


@router.get("/reprocess-stats")
async def reprocess_stats(db: AsyncSession = Depends(get_db)):
    """
    Stats to measure the impact of reprocessing on duplicate and quality outcomes.
    Shows before/after comparison of canonical vs. duplicate counts and quality distribution.
    """
    from sqlalchemy import text
    result = await db.execute(text("""
        SELECT
            COUNT(*)                                                             AS total_active,
            COUNT(*) FILTER (WHERE is_canonical)                                 AS canonical,
            COUNT(*) FILTER (WHERE NOT is_canonical)                             AS duplicates,
            ROUND(COUNT(*) FILTER (WHERE NOT is_canonical) * 100.0 /
                  NULLIF(COUNT(*), 0), 1)                                        AS duplicate_pct,
            -- Quality distribution (canonical only)
            COUNT(*) FILTER (WHERE is_canonical AND quality_score IS NULL)       AS unscored,
            COUNT(*) FILTER (WHERE is_canonical AND quality_score >= 80)         AS excellent,
            COUNT(*) FILTER (WHERE is_canonical AND quality_score >= 60
                             AND quality_score < 80)                             AS good,
            COUNT(*) FILTER (WHERE is_canonical AND quality_score >= 40
                             AND quality_score < 60)                             AS fair,
            COUNT(*) FILTER (WHERE is_canonical AND quality_score >= 0.20
                             AND quality_score < 40)                             AS poor,
            COUNT(*) FILTER (WHERE is_canonical AND quality_score < 20)          AS disqualified,
            -- Description coverage
            COUNT(*) FILTER (WHERE is_canonical
                             AND (description IS NULL OR description = ''))      AS no_description,
            COUNT(*) FILTER (WHERE is_canonical AND LENGTH(description) >= 300)  AS good_description,
            COUNT(*) FILTER (WHERE description_enriched_at IS NOT NULL)          AS enriched
        FROM jobs
        WHERE is_active = true
    """))
    r = result.one()

    # Companies with most false-positive duplicates (title-only matches)
    dup_companies = await db.execute(text("""
        SELECT
            c.name,
            c.domain,
            COUNT(*) FILTER (WHERE j.is_canonical)   AS canonical_count,
            COUNT(*) FILTER (WHERE NOT j.is_canonical) AS duplicate_count,
            ROUND(AVG(j.quality_score) FILTER (WHERE j.is_canonical)::numeric, 1) AS avg_quality
        FROM jobs j
        JOIN companies c ON j.company_id = c.id
        WHERE j.is_active = true
        GROUP BY c.id, c.name, c.domain
        HAVING COUNT(*) FILTER (WHERE NOT j.is_canonical) > 0
        ORDER BY duplicate_count DESC
        LIMIT 15
    """))

    return {
        "totals": {
            "total_active": r.total_active,
            "canonical": r.canonical,
            "duplicates": r.duplicates,
            "duplicate_pct": float(r.duplicate_pct or 0),
        },
        "quality_bands": {
            "unscored": r.unscored,
            "excellent": r.excellent,
            "good": r.good,
            "fair": r.fair,
            "poor": r.poor,
            "disqualified": r.disqualified,
        },
        "description_coverage": {
            "no_description": r.no_description,
            "good_description": r.good_description,
            "enriched": r.enriched,
        },
        "companies_with_most_duplicates": [
            {
                "name": row.name,
                "domain": row.domain,
                "canonical": row.canonical_count,
                "duplicates": row.duplicate_count,
                "avg_quality": float(row.avg_quality or 0),
            }
            for row in dup_companies
        ],
    }


@router.get("/export")
async def export_jobs(
    format: str = Query("csv", pattern="^(csv|json)$"),
    search: Optional[str] = None,
    company_id: Optional[UUID] = None,
    location_country: Optional[str] = None,
    remote_type: Optional[str] = None,
    employment_type: Optional[str] = None,
    seniority_level: Optional[str] = None,
    is_active: Optional[bool] = True,
    quality_band: Optional[str] = Query(None, pattern="^(excellent|good|fair|poor|disqualified)$"),
    canonical_only: Optional[bool] = True,
    db: AsyncSession = Depends(get_db),
):
    from sqlalchemy.orm import joinedload

    q = select(Job).options(joinedload(Job.company))
    q = _apply_job_filters(
        q, search=search, company_id=company_id, location_country=location_country,
        location_city=None, remote_type=remote_type, employment_type=employment_type,
        seniority_level=seniority_level, is_active=is_active, salary_min=None,
        salary_max=None, quality_min=None, quality_band=quality_band,
        canonical_only=canonical_only,
    )
    jobs = list(await db.scalars(q.order_by(Job.first_seen_at.desc())))

    FIELDS = ["id", "title", "company_name", "company_domain", "location_raw", "location_city",
              "location_country", "remote_type", "employment_type", "seniority_level",
              "salary_raw", "salary_min", "salary_max", "salary_currency",
              "quality_score", "extraction_method", "source_url", "date_posted", "first_seen_at"]

    def _row(j: Job):
        return {
            "id": str(j.id),
            "title": j.title,
            "company_name": j.company.name if j.company else "",
            "company_domain": j.company.domain if j.company else "",
            "location_raw": j.location_raw or "",
            "location_city": j.location_city or "",
            "location_country": j.location_country or "",
            "remote_type": j.remote_type or "",
            "employment_type": j.employment_type or "",
            "seniority_level": j.seniority_level or "",
            "salary_raw": j.salary_raw or "",
            "salary_min": float(j.salary_min) if j.salary_min is not None else "",
            "salary_max": float(j.salary_max) if j.salary_max is not None else "",
            "salary_currency": j.salary_currency or "",
            "quality_score": j.quality_score or "",
            "extraction_method": j.extraction_method or "",
            "source_url": j.source_url or "",
            "date_posted": j.date_posted.isoformat() if j.date_posted else "",
            "first_seen_at": j.first_seen_at.isoformat() if j.first_seen_at else "",
        }

    if format == "json":
        return Response(json.dumps([_row(j) for j in jobs], default=str), media_type="application/json",
                        headers={"Content-Disposition": "attachment; filename=jobs.json"})

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=FIELDS)
    writer.writeheader()
    for j in jobs:
        writer.writerow(_row(j))
    return Response(output.getvalue(), media_type="text/csv",
                    headers={"Content-Disposition": "attachment; filename=jobs.csv"})


@router.get("/{job_id}")
async def get_job(job_id: UUID, db: AsyncSession = Depends(get_db)):
    job = await db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
