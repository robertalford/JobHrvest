"""Career page (Sites) endpoints."""

import csv
import io
import json
from typing import Optional
from uuid import UUID

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.base import get_db
from app.models.career_page import CareerPage
from app.models.company import Company
from app.models.site_template import SiteTemplate

router = APIRouter()

_redis: aioredis.Redis | None = None
_LIST_TTL = 30


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


async def _cache_set(key: str, value) -> None:
    try:
        await _get_redis().set(key, json.dumps(value, default=str), ex=_LIST_TTL)
    except Exception:
        pass


# ── Shared SQL helpers ────────────────────────────────────────────────────────

def _sites_where(search_pat, active_only, page_type, discovery_method, is_primary,
                 has_template, requires_js):
    """Build the WHERE clause fragment shared between list and export."""
    clauses = [
        "(CAST(:search AS TEXT) IS NULL OR c.name ILIKE :search OR cp.url ILIKE :search)",
        "(CAST(:active_only AS BOOLEAN) = false OR cp.is_active = true)",
        "(CAST(:page_type AS TEXT) IS NULL OR cp.page_type = :page_type)",
        "(CAST(:discovery_method AS TEXT) IS NULL OR cp.discovery_method = :discovery_method)",
        "(CAST(:is_primary AS BOOLEAN) IS NULL OR cp.is_primary = :is_primary)",
        "(CAST(:requires_js AS BOOLEAN) IS NULL OR cp.requires_js_rendering = :requires_js)",
    ]
    if has_template is True:
        clauses.append("EXISTS (SELECT 1 FROM site_templates st WHERE st.career_page_id = cp.id AND st.is_active = true)")
    elif has_template is False:
        clauses.append("NOT EXISTS (SELECT 1 FROM site_templates st WHERE st.career_page_id = cp.id AND st.is_active = true)")
    return " AND ".join(clauses)


def _sites_params(search_pat, active_only, page_type, discovery_method, is_primary, requires_js):
    return {
        "search": search_pat,
        "active_only": active_only,
        "page_type": page_type,
        "discovery_method": discovery_method,
        "is_primary": is_primary,
        "requires_js": requires_js,
    }


def _compute_expected_jobs(total_crawls, avg_3, last, imported):
    total_crawls = total_crawls or 0
    if total_crawls >= 3 and avg_3 is not None:
        return int(avg_3)
    elif total_crawls >= 1 and last is not None:
        return int(last)
    elif imported is not None:
        return int(imported)
    return None


# ── List ──────────────────────────────────────────────────────────────────────

_SITES_SELECT = """
    SELECT
        cp.id,
        cp.company_id,
        c.name            AS company_name,
        c.domain          AS company_domain,
        cp.url,
        cp.page_type,
        cp.discovery_method,
        cp.discovery_confidence,
        cp.is_primary,
        cp.is_paginated,
        cp.requires_js_rendering,
        cp.last_crawled_at,
        cp.is_active,
        cp.created_at,

        -- Pre-aggregated stats via gold table JOIN (replaces 4 correlated subqueries)
        cs.last_crawl_jobs,
        cs.total_crawls,
        cs.avg_last_3_jobs,
        cs.imported_expected_jobs,
        cs.expected_jobs,

        st.id             AS template_id,
        st.accuracy_score AS template_accuracy,
        st.learned_via    AS template_learned_via,
        st.last_validated_at AS template_last_validated

    FROM career_pages cp
    JOIN companies c ON c.id = cp.company_id
    LEFT JOIN company_stats cs ON cs.company_id = cp.company_id
    LEFT JOIN site_templates st
           ON st.career_page_id = cp.id AND st.is_active = true
"""


@router.get("/")
async def list_career_pages(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = None,
    active_only: bool = Query(False),
    page_type: Optional[str] = None,
    discovery_method: Optional[str] = None,
    is_primary: Optional[bool] = None,
    has_template: Optional[bool] = None,
    requires_js: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
):
    search_pat = f"%{search}%" if search else None
    cache_key = (f"career_pages:list:{page}:{page_size}:{search}:{active_only}:"
                 f"{page_type}:{discovery_method}:{is_primary}:{has_template}:{requires_js}")
    cached = await _cache_get(cache_key)
    if cached is not None:
        return cached

    where = _sites_where(search_pat, active_only, page_type, discovery_method,
                         is_primary, has_template, requires_js)
    params = _sites_params(search_pat, active_only, page_type, discovery_method,
                           is_primary, requires_js)
    is_filtered = bool(search_pat or active_only or page_type or discovery_method
                       or is_primary is not None or has_template is not None
                       or requires_js is not None)

    rows = await db.execute(text(f"""
        {_SITES_SELECT}
        WHERE {where}
        ORDER BY cp.created_at DESC
        LIMIT :limit OFFSET :offset
    """), {**params, "limit": page_size, "offset": (page - 1) * page_size})

    if not is_filtered:
        total_row = await db.execute(text(
            "SELECT reltuples::bigint FROM pg_class WHERE relname = 'career_pages'"
        ))
    else:
        total_row = await db.execute(text(f"""
            SELECT COUNT(*)
            FROM career_pages cp
            JOIN companies c ON c.id = cp.company_id
            WHERE {where}
        """), params)
    total = total_row.scalar() or 0

    items = []
    for r in rows:
        expected_jobs = r.expected_jobs  # pre-computed by gold table trigger
        items.append({
            "id": str(r.id),
            "company_id": str(r.company_id),
            "company_name": r.company_name,
            "company_domain": r.company_domain,
            "url": r.url,
            "page_type": r.page_type,
            "discovery_method": r.discovery_method,
            "discovery_confidence": r.discovery_confidence,
            "is_primary": r.is_primary,
            "is_paginated": r.is_paginated,
            "requires_js_rendering": r.requires_js_rendering,
            "last_crawled_at": r.last_crawled_at.isoformat() if r.last_crawled_at else None,
            "is_active": r.is_active,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "last_crawl_jobs": int(r.last_crawl_jobs) if r.last_crawl_jobs is not None else None,
            "expected_jobs": expected_jobs,
            "has_template": r.template_id is not None,
            "template_accuracy": round(float(r.template_accuracy), 2) if r.template_accuracy else None,
            "template_learned_via": r.template_learned_via,
            "template_last_validated": r.template_last_validated.isoformat() if r.template_last_validated else None,
        })

    result = {"total": total, "page": page, "page_size": page_size, "items": items}
    await _cache_set(cache_key, result)
    return result


# ── Export ─────────────────────────────────────────────────────────────────────

@router.get("/export")
async def export_career_pages(
    format: str = Query("csv"),
    search: Optional[str] = None,
    active_only: bool = Query(False),
    page_type: Optional[str] = None,
    discovery_method: Optional[str] = None,
    is_primary: Optional[bool] = None,
    has_template: Optional[bool] = None,
    requires_js: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
):
    search_pat = f"%{search}%" if search else None
    where = _sites_where(search_pat, active_only, page_type, discovery_method,
                         is_primary, has_template, requires_js)
    params = _sites_params(search_pat, active_only, page_type, discovery_method,
                           is_primary, requires_js)

    rows = await db.execute(text(f"""
        {_SITES_SELECT}
        WHERE {where}
        ORDER BY c.name ASC, cp.created_at DESC
    """), params)

    buf = io.StringIO()
    fields = [
        "id", "company_name", "company_domain", "url", "page_type",
        "discovery_method", "discovery_confidence", "is_primary", "is_paginated",
        "requires_js_rendering", "is_active", "last_crawl_jobs", "expected_jobs",
        "has_template", "template_accuracy", "last_crawled_at", "created_at",
    ]
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()

    for r in rows:
        expected_jobs = _compute_expected_jobs(r.total_crawls, r.avg_last_3_jobs,
                                               r.last_crawl_jobs, r.imported_expected_jobs)
        writer.writerow({
            "id": str(r.id),
            "company_name": r.company_name,
            "company_domain": r.company_domain,
            "url": r.url,
            "page_type": r.page_type,
            "discovery_method": r.discovery_method,
            "discovery_confidence": r.discovery_confidence,
            "is_primary": r.is_primary,
            "is_paginated": r.is_paginated,
            "requires_js_rendering": r.requires_js_rendering,
            "is_active": r.is_active,
            "last_crawl_jobs": int(r.last_crawl_jobs) if r.last_crawl_jobs is not None else "",
            "expected_jobs": expected_jobs if expected_jobs is not None else "",
            "has_template": r.template_id is not None,
            "template_accuracy": round(float(r.template_accuracy), 2) if r.template_accuracy else "",
            "last_crawled_at": r.last_crawled_at.isoformat() if r.last_crawled_at else "",
            "created_at": r.created_at.isoformat() if r.created_at else "",
        })

    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=sites.csv"},
    )


# ── Detail ────────────────────────────────────────────────────────────────────

@router.get("/{page_id}/detail")
async def get_career_page_detail(page_id: UUID, db: AsyncSession = Depends(get_db)):
    """
    Full detail for a single career page — used by the Sites modal.
    Returns the page, its company, active template (with all selectors),
    and the 5 most recent crawl logs.
    """
    page = await db.get(CareerPage, page_id)
    if not page:
        raise HTTPException(status_code=404, detail="Career page not found")

    company = await db.get(Company, page.company_id)

    template = await db.scalar(
        select(SiteTemplate).where(
            SiteTemplate.career_page_id == page_id,
            SiteTemplate.is_active == True,
        )
    )

    log_rows = await db.execute(text("""
        SELECT status, started_at, completed_at, jobs_found, jobs_new,
               duration_seconds, error_message
          FROM crawl_logs
         WHERE company_id = :company_id
         ORDER BY started_at DESC NULLS LAST
         LIMIT 5
    """), {"company_id": str(page.company_id)})

    crawl_history = [
        {
            "status": r.status,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            "jobs_found": r.jobs_found,
            "jobs_new": r.jobs_new,
            "duration_seconds": r.duration_seconds,
            "error_message": r.error_message,
        }
        for r in log_rows
    ]

    return {
        "id": str(page.id),
        "company_id": str(page.company_id),
        "company_name": company.name if company else None,
        "company_domain": company.domain if company else None,
        "url": page.url,
        "page_type": page.page_type,
        "discovery_method": page.discovery_method,
        "discovery_confidence": page.discovery_confidence,
        "is_primary": page.is_primary,
        "is_paginated": page.is_paginated,
        "pagination_type": page.pagination_type,
        "pagination_selector": page.pagination_selector,
        "requires_js_rendering": page.requires_js_rendering,
        "last_crawled_at": page.last_crawled_at.isoformat() if page.last_crawled_at else None,
        "last_extraction_at": page.last_extraction_at.isoformat() if page.last_extraction_at else None,
        "is_active": page.is_active,
        "created_at": page.created_at.isoformat() if page.created_at else None,
        "template": {
            "id": str(template.id),
            "template_type": template.template_type,
            "selectors": template.selectors or {},
            "learned_via": template.learned_via,
            "accuracy_score": template.accuracy_score,
            "last_validated_at": template.last_validated_at.isoformat() if template.last_validated_at else None,
            "created_at": template.created_at.isoformat() if template.created_at else None,
        } if template else None,
        "crawl_history": crawl_history,
    }


# ── Actions ───────────────────────────────────────────────────────────────────

@router.get("/{page_id}")
async def get_career_page(page_id: UUID, db: AsyncSession = Depends(get_db)):
    page = await db.get(CareerPage, page_id)
    if not page:
        raise HTTPException(status_code=404, detail="Career page not found")
    return page


@router.post("/{page_id}/recrawl", status_code=202)
async def recrawl_page(page_id: UUID, db: AsyncSession = Depends(get_db)):
    page = await db.get(CareerPage, page_id)
    if not page:
        raise HTTPException(status_code=404, detail="Career page not found")
    from app.tasks.crawl_tasks import crawl_career_page
    task = crawl_career_page.delay(str(page_id))
    return {"task_id": task.id, "status": "queued"}


@router.post("/{page_id}/validate-template", status_code=202)
async def validate_template(page_id: UUID, db: AsyncSession = Depends(get_db)):
    page = await db.get(CareerPage, page_id)
    if not page:
        raise HTTPException(status_code=404, detail="Career page not found")
    from app.tasks.crawl_tasks import validate_page_template
    task = validate_page_template.delay(str(page_id))
    return {"task_id": task.id, "status": "queued"}
