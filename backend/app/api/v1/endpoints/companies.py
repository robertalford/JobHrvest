"""Company endpoints."""

import csv
import io
import json
from typing import Optional
from uuid import UUID

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query, Response, UploadFile, File
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.base import get_db
from app.models.company import Company
from app.schemas.company import CompanyCreate, CompanyRead, CompanyUpdate, CompanyList

router = APIRouter()

_redis: aioredis.Redis | None = None

async def _get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis

_LIST_TTL = 30  # seconds — short enough to reflect writes, long enough to absorb spikes


async def _cache_get(key: str) -> dict | None:
    try:
        r = await _get_redis()
        raw = await r.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None  # Redis unavailable — fall through to DB


async def _cache_set(key: str, value: dict, ttl: int = _LIST_TTL) -> None:
    try:
        r = await _get_redis()
        await r.set(key, json.dumps(value), ex=ttl)
    except Exception:
        pass  # Redis unavailable — not a hard failure


async def _cache_invalidate_companies() -> None:
    """Delete all cached companies list pages (called on writes)."""
    try:
        r = await _get_redis()
        keys = await r.keys("companies:list:*")
        if keys:
            await r.delete(*keys)
    except Exception:
        pass


@router.get("")
async def list_companies(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = None,
    ats_platform: Optional[str] = None,
    is_active: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
):
    from sqlalchemy import text

    cache_key = f"companies:list:{page}:{page_size}:{search}:{ats_platform}:{is_active}"
    cached = await _cache_get(cache_key)
    if cached is not None:
        return cached

    search_pat = f"%{search}%" if search else None
    is_filtered = bool(search_pat or ats_platform or is_active is not None)

    rows = await db.execute(text("""
        SELECT
            c.id, c.name, c.domain, c.root_url, c.market_code,
            c.ats_platform, c.ats_confidence, c.crawl_priority,
            c.crawl_frequency_hours, c.last_crawl_at, c.next_crawl_at,
            c.is_active, c.requires_js_rendering, c.anti_bot_level,
            c.discovered_via, c.notes, c.created_at, c.updated_at,
            COALESCE(cs.active_site_count, 0) AS site_count,
            COALESCE(cs.sites_json, '[]'::jsonb) AS sites,
            cs.last_crawl_jobs,
            cs.expected_jobs
        FROM companies c
        LEFT JOIN company_stats cs ON cs.company_id = c.id
        WHERE (CAST(:search AS TEXT) IS NULL OR c.name ILIKE :search OR c.domain ILIKE :search)
          AND (CAST(:ats_platform AS TEXT) IS NULL OR c.ats_platform = :ats_platform)
          AND (CAST(:is_active AS BOOLEAN) IS NULL OR c.is_active = :is_active)
        ORDER BY c.name ASC
        LIMIT :limit OFFSET :offset
    """), {
        "search": search_pat,
        "ats_platform": ats_platform,
        "is_active": is_active,
        "limit": page_size,
        "offset": (page - 1) * page_size,
    })

    if not is_filtered:
        # Fast path: Postgres statistics for unfiltered total (< 1ms vs 264ms COUNT).
        # Accurate within autovacuum cycle; good enough for pagination display.
        total_row = await db.execute(text(
            "SELECT reltuples::bigint FROM pg_class WHERE relname = 'companies'"
        ))
    else:
        total_row = await db.execute(text("""
            SELECT COUNT(*)
              FROM companies c
             WHERE (CAST(:search AS TEXT) IS NULL OR c.name ILIKE :search OR c.domain ILIKE :search)
               AND (CAST(:ats_platform AS TEXT) IS NULL OR c.ats_platform = :ats_platform)
               AND (CAST(:is_active AS BOOLEAN) IS NULL OR c.is_active = :is_active)
        """), {"search": search_pat, "ats_platform": ats_platform, "is_active": is_active})
    total = total_row.scalar() or 0

    items = []
    for r in rows:
        items.append({
            "id": str(r.id),
            "name": r.name,
            "domain": r.domain,
            "root_url": r.root_url,
            "market_code": r.market_code,
            "ats_platform": r.ats_platform,
            "ats_confidence": r.ats_confidence,
            "crawl_priority": r.crawl_priority,
            "crawl_frequency_hours": r.crawl_frequency_hours,
            "last_crawl_at": r.last_crawl_at.isoformat() if r.last_crawl_at else None,
            "next_crawl_at": r.next_crawl_at.isoformat() if r.next_crawl_at else None,
            "is_active": r.is_active,
            "requires_js_rendering": r.requires_js_rendering,
            "anti_bot_level": r.anti_bot_level,
            "discovered_via": r.discovered_via,
            "notes": r.notes,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "updated_at": r.updated_at.isoformat() if r.updated_at else None,
            "sites": r.sites if isinstance(r.sites, list) else [],
            "site_count": int(r.site_count or 0),
            "last_crawl_jobs": int(r.last_crawl_jobs) if r.last_crawl_jobs is not None else None,
            "expected_jobs": int(r.expected_jobs) if r.expected_jobs is not None else None,
        })

    result = {"items": items, "total": total, "page": page, "page_size": page_size}
    await _cache_set(cache_key, result)
    return result


@router.get("/export")
async def export_companies(
    format: str = Query("csv"),
    search: Optional[str] = None,
    ats_platform: Optional[str] = None,
    is_active: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
):
    from sqlalchemy import text

    search_pat = f"%{search}%" if search else None

    rows = await db.execute(text("""
        SELECT
            c.id, c.name, c.domain, c.market_code, c.ats_platform,
            c.crawl_priority, c.last_crawl_at, c.is_active, c.discovered_via,
            COALESCE(cs.active_site_count, 0) AS site_count,
            cs.last_crawl_jobs,
            cs.expected_jobs

        FROM companies c
        LEFT JOIN company_stats cs ON cs.company_id = c.id
        WHERE (CAST(:search AS TEXT) IS NULL OR c.name ILIKE :search OR c.domain ILIKE :search)
          AND (CAST(:ats_platform AS TEXT) IS NULL OR c.ats_platform = :ats_platform)
          AND (CAST(:is_active AS BOOLEAN) IS NULL OR c.is_active = :is_active)
        ORDER BY c.name ASC
    """), {"search": search_pat, "ats_platform": ats_platform, "is_active": is_active})

    buf = io.StringIO()
    fields = [
        "id", "name", "domain", "market_code", "ats_platform", "crawl_priority",
        "site_count", "expected_jobs", "last_crawl_jobs", "is_active",
        "discovered_via", "last_crawl_at",
    ]
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()

    for r in rows:
        writer.writerow({
            "id": str(r.id),
            "name": r.name,
            "domain": r.domain,
            "market_code": r.market_code or "",
            "ats_platform": r.ats_platform or "",
            "crawl_priority": r.crawl_priority,
            "site_count": int(r.site_count or 0),
            "expected_jobs": int(r.expected_jobs) if r.expected_jobs is not None else "",
            "last_crawl_jobs": int(r.last_crawl_jobs) if r.last_crawl_jobs is not None else "",
            "is_active": r.is_active,
            "discovered_via": r.discovered_via or "",
            "last_crawl_at": r.last_crawl_at.isoformat() if r.last_crawl_at else "",
        })

    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=companies.csv"},
    )


@router.post("", response_model=CompanyRead, status_code=201)
async def create_company(payload: CompanyCreate, db: AsyncSession = Depends(get_db)):
    from urllib.parse import urlparse
    domain = urlparse(str(payload.root_url)).netloc.lstrip("www.")
    existing = await db.scalar(select(Company).where(Company.domain == domain))
    if existing:
        raise HTTPException(status_code=409, detail="Company with this domain already exists")
    company = Company(
        name=payload.name,
        domain=domain,
        root_url=str(payload.root_url),
        market_code=payload.market_code,
        crawl_priority=payload.crawl_priority,
        notes=payload.notes,
        discovered_via="manual",
    )
    db.add(company)
    await db.commit()
    await db.refresh(company)
    await _cache_invalidate_companies()
    # Auto-enqueue: new company always needs site config extraction
    from app.services import queue_manager
    await queue_manager.enqueue(db, "company_config", company.id, added_by="api_create")
    await db.commit()
    return company


@router.post("/bulk", status_code=202)
async def bulk_import_companies(file: UploadFile = File(...), db: AsyncSession = Depends(get_db)):
    content = await file.read()
    reader = csv.DictReader(io.StringIO(content.decode("utf-8")))
    imported, skipped = 0, 0
    for row in reader:
        url = row.get("url") or row.get("root_url", "")
        name = row.get("name", url)
        if not url:
            skipped += 1
            continue
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lstrip("www.")
        existing = await db.scalar(select(Company).where(Company.domain == domain))
        if existing:
            skipped += 1
            continue
        company = Company(name=name, domain=domain, root_url=url, discovered_via="bulk_import")
        db.add(company)
        imported += 1
    await db.commit()
    await _cache_invalidate_companies()
    # Auto-enqueue all newly imported companies for site config extraction
    if imported > 0:
        from app.services import queue_manager
        from sqlalchemy import select as _select
        new_companies = await db.scalars(
            _select(Company).where(Company.discovered_via == "bulk_import", Company.company_status.is_(None))
        )
        for c in new_companies:
            await queue_manager.enqueue(db, "company_config", c.id, added_by="bulk_import")
        await db.commit()
    return {"imported": imported, "skipped": skipped}


@router.get("/{company_id}", response_model=CompanyRead)
async def get_company(company_id: UUID, db: AsyncSession = Depends(get_db)):
    company = await db.get(Company, company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    return company


@router.patch("/{company_id}", response_model=CompanyRead)
async def update_company(company_id: UUID, payload: CompanyUpdate, db: AsyncSession = Depends(get_db)):
    company = await db.get(Company, company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    update_data = payload.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(company, field, value)
    await db.commit()
    await db.refresh(company)
    await _cache_invalidate_companies()
    # Auto-enqueue: if status changed to non-ok, re-run company config extraction
    if company.company_status and company.company_status != "ok":
        from app.services import queue_manager
        await queue_manager.enqueue(db, "company_config", company.id, added_by="api_status_change")
        await db.commit()
    return company


@router.post("/{company_id}/crawl", status_code=202)
async def trigger_crawl(company_id: UUID, db: AsyncSession = Depends(get_db)):
    company = await db.get(Company, company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    # Enqueue Celery task
    from app.tasks.crawl_tasks import crawl_company
    task = crawl_company.delay(str(company_id))
    return {"task_id": task.id, "status": "queued"}


@router.get("/{company_id}/jobs")
async def get_company_jobs(company_id: UUID, db: AsyncSession = Depends(get_db)):
    from app.models.job import Job
    jobs = await db.scalars(select(Job).where(Job.company_id == company_id, Job.is_active == True))
    return list(jobs)


@router.get("/{company_id}/crawl-history")
async def get_company_crawl_history(company_id: UUID, db: AsyncSession = Depends(get_db)):
    from app.models.crawl_log import CrawlLog
    logs = await db.scalars(
        select(CrawlLog).where(CrawlLog.company_id == company_id).order_by(CrawlLog.started_at.desc()).limit(50)
    )
    return list(logs)
