"""Lead imports endpoints — file upload, validation, batch management, and analytics."""

import csv
import io
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query, UploadFile, File, HTTPException, BackgroundTasks
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import get_db
from app.models.lead_import import LeadImport
from app.models.lead_import_batch import LeadImportBatch

router = APIRouter()

UPLOAD_DIR = "/storage/uploads"
REQUIRED_COLUMNS = {"country_id", "advertiser_name", "origin"}
ALLOWED_COLUMNS = REQUIRED_COLUMNS | {
    "sample_linkout_url", "ad_origin_category",
    "cnt_ads_202504_202509", "origin_rank_by_ads_count", "sample_ad_url",
}

os.makedirs(UPLOAD_DIR, exist_ok=True)


def _validate_csv(content: bytes) -> dict:
    """Validate CSV structure and return summary. Returns errors list (empty = valid)."""
    errors = []
    try:
        text_content = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return {"valid": False, "errors": ["File is not valid UTF-8"], "total_rows": 0, "columns": []}

    reader = csv.DictReader(io.StringIO(text_content))
    columns = reader.fieldnames or []

    missing = REQUIRED_COLUMNS - set(columns)
    if missing:
        errors.append(f"Missing required columns: {', '.join(sorted(missing))}")

    rows = list(reader)
    total_rows = len(rows)

    if total_rows == 0:
        errors.append("File contains no data rows")

    if not errors:
        blank_name = sum(1 for r in rows if not (r.get("advertiser_name") or "").strip())
        blank_origin = sum(1 for r in rows if not (r.get("origin") or "").strip())
        if blank_name > total_rows * 0.5:
            errors.append(f"{blank_name} rows ({blank_name*100//total_rows}%) have no advertiser name")
        if blank_origin > total_rows * 0.5:
            errors.append(f"{blank_origin} rows ({blank_origin*100//total_rows}%) have no origin domain")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "total_rows": total_rows,
        "columns": list(columns),
    }


async def _run_batch_import(batch_id: str, csv_path: str, db_url: str):
    """Background task: import all rows from a CSV file into a batch."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool
    from app.crawlers.domain_blocklist import is_blocked
    from urllib.parse import urlparse

    engine = create_async_engine(db_url, poolclass=NullPool)
    SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    def extract_domain(url: str) -> str | None:
        if not url:
            return None
        url = url.strip()
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        try:
            return urlparse(url).netloc.lower().lstrip("www.") or None
        except Exception:
            return None

    def clean_url(url: str) -> str | None:
        if not url or not url.strip():
            return None
        url = url.strip()
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        return url

    COUNTRY_MAP = {"AU": "AU", "SG": "SG", "PH": "PH", "NZ": "NZ",
                   "MY": "MY", "ID": "ID", "TH": "TH", "HK": "HK"}

    counts = {"success": 0, "failed": 0, "skipped": 0, "blocked": 0, "excluded": 0}

    async with SessionLocal() as db:
        # Mark as importing
        await db.execute(text("""
            UPDATE lead_import_batches
            SET import_status = 'importing', import_started_at = now()
            WHERE id = :id
        """), {"id": batch_id})
        await db.commit()

        try:
            with open(csv_path, encoding="utf-8-sig") as f:
                rows = list(csv.DictReader(f))

            BATCH_SIZE = 500
            for i in range(0, len(rows), BATCH_SIZE):
                batch_rows = rows[i:i + BATCH_SIZE]
                for row in batch_rows:
                    country_id = (row.get("country_id") or "").strip().upper()
                    advertiser_name = (row.get("advertiser_name") or "").strip()
                    origin = (row.get("origin") or "").strip()
                    sample_url = clean_url(row.get("sample_linkout_url") or "")
                    category = (row.get("ad_origin_category") or "").strip()
                    try:
                        expected = int(row.get("cnt_ads_202504_202509") or 0) or None
                    except (ValueError, TypeError):
                        expected = None
                    try:
                        rank = int(row.get("origin_rank_by_ads_count") or 0) or None
                    except (ValueError, TypeError):
                        rank = None

                    if not origin or not advertiser_name:
                        counts["skipped"] += 1
                        continue

                    domain = extract_domain(sample_url) or extract_domain(origin)
                    if not domain:
                        counts["skipped"] += 1
                        continue

                    root_url = sample_url or f"https://{domain}"
                    market_code = COUNTRY_MAP.get(country_id, "AU")
                    lead_id = str(uuid.uuid4())

                    # Route disabled sites to excluded_sites table
                    disabled_val = (row.get("disabled_state") or "").strip().lower()
                    is_disabled = disabled_val in ("true", "1", "yes", "disabled")
                    if is_disabled:
                        await db.execute(text("""
                            INSERT INTO excluded_sites (id, domain, company_name, site_url,
                                site_type, country_code, expected_job_count, reason, source_file)
                            VALUES (:id, :domain, :company_name, :site_url,
                                :site_type, :country_code, :expected, 'disabled_state', :source_file)
                            ON CONFLICT (domain) DO NOTHING
                        """), {"id": str(uuid.uuid4()), "domain": domain,
                               "company_name": advertiser_name, "site_url": root_url,
                               "site_type": category or None, "country_code": country_id or None,
                               "expected": expected, "source_file": csv_path})
                        counts["excluded"] += 1
                        continue

                    if is_blocked(root_url):
                        await db.execute(text("""
                            INSERT INTO lead_imports (id, batch_id, country_id, advertiser_name,
                                origin_domain, sample_linkout_url, ad_origin_category,
                                expected_job_count, origin_rank, status, skip_reason)
                            VALUES (:id, :batch_id, :country_id, :name, :domain, :url,
                                :cat, :expected, :rank, 'blocked', 'Domain is in hard-block list')
                            ON CONFLICT DO NOTHING
                        """), {"id": lead_id, "batch_id": batch_id, "country_id": country_id,
                               "name": advertiser_name, "domain": domain, "url": root_url,
                               "cat": category, "expected": expected, "rank": rank})
                        counts["blocked"] += 1
                        continue

                    try:
                        company_result = await db.execute(text("""
                            INSERT INTO companies (id, name, domain, root_url, market_code,
                                discovered_via, crawl_priority, is_active)
                            VALUES (:id, :name, :domain, :root_url, :market_code, 'csv_import', 3, true)
                            ON CONFLICT (domain) DO UPDATE SET name = EXCLUDED.name, updated_at = now()
                            RETURNING id
                        """), {"id": str(uuid.uuid4()), "name": advertiser_name, "domain": domain,
                               "root_url": root_url, "market_code": market_code})
                        company_row = company_result.fetchone()
                        company_id = str(company_row[0]) if company_row else None

                        await db.execute(text("""
                            INSERT INTO lead_imports (id, batch_id, country_id, advertiser_name,
                                origin_domain, sample_linkout_url, ad_origin_category,
                                expected_job_count, origin_rank, status, company_id, processed_at)
                            VALUES (:id, :batch_id, :country_id, :name, :domain, :url,
                                :cat, :expected, :rank, 'success', :company_id, now())
                            ON CONFLICT DO NOTHING
                        """), {"id": lead_id, "batch_id": batch_id, "country_id": country_id,
                               "name": advertiser_name, "domain": domain, "url": root_url,
                               "cat": category, "expected": expected, "rank": rank,
                               "company_id": company_id})
                        counts["success"] += 1

                    except Exception as e:
                        try:
                            await db.execute(text("""
                                INSERT INTO lead_imports (id, batch_id, country_id, advertiser_name,
                                    origin_domain, sample_linkout_url, ad_origin_category,
                                    expected_job_count, origin_rank, status, error_message, processed_at)
                                VALUES (:id, :batch_id, :country_id, :name, :domain, :url,
                                    :cat, :expected, :rank, 'failed', :err, now())
                                ON CONFLICT DO NOTHING
                            """), {"id": lead_id, "batch_id": batch_id, "country_id": country_id,
                                   "name": advertiser_name, "domain": domain, "url": root_url,
                                   "cat": category, "expected": expected, "rank": rank,
                                   "err": str(e)[:500]})
                        except Exception:
                            pass
                        counts["failed"] += 1

                await db.commit()

            await db.execute(text("""
                UPDATE lead_import_batches
                SET import_status = 'completed', import_completed_at = now(),
                    imported_leads = :success, failed_leads = :failed,
                    blocked_leads = :blocked, skipped_leads = :skipped
                WHERE id = :id
            """), {**counts, "id": batch_id})
            await db.commit()

        except Exception as e:
            await db.execute(text("""
                UPDATE lead_import_batches
                SET import_status = 'failed', import_completed_at = now(), error_message = :err
                WHERE id = :id
            """), {"err": str(e)[:500], "id": batch_id})
            await db.commit()

    await engine.dispose()


# ── Batch endpoints ──────────────────────────────────────────────────────────

@router.post("/batches/upload", status_code=201)
async def upload_batch(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Upload a CSV file, validate it, and create an import batch record."""
    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted")

    content = await file.read()
    validation = _validate_csv(content)

    # Save file to disk
    batch_id = str(uuid.uuid4())
    stored_filename = f"{batch_id}.csv"
    file_path = os.path.join(UPLOAD_DIR, stored_filename)
    with open(file_path, "wb") as f:
        f.write(content)

    batch = LeadImportBatch(
        id=uuid.UUID(batch_id),
        filename=stored_filename,
        original_filename=file.filename,
        file_size_bytes=len(content),
        total_rows=validation["total_rows"],
        validation_status="valid" if validation["valid"] else "invalid",
        validation_errors=validation["errors"] if not validation["valid"] else None,
        import_status="pending",
    )
    db.add(batch)
    await db.commit()
    await db.refresh(batch)

    return {
        "id": str(batch.id),
        "original_filename": batch.original_filename,
        "total_rows": batch.total_rows,
        "validation_status": batch.validation_status,
        "validation_errors": batch.validation_errors,
        "import_status": batch.import_status,
        "created_at": batch.created_at.isoformat() if batch.created_at else None,
    }


@router.post("/batches/{batch_id}/import", status_code=202)
async def trigger_batch_import(
    batch_id: str,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Trigger a background import for a validated batch."""
    batch = await db.get(LeadImportBatch, uuid.UUID(batch_id))
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    if batch.validation_status != "valid":
        raise HTTPException(status_code=400, detail="Batch failed validation — cannot import")
    if batch.import_status in ("importing", "completed"):
        raise HTTPException(status_code=409, detail=f"Batch is already {batch.import_status}")

    from app.core.config import settings as app_settings
    csv_path = os.path.join(UPLOAD_DIR, batch.filename)
    background_tasks.add_task(_run_batch_import, batch_id, csv_path, app_settings.DATABASE_URL)
    return {"status": "importing", "batch_id": batch_id}


@router.get("/batches")
async def list_batches(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """List all import batches, newest first."""
    q = select(LeadImportBatch).order_by(LeadImportBatch.created_at.desc())
    total = await db.scalar(select(func.count()).select_from(q.subquery()))
    batches = await db.scalars(q.offset((page - 1) * page_size).limit(page_size))

    def _fmt(b: LeadImportBatch):
        return {
            "id": str(b.id),
            "original_filename": b.original_filename,
            "file_size_bytes": b.file_size_bytes,
            "total_rows": b.total_rows,
            "validation_status": b.validation_status,
            "validation_errors": b.validation_errors,
            "import_status": b.import_status,
            "imported_leads": b.imported_leads,
            "failed_leads": b.failed_leads,
            "blocked_leads": b.blocked_leads,
            "skipped_leads": b.skipped_leads,
            "created_at": b.created_at.isoformat() if b.created_at else None,
            "import_started_at": b.import_started_at.isoformat() if b.import_started_at else None,
            "import_completed_at": b.import_completed_at.isoformat() if b.import_completed_at else None,
            "error_message": b.error_message,
        }

    return {"items": [_fmt(b) for b in batches], "total": total, "page": page, "page_size": page_size}


@router.get("/batches/{batch_id}")
async def get_batch(batch_id: str, db: AsyncSession = Depends(get_db)):
    batch = await db.get(LeadImportBatch, uuid.UUID(batch_id))
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")

    # Aggregate stats for this batch's leads
    status_rows = await db.execute(
        select(LeadImport.status, func.count().label("n"))
        .where(LeadImport.batch_id == uuid.UUID(batch_id))
        .group_by(LeadImport.status)
    )
    by_status = {r.status: r.n for r in status_rows}

    country_rows = await db.execute(
        select(LeadImport.country_id, LeadImport.status, func.count().label("n"),
               func.sum(LeadImport.jobs_extracted).label("jobs"))
        .where(LeadImport.batch_id == uuid.UUID(batch_id))
        .group_by(LeadImport.country_id, LeadImport.status)
    )
    by_country: dict = {}
    for r in country_rows:
        if r.country_id not in by_country:
            by_country[r.country_id] = {"total": 0, "by_status": {}, "jobs_extracted": 0}
        by_country[r.country_id]["by_status"][r.status] = r.n
        by_country[r.country_id]["total"] += r.n
        by_country[r.country_id]["jobs_extracted"] += int(r.jobs or 0)

    cat_rows = await db.execute(
        select(LeadImport.ad_origin_category, func.count().label("n"),
               func.sum(LeadImport.jobs_extracted).label("jobs"))
        .where(LeadImport.batch_id == uuid.UUID(batch_id))
        .group_by(LeadImport.ad_origin_category)
        .order_by(func.count().desc())
        .limit(15)
    )
    top_categories = [
        {"category": r.ad_origin_category or "Unknown", "total": r.n, "jobs": int(r.jobs or 0)}
        for r in cat_rows
    ]

    total_leads = await db.scalar(
        select(func.count(LeadImport.id)).where(LeadImport.batch_id == uuid.UUID(batch_id))
    ) or 0

    return {
        "id": str(batch.id),
        "original_filename": batch.original_filename,
        "file_size_bytes": batch.file_size_bytes,
        "total_rows": batch.total_rows,
        "validation_status": batch.validation_status,
        "validation_errors": batch.validation_errors,
        "import_status": batch.import_status,
        "created_at": batch.created_at.isoformat() if batch.created_at else None,
        "import_started_at": batch.import_started_at.isoformat() if batch.import_started_at else None,
        "import_completed_at": batch.import_completed_at.isoformat() if batch.import_completed_at else None,
        "error_message": batch.error_message,
        "total_leads": total_leads,
        "by_status": by_status,
        "by_country": by_country,
        "top_categories": top_categories,
    }


@router.get("/batches/{batch_id}/leads")
async def list_batch_leads(
    batch_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: Optional[str] = None,
    country: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    q = select(LeadImport).where(LeadImport.batch_id == uuid.UUID(batch_id))
    if status:
        q = q.where(LeadImport.status == status)
    if country:
        q = q.where(LeadImport.country_id == country.upper())
    q = q.order_by(LeadImport.imported_at.desc())

    total = await db.scalar(select(func.count()).select_from(q.subquery()))
    rows = await db.scalars(q.offset((page - 1) * page_size).limit(page_size))

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [
            {
                "id": str(r.id),
                "country_id": r.country_id,
                "advertiser_name": r.advertiser_name,
                "origin_domain": r.origin_domain,
                "sample_linkout_url": r.sample_linkout_url,
                "ad_origin_category": r.ad_origin_category,
                "expected_job_count": r.expected_job_count,
                "status": r.status,
                "company_id": str(r.company_id) if r.company_id else None,
                "jobs_extracted": r.jobs_extracted,
                "error_message": r.error_message,
                "skip_reason": r.skip_reason,
                "imported_at": r.imported_at.isoformat() if r.imported_at else None,
            }
            for r in rows
        ],
    }


# ── Legacy summary + list endpoints (kept for backward compat) ───────────────

@router.get("/summary")
async def lead_import_summary(db: AsyncSession = Depends(get_db)):
    total = await db.scalar(select(func.count(LeadImport.id)))
    if not total:
        return {"total": 0, "by_status": {}, "by_country": {}, "by_category": {}}

    status_result = await db.execute(
        select(LeadImport.status, func.count().label("count")).group_by(LeadImport.status)
    )
    by_status = {r.status: r.count for r in status_result}

    country_result = await db.execute(
        select(LeadImport.country_id, LeadImport.status, func.count().label("count"),
               func.sum(LeadImport.jobs_extracted).label("total_jobs"),
               func.sum(LeadImport.career_pages_found).label("total_pages"))
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

    cat_result = await db.execute(
        select(LeadImport.ad_origin_category, LeadImport.status, func.count().label("count"))
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

    return {"total": total, "by_status": by_status, "by_country": by_country, "by_category": by_category}


@router.get("/")
async def list_lead_imports(
    db: AsyncSession = Depends(get_db),
    country: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    limit: int = Query(50, le=500),
    offset: int = Query(0),
):
    q = select(LeadImport)
    if country:
        q = q.where(LeadImport.country_id == country.upper())
    if status:
        q = q.where(LeadImport.status == status)
    if category:
        q = q.where(LeadImport.ad_origin_category == category)
    q = q.order_by(LeadImport.imported_at.desc()).limit(limit).offset(offset)

    rows = (await db.execute(q)).scalars().all()

    total_q = select(func.count(LeadImport.id))
    if country:
        total_q = total_q.where(LeadImport.country_id == country.upper())
    if status:
        total_q = total_q.where(LeadImport.status == status)
    if category:
        total_q = total_q.where(LeadImport.ad_origin_category == category)
    total = await db.scalar(total_q)

    return {
        "total": total, "limit": limit, "offset": offset,
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
