"""Company enrichment endpoints — CSV upload, Codex-backed batch runs, and CSV output."""

from __future__ import annotations

import csv
import io
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy import and_, func, or_, select, text, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import get_db
from app.models.company_enrichment_run import CompanyEnrichmentRun
from app.models.company_enrichment_row import CompanyEnrichmentRow
from app.services.company_enrichment_codex import CompanyEnrichmentCodexClient
from app.core.config import settings

router = APIRouter()

UPLOAD_DIR = "/storage/company_enrichment/uploads"
OUTPUT_DIR = "/storage/company_enrichment/outputs"
INPUT_COLUMNS = ["company", "country"]
OUTPUT_COLUMNS = ["company", "country", "job_page_url", "job_count", "comment"]

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


def _validate_csv(content: bytes) -> dict:
    try:
        text_content = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return {"valid": False, "errors": ["File is not valid UTF-8"], "total_rows": 0}

    reader = csv.DictReader(io.StringIO(text_content))
    columns = [c.strip() for c in (reader.fieldnames or [])]
    expected = set(INPUT_COLUMNS)
    missing = expected - set(columns)
    rows = list(reader)

    errors: list[str] = []
    if missing:
        errors.append(f"Missing required columns: {', '.join(sorted(missing))}")
    if not rows:
        errors.append("File contains no data rows")

    return {
        "valid": not errors,
        "errors": errors,
        "total_rows": len(rows),
        "columns": columns,
    }


def _build_output_csv(rows: list[CompanyEnrichmentRow]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(OUTPUT_COLUMNS)
    for row in rows:
        writer.writerow([
            row.company,
            row.country,
            row.job_page_url or "not found",
            row.job_count or "not found",
            row.comment or (row.error_message or "not found"),
        ])
    return buf.getvalue()


def _iso_or_none(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


async def _get_cached_results(
    db: AsyncSession,
    company_country_pairs: list[tuple[str, str]],
) -> dict[tuple[str, str], CompanyEnrichmentRow]:
    if not company_country_pairs:
        return {}

    cutoff = datetime.now(timezone.utc) - timedelta(hours=settings.COMPANY_ENRICHMENT_CACHE_TTL_HOURS)
    normalized_pairs = list({(company.strip().lower(), country.strip().lower()) for company, country in company_country_pairs if company and country})
    if not normalized_pairs:
        return {}

    cached_rows = list(await db.scalars(
        select(CompanyEnrichmentRow)
        .where(
            CompanyEnrichmentRow.status == "completed",
            CompanyEnrichmentRow.completed_at.is_not(None),
            CompanyEnrichmentRow.completed_at >= cutoff,
            tuple_(
                func.lower(func.trim(CompanyEnrichmentRow.company)),
                func.lower(func.trim(CompanyEnrichmentRow.country)),
            ).in_(normalized_pairs),
        )
        .order_by(
            func.lower(func.trim(CompanyEnrichmentRow.company)).asc(),
            func.lower(func.trim(CompanyEnrichmentRow.country)).asc(),
            CompanyEnrichmentRow.completed_at.desc(),
        )
    ))

    cache: dict[tuple[str, str], CompanyEnrichmentRow] = {}
    for row in cached_rows:
        key = (row.company.strip().lower(), row.country.strip().lower())
        cache.setdefault(key, row)
    return cache


def _build_run_metrics(
    run: CompanyEnrichmentRun,
    by_status: dict[str, int],
    *,
    active_workers: int,
    cached_rows: int,
    avg_completed_seconds: float | None,
) -> dict[str, object]:
    total_items = sum(by_status.values()) if by_status else (run.total_rows or 0)
    terminal = by_status.get("completed", 0) + by_status.get("failed", 0) + by_status.get("skipped", 0)
    progress_pct = round((terminal / total_items) * 100, 1) if total_items else 0.0

    now = datetime.now(timezone.utc)
    elapsed_seconds = None
    processing_rate_per_min = None
    eta_seconds = None
    if run.run_started_at:
        elapsed_seconds = max((now - run.run_started_at).total_seconds(), 0.0)
        completed = by_status.get("completed", 0)
        if elapsed_seconds > 0 and completed > 0:
            processing_rate_per_min = round((completed / elapsed_seconds) * 60, 2)
        if processing_rate_per_min and processing_rate_per_min > 0:
            remaining = max(total_items - terminal, 0)
            eta_seconds = int(round((remaining / processing_rate_per_min) * 60))

    return {
        "by_status": by_status,
        "progress_pct": progress_pct,
        "active_workers": active_workers or 0,
        "cached_rows": cached_rows,
        "avg_completed_seconds": round(avg_completed_seconds, 1) if avg_completed_seconds is not None else None,
        "elapsed_seconds": round(elapsed_seconds, 1) if elapsed_seconds is not None else None,
        "processing_rate_per_min": processing_rate_per_min,
        "eta_seconds": eta_seconds,
    }


async def _refresh_run_progress(db: AsyncSession, run_id: str) -> None:
    status_rows = await db.execute(
        select(CompanyEnrichmentRow.status, func.count().label("n"))
        .where(CompanyEnrichmentRow.run_id == uuid.UUID(run_id))
        .group_by(CompanyEnrichmentRow.status)
    )
    by_status = {row.status: row.n for row in status_rows}
    await db.execute(
        text("""
            UPDATE company_enrichment_runs
            SET completed_rows = :completed,
                failed_rows = :failed,
                skipped_rows = :skipped
            WHERE id = :id
        """),
        {
            "id": run_id,
            "completed": by_status.get("completed", 0),
            "failed": by_status.get("failed", 0),
            "skipped": by_status.get("skipped", 0),
        },
    )
    await db.commit()


async def _write_output_csv_for_run(db: AsyncSession, run_id: str) -> str:
    rows = list(await db.scalars(
        select(CompanyEnrichmentRow)
        .where(CompanyEnrichmentRow.run_id == uuid.UUID(run_id))
        .order_by(CompanyEnrichmentRow.row_number.asc())
    ))
    output_csv = _build_output_csv(rows)
    output_filename = f"{run_id}.csv"
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        f.write(output_csv)
    return output_filename


async def _reconcile_running_runs(db: AsyncSession, run_id: str | None = None) -> None:
    worker_alive = CompanyEnrichmentCodexClient().get_worker_health().get("alive", False)
    where_clause = "WHERE run_status = 'running'"
    params: dict[str, object] = {
        "row_stale_after": settings.COMPANY_ENRICHMENT_ROW_STALE_AFTER_SEC,
        "worker_stale_after": settings.COMPANY_ENRICHMENT_WORKER_STALE_AFTER_SEC,
    }
    if run_id:
        where_clause += " AND id = CAST(:run_id AS UUID)"
        params["run_id"] = run_id

    running_run_ids = [
        str(row[0]) for row in (
            await db.execute(text(f"SELECT id FROM company_enrichment_runs {where_clause}"), params)
        ).all()
    ]
    if not running_run_ids:
        return

    await db.execute(text("""
        UPDATE company_enrichment_rows
        SET status = 'failed',
            error_message = COALESCE(error_message, 'Processing timed out or worker stopped'),
            completed_at = now(),
            worker_id = NULL
        WHERE status = 'processing'
          AND started_at IS NOT NULL
          AND started_at < (now() - make_interval(secs => :row_stale_after))
    """), {"row_stale_after": settings.COMPANY_ENRICHMENT_ROW_STALE_AFTER_SEC})
    await db.commit()

    if not worker_alive:
        await db.execute(text(f"""
            UPDATE company_enrichment_runs
            SET error_message = 'Waiting for host enrichment worker heartbeat'
            {where_clause.replace('WHERE', 'WHERE')}
        """), params)
        await db.commit()
    else:
        await db.execute(text(f"""
            UPDATE company_enrichment_runs
            SET error_message = NULL
            {where_clause.replace('WHERE', 'WHERE')}
        """), params)
        await db.commit()

    for current_run_id in running_run_ids:
        status_rows = await db.execute(
            select(CompanyEnrichmentRow.status, func.count().label("n"))
            .where(CompanyEnrichmentRow.run_id == uuid.UUID(current_run_id))
            .group_by(CompanyEnrichmentRow.status)
        )
        by_status = {row.status: int(row.n) for row in status_rows}
        total = sum(by_status.values())
        pending = by_status.get("pending", 0)
        processing = by_status.get("processing", 0)
        terminal = by_status.get("completed", 0) + by_status.get("failed", 0) + by_status.get("skipped", 0)
        if total and pending == 0 and processing == 0 and terminal == total:
            output_filename = await _write_output_csv_for_run(db, current_run_id)
            run_status = "failed" if by_status.get("failed", 0) > 0 and by_status.get("completed", 0) == 0 else "completed"
            await db.execute(text("""
                UPDATE company_enrichment_runs
                SET run_status = :run_status,
                    run_completed_at = COALESCE(run_completed_at, now()),
                    output_filename = :output_filename
                WHERE id = :run_id
            """), {
                "run_id": current_run_id,
                "run_status": run_status,
                "output_filename": output_filename,
            })
            await db.commit()
        await _refresh_run_progress(db, current_run_id)


@router.get("/schema")
async def schema():
    return {"input_columns": INPUT_COLUMNS, "output_columns": OUTPUT_COLUMNS}


@router.get("/worker-health")
async def worker_health():
    return CompanyEnrichmentCodexClient().get_worker_health()


@router.post("/runs/upload", status_code=201)
async def upload_run(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted")

    content = await file.read()
    validation = _validate_csv(content)

    run_id = str(uuid.uuid4())
    stored_filename = f"{run_id}.csv"
    file_path = os.path.join(UPLOAD_DIR, stored_filename)
    with open(file_path, "wb") as f:
        f.write(content)

    run = CompanyEnrichmentRun(
        id=uuid.UUID(run_id),
        filename=stored_filename,
        original_filename=file.filename,
        file_size_bytes=len(content),
        total_rows=validation["total_rows"],
        validation_status="valid" if validation["valid"] else "invalid",
        validation_errors=validation["errors"] if not validation["valid"] else None,
        run_status="pending",
    )
    db.add(run)
    await db.commit()
    await db.refresh(run)

    return {
        "id": str(run.id),
        "original_filename": run.original_filename,
        "total_rows": run.total_rows,
        "validation_status": run.validation_status,
        "validation_errors": run.validation_errors,
        "run_status": run.run_status,
        "created_at": run.created_at.isoformat() if run.created_at else None,
    }


@router.post("/runs/{run_id}/start", status_code=202)
async def start_run(
    run_id: str,
    db: AsyncSession = Depends(get_db),
):
    run = await db.get(CompanyEnrichmentRun, uuid.UUID(run_id))
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.validation_status != "valid":
        raise HTTPException(status_code=400, detail="Run failed validation")
    if run.run_status in ("running", "completed"):
        raise HTTPException(status_code=409, detail=f"Run is already {run.run_status}")
    try:
        CompanyEnrichmentCodexClient().assert_worker_available()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    existing_rows = await db.scalar(
        select(func.count()).select_from(CompanyEnrichmentRow).where(CompanyEnrichmentRow.run_id == uuid.UUID(run_id))
    )
    if existing_rows:
        raise HTTPException(status_code=409, detail="Run rows already exist")

    csv_path = os.path.join(UPLOAD_DIR, run.filename)
    try:
        with open(csv_path, encoding="utf-8-sig") as f:
            source_rows = list(csv.DictReader(f))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Uploaded CSV is missing") from exc

    cache_pairs = [
        ((source.get("company") or "").strip(), (source.get("country") or "").strip())
        for source in source_rows
    ]
    cached_results = await _get_cached_results(db, cache_pairs)

    for idx, source in enumerate(source_rows, start=1):
        company = (source.get("company") or "").strip()
        country = (source.get("country") or "").strip()
        cache_key = (company.lower(), country.lower()) if company and country else None
        cached_row = cached_results.get(cache_key) if cache_key else None
        row = CompanyEnrichmentRow(
            run_id=uuid.UUID(run_id),
            row_number=idx,
            company=company,
            country=country,
            status=(
                "skipped" if not company or not country
                else "completed" if cached_row
                else "pending"
            ),
            job_page_url=cached_row.job_page_url if cached_row else None,
            job_count=cached_row.job_count if cached_row else None,
            comment=cached_row.comment if cached_row else None,
            raw_response_text="CACHE_HIT" if cached_row else None,
            raw_response_json=(
                {
                    "cache_hit": True,
                    "source_run_id": str(cached_row.run_id),
                    "source_row_id": str(cached_row.id),
                    "cached_completed_at": _iso_or_none(cached_row.completed_at),
                }
                if cached_row else None
            ),
            started_at=datetime.now(timezone.utc) if cached_row else None,
            completed_at=datetime.now(timezone.utc) if cached_row else None,
            error_message="Missing company or country" if not company or not country else None,
        )
        db.add(row)

    await db.execute(text("""
        UPDATE company_enrichment_runs
        SET run_status = 'running',
            run_started_at = now(),
            run_completed_at = NULL,
            output_filename = NULL,
            error_message = NULL,
            completed_rows = 0,
            failed_rows = 0,
            skipped_rows = 0
        WHERE id = :id
    """), {"id": run_id})
    await db.commit()
    await _refresh_run_progress(db, run_id)
    return {"status": "running", "run_id": run_id}


@router.get("/runs")
async def list_runs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    await _reconcile_running_runs(db)
    q = select(CompanyEnrichmentRun).order_by(CompanyEnrichmentRun.created_at.desc())
    total = await db.scalar(select(func.count()).select_from(q.subquery()))
    runs = await db.scalars(q.offset((page - 1) * page_size).limit(page_size))
    items = []
    for run in runs:
        cached_rows = await db.scalar(
            select(func.count())
            .select_from(CompanyEnrichmentRow)
            .where(
                CompanyEnrichmentRow.run_id == run.id,
                CompanyEnrichmentRow.raw_response_json.is_not(None),
                CompanyEnrichmentRow.raw_response_json["cache_hit"].as_boolean() == True,
            )
        ) or 0
        items.append({
            "id": str(run.id),
            "original_filename": run.original_filename,
            "total_rows": run.total_rows,
            "validation_status": run.validation_status,
            "validation_errors": run.validation_errors,
            "run_status": run.run_status,
            "completed_rows": run.completed_rows,
            "failed_rows": run.failed_rows,
            "skipped_rows": run.skipped_rows,
            "created_at": run.created_at.isoformat() if run.created_at else None,
            "run_started_at": run.run_started_at.isoformat() if run.run_started_at else None,
            "run_completed_at": run.run_completed_at.isoformat() if run.run_completed_at else None,
            "error_message": run.error_message,
            "download_ready": bool(run.output_filename),
            "cached_rows": int(cached_rows),
        })
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.get("/runs/{run_id}")
async def get_run(run_id: str, db: AsyncSession = Depends(get_db)):
    await _reconcile_running_runs(db, run_id)
    run = await db.get(CompanyEnrichmentRun, uuid.UUID(run_id))
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    status_rows = await db.execute(
        select(CompanyEnrichmentRow.status, func.count().label("n"))
        .where(CompanyEnrichmentRow.run_id == uuid.UUID(run_id))
        .group_by(CompanyEnrichmentRow.status)
    )
    by_status = {row.status: row.n for row in status_rows}
    active_workers = await db.scalar(
        select(func.count(func.distinct(CompanyEnrichmentRow.worker_id)))
        .where(
            CompanyEnrichmentRow.run_id == uuid.UUID(run_id),
            CompanyEnrichmentRow.status == "processing",
            CompanyEnrichmentRow.worker_id.is_not(None),
        )
    )
    cached_rows = await db.scalar(
        select(func.count())
        .select_from(CompanyEnrichmentRow)
        .where(
            CompanyEnrichmentRow.run_id == uuid.UUID(run_id),
            CompanyEnrichmentRow.raw_response_json.is_not(None),
            CompanyEnrichmentRow.raw_response_json["cache_hit"].as_boolean() == True,
        )
    ) or 0
    avg_completed_seconds = await db.scalar(
        select(func.avg(func.extract("epoch", CompanyEnrichmentRow.completed_at - CompanyEnrichmentRow.started_at)))
        .where(
            CompanyEnrichmentRow.run_id == uuid.UUID(run_id),
            CompanyEnrichmentRow.status == "completed",
            CompanyEnrichmentRow.started_at.is_not(None),
            CompanyEnrichmentRow.completed_at.is_not(None),
        )
    )
    metrics = _build_run_metrics(
        run,
        {k: int(v) for k, v in by_status.items()},
        active_workers=int(active_workers or 0),
        cached_rows=int(cached_rows),
        avg_completed_seconds=float(avg_completed_seconds) if avg_completed_seconds is not None else None,
    )

    return {
        "id": str(run.id),
        "original_filename": run.original_filename,
        "total_rows": run.total_rows,
        "validation_status": run.validation_status,
        "validation_errors": run.validation_errors,
        "run_status": run.run_status,
        "completed_rows": run.completed_rows,
        "failed_rows": run.failed_rows,
        "skipped_rows": run.skipped_rows,
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "run_started_at": run.run_started_at.isoformat() if run.run_started_at else None,
        "run_completed_at": run.run_completed_at.isoformat() if run.run_completed_at else None,
        "error_message": run.error_message,
        **metrics,
        "download_url": f"/api/v1/company-enrichment/runs/{run_id}/download" if run.output_filename else None,
    }


@router.get("/runs/{run_id}/rows")
async def list_run_rows(
    run_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    await _reconcile_running_runs(db, run_id)
    q = select(CompanyEnrichmentRow).where(CompanyEnrichmentRow.run_id == uuid.UUID(run_id))
    if status:
        q = q.where(CompanyEnrichmentRow.status == status)
    q = q.order_by(CompanyEnrichmentRow.row_number.asc())
    total = await db.scalar(select(func.count()).select_from(q.subquery()))
    rows = await db.scalars(q.offset((page - 1) * page_size).limit(page_size))
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [
            {
                "id": str(row.id),
                "row_number": row.row_number,
                "company": row.company,
                "country": row.country,
                "status": row.status,
                "job_page_url": row.job_page_url,
                "job_count": row.job_count,
                "comment": row.comment,
                "error_message": row.error_message,
                "attempt_count": row.attempt_count,
                "worker_id": row.worker_id,
                "started_at": row.started_at.isoformat() if row.started_at else None,
                "completed_at": row.completed_at.isoformat() if row.completed_at else None,
            }
            for row in rows
        ],
    }


@router.get("/runs/{run_id}/download")
async def download_run_output(run_id: str, db: AsyncSession = Depends(get_db)):
    run = await db.get(CompanyEnrichmentRun, uuid.UUID(run_id))
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if not run.output_filename:
        raise HTTPException(status_code=409, detail="Output CSV is not ready yet")

    output_path = os.path.join(OUTPUT_DIR, run.output_filename)
    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="Output file missing")

    return FileResponse(
        output_path,
        media_type="text/csv",
        filename=f"company_enrichment_{run_id}.csv",
    )
