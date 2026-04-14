"""Crawl management endpoints."""

from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

AEST = ZoneInfo("Australia/Sydney")
from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import get_db
from app.models.crawl_log import CrawlLog
from app.models.company import Company

router = APIRouter()


@router.get("/stats")
async def get_crawl_stats(db: AsyncSession = Depends(get_db)):
    """Real-time crawl progress stats — DB-backed for accuracy."""
    today_start = datetime.now(AEST).replace(hour=0, minute=0, second=0, microsecond=0)

    running = await db.scalar(select(func.count(CrawlLog.id)).where(CrawlLog.status == "running"))
    completed_today = await db.scalar(
        select(func.count(CrawlLog.id)).where(
            CrawlLog.status == "success",
            CrawlLog.started_at >= today_start,
        )
    )
    failed_today = await db.scalar(
        select(func.count(CrawlLog.id)).where(
            CrawlLog.status == "failed",
            CrawlLog.started_at >= today_start,
        )
    )
    jobs_today = await db.scalar(
        select(func.sum(CrawlLog.jobs_found)).where(
            CrawlLog.status == "success",
            CrawlLog.started_at >= today_start,
        )
    ) or 0
    new_jobs_today = await db.scalar(
        select(func.sum(CrawlLog.jobs_new)).where(
            CrawlLog.status == "success",
            CrawlLog.started_at >= today_start,
        )
    ) or 0

    # Total companies due for crawl (queued in Redis OR not yet started)
    total_due = await db.scalar(
        select(func.count(Company.id)).where(
            Company.is_active == True,
            (Company.next_crawl_at <= datetime.now(timezone.utc)) | (Company.next_crawl_at.is_(None)),
        )
    ) or 0

    # Redis queue depth (tasks immediately available to workers)
    try:
        from app.core.config import settings
        import redis as redis_lib
        r = redis_lib.from_url(settings.CELERY_BROKER_URL)
        queue_depth = r.llen("crawl") + r.llen("default")
        r.close()
    except Exception:
        queue_depth = 0

    processed_today = int(completed_today or 0) + int(failed_today or 0) + int(running or 0)
    remaining = max(0, total_due - processed_today)

    # Speed metrics: rolling 60-minute window of completed crawls
    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    pages_last_hour = await db.scalar(
        select(func.sum(CrawlLog.pages_crawled)).where(
            CrawlLog.status == "success",
            CrawlLog.completed_at >= one_hour_ago,
        )
    ) or 0
    sites_last_hour = await db.scalar(
        select(func.count(CrawlLog.id)).where(
            CrawlLog.status == "success",
            CrawlLog.completed_at >= one_hour_ago,
        )
    ) or 0
    ops_per_minute = round(int(pages_last_hour) / 60, 2)
    avg_pages_per_site = round(int(pages_last_hour) / max(1, int(sites_last_hour)), 2)
    sites_per_minute = round(int(sites_last_hour) / 60, 3)

    # Quality funnel — all counts use canonical jobs only to match the dashboard
    from app.models.job import Job
    from sqlalchemy import text as sa_text
    funnel = await db.execute(sa_text("""
        SELECT
            COUNT(*)                                                          AS total_jobs,
            COUNT(*) FILTER (WHERE is_canonical)                              AS canonical_jobs,
            COUNT(*) FILTER (WHERE is_canonical AND quality_score IS NOT NULL) AS jobs_scored,
            COUNT(*) FILTER (WHERE is_canonical AND quality_score >= 40)      AS jobs_passing
        FROM jobs WHERE is_active = true
    """))
    frow = funnel.one()
    total_jobs      = int(frow.total_jobs)
    canonical_jobs  = int(frow.canonical_jobs)
    jobs_scored     = int(frow.jobs_scored)
    jobs_passing    = int(frow.jobs_passing)
    quality_pass_rate = round((jobs_passing / max(1, jobs_scored)) * 100, 1) if jobs_scored > 0 else None
    avg_jobs_per_site = round(int(jobs_today) / max(1, int(completed_today or 1)), 2) if completed_today else 0

    return {
        "running": int(running or 0),
        # redis_queue_depth is raw Celery task count (can exceed total_due if cycles overlap)
        "redis_queue_depth": queue_depth,
        # queued = alias for the frontend card — show companies due, not raw Redis depth
        "queued": total_due,
        "remaining": remaining,
        "total_due": total_due,
        "completed_today": int(completed_today or 0),
        "failed_today": int(failed_today or 0),
        "jobs_found_today": int(jobs_today),
        "new_jobs_today": int(new_jobs_today),
        # Speed stats (rolling 60-min window)
        "ops_per_minute": ops_per_minute,
        "sites_per_minute": sites_per_minute,
        "avg_pages_per_site": avg_pages_per_site,
        "sites_last_hour": int(sites_last_hour),
        # Quality funnel — canonical jobs only (matches dashboard "Unique Active Jobs")
        "total_jobs": canonical_jobs,       # unique jobs (post-dedup)
        "total_jobs_raw": total_jobs,       # includes duplicates
        "jobs_scored": jobs_scored,
        "jobs_passing_quality": jobs_passing,
        "quality_pass_rate": quality_pass_rate,
        "avg_jobs_per_site": avg_jobs_per_site,
    }


@router.get("/active")
async def get_active_crawls(db: AsyncSession = Depends(get_db)):
    from sqlalchemy.orm import joinedload
    logs = await db.scalars(
        select(CrawlLog)
        .options(joinedload(CrawlLog.company))
        .where(CrawlLog.status == "running")
        .order_by(CrawlLog.started_at.desc())
    )
    result = []
    for log in logs:
        result.append({
            "id": str(log.id),
            "crawl_type": log.crawl_type,
            "company_id": str(log.company_id) if log.company_id else None,
            "company_name": log.company.name if log.company else None,
            "company_domain": log.company.domain if log.company else None,
            "status": log.status,
            "started_at": log.started_at.isoformat() if log.started_at else None,
            "pages_crawled": log.pages_crawled,
            "jobs_found": log.jobs_found,
        })
    return result


@router.get("/history")
async def get_crawl_history(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: str = Query(None),
    crawl_type: str = Query(None),
    db: AsyncSession = Depends(get_db),
):
    from sqlalchemy.orm import joinedload
    q = (
        select(CrawlLog)
        .options(joinedload(CrawlLog.company))
        .order_by(CrawlLog.started_at.desc())
    )
    if status:
        q = q.where(CrawlLog.status == status)
    if crawl_type:
        q = q.where(CrawlLog.crawl_type == crawl_type)

    total = await db.scalar(select(func.count()).select_from(q.subquery()))
    logs = await db.scalars(q.offset((page - 1) * page_size).limit(page_size))

    result = []
    for log in logs:
        result.append({
            "id": str(log.id),
            "crawl_type": log.crawl_type,
            "company_id": str(log.company_id) if log.company_id else None,
            "company_name": log.company.name if log.company else None,
            "company_domain": log.company.domain if log.company else None,
            "status": log.status,
            "started_at": log.started_at.isoformat() if log.started_at else None,
            "completed_at": log.completed_at.isoformat() if log.completed_at else None,
            "duration_seconds": log.duration_seconds,
            "pages_crawled": log.pages_crawled,
            "jobs_found": log.jobs_found,
            "jobs_new": log.jobs_new,
            "error_message": log.error_message,
        })
    return {"items": result, "total": total, "page": page, "page_size": page_size}


@router.post("/trigger-full", status_code=202)
async def trigger_full_crawl(db: AsyncSession = Depends(get_db)):
    from app.tasks.crawl_tasks import full_crawl_cycle
    task = full_crawl_cycle.delay()
    return {"task_id": task.id, "status": "queued"}


@router.post("/trigger-harvest", status_code=202)
async def trigger_aggregator_harvest():
    """Trigger the Indeed AU aggregator harvester to discover new company career pages."""
    from app.tasks.crawl_tasks import harvest_aggregators
    task = harvest_aggregators.delay()
    return {"task_id": task.id, "status": "queued"}


@router.get("/queue-stats/")
@router.get("/queue-stats")
async def get_queue_stats(
    from_dt: Optional[str] = Query(None, description="ISO datetime filter start"),
    to_dt: Optional[str] = Query(None, description="ISO datetime filter end"),
    db: AsyncSession = Depends(get_db),
):
    """Return current queue depths by type and status, optionally filtered by date range."""
    from app.services import queue_manager
    f = datetime.fromisoformat(from_dt) if from_dt else None
    t = datetime.fromisoformat(to_dt) if to_dt else None
    return await queue_manager.get_stats(db, from_dt=f, to_dt=t)


@router.post("/trigger/{run_type}/", status_code=202)
@router.post("/trigger/{run_type}", status_code=202)
async def trigger_run(run_type: str):
    """Trigger one of the 4 run types immediately — populates queue then drains."""
    from app.tasks.crawl_tasks import (
        populate_queues,
        drain_company_config, drain_site_config,
        drain_job_crawling, drain_discovery,
    )
    valid_types = {"job_crawling", "discovery", "company_config", "site_config"}
    if run_type not in valid_types:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"Unknown run type: {run_type}")

    # Populate queues first, then fire the matching drain immediately
    pop_task = populate_queues.delay()

    drain_map = {
        "company_config": drain_company_config,
        "site_config": drain_site_config,
        "job_crawling": drain_job_crawling,
        "discovery": drain_discovery,
    }
    drain_task = drain_map[run_type].delay()
    return {"task_id": drain_task.id, "populate_task_id": pop_task.id, "run_type": run_type, "status": "queued"}


_SCHEDULE_REDIS_KEY = "jobharvest:schedule_settings"
_SCHEDULE_DEFAULTS = {
    "discovery":       {"enabled": True, "interval_hours": 2},
    "company_config":  {"enabled": True, "interval_hours": 2},
    "site_config":     {"enabled": True, "interval_hours": 2},
    "job_crawling":    {"enabled": True, "interval_hours": 2},
}


def _get_redis():
    import redis as redis_lib
    from app.core.config import settings
    return redis_lib.from_url(settings.CELERY_BROKER_URL, decode_responses=True)


@router.get("/schedule-settings/")
@router.get("/schedule-settings")
async def get_schedule_settings():
    """Return current schedule settings (stored in Redis)."""
    import json
    try:
        r = _get_redis()
        raw = r.get(_SCHEDULE_REDIS_KEY)
        r.close()
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return _SCHEDULE_DEFAULTS


@router.put("/schedule-settings/")
@router.put("/schedule-settings")
async def update_schedule_settings(body: dict):
    """Persist schedule settings to Redis. Body keys: discovery, company_config, site_config, job_crawling."""
    import json
    valid_keys = set(_SCHEDULE_DEFAULTS.keys())
    settings_out = dict(_SCHEDULE_DEFAULTS)
    for key in valid_keys:
        if key in body and isinstance(body[key], dict):
            entry = body[key]
            settings_out[key] = {
                "enabled": bool(entry.get("enabled", True)),
                "interval_hours": max(1, int(entry.get("interval_hours", 2))),
            }
    try:
        r = _get_redis()
        r.set(_SCHEDULE_REDIS_KEY, json.dumps(settings_out))
        r.close()
    except Exception as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"Redis write failed: {e}")
    return settings_out


_PAUSE_REDIS_KEY = "jobharvest:queue_paused"
_VALID_QUEUE_TYPES = {"discovery", "company_config", "site_config", "job_crawling"}


@router.get("/queue/pause-state")
async def get_pause_state():
    """Return which queues are paused."""
    import json
    result = {qt: False for qt in _VALID_QUEUE_TYPES}
    try:
        r = _get_redis()
        raw = r.hgetall(_PAUSE_REDIS_KEY)
        r.close()
        for qt in _VALID_QUEUE_TYPES:
            result[qt] = raw.get(qt) == "1"
    except Exception:
        pass
    return result


@router.post("/queue/pause/{queue_type}")
async def pause_queue(queue_type: str):
    """Pause a specific queue type."""
    if queue_type not in _VALID_QUEUE_TYPES:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"Invalid queue type: {queue_type}. Valid: {', '.join(sorted(_VALID_QUEUE_TYPES))}")
    try:
        r = _get_redis()
        r.hset(_PAUSE_REDIS_KEY, queue_type, "1")
        r.close()
    except Exception as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"Redis write failed: {e}")
    return {"queue_type": queue_type, "paused": True}


@router.post("/queue/resume/{queue_type}")
async def resume_queue(queue_type: str):
    """Resume a specific queue type."""
    if queue_type not in _VALID_QUEUE_TYPES:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"Invalid queue type: {queue_type}. Valid: {', '.join(sorted(_VALID_QUEUE_TYPES))}")
    try:
        r = _get_redis()
        r.hset(_PAUSE_REDIS_KEY, queue_type, "0")
        r.close()
    except Exception as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"Redis write failed: {e}")
    return {"queue_type": queue_type, "paused": False}


@router.delete("/cancel/{task_id}", status_code=200)
async def cancel_task(task_id: str):
    from app.tasks.celery_app import celery_app
    celery_app.control.revoke(task_id, terminate=True)
    return {"task_id": task_id, "status": "cancelled"}


@router.get("/worker-stats")
async def get_worker_stats(db: AsyncSession = Depends(get_db)):
    """Return Celery worker info + today's aggregate crawl stats per worker."""
    from datetime import date
    from sqlalchemy import text

    # Query Celery inspect for live worker info
    workers_info = []
    try:
        from app.tasks.celery_app import celery_app
        inspect = celery_app.control.inspect(timeout=2)
        active_tasks = inspect.active() or {}
        worker_stats_raw = inspect.stats() or {}

        worker_num = 1
        for hostname, stats in worker_stats_raw.items():
            pool = stats.get("pool", {})
            concurrency = pool.get("max-concurrency") or pool.get("processes") or stats.get("total", {})
            if isinstance(concurrency, dict):
                concurrency = len(concurrency)
            active = active_tasks.get(hostname, [])
            total_tasks = sum(stats.get("total", {}).values()) if isinstance(stats.get("total"), dict) else 0
            workers_info.append({
                "name": f"Worker {worker_num}",
                "hostname": hostname,
                "concurrency": concurrency,
                "active_tasks": len(active),
                "total_tasks_processed": total_tasks,
            })
            worker_num += 1
    except Exception:
        pass

    # If Celery inspect timed out, estimate from DB
    if not workers_info:
        from app.models.crawl_log import CrawlLog
        running_count = await db.scalar(select(func.count(CrawlLog.id)).where(CrawlLog.status == "running")) or 0
        workers_info = [{
            "name": "Worker 1",
            "hostname": "celery@jobharvest-worker",
            "concurrency": 8,
            "active_tasks": running_count,
            "total_tasks_processed": None,
        }]

    # Today's aggregate stats from DB (AEST midnight)
    today_start = datetime.now(AEST).replace(hour=0, minute=0, second=0, microsecond=0)
    today_result = await db.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE status = 'success') as completed,
            COUNT(*) FILTER (WHERE status = 'failed') as failed,
            COALESCE(SUM(pages_crawled), 0) as pages,
            COALESCE(SUM(jobs_found), 0) as jobs
        FROM crawl_logs
        WHERE started_at >= :today
    """), {"today": today_start})
    row = today_result.fetchone()
    today_totals = {
        "sites_completed": row[0] or 0,
        "sites_failed": row[1] or 0,
        "pages_crawled": row[2] or 0,
        "jobs_found": row[3] or 0,
    }

    # Distribute today's totals across workers proportionally by total_tasks_processed
    # (or equally if we can't tell)
    n = len(workers_info)
    total_weight = sum(w.get("total_tasks_processed") or 0 for w in workers_info)
    for w in workers_info:
        if total_weight > 0 and w.get("total_tasks_processed"):
            ratio = w["total_tasks_processed"] / total_weight
        else:
            ratio = 1 / n if n else 1
        w["today"] = {
            "sites_completed": round(today_totals["sites_completed"] * ratio),
            "pages_crawled": round(today_totals["pages_crawled"] * ratio),
            "jobs_found": round(today_totals["jobs_found"] * ratio),
        }

    return {
        "workers": workers_info,
        "total_concurrency": sum(int(w.get("concurrency") or 0) for w in workers_info),
        "today_totals": today_totals,
    }


@router.post("/queue/reset-stale", status_code=200)
async def reset_stale_queue_items(
    stale_after_minutes: int = Query(default=120, ge=10),
    db: AsyncSession = Depends(get_db),
):
    """Reset run_queue items stuck in 'processing' back to 'pending'.
    Use when tasks have died without completing and items are permanently stuck."""
    from app.services import queue_manager
    count = await queue_manager.reset_stale_processing(db, stale_after_minutes=stale_after_minutes)
    await db.commit()
    return {"reset": count, "stale_after_minutes": stale_after_minutes}
