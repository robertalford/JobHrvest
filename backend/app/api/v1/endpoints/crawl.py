"""Crawl management endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import get_db
from app.models.crawl_log import CrawlLog

router = APIRouter()


@router.get("/active")
async def get_active_crawls(db: AsyncSession = Depends(get_db)):
    logs = await db.scalars(
        select(CrawlLog).where(CrawlLog.status == "running").order_by(CrawlLog.started_at.desc())
    )
    return list(logs)


@router.get("/queue")
async def get_queued_crawls(db: AsyncSession = Depends(get_db)):
    from app.tasks.celery_app import celery_app
    inspect = celery_app.control.inspect()
    queued = inspect.reserved() or {}
    scheduled = inspect.scheduled() or {}
    return {"reserved": queued, "scheduled": scheduled}


@router.get("/history")
async def get_crawl_history(
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
):
    logs = await db.scalars(
        select(CrawlLog).order_by(CrawlLog.started_at.desc()).limit(limit)
    )
    return list(logs)


@router.post("/trigger-full", status_code=202)
async def trigger_full_crawl(db: AsyncSession = Depends(get_db)):
    from app.tasks.crawl_tasks import full_crawl_cycle
    task = full_crawl_cycle.delay()
    return {"task_id": task.id, "status": "queued"}


@router.delete("/cancel/{task_id}", status_code=200)
async def cancel_task(task_id: str):
    from app.tasks.celery_app import celery_app
    celery_app.control.revoke(task_id, terminate=True)
    return {"task_id": task_id, "status": "cancelled"}
