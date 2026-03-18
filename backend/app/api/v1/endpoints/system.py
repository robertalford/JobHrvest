"""System management endpoints."""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.base import get_db

router = APIRouter()


@router.get("/health")
async def system_health():
    """Alias for the root health check."""
    import httpx
    import redis.asyncio as aioredis
    from sqlalchemy import text
    from app.db.base import AsyncSessionLocal

    result = {"status": "ok", "services": {}}

    try:
        async with AsyncSessionLocal() as s:
            await s.execute(text("SELECT 1"))
        result["services"]["postgres"] = "ok"
    except Exception as e:
        result["services"]["postgres"] = f"error: {e}"
        result["status"] = "degraded"

    try:
        r = aioredis.from_url(settings.REDIS_URL)
        await r.ping()
        await r.aclose()
        result["services"]["redis"] = "ok"
    except Exception as e:
        result["services"]["redis"] = f"error: {e}"
        result["status"] = "degraded"

    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{settings.OLLAMA_BASE_URL}/api/tags")
        result["services"]["ollama"] = "ok"
    except Exception as e:
        result["services"]["ollama"] = f"error: {e}"
        result["status"] = "degraded"

    return result


@router.get("/config")
async def get_config():
    return {
        "crawl_rate_limit_seconds": settings.CRAWL_RATE_LIMIT_SECONDS,
        "crawl_max_concurrent": settings.CRAWL_MAX_CONCURRENT,
        "crawl_max_depth": settings.CRAWL_MAX_DEPTH,
        "ollama_model": settings.OLLAMA_MODEL,
    }


@router.post("/retrain-classifier", status_code=202)
async def retrain_classifier():
    from app.tasks.ml_tasks import retrain_page_classifier
    task = retrain_page_classifier.delay()
    return {"task_id": task.id, "status": "queued"}


@router.post("/rebuild-templates", status_code=202)
async def rebuild_templates():
    from app.tasks.ml_tasks import rebuild_all_templates
    task = rebuild_all_templates.delay()
    return {"task_id": task.id, "status": "queued"}
