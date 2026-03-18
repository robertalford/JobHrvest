"""Health check endpoint."""

import httpx
import redis.asyncio as aioredis
from fastapi import APIRouter
from sqlalchemy import text

from app.core.config import settings
from app.db.base import AsyncSessionLocal

router = APIRouter()


@router.get("/health")
async def health_check():
    """Check health of all system components."""
    status = {"status": "ok", "services": {}}

    # Postgres
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        status["services"]["postgres"] = "ok"
    except Exception as e:
        status["services"]["postgres"] = f"error: {str(e)}"
        status["status"] = "degraded"

    # Redis
    try:
        r = aioredis.from_url(settings.REDIS_URL)
        await r.ping()
        await r.aclose()
        status["services"]["redis"] = "ok"
    except Exception as e:
        status["services"]["redis"] = f"error: {str(e)}"
        status["status"] = "degraded"

    # Ollama
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{settings.OLLAMA_BASE_URL}/api/tags")
            resp.raise_for_status()
        status["services"]["ollama"] = "ok"
    except Exception as e:
        status["services"]["ollama"] = f"error: {str(e)}"
        status["status"] = "degraded"

    return status
