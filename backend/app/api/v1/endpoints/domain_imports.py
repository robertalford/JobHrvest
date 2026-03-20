"""Domain import endpoints — trigger bulk company seeding from external sources."""

from typing import Optional, List
from fastapi import APIRouter, Query
from sqlalchemy import text

router = APIRouter()


@router.get("/stats")
@router.get("/stats/")
async def domain_import_stats():
    """Return company counts broken down by discovery source."""
    from app.db.base import AsyncSessionLocal
    async with AsyncSessionLocal() as db:
        rows = await db.execute(text("""
            SELECT
                discovered_via,
                COUNT(*)                                                AS total,
                COUNT(*) FILTER (WHERE company_status = 'no_sites_new') AS pending_config,
                COUNT(*) FILTER (WHERE company_status != 'no_sites_new') AS configured
            FROM companies
            GROUP BY discovered_via
            ORDER BY total DESC
        """))
        sources = [
            {
                "source": r[0] or "unknown",
                "total": r[1],
                "pending_config": r[2],
                "configured": r[3],
            }
            for r in rows.fetchall()
        ]
        total_row = await db.execute(text("SELECT COUNT(*) FROM companies"))
        total = total_row.scalar() or 0
    return {"total_companies": total, "by_source": sources}


@router.post("/trigger/tranco")
async def trigger_tranco():
    """Trigger Tranco Top-1M domain import (filters by market TLDs)."""
    from app.tasks.domain_import_tasks import import_tranco_domains
    task = import_tranco_domains.apply_async(queue="default")
    return {"task_id": task.id, "status": "queued", "source": "tranco"}


@router.post("/trigger/majestic")
async def trigger_majestic():
    """Trigger Majestic Million domain import (filters by market TLDs)."""
    from app.tasks.domain_import_tasks import import_majestic_domains
    task = import_majestic_domains.apply_async(queue="default")
    return {"task_id": task.id, "status": "queued", "source": "majestic"}


@router.post("/trigger/wikidata")
async def trigger_wikidata(
    markets: Optional[List[str]] = Query(None),
    limit_per_market: int = Query(10_000, ge=100, le=50_000),
):
    """
    Trigger Wikidata SPARQL import.
    Finds organisations with official websites (P856) registered in each target market.
    Catches companies that use .com or non-country-TLD domains.
    """
    from app.tasks.domain_import_tasks import import_wikidata_companies
    task = import_wikidata_companies.apply_async(
        kwargs={"markets": markets or None, "limit_per_market": limit_per_market},
        queue="default",
    )
    return {"task_id": task.id, "status": "queued", "source": "wikidata",
            "markets": markets or "all", "limit_per_market": limit_per_market}
