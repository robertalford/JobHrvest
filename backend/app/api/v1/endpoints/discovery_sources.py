"""Discovery Sources (aggregator_sources) CRUD endpoints."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import get_db
from app.models.aggregator_source import AggregatorSource

router = APIRouter()


@router.get("/")
@router.get("")
async def list_sources(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: str = Query(None),
    market: str = Query(None),
    is_active: str = Query(None),
    db: AsyncSession = Depends(get_db),
):
    q = select(AggregatorSource).order_by(AggregatorSource.name)
    if search:
        q = q.where(
            AggregatorSource.name.ilike(f"%{search}%") |
            AggregatorSource.base_url.ilike(f"%{search}%")
        )
    if market:
        q = q.where(AggregatorSource.market == market)
    if is_active is not None:
        q = q.where(AggregatorSource.is_active == (is_active == "true"))

    total = await db.scalar(select(func.count()).select_from(q.subquery()))
    rows = await db.scalars(q.offset((page - 1) * page_size).limit(page_size))

    items = []
    for s in rows:
        items.append({
            "id": str(s.id),
            "name": s.name,
            "base_url": s.base_url,
            "market": s.market,
            "is_active": s.is_active,
            "purpose": s.purpose,
            "last_link_harvest_at": s.last_link_harvest_at.isoformat() if s.last_link_harvest_at else None,
        })
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.post("/", status_code=201)
@router.post("", status_code=201)
async def create_source(body: dict, db: AsyncSession = Depends(get_db)):
    source = AggregatorSource(
        name=body["name"],
        base_url=body["base_url"],
        market=body.get("market", "AU"),
        is_active=body.get("is_active", True),
        purpose=body.get("purpose", "link_discovery_only"),
    )
    db.add(source)
    await db.commit()
    await db.refresh(source)
    return {"id": str(source.id), "name": source.name, "base_url": source.base_url, "market": source.market, "is_active": source.is_active}


@router.put("/{source_id}/")
@router.put("/{source_id}")
async def update_source(source_id: str, body: dict, db: AsyncSession = Depends(get_db)):
    import uuid
    source = await db.get(AggregatorSource, uuid.UUID(source_id))
    if not source:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Not found")
    for field in ("name", "base_url", "market", "is_active", "purpose"):
        if field in body:
            setattr(source, field, body[field])
    await db.commit()
    return {"id": str(source.id), "name": source.name, "is_active": source.is_active}


@router.delete("/{source_id}/")
@router.delete("/{source_id}")
async def delete_source(source_id: str, db: AsyncSession = Depends(get_db)):
    import uuid
    source = await db.get(AggregatorSource, uuid.UUID(source_id))
    if not source:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Not found")
    await db.delete(source)
    await db.commit()
    return {"deleted": source_id}
