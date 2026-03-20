"""Geocoder API — browse geo_locations, geocode_cache, test resolver, trigger tasks."""

import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.db.base import get_db

router = APIRouter()


# ── schemas ───────────────────────────────────────────────────────────────────

class GeoLocationOut(BaseModel):
    id: str
    level: int
    name: str
    ascii_name: Optional[str]
    parent_id: Optional[str]
    parent_name: Optional[str]
    full_path: Optional[str]
    market_code: Optional[str]
    country_code: Optional[str]
    lat: Optional[float]
    lng: Optional[float]
    population: Optional[int]
    timezone: Optional[str]
    feature_code: Optional[str]
    geonames_id: Optional[int]
    is_active: bool


class CacheEntryOut(BaseModel):
    id: str
    raw_text: str
    market_code: Optional[str]
    geo_location_id: Optional[str]
    resolved_name: Optional[str]
    resolved_path: Optional[str]
    confidence: Optional[float]
    resolution_method: Optional[str]
    use_count: int
    last_used_at: str


class TestGeocodeRequest(BaseModel):
    text: str
    market_code: str = "AU"


# ── helpers ───────────────────────────────────────────────────────────────────

async def _build_path(db: AsyncSession, geo_id, name: str, parent_id) -> str:
    parts = [name]
    current = parent_id
    visited: set = set()
    while current and current not in visited:
        visited.add(current)
        r = await db.execute(
            text("SELECT name, parent_id FROM geo_locations WHERE id = :id"),
            {"id": str(current)},
        )
        row = r.fetchone()
        if not row:
            break
        parts.append(row[0])
        current = row[1]
    return ", ".join(parts)


# ── locations list ────────────────────────────────────────────────────────────

@router.get("/")
@router.get("")
async def list_geo_locations(
    search: Optional[str] = Query(None),
    level: Optional[int] = Query(None),
    market_code: Optional[str] = Query(None),
    country_code: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * page_size
    filters = ["g.is_active = true"]
    params: dict = {"limit": page_size, "offset": offset}

    if search:
        filters.append("(lower(g.name) LIKE :search OR lower(g.ascii_name) LIKE :search)")
        params["search"] = f"%{search.lower()}%"
    if level is not None:
        filters.append("g.level = :level")
        params["level"] = level
    if market_code:
        filters.append("g.market_code = :mc")
        params["mc"] = market_code.upper()
    if country_code:
        filters.append("g.country_code = :cc")
        params["cc"] = country_code.upper()

    where = " AND ".join(filters)

    total_r = await db.execute(
        text(f"SELECT COUNT(*) FROM geo_locations g WHERE {where}"), params
    )
    total = total_r.scalar() or 0

    rows = await db.execute(text(f"""
        SELECT g.id, g.level, g.name, g.ascii_name, g.parent_id,
               p.name AS parent_name,
               g.market_code, g.country_code, g.lat, g.lng,
               g.population, g.timezone, g.feature_code, g.geonames_id, g.is_active
        FROM geo_locations g
        LEFT JOIN geo_locations p ON p.id = g.parent_id
        WHERE {where}
        ORDER BY g.level, g.country_code, g.name
        LIMIT :limit OFFSET :offset
    """), params)

    items = []
    for row in rows.fetchall():
        geo_id, level_v, name, ascii_name, parent_id, parent_name, \
            mc, cc, lat, lng, pop, tz, fc, gid, is_active = row

        # Build abbreviated path (up to 3 ancestors)
        path_r = await db.execute(text("""
            WITH RECURSIVE anc AS (
                SELECT id, name, parent_id, 0 AS depth
                FROM geo_locations WHERE id = :id
                UNION ALL
                SELECT g.id, g.name, g.parent_id, a.depth + 1
                FROM geo_locations g JOIN anc a ON g.id = a.parent_id
                WHERE a.depth < 4
            )
            SELECT string_agg(name, ', ' ORDER BY depth) FROM anc
        """), {"id": str(geo_id)})
        full_path = path_r.scalar()

        items.append(GeoLocationOut(
            id=str(geo_id), level=level_v, name=name, ascii_name=ascii_name,
            parent_id=str(parent_id) if parent_id else None,
            parent_name=parent_name, full_path=full_path,
            market_code=mc, country_code=cc,
            lat=float(lat) if lat else None,
            lng=float(lng) if lng else None,
            population=pop, timezone=tz, feature_code=fc,
            geonames_id=gid, is_active=is_active,
        ))

    return {"items": items, "total": total, "page": page, "page_size": page_size}


# ── stats ─────────────────────────────────────────────────────────────────────

@router.get("/stats")
@router.get("/stats/")
async def geocoder_stats(db: AsyncSession = Depends(get_db)):
    level_r = await db.execute(text("""
        SELECT level, COUNT(*) AS cnt
        FROM geo_locations WHERE is_active = true
        GROUP BY level ORDER BY level
    """))
    by_level = {row[0]: row[1] for row in level_r.fetchall()}

    market_r = await db.execute(text("""
        SELECT market_code, COUNT(*) AS cnt
        FROM geo_locations WHERE is_active = true AND market_code IS NOT NULL
        GROUP BY market_code ORDER BY cnt DESC
    """))
    by_market = {row[0]: row[1] for row in market_r.fetchall()}

    cache_r = await db.execute(text("""
        SELECT
            COUNT(*) AS total,
            COUNT(geo_location_id) AS hits,
            COUNT(*) FILTER (WHERE resolution_method = 'exact')   AS exact_hits,
            COUNT(*) FILTER (WHERE resolution_method LIKE '%fuzzy%') AS fuzzy_hits,
            COUNT(*) FILTER (WHERE resolution_method LIKE 'llm%')  AS llm_hits,
            COUNT(*) FILTER (WHERE resolution_method = 'unresolved') AS failures,
            COALESCE(SUM(use_count), 0) AS total_lookups
        FROM geocode_cache
    """))
    cache_row = cache_r.fetchone()

    job_r = await db.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE geo_resolved = true)  AS resolved,
            COUNT(*) FILTER (WHERE geo_resolved = false) AS failed,
            COUNT(*) FILTER (WHERE geo_resolved IS NULL) AS pending
        FROM jobs
    """))
    job_row = job_r.fetchone()

    return {
        "locations": {
            "total": sum(by_level.values()),
            "by_level": {
                "countries": by_level.get(1, 0),
                "regions": by_level.get(2, 0),
                "cities": by_level.get(3, 0),
                "suburbs": by_level.get(4, 0),
            },
            "by_market": by_market,
        },
        "cache": {
            "total": cache_row[0],
            "resolved": cache_row[1],
            "exact_hits": cache_row[2],
            "fuzzy_hits": cache_row[3],
            "llm_hits": cache_row[4],
            "failures": cache_row[5],
            "total_lookups": cache_row[6],
        },
        "jobs": {
            "resolved": job_row[0],
            "failed": job_row[1],
            "pending": job_row[2],
        },
    }


# ── cache browser ─────────────────────────────────────────────────────────────

@router.get("/cache")
@router.get("/cache/")
async def list_cache(
    method: Optional[str] = Query(None),
    market_code: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * page_size
    filters: list[str] = []
    params: dict = {"limit": page_size, "offset": offset}

    if method:
        filters.append("gc.resolution_method = :method")
        params["method"] = method
    if market_code:
        filters.append("gc.market_code = :mc")
        params["mc"] = market_code.upper()

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    total = (await db.execute(
        text(f"SELECT COUNT(*) FROM geocode_cache gc {where}"), params
    )).scalar() or 0

    rows = await db.execute(text(f"""
        SELECT gc.id, gc.raw_text, gc.market_code, gc.geo_location_id,
               g.name AS resolved_name, gc.confidence, gc.resolution_method,
               gc.use_count, gc.last_used_at
        FROM geocode_cache gc
        LEFT JOIN geo_locations g ON g.id = gc.geo_location_id
        {where}
        ORDER BY gc.last_used_at DESC
        LIMIT :limit OFFSET :offset
    """), params)

    items = []
    for row in rows.fetchall():
        gc_id, raw_text, mc, geo_id, res_name, conf, res_method, use_count, used_at = row
        # Build path for resolved entries
        resolved_path = None
        if geo_id and res_name:
            path_r = await db.execute(text("""
                WITH RECURSIVE anc AS (
                    SELECT id, name, parent_id, 0 AS depth
                    FROM geo_locations WHERE id = :id
                    UNION ALL
                    SELECT g.id, g.name, g.parent_id, a.depth+1
                    FROM geo_locations g JOIN anc a ON g.id = a.parent_id
                    WHERE a.depth < 4
                )
                SELECT string_agg(name, ', ' ORDER BY depth) FROM anc
            """), {"id": str(geo_id)})
            resolved_path = path_r.scalar()

        items.append(CacheEntryOut(
            id=str(gc_id), raw_text=raw_text, market_code=mc,
            geo_location_id=str(geo_id) if geo_id else None,
            resolved_name=res_name, resolved_path=resolved_path,
            confidence=float(conf) if conf else None,
            resolution_method=res_method,
            use_count=use_count or 1,
            last_used_at=str(used_at),
        ))

    return {"items": items, "total": total, "page": page, "page_size": page_size}


# ── test resolver ─────────────────────────────────────────────────────────────

@router.post("/test")
@router.post("/test/")
async def test_geocode(req: TestGeocodeRequest, db: AsyncSession = Depends(get_db)):
    from app.services.geocoder import geocoder_service
    result = await geocoder_service.geocode(db, req.text, req.market_code)
    if result:
        return {
            "resolved": True,
            "geo_location_id": result.geo_location_id,
            "level": result.level,
            "name": result.name,
            "full_path": result.full_path,
            "lat": result.lat,
            "lng": result.lng,
            "country_code": result.country_code,
            "confidence": result.confidence,
            "method": result.method,
        }
    return {"resolved": False}


# ── trigger tasks ─────────────────────────────────────────────────────────────

@router.post("/seed")
@router.post("/seed/")
async def trigger_seed(countries: Optional[List[str]] = Query(None)):
    from app.tasks.geocoder_tasks import seed_geonames
    task = seed_geonames.delay(countries=countries or None)
    return {"task_id": task.id, "status": "queued"}


@router.post("/retro")
@router.post("/retro/")
async def trigger_retro(retry_failed: bool = False):
    from app.tasks.geocoder_tasks import retro_geocode_jobs
    task = retro_geocode_jobs.delay(retry_failed=retry_failed)
    return {"task_id": task.id, "status": "queued"}
