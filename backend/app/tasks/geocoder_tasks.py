"""Geocoder Celery tasks.

  seed_geonames        — downloads GeoNames data for all 8 supported markets
  geocode_new_jobs     — geocodes jobs where geo_resolved IS NULL (beat: every 2 min)
  retro_geocode_jobs   — retroactively geocodes all unresolved jobs
"""

import asyncio
import io
import logging
import zipfile
from typing import Optional
import urllib.request

from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)

# Market → ISO-3166-1 alpha-2 country code (they match for our markets)
MARKET_COUNTRY: dict[str, str] = {
    "AU": "AU", "NZ": "NZ", "SG": "SG", "MY": "MY",
    "HK": "HK", "PH": "PH", "ID": "ID", "TH": "TH",
}

# Manually-defined L1 country seeds (centroid lat/lng)
COUNTRIES = {
    "AU": {"name": "Australia",    "ascii_name": "Australia",    "lat": -25.2744, "lng": 133.7751},
    "NZ": {"name": "New Zealand",  "ascii_name": "New Zealand",  "lat": -40.9006, "lng": 174.8860},
    "SG": {"name": "Singapore",    "ascii_name": "Singapore",    "lat":   1.3521, "lng": 103.8198},
    "MY": {"name": "Malaysia",     "ascii_name": "Malaysia",     "lat":   4.2105, "lng": 101.9758},
    "HK": {"name": "Hong Kong",    "ascii_name": "Hong Kong",    "lat":  22.3193, "lng": 114.1694},
    "PH": {"name": "Philippines",  "ascii_name": "Philippines",  "lat":  12.8797, "lng": 121.7740},
    "ID": {"name": "Indonesia",    "ascii_name": "Indonesia",    "lat":  -0.7893, "lng": 113.9213},
    "TH": {"name": "Thailand",     "ascii_name": "Thailand",     "lat":  15.8700, "lng": 100.9925},
}

# GeoNames feature codes we care about, mapped to initial level (PPL gets refined by population)
FEATURE_CODES: dict[str, int] = {
    "ADM1": 2, "ADM1H": 2,
    "PPLC": 3, "PPLA": 3, "PPLA2": 3, "PPLA3": 3,
    "PPL": 3,  "PPLS": 3,
    "PPLF": 4, "PPLX": 4, "PPLA4": 4,
}

# Min population for a PPL/PPLS to be kept at L3 (city) rather than L4 (suburb)
L3_MIN_POP: dict[str, int] = {
    "AU": 5_000, "NZ": 2_000, "SG": 0, "MY": 3_000,
    "HK": 0, "PH": 5_000, "ID": 10_000, "TH": 5_000,
}


def _run(coro):
    return asyncio.run(coro)


# ═══════════════════════════════════════════════════════════════════════════════
# TASK: seed_geonames
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task(name="geocoder.seed_geonames", bind=True, max_retries=2, time_limit=3600)
def seed_geonames(self, countries: Optional[list] = None):
    """Download GeoNames dump for each market and import into geo_locations."""
    if countries is None:
        countries = list(MARKET_COUNTRY.keys())

    totals: dict[str, int] = {}
    for market in countries:
        cc = MARKET_COUNTRY[market]
        logger.info("GeoNames seed: starting %s", cc)
        try:
            stats = _run(_seed_one(cc, market))
            for k, v in stats.items():
                totals[k] = totals.get(k, 0) + v
            logger.info("GeoNames seed %s: %s", cc, stats)
        except Exception as exc:
            logger.error("GeoNames seed %s failed: %s", cc, exc, exc_info=True)

    logger.info("GeoNames seed complete: %s", totals)
    return totals


async def _seed_one(country_code: str, market_code: str) -> dict:
    from app.db.base import AsyncSessionLocal
    from sqlalchemy import text

    stats = {"countries": 0, "regions": 0, "cities": 0, "suburbs": 0, "skipped": 0}

    async with AsyncSessionLocal() as db:
        # ── L1 country record ──
        row = await db.execute(text("""
            SELECT id FROM geo_locations WHERE country_code = :cc AND level = 1 LIMIT 1
        """), {"cc": country_code})
        existing = row.fetchone()

        if existing:
            country_id = existing[0]
        else:
            cd = COUNTRIES[country_code]
            r2 = await db.execute(text("""
                INSERT INTO geo_locations
                    (level, name, ascii_name, market_code, country_code, lat, lng, feature_code)
                VALUES (1, :name, :asc, :mc, :cc, :lat, :lng, 'PCLI')
                ON CONFLICT (country_code) WHERE level = 1 DO NOTHING
                RETURNING id
            """), {"name": cd["name"], "asc": cd["ascii_name"], "mc": market_code,
                   "cc": country_code, "lat": cd["lat"], "lng": cd["lng"]})
            r3 = r2.fetchone()
            if r3:
                country_id = r3[0]
            else:
                r4 = await db.execute(text(
                    "SELECT id FROM geo_locations WHERE country_code=:cc AND level=1 LIMIT 1"
                ), {"cc": country_code})
                country_id = r4.scalar()
            await db.commit()
        stats["countries"] += 1

    # ── Download GeoNames dump ──
    url = f"https://download.geonames.org/export/dump/{country_code}.zip"
    logger.info("  Downloading %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": "JobHarvest/1.0 geocoder-seed"})
    with urllib.request.urlopen(req, timeout=300) as resp:
        data = resp.read()

    # ── Parse tab-separated GeoNames format ──
    records = []
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        txt_name = next(
            n for n in zf.namelist()
            if n.endswith(".txt") and not n.startswith("readme")
        )
        with zf.open(txt_name) as f:
            for line in io.TextIOWrapper(f, encoding="utf-8"):
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 19:
                    continue
                fc = parts[7]
                if parts[6] not in ("A", "P") or fc not in FEATURE_CODES:
                    continue
                try:
                    lat = float(parts[4])
                    lng = float(parts[5])
                    pop = int(parts[14]) if parts[14] else 0
                except (ValueError, TypeError):
                    continue
                alt = [a.strip() for a in parts[3].split(",") if a.strip()][:15]
                records.append({
                    "gid": int(parts[0]),
                    "name": parts[1],
                    "ascii": parts[2],
                    "alt": alt,
                    "lat": lat, "lng": lng,
                    "fc": fc,
                    "adm1": parts[10],
                    "pop": pop,
                    "tz": parts[17],
                })

    logger.info("  Parsed %d features for %s", len(records), country_code)

    # ── Pass 1: ADM1 → L2 regions ──
    async with AsyncSessionLocal() as db:
        admin1_map: dict[str, object] = {}
        for rec in [r for r in records if r["fc"].startswith("ADM1")]:
            try:
                r2 = await db.execute(text("""
                    INSERT INTO geo_locations
                        (geonames_id, level, name, ascii_name, alt_names, parent_id,
                         market_code, country_code, lat, lng, population, timezone,
                         admin1_code, feature_code)
                    VALUES
                        (:gid, 2, :name, :asc, :alt, :parent,
                         :mc, :cc, :lat, :lng, :pop, :tz, :adm1, :fc)
                    ON CONFLICT (geonames_id) WHERE geonames_id IS NOT NULL
                    DO UPDATE SET updated_at = NOW()
                    RETURNING id
                """), {
                    "gid": rec["gid"], "name": rec["name"], "asc": rec["ascii"],
                    "alt": rec["alt"], "parent": str(country_id),
                    "mc": market_code, "cc": country_code,
                    "lat": rec["lat"], "lng": rec["lng"],
                    "pop": rec["pop"], "tz": rec["tz"],
                    "adm1": rec["adm1"], "fc": rec["fc"],
                })
                row = r2.fetchone()
                if row:
                    admin1_map[rec["adm1"]] = row[0]
                    stats["regions"] += 1
            except Exception as exc:
                logger.debug("ADM1 skip %s: %s", rec["name"], exc)
        await db.commit()

        # If ON CONFLICT skipped rows (existing), fetch them
        if not admin1_map:
            r3 = await db.execute(text(
                "SELECT admin1_code, id FROM geo_locations WHERE country_code=:cc AND level=2"
            ), {"cc": country_code})
            admin1_map = {row[0]: row[1] for row in r3.fetchall()}

        # ── Pass 2: PPL* → L3/L4 ──
        ppl_recs = [r for r in records if not r["fc"].startswith("ADM")]
        min_pop = L3_MIN_POP.get(country_code, 5_000)
        BATCH = 500

        for i in range(0, len(ppl_recs), BATCH):
            batch = ppl_recs[i : i + BATCH]
            for rec in batch:
                fc = rec["fc"]
                base_level = FEATURE_CODES.get(fc, 3)
                if fc in ("PPL", "PPLS"):
                    level = 3 if rec["pop"] >= min_pop else 4
                else:
                    level = base_level

                parent_id = admin1_map.get(rec["adm1"]) or country_id
                try:
                    await db.execute(text("""
                        INSERT INTO geo_locations
                            (geonames_id, level, name, ascii_name, alt_names, parent_id,
                             market_code, country_code, lat, lng, population, timezone,
                             admin1_code, feature_code)
                        VALUES
                            (:gid, :level, :name, :asc, :alt, :parent,
                             :mc, :cc, :lat, :lng, :pop, :tz, :adm1, :fc)
                        ON CONFLICT (geonames_id) WHERE geonames_id IS NOT NULL
                        DO UPDATE SET updated_at = NOW()
                    """), {
                        "gid": rec["gid"], "level": level,
                        "name": rec["name"], "asc": rec["ascii"], "alt": rec["alt"],
                        "parent": str(parent_id), "mc": market_code, "cc": country_code,
                        "lat": rec["lat"], "lng": rec["lng"],
                        "pop": rec["pop"], "tz": rec["tz"],
                        "adm1": rec["adm1"], "fc": rec["fc"],
                    })
                    if level == 3:
                        stats["cities"] += 1
                    else:
                        stats["suburbs"] += 1
                except Exception as exc:
                    logger.debug("PPL skip %s: %s", rec["name"], exc)
                    stats["skipped"] += 1

            await db.commit()
            logger.info("  %s batch %d/%d", country_code, i // BATCH + 1, len(ppl_recs) // BATCH + 1)

    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# TASK: geocode_new_jobs  (beat: every 2 min)
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task(name="geocoder.geocode_new_jobs", bind=True, time_limit=600)
def geocode_new_jobs(self, limit: int = 200):
    """Geocode recently-crawled jobs that have geo_resolved IS NULL."""
    return _run(_geocode_batch(limit=limit, include_failed=False))


# ═══════════════════════════════════════════════════════════════════════════════
# TASK: retro_geocode_jobs
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task(name="geocoder.retro_geocode_jobs", bind=True, max_retries=1, time_limit=7200)
def retro_geocode_jobs(self, batch_size: int = 200, retry_failed: bool = False):
    """Retroactively geocode all jobs with geo_resolved IS NULL (or FALSE if retry_failed)."""
    stats = {"processed": 0, "resolved": 0, "failed": 0}

    # Clear stale "unresolved" cache entries so locations get a fresh attempt
    # against the now-populated geo_locations table.
    if retry_failed:
        _run(_clear_unresolved_cache())

    while True:
        result = _run(_geocode_batch(limit=batch_size, include_failed=retry_failed))
        if result["processed"] == 0:
            break
        for k in stats:
            stats[k] += result[k]
        logger.info("retro_geocode_jobs progress: %s", stats)
    logger.info("retro_geocode_jobs complete: %s", stats)
    return stats


# ── shared batch helper ───────────────────────────────────────────────────────

async def _clear_unresolved_cache() -> int:
    from app.db.base import AsyncSessionLocal
    from sqlalchemy import text
    async with AsyncSessionLocal() as db:
        # Delete unresolved and placeholder cache entries to allow fresh attempts
        r = await db.execute(text(
            "DELETE FROM geocode_cache WHERE resolution_method IN ('unresolved', 'no_location')"
        ))
        await db.commit()
        n = r.rowcount or 0
        if n:
            logger.info("retro_geocode_jobs: cleared %d stale unresolved cache entries", n)
        return n


_country_geo_cache: dict[str, object] = {}

async def _get_country_geo_id(db, market_code: str):
    """Return the geo_locations.id for the country-level record of market_code."""
    if market_code in _country_geo_cache:
        return _country_geo_cache[market_code]
    from sqlalchemy import text as _text
    cc = MARKET_COUNTRY.get(market_code)
    if not cc:
        return None
    r = await db.execute(_text(
        "SELECT id FROM geo_locations WHERE country_code = :cc AND level = 1 AND is_active = true LIMIT 1"
    ), {"cc": cc})
    row = r.fetchone()
    geo_id = row[0] if row else None
    _country_geo_cache[market_code] = geo_id
    return geo_id


async def _geocode_batch(limit: int, include_failed: bool) -> dict:
    from app.db.base import AsyncSessionLocal
    from app.services.geocoder import geocoder_service
    from sqlalchemy import text

    stats = {"processed": 0, "resolved": 0, "failed": 0}

    async with AsyncSessionLocal() as db:
        # Guard: don't mark jobs as failed if the location database hasn't been seeded yet
        loc_count = (await db.execute(text("SELECT COUNT(*) FROM geo_locations"))).scalar() or 0
        if loc_count == 0:
            logger.info("_geocode_batch: geo_locations is empty — skipping until seed completes")
            return stats


        status_filter = "geo_resolved IS NULL" if not include_failed else \
                        "(geo_resolved IS NULL OR geo_resolved = false)"
        result = await db.execute(text(f"""
            SELECT j.id, j.location_raw, j.location_city, j.location_state,
                   j.location_country, c.market_code
            FROM jobs j
            JOIN companies c ON c.id = j.company_id
            WHERE {status_filter}
            LIMIT :lim
            FOR UPDATE OF j SKIP LOCKED
        """), {"lim": limit})
        rows = result.fetchall()

        if not rows:
            return stats

        for job_id, loc_raw, loc_city, loc_state, loc_country, market_code in rows:
            # Build best available location string
            loc_text = loc_raw or ", ".join(
                filter(None, [loc_city, loc_state])
            ) or None

            if not loc_text:
                # Fallback: assign country-level geo for the job's market
                country_geo_id = await _get_country_geo_id(db, market_code or "AU")
                if country_geo_id:
                    await db.execute(text("""
                        UPDATE jobs SET
                            geo_location_id       = :geo_id,
                            geo_level             = 1,
                            geo_confidence        = 0.3,
                            geo_resolution_method = 'market_country_fallback',
                            geo_resolved          = true
                        WHERE id = :id
                    """), {"geo_id": str(country_geo_id), "id": str(job_id)})
                    stats["resolved"] += 1
                else:
                    await db.execute(text("""
                        UPDATE jobs
                        SET geo_resolved = false, geo_resolution_method = 'no_location'
                        WHERE id = :id
                    """), {"id": str(job_id)})
                    stats["failed"] += 1
            else:
                geo = await geocoder_service.geocode(db, loc_text, market_code or "AU")
                if geo:
                    await db.execute(text("""
                        UPDATE jobs SET
                            geo_location_id      = :geo_id,
                            geo_level            = :level,
                            geo_confidence       = :conf,
                            geo_resolution_method = :method,
                            geo_resolved         = true
                        WHERE id = :id
                    """), {
                        "geo_id": geo.geo_location_id, "level": geo.level,
                        "conf": geo.confidence, "method": geo.method,
                        "id": str(job_id),
                    })
                    stats["resolved"] += 1
                else:
                    await db.execute(text("""
                        UPDATE jobs
                        SET geo_resolved = false, geo_resolution_method = 'unresolvable'
                        WHERE id = :id
                    """), {"id": str(job_id)})
                    stats["failed"] += 1

            stats["processed"] += 1
            # Commit every 50 jobs so progress is visible and locks are released sooner
            if stats["processed"] % 50 == 0:
                await db.commit()

        await db.commit()

    return stats


@celery_app.task(name="geocoder.geocode_all_failed", bind=True, time_limit=7200)
def geocode_all_failed(self):
    """One-shot: reset no_location jobs + clear bad cache + retro geocode everything."""
    import asyncio
    from app.db.base import AsyncSessionLocal
    from sqlalchemy import text

    async def _run_all():
        async with AsyncSessionLocal() as db:
            # 1. Reset jobs stuck as geo_resolved=false so they retry
            r = await db.execute(text("""
                UPDATE jobs SET geo_resolved = NULL, geo_resolution_method = NULL,
                    geo_location_id = NULL, geo_level = NULL, geo_confidence = NULL
                WHERE geo_resolved = false
                  OR (geo_resolved = true AND geo_resolution_method = 'market_country_fallback'
                      AND (location_raw IS NOT NULL AND location_raw != ''))
            """))
            reset_count = r.rowcount or 0
            await db.commit()
            logger.info("geocode_all_failed: reset %d jobs", reset_count)

            # 2. Clear polluted cache entries
            await db.execute(text("""
                DELETE FROM geocode_cache
                WHERE resolution_method IN ('unresolved', 'no_location')
                   OR (geo_location_id IS NULL AND use_count > 100)
            """))
            await db.commit()

        # 3. Run retro geocode in batches until complete
        total = {"processed": 0, "resolved": 0, "failed": 0}
        while True:
            result = await _geocode_batch(limit=500, include_failed=True)
            if result["processed"] == 0:
                break
            for k in total:
                total[k] += result[k]
            logger.info("geocode_all_failed progress: %s", total)
        return total

    return asyncio.run(_run_all())
