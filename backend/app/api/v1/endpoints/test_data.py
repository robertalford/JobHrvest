"""Test data endpoints — list/paginate imported CSV test data and trigger imports."""

import csv
import json
import os
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import get_db
from app.models.test_data import (
    CrawlerTestData,
    JobSiteTestData,
    SiteUrlTestData,
    SiteWrapperTestData,
)

router = APIRouter()

_DATA_DIR = "/app/data"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _serialize(row, model_cls):
    """Generic row-to-dict serialiser for test data models."""
    d = {}
    for col in model_cls.__table__.columns:
        val = getattr(row, col.name, None)
        if val is None:
            d[col.name] = None
        elif hasattr(val, "isoformat"):
            d[col.name] = val.isoformat()
        elif isinstance(val, dict) or isinstance(val, list):
            d[col.name] = val
        else:
            d[col.name] = str(val) if col.name == "id" else val
    return d


async def _paginated_list(db: AsyncSession, model_cls, page: int, page_size: int, search: Optional[str] = None):
    """Return a paginated response for a test data table."""
    q = select(model_cls)
    count_q = select(func.count()).select_from(model_cls)

    if search and hasattr(model_cls, "name"):
        q = q.where(model_cls.name.ilike(f"%{search}%"))
        count_q = count_q.where(model_cls.name.ilike(f"%{search}%"))

    total = await db.scalar(count_q) or 0
    rows = list(await db.scalars(
        q.order_by(model_cls.created_at.desc())
         .offset((page - 1) * page_size)
         .limit(page_size)
    ))
    items = [_serialize(r, model_cls) for r in rows]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


# ---------------------------------------------------------------------------
# List endpoints
# ---------------------------------------------------------------------------

@router.get("/crawlers")
async def list_crawlers(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    return await _paginated_list(db, CrawlerTestData, page, page_size, search)


@router.get("/job-sites")
async def list_job_sites(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    return await _paginated_list(db, JobSiteTestData, page, page_size, search)


@router.get("/site-urls")
async def list_site_urls(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    return await _paginated_list(db, SiteUrlTestData, page, page_size)


@router.get("/site-wrappers")
async def list_site_wrappers(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    return await _paginated_list(db, SiteWrapperTestData, page, page_size)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@router.get("/stats")
async def test_data_stats(db: AsyncSession = Depends(get_db)):
    """Return row counts for all four test data tables."""
    crawlers = await db.scalar(select(func.count()).select_from(CrawlerTestData)) or 0
    job_sites = await db.scalar(select(func.count()).select_from(JobSiteTestData)) or 0
    site_urls = await db.scalar(select(func.count()).select_from(SiteUrlTestData)) or 0
    site_wrappers = await db.scalar(select(func.count()).select_from(SiteWrapperTestData)) or 0
    return {
        "crawlers": crawlers,
        "job_sites": job_sites,
        "site_urls": site_urls,
        "site_wrappers": site_wrappers,
    }


# ---------------------------------------------------------------------------
# Import from CSV
# ---------------------------------------------------------------------------

def _parse_json_field(value: str):
    """Try to parse a string as JSON; return None if empty or invalid."""
    if not value or value.strip().lower() in ("", "null"):
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, ValueError):
        return value


def _to_bool(value: str) -> bool:
    return value.strip().lower() in ("true", "1", "yes", "t")


def _to_optional_int(value: str):
    if not value or value.strip() == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


@router.post("/import")
async def import_test_data(db: AsyncSession = Depends(get_db)):
    """Import test data from CSV files in /app/data/, then delete the source files."""
    counts = {"crawlers": 0, "job_sites": 0, "site_urls": 0, "site_wrappers": 0}
    errors = []

    # --- Crawlers ---
    crawlers_path = os.path.join(_DATA_DIR, "crawlers.csv")
    if os.path.isfile(crawlers_path):
        try:
            with open(crawlers_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    obj = CrawlerTestData(
                        external_id=row.get("id", ""),
                        job_site_id=row.get("job_site_id", ""),
                        name=row.get("name", ""),
                        crawler_type=row.get("crawler_type", "web"),
                        country=row.get("country") or None,
                        country_code=row.get("country_code") or None,
                        frequency=_to_optional_int(row.get("frequency", "")),
                        status=row.get("status") or None,
                        current_status=row.get("current_status") or None,
                        disabled=_to_bool(row.get("disabled", "false")),
                        statistics_data=_parse_json_field(row.get("statistics_data", "")),
                    )
                    db.add(obj)
                    counts["crawlers"] += 1
            await db.flush()
            os.remove(crawlers_path)
        except Exception as e:
            errors.append(f"crawlers.csv: {e}")

    # --- Job Sites ---
    job_sites_path = os.path.join(_DATA_DIR, "job_sites.csv")
    if os.path.isfile(job_sites_path):
        try:
            with open(job_sites_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    obj = JobSiteTestData(
                        external_id=row.get("id", ""),
                        name=row.get("name", ""),
                        site_type=row.get("site_type", ""),
                        num_of_jobs=_to_optional_int(row.get("num_of_jobs", "")),
                        expected_job_count=_to_optional_int(row.get("expected_job_count", "")),
                        disabled=_to_bool(row.get("disabled", "false")),
                        uncrawlable_reason=row.get("uncrawlable_reason") or None,
                        tags=_parse_json_field(row.get("tags", "")),
                    )
                    db.add(obj)
                    counts["job_sites"] += 1
            await db.flush()
            os.remove(job_sites_path)
        except Exception as e:
            errors.append(f"job_sites.csv: {e}")

    # --- Site URLs ---
    site_urls_path = os.path.join(_DATA_DIR, "site_urls.csv")
    if os.path.isfile(site_urls_path):
        try:
            with open(site_urls_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    obj = SiteUrlTestData(
                        site_id=row.get("site_id", ""),
                        url=row.get("url", ""),
                    )
                    db.add(obj)
                    counts["site_urls"] += 1
            await db.flush()
            os.remove(site_urls_path)
        except Exception as e:
            errors.append(f"site_urls.csv: {e}")

    # --- Site Wrappers ---
    # Consolidate selector columns into a single JSONB `selectors` field.
    _SELECTOR_FIELDS = [
        "min_container_path", "record_boundary_path", "job_title_path",
        "job_title_url_pattern", "row_location_paths", "row_listed_date_path",
        "row_source_path", "row_internal_id_script", "row_url_script",
        "next_page_path", "page_description_path", "row_script",
        "details_page_script", "row_details_page_link_path", "internal_id_path",
        "xml_namespaces", "details_page_job_title_path", "details_page_source_path",
        "details_page_location_paths", "details_page_listed_date_path",
        "details_page_apply_email_path", "details_page_closing_date_path",
        "row_closing_date_path", "row_description_path", "row_description_node",
        "details_page_salary_path", "row_apply_email_path", "row_description_paths",
        "details_page_description_paths", "row_apply_url_path",
        "details_page_apply_url_path", "details_page_min_container_path",
        "date_format", "row_salary_paths", "row_job_type_paths",
        "details_page_job_type_paths",
    ]
    wrappers_path = os.path.join(_DATA_DIR, "site_wrappers.csv")
    if os.path.isfile(wrappers_path):
        try:
            with open(wrappers_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    selectors = {}
                    for field in _SELECTOR_FIELDS:
                        val = row.get(field, "")
                        if val and val.strip():
                            selectors[field] = val.strip()

                    paths_raw = row.get("paths", "")
                    paths_config = _parse_json_field(paths_raw)

                    obj = SiteWrapperTestData(
                        external_id=row.get("id", ""),
                        crawler_id=row.get("crawler_id", ""),
                        selectors=selectors,
                        paths_config=paths_config if isinstance(paths_config, dict) else None,
                        has_detail_page=bool(row.get("row_details_page_link_path", "").strip()),
                    )
                    db.add(obj)
                    counts["site_wrappers"] += 1
            await db.flush()
            os.remove(wrappers_path)
        except Exception as e:
            errors.append(f"site_wrappers.csv: {e}")

    await db.commit()

    result = {"imported": counts, "total": sum(counts.values())}
    if errors:
        result["errors"] = errors
    return result
