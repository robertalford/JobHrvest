#!/usr/bin/env python3
"""
CSV Lead Importer — reads ad_gap_data_all_markets.csv and creates Company records.

Reads: /storage/ad_gap_data_all_markets.csv  (or path via CLI arg)
Creates: companies + lead_imports tracking rows

Usage:
    python scripts/import_leads.py [csv_path] [--limit N] [--country AU]

CSV columns:
    country_id, ad_origin_category, origin, advertiser_name,
    cnt_ads_202504_202509, sample_linkout_url, sample_ad_url, origin_rank_by_ads_count
"""

import asyncio
import csv
import sys
import uuid
import logging
import argparse
from datetime import datetime, timezone
from urllib.parse import urlparse

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings
from app.crawlers.domain_blocklist import is_blocked

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

engine = create_async_engine(settings.DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# Map CSV country_id → market code
COUNTRY_MAP = {
    "AU": "AU",
    "SG": "SG",
    "PH": "PH",
    "NZ": "NZ",
    "MY": "MY",
    "ID": "ID",
    "TH": "TH",
    "HK": "HK",
}

BATCH_SIZE = 500


def extract_domain(url: str) -> str | None:
    """Extract clean domain from URL or raw domain string."""
    if not url:
        return None
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower().lstrip("www.") or None
    except Exception:
        return None


def clean_url(url: str) -> str | None:
    if not url or not url.strip():
        return None
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


async def import_batch(db: AsyncSession, rows: list[dict]) -> dict:
    counts = {"success": 0, "failed": 0, "skipped": 0, "blocked": 0}

    for row in rows:
        country_id = row.get("country_id", "").strip().upper()
        advertiser_name = row.get("advertiser_name", "").strip()
        origin = row.get("origin", "").strip()
        sample_linkout_url = clean_url(row.get("sample_linkout_url", ""))
        ad_origin_category = row.get("ad_origin_category", "").strip()
        expected_job_count = None
        try:
            expected_job_count = int(row.get("cnt_ads_202504_202509") or 0) or None
        except (ValueError, TypeError):
            pass
        origin_rank = None
        try:
            origin_rank = int(row.get("origin_rank_by_ads_count") or 0) or None
        except (ValueError, TypeError):
            pass

        if not origin or not advertiser_name:
            counts["skipped"] += 1
            continue

        # Determine domain — prefer sample_linkout_url, fall back to origin column
        domain = extract_domain(sample_linkout_url) or extract_domain(origin)
        if not domain:
            counts["skipped"] += 1
            continue

        root_url = sample_linkout_url or f"https://{domain}"
        market_code = COUNTRY_MAP.get(country_id, "AU")
        lead_id = str(uuid.uuid4())

        # Check blocklist
        if is_blocked(root_url):
            await db.execute(text("""
                INSERT INTO lead_imports (
                    id, country_id, advertiser_name, origin_domain,
                    sample_linkout_url, ad_origin_category, expected_job_count,
                    origin_rank, status, skip_reason
                ) VALUES (
                    :id, :country_id, :advertiser_name, :origin_domain,
                    :sample_linkout_url, :ad_origin_category, :expected_job_count,
                    :origin_rank, 'blocked', 'Domain is in hard-block list'
                ) ON CONFLICT DO NOTHING
            """), {
                "id": lead_id, "country_id": country_id,
                "advertiser_name": advertiser_name, "origin_domain": domain,
                "sample_linkout_url": root_url, "ad_origin_category": ad_origin_category,
                "expected_job_count": expected_job_count, "origin_rank": origin_rank,
            })
            counts["blocked"] += 1
            continue

        try:
            # Upsert company
            company_result = await db.execute(text("""
                INSERT INTO companies (
                    id, name, domain, root_url, market_code, discovered_via,
                    crawl_priority, is_active
                ) VALUES (
                    :id, :name, :domain, :root_url, :market_code, 'csv_import',
                    3, true
                )
                ON CONFLICT (domain) DO UPDATE SET
                    name = EXCLUDED.name,
                    updated_at = now()
                RETURNING id
            """), {
                "id": str(uuid.uuid4()), "name": advertiser_name, "domain": domain,
                "root_url": root_url, "market_code": market_code,
            })
            company_row = company_result.fetchone()
            company_id = str(company_row[0]) if company_row else None

            # Record lead import
            await db.execute(text("""
                INSERT INTO lead_imports (
                    id, country_id, advertiser_name, origin_domain,
                    sample_linkout_url, ad_origin_category, expected_job_count,
                    origin_rank, status, company_id, processed_at
                ) VALUES (
                    :id, :country_id, :advertiser_name, :origin_domain,
                    :sample_linkout_url, :ad_origin_category, :expected_job_count,
                    :origin_rank, 'success', :company_id, now()
                ) ON CONFLICT DO NOTHING
            """), {
                "id": lead_id, "country_id": country_id,
                "advertiser_name": advertiser_name, "origin_domain": domain,
                "sample_linkout_url": root_url, "ad_origin_category": ad_origin_category,
                "expected_job_count": expected_job_count, "origin_rank": origin_rank,
                "company_id": company_id,
            })
            counts["success"] += 1

        except Exception as e:
            error_msg = str(e)[:500]
            try:
                await db.execute(text("""
                    INSERT INTO lead_imports (
                        id, country_id, advertiser_name, origin_domain,
                        sample_linkout_url, ad_origin_category, expected_job_count,
                        origin_rank, status, error_message, processed_at
                    ) VALUES (
                        :id, :country_id, :advertiser_name, :origin_domain,
                        :sample_linkout_url, :ad_origin_category, :expected_job_count,
                        :origin_rank, 'failed', :error_message, now()
                    ) ON CONFLICT DO NOTHING
                """), {
                    "id": lead_id, "country_id": country_id,
                    "advertiser_name": advertiser_name, "origin_domain": domain,
                    "sample_linkout_url": root_url, "ad_origin_category": ad_origin_category,
                    "expected_job_count": expected_job_count, "origin_rank": origin_rank,
                    "error_message": error_msg,
                })
            except Exception:
                pass
            counts["failed"] += 1

    await db.commit()
    return counts


async def run_import(csv_path: str, limit: int | None = None, country_filter: str | None = None):
    log.info(f"Opening CSV: {csv_path}")

    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if country_filter:
        rows = [r for r in rows if r.get("country_id", "").strip().upper() == country_filter.upper()]
        log.info(f"Filtered to {len(rows)} rows for country {country_filter}")

    if limit:
        rows = rows[:limit]
        log.info(f"Limited to {limit} rows")

    total = len(rows)
    log.info(f"Importing {total} leads...")

    grand = {"success": 0, "failed": 0, "skipped": 0, "blocked": 0}

    async with AsyncSessionLocal() as db:
        for i in range(0, total, BATCH_SIZE):
            batch = rows[i: i + BATCH_SIZE]
            counts = await import_batch(db, batch)
            for k in grand:
                grand[k] += counts[k]
            done = min(i + BATCH_SIZE, total)
            log.info(
                f"  {done}/{total} processed — "
                f"success={grand['success']} failed={grand['failed']} "
                f"skipped={grand['skipped']} blocked={grand['blocked']}"
            )

    log.info(f"✅ Import complete: {grand}")
    return grand


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import lead CSV into JobHarvest")
    parser.add_argument("csv_path", nargs="?", default="/storage/ad_gap_data_all_markets.csv")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--country", type=str, default=None)
    args = parser.parse_args()

    asyncio.run(run_import(args.csv_path, args.limit, args.country))
