#!/usr/bin/env python3
"""
Site Wrapper Importer — reads 4 CSV exports from an external crawling system
and imports proven extraction configs into JobHarvest's PostgreSQL database.

Join chain: site_wrappers.crawler_id → crawlers.id
            crawlers.job_site_id → job_sites.id
            job_sites.id = site_urls.site_id → url

Usage (inside the Docker container):
    python scripts/import_site_wrappers.py
    python scripts/import_site_wrappers.py --csv-dir /data/csvs
    python scripts/import_site_wrappers.py --dry-run
    python scripts/import_site_wrappers.py --limit 100
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import uuid
from collections import defaultdict
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_MARKETS = {"AU", "HK", "ID", "MY", "NZ", "PH", "SG", "TH"}
DEFAULT_MARKET = "AU"

EXCLUDED_DOMAINS = {
    "seek.com.au",
    "jora.com",
    "jobsdb.com",
    "jobstreet.com",
    "ricebowl.my",
}

ATS_PATTERNS = [
    (re.compile(r"workday|myworkdayjobs", re.I), "workday"),
    (re.compile(r"greenhouse\.io", re.I), "greenhouse"),
    (re.compile(r"lever\.co", re.I), "lever"),
    (re.compile(r"icims", re.I), "icims"),
    (re.compile(r"bamboohr", re.I), "bamboohr"),
    (re.compile(r"jobvite", re.I), "jobvite"),
    (re.compile(r"smartrecruiters", re.I), "smartrecruiters"),
    (re.compile(r"pageup", re.I), "pageup"),
    (re.compile(r"applynow", re.I), "applynow"),
    (re.compile(r"taleo", re.I), "taleo"),
    (re.compile(r"successfactors", re.I), "successfactors"),
]

BATCH_SIZE = 1000

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_db_connection():
    """Build a psycopg2 connection from environment or defaults."""
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "jobharvest"),
        user=os.environ.get("POSTGRES_USER", "jobharvest"),
        password=os.environ.get("POSTGRES_PASSWORD", "jh-moonlight-2026"),
    )


def extract_domain(url: str) -> str | None:
    """Return the registrable domain from a URL, or None on failure."""
    try:
        parsed = urlparse(url)
        host = parsed.hostname
        if not host:
            return None
        # Strip www. prefix
        if host.startswith("www."):
            host = host[4:]
        return host.lower()
    except Exception:
        return None


def is_blocked_domain(domain: str) -> bool:
    """Check if domain or any parent domain is in the blocklist."""
    if not domain:
        return True
    for blocked in EXCLUDED_DOMAINS:
        if domain == blocked or domain.endswith("." + blocked):
            return True
    return False


def detect_ats(url: str) -> str | None:
    """Try to detect ATS platform from the URL."""
    for pattern, ats_name in ATS_PATTERNS:
        if pattern.search(url):
            return ats_name
    return None


def crawl_priority_from_jobs(num_of_jobs) -> int:
    """Map job count to priority (1 = highest, 10 = lowest)."""
    try:
        n = int(num_of_jobs)
    except (TypeError, ValueError):
        return 5
    if n >= 100:
        return 1
    if n >= 20:
        return 2
    if n >= 5:
        return 3
    return 5


def clean_value(val: str | None) -> str | None:
    """Normalise empty/null CSV values to None."""
    if val is None:
        return None
    stripped = val.strip()
    if stripped in ("", "null", "NULL", "None", "---\n- ''", "---\n- \"\""):
        return None
    return stripped


def parse_yaml_list(val: str | None) -> list | None:
    """Parse a YAML-formatted list field. Returns list or None."""
    cleaned = clean_value(val)
    if cleaned is None:
        return None
    try:
        parsed = yaml.safe_load(cleaned)
        if isinstance(parsed, list):
            # Filter out empty strings
            return [item for item in parsed if item] or None
        return None
    except Exception:
        # Store as single-element list if it looks like a selector
        if cleaned.startswith(".") or cleaned.startswith("/"):
            return [cleaned]
        return None


def parse_json_array(val: str | None) -> list | None:
    """Parse a JSON array field. Returns list or None."""
    cleaned = clean_value(val)
    if cleaned is None:
        return None
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, list):
            return [item for item in parsed if item] or None
        return None
    except Exception:
        return None


def parse_json_object(val: str | None) -> dict | None:
    """Parse a JSON object field. Returns dict or None."""
    cleaned = clean_value(val)
    if cleaned is None:
        return None
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
        return None
    except Exception:
        return None


def build_listing_selectors(wrapper: dict) -> dict:
    """Build the JSONB selectors dict for a listing_page template."""
    selectors = {}

    # Simple string fields
    simple_fields = [
        "min_container_path",
        "record_boundary_path",
        "job_title_path",
        "job_title_url_pattern",
        "row_listed_date_path",
        "row_source_path",
        "row_closing_date_path",
        "row_apply_url_path",
        "row_details_page_link_path",
        "next_page_path",
        "row_script",
        "date_format",
        "internal_id_path",
        "row_internal_id_script",
        "row_url_script",
        "row_apply_email_path",
        "row_description_path",
        "row_description_node",
    ]
    for field in simple_fields:
        val = clean_value(wrapper.get(field))
        if val is not None:
            selectors[field] = val

    # YAML list fields
    yaml_fields = ["row_location_paths"]
    for field in yaml_fields:
        parsed = parse_yaml_list(wrapper.get(field))
        if parsed:
            selectors[field] = parsed

    # JSON array fields
    json_fields = [
        "row_description_paths",
        "row_salary_paths",
        "row_job_type_paths",
    ]
    for field in json_fields:
        parsed = parse_json_array(wrapper.get(field))
        if parsed:
            selectors[field] = parsed

    return selectors


def build_detail_selectors(wrapper: dict) -> dict:
    """Build the JSONB selectors dict for a detail_page template."""
    selectors = {}

    # Simple string fields
    simple_fields = [
        "details_page_job_title_path",
        "details_page_salary_path",
        "details_page_listed_date_path",
        "details_page_closing_date_path",
        "details_page_apply_url_path",
        "details_page_apply_email_path",
        "details_page_source_path",
        "details_page_min_container_path",
        "details_page_script",
    ]
    for field in simple_fields:
        val = clean_value(wrapper.get(field))
        if val is not None:
            selectors[field] = val

    # YAML list fields
    yaml_fields = ["details_page_location_paths"]
    for field in yaml_fields:
        parsed = parse_yaml_list(wrapper.get(field))
        if parsed:
            selectors[field] = parsed

    # JSON array fields
    json_fields = [
        "details_page_description_paths",
        "details_page_job_type_paths",
    ]
    for field in json_fields:
        parsed = parse_json_array(wrapper.get(field))
        if parsed:
            selectors[field] = parsed

    return selectors


def has_detail_fields(selectors: dict) -> bool:
    """Check if detail selectors dict has any meaningful content."""
    return len(selectors) > 0


def merge_paths_into_selectors(
    paths_json: dict | None,
    listing_selectors: dict,
    detail_selectors: dict,
):
    """Merge the `paths` JSON column fields into the appropriate template selectors."""
    if not paths_json:
        return

    # Listing-page fields from paths
    listing_keys = [
        "job_count_path",
        "job_count_type",
        "job_count_regex",
        "inline_description",
        "row_raw_location_type",
        "row_locations_separator",
    ]
    for key in listing_keys:
        val = paths_json.get(key)
        if val is not None and val != "" and val is not False:
            listing_selectors[key] = val

    # Detail-page fields from paths
    detail_keys = [
        "details_page_salary_paths",
        "details_page_internal_id_path",
        "details_page_raw_location_type",
        "details_page_locations_separator",
        "details_page_sleep",
    ]
    for key in detail_keys:
        val = paths_json.get(key)
        if val is not None and val != "" and val != [] and val is not False:
            detail_selectors[key] = val

    # Boolean flags go to listing
    bool_keys = [
        "dynamic_bidding",
        "skip_label_detection",
        "skip_details_page_iframe_detection",
        "skip_readability_clean_conditionally",
    ]
    for key in bool_keys:
        val = paths_json.get(key)
        if val is True:
            listing_selectors[key] = True


# ---------------------------------------------------------------------------
# CSV Loading
# ---------------------------------------------------------------------------


def load_csv(filepath: str) -> list[dict]:
    """Load a CSV into a list of dicts."""
    log.info(f"Loading {filepath}...")
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    log.info(f"  Loaded {len(rows)} rows from {os.path.basename(filepath)}")
    return rows


def build_joined_records(csv_dir: str) -> list[dict]:
    """Load all 4 CSVs, join them, and return the merged records."""
    wrappers = load_csv(os.path.join(csv_dir, "site_wrappers.csv"))
    crawlers = load_csv(os.path.join(csv_dir, "crawlers.csv"))
    job_sites = load_csv(os.path.join(csv_dir, "job_sites.csv"))
    site_urls = load_csv(os.path.join(csv_dir, "site_urls.csv"))

    # Build lookup maps
    crawler_map = {row["id"]: row for row in crawlers}
    job_site_map = {row["id"]: row for row in job_sites}
    url_map = {}  # site_id → url
    for row in site_urls:
        url_map[row["site_id"]] = row["url"]

    log.info("Joining records across 4 CSVs...")

    stats = defaultdict(int)
    joined = []

    for wrapper in wrappers:
        stats["total_wrappers"] += 1
        crawler_id = wrapper.get("crawler_id")
        crawler = crawler_map.get(crawler_id)
        if not crawler:
            stats["skip_no_crawler"] += 1
            continue

        job_site_id = crawler.get("job_site_id")
        job_site = job_site_map.get(job_site_id)
        if not job_site:
            stats["skip_no_job_site"] += 1
            continue

        url = url_map.get(job_site_id)
        if not url:
            stats["skip_no_url"] += 1
            continue

        # Filter: no file:// URLs
        if url.startswith("file://"):
            stats["skip_file_url"] += 1
            continue

        # Filter: extract domain and check blocklist
        domain = extract_domain(url)
        if not domain:
            stats["skip_bad_domain"] += 1
            continue
        if is_blocked_domain(domain):
            stats["skip_blocked_domain"] += 1
            continue

        # Filter: disabled crawlers (disabled=true AND current_status != OK)
        is_disabled = clean_value(crawler.get("disabled", "false"))
        current_status = clean_value(crawler.get("current_status"))
        if is_disabled == "true" and current_status != "OK":
            stats["skip_disabled_crawler"] += 1
            continue

        # Filter: job_board site type
        site_type = clean_value(job_site.get("site_type"))
        if site_type == "job_board":
            stats["skip_job_board"] += 1
            continue

        # Filter: uncrawlable
        uncrawlable = clean_value(job_site.get("uncrawlable_reason"))
        if uncrawlable:
            stats["skip_uncrawlable"] += 1
            continue

        # Map country to market
        country = clean_value(crawler.get("country"))
        market_code = country if country in VALID_MARKETS else DEFAULT_MARKET

        joined.append({
            "wrapper": wrapper,
            "crawler": crawler,
            "job_site": job_site,
            "url": url,
            "domain": domain,
            "market_code": market_code,
        })

    stats["joined_records"] = len(joined)

    log.info("Join statistics:")
    for key, val in sorted(stats.items()):
        log.info(f"  {key}: {val}")

    return joined


# ---------------------------------------------------------------------------
# Import Logic
# ---------------------------------------------------------------------------


def run_import(csv_dir: str, dry_run: bool = False, limit: int | None = None):
    """Main import routine."""
    records = build_joined_records(csv_dir)

    if limit:
        records = records[:limit]
        log.info(f"Limited to {limit} records")

    if not records:
        log.warning("No records to import after filtering.")
        return

    # De-duplicate by domain — keep the record with the highest num_of_jobs
    domain_groups: dict[str, list[dict]] = defaultdict(list)
    for rec in records:
        domain_groups[rec["domain"]].append(rec)

    best_per_domain: dict[str, dict] = {}
    for domain, recs in domain_groups.items():
        best = max(recs, key=lambda r: _safe_int(r["job_site"].get("num_of_jobs")))
        best_per_domain[domain] = best

    # Group all records by URL (not domain+url) to handle the URL unique constraint.
    # Multiple domains can map to the same URL; we keep the first occurrence.
    url_groups: dict[str, list[dict]] = defaultdict(list)
    for rec in records:
        url_groups[rec["url"]].append(rec)

    log.info(f"Unique domains: {len(best_per_domain)}")
    log.info(f"Unique URLs: {len(url_groups)}")

    conn = get_db_connection()
    conn.autocommit = False
    cur = conn.cursor()

    stats = defaultdict(int)

    try:
        # Acquire an advisory lock to prevent deadlocks with concurrent processes
        cur.execute("SELECT pg_advisory_lock(hashtext('import_site_wrappers'))")

        # =====================================================================
        # Phase 1: Upsert companies
        # =====================================================================
        log.info("Phase 1: Upserting companies...")
        company_rows = []
        for domain, rec in best_per_domain.items():
            company_rows.append({
                "id": str(uuid.uuid4()),
                "name": rec["job_site"].get("name", domain),
                "domain": domain,
                "root_url": rec["url"],
                "market_code": rec["market_code"],
                "discovered_via": "site_wrapper_import",
                "ats_platform": detect_ats(rec["url"]),
                "crawl_priority": crawl_priority_from_jobs(rec["job_site"].get("num_of_jobs")),
                "crawl_frequency_hours": 24,
                "is_active": True,
                "requires_js_rendering": False,
                "company_status": "ok",
            })

        company_sql = """
            INSERT INTO companies (
                id, name, domain, root_url, market_code, discovered_via,
                ats_platform, crawl_priority, crawl_frequency_hours,
                is_active, requires_js_rendering, company_status
            ) VALUES (
                %(id)s, %(name)s, %(domain)s, %(root_url)s, %(market_code)s,
                %(discovered_via)s, %(ats_platform)s, %(crawl_priority)s,
                %(crawl_frequency_hours)s, %(is_active)s, %(requires_js_rendering)s,
                %(company_status)s
            )
            ON CONFLICT (domain) DO NOTHING
        """

        for i in range(0, len(company_rows), BATCH_SIZE):
            batch = company_rows[i : i + BATCH_SIZE]
            psycopg2.extras.execute_batch(cur, company_sql, batch, page_size=BATCH_SIZE)
        if not dry_run:
            conn.commit()

        # Count how many were actually inserted
        cur.execute(
            "SELECT count(*) FROM companies WHERE discovered_via = 'site_wrapper_import'"
        )
        total_wrapper_companies = cur.fetchone()[0]
        log.info(f"  Companies with discovered_via='site_wrapper_import': {total_wrapper_companies}")

        # =====================================================================
        # Build domain → company_id lookup
        # =====================================================================
        log.info("Building domain → company_id lookup...")
        all_domains = list(best_per_domain.keys())
        domain_to_company = {}

        for i in range(0, len(all_domains), BATCH_SIZE):
            batch = all_domains[i : i + BATCH_SIZE]
            placeholders = ",".join(["%s"] * len(batch))
            cur.execute(
                f"SELECT domain, id FROM companies WHERE domain IN ({placeholders})",
                batch,
            )
            for row in cur.fetchall():
                domain_to_company[row[0]] = row[1]

        stats["companies_matched"] = len(domain_to_company)
        stats["companies_prepared"] = len(company_rows)
        log.info(f"  Matched {len(domain_to_company)} companies in DB")

        # =====================================================================
        # Phase 2: Upsert career pages (deduplicated by URL)
        # =====================================================================
        log.info("Phase 2: Upserting career pages...")
        career_page_rows = []
        seen_urls = set()

        for url, recs in url_groups.items():
            if url in seen_urls:
                continue
            seen_urls.add(url)

            # Pick the record whose domain matched a company
            rec = None
            company_id = None
            for r in recs:
                cid = domain_to_company.get(r["domain"])
                if cid:
                    rec = r
                    company_id = cid
                    break
            if not rec or not company_id:
                stats["skip_no_company_match"] += 1
                continue

            wrapper = rec["wrapper"]
            next_page = clean_value(wrapper.get("next_page_path"))
            is_paginated = next_page is not None

            career_page_rows.append({
                "id": str(uuid.uuid4()),
                "company_id": str(company_id),
                "url": url,
                "page_type": "listing_page",
                "discovery_method": "site_wrapper_import",
                "discovery_confidence": 1.0,
                "is_primary": True,
                "is_paginated": is_paginated,
                "pagination_type": "next_page" if is_paginated else None,
                "pagination_selector": next_page,
                "requires_js_rendering": False,
                "is_active": True,
                "site_status": "ok",
            })

        # No unique constraint on url — check for existing pages and skip duplicates
        existing_urls = set()
        for i in range(0, len(career_page_rows), BATCH_SIZE):
            batch = [r["url"] for r in career_page_rows[i : i + BATCH_SIZE]]
            placeholders = ",".join(["%s"] * len(batch))
            cur.execute(
                f"SELECT url FROM career_pages WHERE url IN ({placeholders})", batch
            )
            for row in cur.fetchall():
                existing_urls.add(row[0])

        new_career_pages = [r for r in career_page_rows if r["url"] not in existing_urls]
        log.info(f"  {len(existing_urls)} URLs already exist, inserting {len(new_career_pages)} new pages")

        career_page_sql = """
            INSERT INTO career_pages (
                id, company_id, url, page_type, discovery_method,
                discovery_confidence, is_primary, is_paginated,
                pagination_type, pagination_selector,
                requires_js_rendering, is_active, site_status
            ) VALUES (
                %(id)s, %(company_id)s, %(url)s, %(page_type)s, %(discovery_method)s,
                %(discovery_confidence)s, %(is_primary)s, %(is_paginated)s,
                %(pagination_type)s, %(pagination_selector)s,
                %(requires_js_rendering)s, %(is_active)s, %(site_status)s
            )
        """

        for i in range(0, len(new_career_pages), BATCH_SIZE):
            batch = new_career_pages[i : i + BATCH_SIZE]
            psycopg2.extras.execute_batch(cur, career_page_sql, batch, page_size=BATCH_SIZE)
        if not dry_run:
            conn.commit()

        stats["career_pages_prepared"] = len(career_page_rows)
        stats["career_pages_new"] = len(new_career_pages)
        stats["career_pages_existing"] = len(existing_urls)

        # =====================================================================
        # Build url → career_page_id lookup
        # =====================================================================
        log.info("Building url → career_page_id lookup...")
        all_urls = [r["url"] for r in career_page_rows]
        url_to_page = {}

        for i in range(0, len(all_urls), BATCH_SIZE):
            batch = all_urls[i : i + BATCH_SIZE]
            placeholders = ",".join(["%s"] * len(batch))
            cur.execute(
                f"SELECT url, id, company_id FROM career_pages WHERE url IN ({placeholders})",
                batch,
            )
            for row in cur.fetchall():
                url_to_page[row[0]] = {"id": row[1], "company_id": row[2]}

        stats["career_pages_matched"] = len(url_to_page)
        log.info(f"  Matched {len(url_to_page)} career pages in DB")

        # =====================================================================
        # Phase 3: Create site_templates
        # =====================================================================
        log.info("Phase 3: Creating site templates...")
        template_rows = []

        for url, recs in url_groups.items():
            page_info = url_to_page.get(url)
            if not page_info:
                stats["skip_no_page_match"] += 1
                continue

            company_id = str(page_info["company_id"])
            career_page_id = str(page_info["id"])

            # Use the first wrapper for this URL
            wrapper = recs[0]["wrapper"]

            # Build listing page selectors
            listing_sel = build_listing_selectors(wrapper)

            # Build detail page selectors
            detail_sel = build_detail_selectors(wrapper)

            # Parse and merge the `paths` JSON field
            paths_json = parse_json_object(wrapper.get("paths"))
            merge_paths_into_selectors(paths_json, listing_sel, detail_sel)

            # Create listing template if it has content
            if listing_sel:
                template_rows.append({
                    "id": str(uuid.uuid4()),
                    "company_id": company_id,
                    "career_page_id": career_page_id,
                    "template_type": "listing_page",
                    "selectors": json.dumps(listing_sel),
                    "learned_via": "site_wrapper_import",
                    "accuracy_score": None,
                    "is_active": True,
                })
                stats["listing_templates_created"] += 1

            # Create detail template if it has content
            if has_detail_fields(detail_sel):
                template_rows.append({
                    "id": str(uuid.uuid4()),
                    "company_id": company_id,
                    "career_page_id": career_page_id,
                    "template_type": "detail_page",
                    "selectors": json.dumps(detail_sel),
                    "learned_via": "site_wrapper_import",
                    "accuracy_score": None,
                    "is_active": True,
                })
                stats["detail_templates_created"] += 1

        template_sql = """
            INSERT INTO site_templates (
                id, company_id, career_page_id, template_type,
                selectors, learned_via, accuracy_score, is_active
            ) VALUES (
                %(id)s, %(company_id)s, %(career_page_id)s, %(template_type)s,
                %(selectors)s, %(learned_via)s, %(accuracy_score)s, %(is_active)s
            )
        """

        for i in range(0, len(template_rows), BATCH_SIZE):
            batch = template_rows[i : i + BATCH_SIZE]
            psycopg2.extras.execute_batch(cur, template_sql, batch, page_size=BATCH_SIZE)
        if not dry_run:
            conn.commit()

        stats["templates_total"] = len(template_rows)

        # =====================================================================
        # Phase 4: Refresh materialized view if it exists
        # =====================================================================
        log.info("Phase 4: Refreshing materialized views...")
        try:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY company_stats")
            if not dry_run:
                conn.commit()
            log.info("  Refreshed company_stats materialized view")
        except psycopg2.Error:
            conn.rollback()
            log.info("  company_stats materialized view does not exist, skipping")

        # =====================================================================
        # Final: rollback if dry run
        # =====================================================================
        if dry_run:
            log.info("DRY RUN — rolling back all changes")
            conn.rollback()
        else:
            log.info("All changes committed")

        # Release advisory lock
        cur.execute("SELECT pg_advisory_unlock(hashtext('import_site_wrappers'))")
        conn.commit()

        # --- Print final stats ---
        log.info("")
        log.info("=" * 60)
        log.info("IMPORT COMPLETE — Summary")
        log.info("=" * 60)
        log.info(f"  Companies prepared:           {stats['companies_prepared']}")
        log.info(f"  Companies matched in DB:       {stats['companies_matched']}")
        log.info(f"  Career pages prepared:         {stats['career_pages_prepared']}")
        log.info(f"  Career pages matched in DB:    {stats['career_pages_matched']}")
        log.info(f"  Listing templates created:     {stats.get('listing_templates_created', 0)}")
        log.info(f"  Detail templates created:      {stats.get('detail_templates_created', 0)}")
        log.info(f"  Templates total:               {stats.get('templates_total', 0)}")
        log.info(f"  Skipped (no company match):    {stats.get('skip_no_company_match', 0)}")
        log.info(f"  Skipped (no page match):       {stats.get('skip_no_page_match', 0)}")
        if dry_run:
            log.info("  MODE: DRY RUN (no data written)")
        log.info("=" * 60)

    except Exception:
        conn.rollback()
        log.exception("Import failed, rolled back")
        raise
    finally:
        cur.close()
        conn.close()


def _safe_int(val) -> int:
    """Safely convert a value to int, defaulting to 0."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Import site wrapper configs from CSV exports into JobHarvest DB"
    )
    parser.add_argument(
        "--csv-dir",
        default=os.path.join(os.path.dirname(__file__), "..", ".."),
        help="Directory containing the 4 CSV files (default: project root)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be imported without committing to the database",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of joined records to process",
    )
    args = parser.parse_args()

    csv_dir = os.path.abspath(args.csv_dir)
    log.info(f"CSV directory: {csv_dir}")

    # Verify CSV files exist
    required_files = [
        "site_wrappers.csv",
        "crawlers.csv",
        "job_sites.csv",
        "site_urls.csv",
    ]
    for fname in required_files:
        path = os.path.join(csv_dir, fname)
        if not os.path.exists(path):
            log.error(f"Missing required CSV file: {path}")
            sys.exit(1)

    run_import(csv_dir, dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()
