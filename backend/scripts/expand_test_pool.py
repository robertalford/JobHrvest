#!/usr/bin/env python3
"""
Expand the fixed_test_sites pool by validating untested sites from the Jobstream data.

Runs continuously in the background, picking batches of untested sites, validating
them (can the baseline selectors extract real jobs?), and adding working ones to
fixed_test_sites. Saves progress so it can be stopped and resumed.

Run: nohup python3 -B -u backend/scripts/expand_test_pool.py &
"""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 20       # Sites per batch
CONCURRENCY = 5       # Parallel validations
PAUSE_BETWEEN = 10    # Seconds between batches
PROGRESS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "storage", "expand_pool_progress.json",
)

# Add project to path
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_DIR)


def _load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"tested_urls": [], "added": 0, "failed": 0, "total_tested": 0}


def _save_progress(progress: dict):
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2, default=str)


async def validate_site(url: str, company: str, selectors: dict, semaphore: asyncio.Semaphore) -> dict:
    """Validate a single site: can we extract real jobs using the known selectors?"""
    import httpx
    from app.crawlers.job_extractor import JobExtractor

    result = {
        "url": url,
        "company": company,
        "status": "unknown",
        "jobs_total": 0,
        "jobs_complete": 0,
        "method": "http",
    }

    async with semaphore:
        try:
            # Pass 1: Plain HTTP
            html = ""
            async with httpx.AsyncClient(
                timeout=15, follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            ) as client:
                try:
                    resp = await client.get(url)
                    html = resp.text or ""
                except Exception as e:
                    result["status"] = "http_error"
                    result["error"] = str(e)[:80]
                    return result

            if len(html) < 100:
                result["status"] = "empty_page"
                return result

            jobs = JobExtractor._static_extract_wrapper(html, url, selectors)
            if jobs:
                result["method"] = "http"
                result["jobs_total"] = len(jobs)
                result["jobs_complete"] = sum(
                    1 for j in jobs
                    if j.get("title") and len(j["title"]) > 2
                    and j.get("source_url") and len(j["source_url"]) > 5
                )
                result["status"] = "success" if result["jobs_total"] > 0 else "no_jobs"
                return result

            # Pass 2: Playwright rendering
            try:
                from playwright.async_api import async_playwright
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    page = await browser.new_page(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                    )
                    try:
                        await page.goto(url, wait_until="networkidle", timeout=25000)
                        await page.wait_for_timeout(5000)
                        # Cookie dismissal
                        for sel in ["button:has-text('Accept')", "button:has-text('OK')", "[class*=consent] button"]:
                            try:
                                btn = page.locator(sel).first
                                if await btn.is_visible(timeout=1000):
                                    await btn.click()
                                    await page.wait_for_timeout(500)
                                    break
                            except Exception:
                                pass
                        pw_html = await page.content()
                        result["method"] = "playwright"
                    finally:
                        await browser.close()

                    if pw_html and len(pw_html) > 200:
                        jobs = JobExtractor._static_extract_wrapper(pw_html, url, selectors)
                        if jobs:
                            result["jobs_total"] = len(jobs)
                            result["jobs_complete"] = sum(
                                1 for j in jobs
                                if j.get("title") and len(j["title"]) > 2
                                and j.get("source_url") and len(j["source_url"]) > 5
                            )
                            result["status"] = "success" if result["jobs_total"] > 0 else "no_jobs"
                            return result
            except Exception:
                pass

            result["status"] = "extraction_failed"
            return result

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)[:120]
            return result


async def run_batch(batch: list, semaphore: asyncio.Semaphore) -> list:
    """Validate a batch of sites concurrently."""
    tasks = []
    for url, company, sels_raw in batch:
        sels = sels_raw if isinstance(sels_raw, dict) else (json.loads(sels_raw or "{}") if sels_raw else {})
        tasks.append(validate_site(url, company, sels, semaphore))
    return await asyncio.gather(*tasks)


async def main():
    from app.db.base import AsyncSessionLocal
    from sqlalchemy import text

    progress = _load_progress()
    tested_urls = set(progress.get("tested_urls", []))
    total_added = progress.get("added", 0)
    total_failed = progress.get("failed", 0)
    total_tested = progress.get("total_tested", 0)

    logger.info(f"Starting pool expansion. Already tested: {total_tested}, added: {total_added}")

    semaphore = asyncio.Semaphore(CONCURRENCY)

    while True:
        # Fetch next batch of untested sites
        async with AsyncSessionLocal() as db:
            # Get sites NOT already in fixed_test_sites and NOT already tested
            placeholders = ",".join(f"'{u}'" for u in list(tested_urls)[-5000:]) if tested_urls else "''"
            q = text(f"""
                SELECT su.url, js.name as company_name, sw.selectors as known_selectors
                FROM site_url_test_data su
                JOIN job_site_test_data js ON js.external_id = su.site_id
                JOIN crawler_test_data ct ON ct.job_site_id = su.site_id
                JOIN site_wrapper_test_data sw ON sw.crawler_id = ct.external_id
                WHERE su.url LIKE 'http%'
                  AND su.url NOT LIKE 'file://%'
                  AND js.site_type IN ('employer', 'recruiter')
                  AND (js.uncrawlable_reason IS NULL OR js.uncrawlable_reason IN ('', 'null'))
                  AND su.url NOT IN (SELECT url FROM fixed_test_sites)
                  AND su.url NOT IN ({placeholders})
                ORDER BY random()
                LIMIT {BATCH_SIZE}
            """)
            result = await db.execute(q)
            batch = result.fetchall()

        if not batch:
            logger.info("No more untested sites. Pool expansion complete!")
            break

        # Validate batch
        start = time.time()
        results = await run_batch(batch, semaphore)
        elapsed = time.time() - start

        # Process results
        batch_added = 0
        batch_failed = 0
        for r in results:
            tested_urls.add(r["url"])
            total_tested += 1

            if r["status"] == "success" and r["jobs_total"] >= 1:
                # Add to fixed_test_sites
                try:
                    async with AsyncSessionLocal() as db:
                        # Find the selectors for this site
                        sel_row = await db.execute(
                            text("""
                                SELECT sw.selectors FROM site_url_test_data su
                                JOIN crawler_test_data ct ON ct.job_site_id = su.site_id
                                JOIN site_wrapper_test_data sw ON sw.crawler_id = ct.external_id
                                WHERE su.url = :url LIMIT 1
                            """),
                            {"url": r["url"]},
                        )
                        sel = sel_row.fetchone()
                        if sel:
                            await db.execute(
                                text("""
                                    INSERT INTO fixed_test_sites (url, company_name, known_selectors)
                                    VALUES (:url, :company, :selectors)
                                    ON CONFLICT DO NOTHING
                                """),
                                {"url": r["url"], "company": r["company"], "selectors": json.dumps(sel[0]) if isinstance(sel[0], dict) else sel[0]},
                            )
                            await db.commit()
                            batch_added += 1
                            total_added += 1
                except Exception as e:
                    logger.warning(f"Failed to insert {r['url']}: {e}")
            else:
                batch_failed += 1
                total_failed += 1

        # Save progress
        # Keep only last 10000 tested URLs in progress file to prevent bloat
        progress = {
            "tested_urls": list(tested_urls)[-10000:],
            "added": total_added,
            "failed": total_failed,
            "total_tested": total_tested,
            "last_batch_at": datetime.now(timezone.utc).isoformat(),
        }
        _save_progress(progress)

        # Get current pool size
        async with AsyncSessionLocal() as db:
            count = await db.execute(text("SELECT count(*) FROM fixed_test_sites"))
            pool_size = count.scalar()

        logger.info(
            f"Batch done in {elapsed:.0f}s: +{batch_added} added, {batch_failed} failed | "
            f"Pool: {pool_size} | Tested: {total_tested} total ({total_added} added, {total_failed} failed)"
        )

        # Pause between batches
        await asyncio.sleep(PAUSE_BETWEEN)


if __name__ == "__main__":
    asyncio.run(main())
