"""
Validate all fixed_test_sites by running baseline extraction on each.
Reports which sites successfully extract jobs and which fail.
Stores results in /storage/test_site_validation.json for later review.
"""

import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Increase to see per-site progress
logger.setLevel(logging.INFO)


async def validate_site(url: str, company: str, selectors: dict, semaphore: asyncio.Semaphore) -> dict:
    """Validate a single test site by running baseline extraction."""
    import httpx
    from app.crawlers.job_extractor import JobExtractor

    result = {
        "url": url,
        "company": company,
        "status": "unknown",
        "jobs_total": 0,
        "jobs_complete": 0,
        "error": None,
        "method": "http",
        "html_length": 0,
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
                    result["html_length"] = len(html)
                except Exception as e:
                    result["error"] = f"HTTP fetch failed: {str(e)[:80]}"
                    result["status"] = "http_error"
                    return result

            if len(html) < 100:
                result["error"] = f"HTML too short: {len(html)} bytes"
                result["status"] = "empty_page"
                return result

            # Try wrapper extraction on static HTML
            jobs = JobExtractor._static_extract_wrapper(html, url, selectors)
            if jobs:
                result["method"] = "http"
                result["jobs_total"] = len(jobs)
                result["jobs_complete"] = sum(
                    1 for j in jobs
                    if j.get("title") and len(j["title"]) > 2
                    and j.get("source_url") and len(j["source_url"]) > 5
                    and j.get("location_raw") and len(j["location_raw"]) > 1
                    and j.get("description") and len(j["description"]) > 50
                )
                result["status"] = "success" if result["jobs_complete"] > 0 else "incomplete_only"
                result["sample_titles"] = [j["title"][:60] for j in jobs[:3]]
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
                        result["html_length"] = len(pw_html)
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
                                and j.get("location_raw") and len(j["location_raw"]) > 1
                                and j.get("description") and len(j["description"]) > 50
                            )
                            result["status"] = "success" if result["jobs_complete"] > 0 else "incomplete_only"
                            result["sample_titles"] = [j["title"][:60] for j in jobs[:3]]
                            return result

            except Exception as e:
                result["error"] = f"Playwright failed: {str(e)[:80]}"

            # Both passes failed
            result["status"] = "extraction_failed"
            result["error"] = result.get("error") or "Wrapper selectors matched 0 jobs on both HTTP and Playwright HTML"
            return result

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)[:120]
            return result


async def main():
    from app.db.base import AsyncSessionLocal
    from sqlalchemy import text

    async with AsyncSessionLocal() as db:
        r = await db.execute(text("SELECT url, company_name, known_selectors FROM fixed_test_sites ORDER BY md5(url)"))
        rows = r.fetchall()

    total = len(rows)
    print(f"Validating {total} test sites...")
    print(f"Started at {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")
    print()

    # Run with concurrency limit (5 Playwright instances at a time)
    semaphore = asyncio.Semaphore(5)
    start = time.time()

    tasks = []
    for url, company, sels_raw in rows:
        sels = sels_raw if isinstance(sels_raw, dict) else (json.loads(sels_raw or "{}") if sels_raw else {})
        tasks.append(validate_site(url, company, sels, semaphore))

    results = []
    done = 0
    for coro in asyncio.as_completed(tasks):
        result = await coro
        results.append(result)
        done += 1
        status_icon = "✓" if result["status"] == "success" else "△" if result["status"] == "incomplete_only" else "✗"
        if done % 10 == 0 or done == total:
            elapsed = time.time() - start
            rate = done / elapsed * 60
            print(f"  [{done}/{total}] {rate:.0f} sites/min — {result['company'][:30]} {status_icon}")

    elapsed = time.time() - start
    print(f"\nCompleted in {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print()

    # Summarize
    success = [r for r in results if r["status"] == "success"]
    incomplete = [r for r in results if r["status"] == "incomplete_only"]
    failed = [r for r in results if r["status"] not in ("success", "incomplete_only")]

    print(f"=== RESULTS ===")
    print(f"  Success (complete jobs):    {len(success)}/{total} ({round(len(success)/max(1,total)*100)}%)")
    print(f"  Incomplete only:            {len(incomplete)}/{total} ({round(len(incomplete)/max(1,total)*100)}%)")
    print(f"  Failed (0 jobs):            {len(failed)}/{total} ({round(len(failed)/max(1,total)*100)}%)")
    print()

    # Method breakdown
    http_count = sum(1 for r in success + incomplete if r["method"] == "http")
    pw_count = sum(1 for r in success + incomplete if r["method"] == "playwright")
    print(f"  Extracted via HTTP:         {http_count}")
    print(f"  Extracted via Playwright:   {pw_count}")
    print()

    # Show failures
    if failed:
        print(f"=== FAILED SITES ({len(failed)}) ===")
        for r in sorted(failed, key=lambda x: x["company"]):
            print(f"  {r['company']:<35} {r['url'][:50]}")
            print(f"    Status: {r['status']}, Error: {r['error'][:80] if r['error'] else 'none'}")
        print()

    if incomplete:
        print(f"=== INCOMPLETE ONLY ({len(incomplete)}) ===")
        for r in sorted(incomplete, key=lambda x: x["company"])[:20]:
            print(f"  {r['company']:<35} total={r['jobs_total']}, complete={r['jobs_complete']}, method={r['method']}")
        if len(incomplete) > 20:
            print(f"  ... and {len(incomplete) - 20} more")

    # Save results
    output = {
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "total": total,
        "success": len(success),
        "incomplete_only": len(incomplete),
        "failed": len(failed),
        "results": results,
    }
    with open("/storage/test_site_validation.json", "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nResults saved to /storage/test_site_validation.json")


if __name__ == "__main__":
    asyncio.run(main())
