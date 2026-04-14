"""
Find 300 test sites that produce complete job extractions (all 4 core fields).

Mimics production Capybara behavior:
  - Sites with sleeper/navigator steps → Playwright rendering (like Capybara headless Chrome)
  - Simple sites → HTTP fetch first, Playwright fallback
  - Detail page follow for description/location enrichment using detail-page selectors

A site "works" if it produces ≥1 job with: title, source_url, location_raw, description(>50ch).
"""

import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

TARGET = 99999  # Test ALL sites — no limit
CONCURRENCY = 20        # total concurrent sites
PW_CONCURRENCY = 6      # max simultaneous Playwright browsers
OUTPUT_FILE = "/storage/working_test_sites.json"
PROGRESS_FILE = "/storage/working_sites_progress.json"


async def _fetch_html(url: str, steps: list, pw_sem: asyncio.Semaphore) -> tuple[str, str]:
    """Fetch page HTML using the right rendering strategy based on crawl steps.
    Production Capybara always uses a browser — we use Playwright for sites with
    sleeper/navigator steps, plain HTTP for simple url_opener-only sites."""
    import httpx

    needs_browser = any(
        s["name"] in ("sleeper", "link_navigator", "frame_switcher", "form_locator", "form_submitter")
        for s in steps
    )

    # Simple sites: try HTTP first
    if not needs_browser:
        try:
            async with httpx.AsyncClient(
                timeout=12, follow_redirects=True, verify=False,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            ) as client:
                resp = await client.get(url)
                if resp.status_code == 200 and len(resp.text or "") >= 200:
                    return resp.text, "http"
        except Exception:
            pass

    # Browser rendering (always for sites with steps, fallback for simple sites)
    async with pw_sem:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                try:
                    await page.goto(url, wait_until="networkidle", timeout=25000)

                    # Execute crawl steps (mimic Capybara)
                    for step in steps:
                        name = step["name"]
                        opts = step.get("options") or {}
                        if isinstance(opts, str):
                            try:
                                opts = json.loads(opts)
                            except Exception:
                                opts = {}

                        if name == "sleeper":
                            secs = 5
                            try:
                                secs = int(opts.get("seconds", 5))
                            except (ValueError, TypeError):
                                pass
                            await page.wait_for_timeout(min(secs, 12) * 1000)

                        elif name == "link_navigator":
                            sel = opts.get("selector", "")
                            if sel:
                                try:
                                    loc = page.locator(f"xpath={sel}").first if (sel.startswith("//") or sel.startswith("/html")) else page.locator(sel).first
                                    if await loc.is_visible(timeout=3000):
                                        await loc.click()
                                        await page.wait_for_timeout(2000)
                                except Exception:
                                    pass

                        elif name == "form_locator":
                            fp = opts.get("form_path", "")
                            if fp:
                                try:
                                    loc = page.locator(f"xpath={fp}").first if fp.startswith("//") else page.locator(fp).first
                                    if await loc.is_visible(timeout=2000):
                                        await loc.click()
                                        await page.wait_for_timeout(500)
                                except Exception:
                                    pass

                        elif name == "form_submitter":
                            sub_sel = opts.get("submit_button_selector", "")
                            try:
                                if sub_sel:
                                    btn = page.locator(f"xpath={sub_sel}").first if sub_sel.startswith("//") else page.locator(sub_sel).first
                                else:
                                    btn = page.locator("button[type=submit], input[type=submit]").first
                                if await btn.is_visible(timeout=2000):
                                    await btn.click()
                                    await page.wait_for_timeout(3000)
                            except Exception:
                                pass

                        elif name == "frame_switcher":
                            fp = opts.get("frame_path", "")
                            if fp:
                                try:
                                    loc = page.locator(f"xpath={fp}").first if fp.startswith("//") else page.locator(fp).first
                                    frame = await loc.content_frame()
                                    if frame:
                                        page = frame
                                except Exception:
                                    pass

                    # Cookie dismissal
                    for sel in ["button:has-text('Accept')", "button:has-text('OK')", "[class*=consent] button"]:
                        try:
                            btn = page.locator(sel).first
                            if await btn.is_visible(timeout=600):
                                await btn.click()
                                await page.wait_for_timeout(300)
                                break
                        except Exception:
                            pass

                    # Extra wait if no sleeper step
                    if not any(s["name"] == "sleeper" for s in steps):
                        await page.wait_for_timeout(2000)

                    return await page.content() or "", "playwright"
                finally:
                    await browser.close()
        except Exception as e:
            return "", f"pw_error:{str(e)[:40]}"


def _extract_from_detail(html: str, url: str, selectors: dict) -> dict:
    """Extract fields from a job detail page using the wrapper's detail-page selectors."""
    from lxml import etree
    from app.crawlers.job_extractor import JobExtractor
    _parse = JobExtractor._parse_selector_paths

    detail_desc = _parse(selectors.get("details_page_description_paths", []))
    detail_loc = _parse(selectors.get("details_page_location_paths", []))
    detail_sal = _parse(selectors.get("details_page_salary_path", ""))
    detail_type = _parse(selectors.get("details_page_job_type_paths", []))

    result = {}
    try:
        parser = etree.HTMLParser(encoding="utf-8")
        tree = etree.fromstring(html.encode("utf-8", errors="replace"), parser)
    except Exception:
        return result

    def _try(paths):
        for sel in paths:
            if not sel or sel in ("null", ""):
                continue
            try:
                is_xp = sel.startswith("//") or sel.startswith(".//") or sel.startswith("(")
                els = tree.xpath(sel) if is_xp else tree.cssselect(sel)
                if els:
                    txt = etree.tostring(els[0], method="text", encoding="unicode").strip()
                    if txt and len(txt) > 1:
                        return txt
            except Exception:
                continue
        return None

    d = _try(detail_desc)
    if d and len(d) > 50:
        result["description"] = d[:5000]
    loc = _try(detail_loc)
    if loc and 1 < len(loc) < 200:
        result["location_raw"] = loc
    sal = _try(detail_sal)
    if sal:
        result["salary_raw"] = sal
    jt = _try(detail_type)
    if jt:
        result["employment_type"] = jt
    return result


async def validate_site(url, company, selectors, steps, sem, pw_sem) -> dict:
    """Validate one site: fetch (with steps), extract listing, follow detail pages."""
    import httpx
    from app.crawlers.job_extractor import JobExtractor
    _parse = JobExtractor._parse_selector_paths

    r = {"url": url, "company": company, "passed": False, "jobs_total": 0,
         "jobs_complete": 0, "method": "", "error": None}

    async with sem:
        try:
            html, method = await _fetch_html(url, steps, pw_sem)
            r["method"] = method
            if not html or len(html) < 200:
                r["error"] = f"Empty ({len(html)}b, {method})"
                return r

            jobs = JobExtractor._static_extract_wrapper(html, url, selectors)
            if not jobs:
                r["error"] = f"0 jobs ({method}, {len(html)}b HTML)"
                return r

            r["jobs_total"] = len(jobs)

            # Detail page enrichment
            detail_desc = _parse(selectors.get("details_page_description_paths", []))
            detail_loc = _parse(selectors.get("details_page_location_paths", []))
            has_detail = bool(detail_desc or detail_loc)

            complete = 0
            async with httpx.AsyncClient(timeout=8, follow_redirects=True, verify=False,
                    headers={"User-Agent": "Mozilla/5.0"}) as client:
                for job in jobs[:5]:
                    t = job.get("title", "")
                    su = job.get("source_url", "")
                    loc = job.get("location_raw", "")
                    desc = job.get("description", "")

                    if (t and len(t) > 2 and su and len(su) > 5 and su != url
                            and loc and len(loc) > 1 and desc and len(desc) > 50):
                        complete += 1
                        continue

                    if has_detail and su and su != url:
                        try:
                            dr = await client.get(su)
                            if dr.status_code == 200 and len(dr.text) > 200:
                                df = _extract_from_detail(dr.text, su, selectors)
                                if not loc and df.get("location_raw"):
                                    loc = df["location_raw"]
                                if (not desc or len(desc) < 50) and df.get("description"):
                                    desc = df["description"]
                        except Exception:
                            pass

                    if (t and len(t) > 2 and su and len(su) > 5 and su != url
                            and loc and len(loc) > 1 and desc and len(desc) > 50):
                        complete += 1
                    if complete >= 1:
                        break

            r["jobs_complete"] = complete
            r["passed"] = complete >= 1
            if not r["passed"]:
                r["error"] = f"{len(jobs)} jobs, 0 complete"
            return r
        except Exception as e:
            r["error"] = str(e)[:80]
            return r


async def main():
    from app.db.base import AsyncSessionLocal
    from sqlalchemy import text

    print(f"Finding {TARGET} working sites (4-field: title+url+loc+desc)...")
    print(f"Using Playwright for JS-rendered sites (like production Capybara)")
    print(f"Concurrency: {CONCURRENCY} sites, {PW_CONCURRENCY} browsers")
    print(f"Started at {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")
    print()

    # Load ALL eligible sites with their crawl steps
    async with AsyncSessionLocal() as db:
        r = await db.execute(text("""
            SELECT su.url, js.name, sw.selectors
            FROM site_url_test_data su
            JOIN job_site_test_data js ON js.external_id = su.site_id
            JOIN crawler_test_data ct ON ct.job_site_id = su.site_id
            JOIN site_wrapper_test_data sw ON sw.crawler_id = ct.external_id
            WHERE su.url LIKE 'http%'
              AND su.url NOT LIKE 'file://%'
              AND js.site_type IN ('employer', 'recruiter')
              AND (js.uncrawlable_reason IS NULL OR js.uncrawlable_reason IN ('', 'null'))
              AND sw.selectors IS NOT NULL
              AND sw.selectors->>'record_boundary_path' IS NOT NULL
              AND sw.selectors->>'record_boundary_path' NOT IN ('', 'null')
              AND sw.selectors->>'job_title_path' IS NOT NULL
              AND sw.selectors->>'job_title_path' NOT IN ('', 'null')
            ORDER BY random()
        """))
        all_sites = []
        for url, name, sels_raw in r.fetchall():
            sels = sels_raw if isinstance(sels_raw, dict) else (json.loads(sels_raw or "{}") if sels_raw else {})
            all_sites.append((url, name, sels))

        # Load crawl steps for all sites in batch
        steps_r = await db.execute(text("""
            SELECT su.url, cs.step_name, cs.step_index, cs.options
            FROM crawl_steps_test_data cs
            JOIN crawler_test_data ct ON ct.external_id = cs.crawler_id
            JOIN site_url_test_data su ON su.site_id = ct.job_site_id
            ORDER BY su.url, cs.step_index
        """))
        steps_by_url = defaultdict(list)
        for surl, sname, sidx, sopts in steps_r.fetchall():
            opts = sopts if isinstance(sopts, dict) else (json.loads(sopts or "{}") if sopts else {})
            steps_by_url[surl].append({"name": sname, "index": sidx, "options": opts})

    print(f"Loaded {len(all_sites)} candidate sites, {len(steps_by_url)} with crawl steps")
    print()

    sem = asyncio.Semaphore(CONCURRENCY)
    pw_sem = asyncio.Semaphore(PW_CONCURRENCY)
    start = time.time()

    # Resume from previous run — load already-found sites
    passed = []
    already_found_urls = set()
    try:
        with open(OUTPUT_FILE) as f:
            prev = json.load(f)
        for s in prev.get("passed_sites", []):
            passed.append({"url": s["url"], "company": s["company"], "jobs_total": s.get("jobs_total", 0),
                           "jobs_complete": s.get("jobs_complete", 0), "method": s.get("method", "")})
            already_found_urls.add(s["url"])
        print(f"Resumed with {len(passed)} previously found sites")
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    # Skip sites already found
    all_sites = [s for s in all_sites if s[0] not in already_found_urls]
    print(f"Remaining candidates after skipping found: {len(all_sites)}")

    failed = []

    batch_size = 50
    for batch_start in range(0, len(all_sites), batch_size):
        if len(passed) >= TARGET:
            break

        batch = all_sites[batch_start:batch_start + batch_size]
        tasks = []
        for url, company, sels in batch:
            site_steps = steps_by_url.get(url, [])
            tasks.append(validate_site(url, company, sels, site_steps, sem, pw_sem))

        results = await asyncio.gather(*tasks)
        for r in results:
            if r["passed"]:
                passed.append(r)
            else:
                failed.append(r)

        tested = batch_start + len(batch)
        elapsed = time.time() - start
        rate = tested / elapsed * 60 if elapsed > 0 else 0
        pct = len(passed) / max(1, tested) * 100
        print(f"  [{tested} tested, {len(passed)}/{TARGET} passed ({pct:.0f}%), {len(failed)} failed] {rate:.0f}/min")

        with open(PROGRESS_FILE, "w") as f:
            json.dump({"updated_at": datetime.now(timezone.utc).isoformat(),
                        "target": TARGET, "tested": tested,
                        "passed": len(passed), "failed": len(failed)}, f)

        # Save passed sites incrementally so they can be used immediately
        with open(OUTPUT_FILE, "w") as f:
            json.dump({
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "target": TARGET, "tested": tested, "in_progress": True,
                "passed_count": len(passed), "failed_count": len(failed),
                "pass_rate": round(len(passed) / max(1, tested) * 100, 1),
                "passed_sites": [{"url": r["url"], "company": r["company"],
                                  "jobs_total": r["jobs_total"], "jobs_complete": r["jobs_complete"],
                                  "method": r["method"]} for r in passed],
            }, f, indent=2, default=str)

    elapsed = time.time() - start
    print(f"\nDone in {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"Tested: {len(passed) + len(failed)}, Passed: {len(passed)}, Failed: {len(failed)}")
    print(f"Pass rate: {len(passed)/max(1, len(passed)+len(failed))*100:.1f}%")

    with open(OUTPUT_FILE, "w") as f:
        json.dump({
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "target": TARGET, "tested": len(passed) + len(failed),
            "passed_count": len(passed), "failed_count": len(failed),
            "pass_rate": round(len(passed) / max(1, len(passed) + len(failed)) * 100, 1),
            "passed_sites": [{"url": r["url"], "company": r["company"],
                              "jobs_total": r["jobs_total"], "jobs_complete": r["jobs_complete"],
                              "method": r["method"]} for r in passed],
            "failed_sites": [{"url": r["url"], "company": r["company"],
                              "error": r["error"], "method": r["method"]} for r in failed[:1000]],
        }, f, indent=2, default=str)
    print(f"Results saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
