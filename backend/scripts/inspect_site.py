#!/usr/bin/env python3
"""
Site Inspector — visit a URL with headless Chrome and report what's visible.

Used by the auto-improve Codex agent to visually inspect career pages
and understand what the rendered DOM actually contains.

Usage:
    python scripts/inspect_site.py https://example.com/careers
    python scripts/inspect_site.py https://example.com/careers --screenshot /tmp/site.png
    python scripts/inspect_site.py https://example.com/careers --click-first-job
    python scripts/inspect_site.py https://example.com/careers --full-report
"""

import argparse
import asyncio
import json
import re
import sys


async def inspect(url: str, screenshot_path: str = None, click_first_job: bool = False,
                  full_report: bool = False):
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )

        print(f"Visiting: {url}")
        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
        except Exception as e:
            print(f"Navigation error: {e}")
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)

        await page.wait_for_timeout(3000)

        # Get rendered HTML
        html = await page.content()
        print(f"\nRendered HTML: {len(html):,} bytes")

        # Get visible text
        body_text = await page.inner_text("body")
        print(f"Visible text: {len(body_text):,} chars")

        # Screenshot
        if screenshot_path:
            await page.screenshot(path=screenshot_path, full_page=True)
            print(f"Screenshot saved: {screenshot_path}")

        # Count job signals
        signals = {}

        # Job-related CSS classes
        for selector in ["[class*='job']", "[class*='position']", "[class*='vacancy']",
                         "[class*='opening']", "[class*='career']", "[class*='listing']",
                         "[class*='posting']"]:
            try:
                count = len(await page.query_selector_all(selector))
                if count > 0:
                    signals[selector] = count
            except Exception:
                pass

        # Apply buttons
        for text in ["Apply", "Apply Now", "Apply Here"]:
            try:
                count = len(await page.get_by_text(text, exact=False).all())
                if count > 0:
                    signals[f'"{text}" buttons'] = count
            except Exception:
                pass

        # Read More / View Details
        for text in ["Read more", "View details", "Learn more", "View job", "See details"]:
            try:
                count = len(await page.get_by_text(text, exact=False).all())
                if count > 0:
                    signals[f'"{text}" links'] = count
            except Exception:
                pass

        print(f"\nJob signals on page:")
        for sig, count in sorted(signals.items(), key=lambda x: -x[1]):
            print(f"  {sig}: {count}")

        # Count links with job-related URLs
        links = await page.query_selector_all("a[href]")
        job_links = []
        for link in links:
            href = await link.get_attribute("href") or ""
            text = (await link.inner_text()).strip()
            if re.search(r"/(?:job|career|position|vacancy|opening|apply)", href, re.I):
                if text and len(text) > 5 and len(text) < 100:
                    job_links.append({"text": text[:60], "href": href[:100]})

        print(f"\nJob-related links: {len(job_links)}")
        for jl in job_links[:10]:
            print(f"  {jl['text']}: {jl['href']}")

        # Common job title nouns in visible text
        title_nouns = {
            "accountant", "administrator", "analyst", "architect", "assistant",
            "consultant", "coordinator", "designer", "developer", "director",
            "electrician", "engineer", "executive", "manager", "mechanic",
            "nurse", "officer", "operator", "planner", "programmer",
            "receptionist", "recruiter", "scientist", "specialist", "supervisor",
            "teacher", "technician", "therapist", "trainer", "writer",
        }
        found_nouns = set()
        text_lower = body_text.lower()
        for noun in title_nouns:
            if noun in text_lower:
                found_nouns.add(noun)
        print(f"\nJob title nouns found: {len(found_nouns)}")
        if found_nouns:
            print(f"  {', '.join(sorted(found_nouns))}")

        # First 500 chars of visible text
        print(f"\nFirst 500 chars of visible text:")
        print(f"  {body_text[:500]}")

        if full_report:
            # Repeating structures (potential job listings)
            print(f"\n--- STRUCTURAL ANALYSIS ---")
            for tag in ["li", "article", "div", "tr"]:
                els = await page.query_selector_all(tag)
                if len(els) > 3:
                    # Check if multiple elements have similar structure
                    texts = []
                    for el in els[:20]:
                        t = (await el.inner_text()).strip()
                        if 10 < len(t) < 300:
                            texts.append(t[:80])
                    if len(texts) >= 3:
                        print(f"\n  <{tag}> with short text ({len(texts)} of {len(els)}):")
                        for t in texts[:5]:
                            print(f"    {t}")

        if click_first_job:
            print(f"\n--- CLICKING FIRST JOB LINK ---")
            if job_links:
                first = job_links[0]
                print(f"Clicking: {first['text']} → {first['href']}")
                try:
                    link_el = await page.query_selector(f"a[href*='{first['href'][:30]}']")
                    if link_el:
                        await link_el.click()
                        await page.wait_for_load_state("networkidle", timeout=10000)
                        await page.wait_for_timeout(2000)

                        detail_text = await page.inner_text("body")
                        print(f"Detail page text: {len(detail_text):,} chars")

                        # Look for description
                        for sel in ["[class*='description']", "[class*='detail']",
                                    "[class*='content']", "article"]:
                            try:
                                desc_els = await page.query_selector_all(sel)
                                for d in desc_els:
                                    dt = (await d.inner_text()).strip()
                                    if len(dt) > 100:
                                        print(f"\n  Description ({sel}, {len(dt)} chars):")
                                        print(f"  {dt[:300]}...")
                                        break
                            except Exception:
                                pass

                        # Look for salary
                        salary_match = re.search(r"\$[\d,]+(?:\s*[-–]\s*\$?[\d,]+)?", detail_text)
                        if salary_match:
                            print(f"\n  Salary found: {salary_match.group()}")

                        if screenshot_path:
                            detail_ss = screenshot_path.replace(".png", "_detail.png")
                            await page.screenshot(path=detail_ss, full_page=True)
                            print(f"  Detail screenshot: {detail_ss}")
                except Exception as e:
                    print(f"  Click failed: {e}")

        await browser.close()

        # Return structured data for programmatic use
        return {
            "url": url,
            "html_length": len(html),
            "text_length": len(body_text),
            "signals": signals,
            "job_links": job_links[:20],
            "title_nouns": list(found_nouns),
        }


def main():
    parser = argparse.ArgumentParser(description="Inspect a career page with headless Chrome")
    parser.add_argument("url", help="URL to inspect")
    parser.add_argument("--screenshot", "-s", help="Save screenshot to path")
    parser.add_argument("--click-first-job", "-c", action="store_true", help="Click the first job link")
    parser.add_argument("--full-report", "-f", action="store_true", help="Full structural analysis")
    parser.add_argument("--json", "-j", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    result = asyncio.run(inspect(
        args.url,
        screenshot_path=args.screenshot,
        click_first_job=args.click_first_job,
        full_report=args.full_report,
    ))

    if args.json:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
