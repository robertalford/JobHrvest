"""
Tiered Extraction Engine v1.4 — extends v1.3 with:

1. Pagination following: detect next-page links and fetch additional pages
2. Detail page enrichment: visit individual job URLs to extract description,
   salary, requirements, and other rich fields
3. Uses CareerPageFinderV2 for discovery (ATS/subdomain awareness)

Inherits all v1.3 extraction improvements.
"""

import logging
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

from lxml import etree

from app.crawlers.tiered_extractor_v13 import TieredExtractorV13
from app.crawlers.tiered_extractor import (
    _parse_html, _text, _href, _is_valid_title, _detect_spa,
    MAX_JOBS_PER_PAGE, MIN_JOBS_FOR_SUCCESS,
    _LOCATION_CLASS_PATTERN, _SALARY_CLASS_PATTERN,
    _SALARY_PATTERN, _JOB_TYPE_PATTERN, _AU_LOCATIONS,
)

logger = logging.getLogger(__name__)

# Pagination selectors to try (ordered by commonality)
_PAGINATION_SELECTORS = [
    "a.next", "a.pagination__next", "a.page-link-next",
    "a[rel='next']", "li.next a", ".pagination .next a",
    "a.more-link", "button.load-more", "a.load-more",
    "a[aria-label='next']", "a[aria-label='Next']",
    "button[aria-label='next']",
    "a:contains('Next')", "a:contains('next')",
]

_PAGINATION_XPATHS = [
    "//a[@aria-label='next']",
    "//a[@aria-label='Next']",
    "//button[@aria-label='next']",
    "//a[contains(@class, 'next')]",
    "//a[contains(@class, 'pagination-next')]",
    "//a[span[@title='Next page of results']]",
    "//a[@class='jv-pagination-next']",
]

# Detail page selectors for rich fields
_DETAIL_DESC_SELECTORS = [
    "div.job-description", "div.description", "div#job-description",
    "[class*='description']", "[class*='job-detail']",
    "div.content", "article", "div.posting-description",
    "div.job-content", "div.vacancy-description",
    "[itemprop='description']",
]

_DETAIL_SALARY_SELECTORS = [
    "[class*='salary']", "[class*='compensation']", "[class*='pay']",
    "[itemprop='baseSalary']", "[data-testid*='salary']",
]

_DETAIL_TYPE_SELECTORS = [
    "[class*='employment-type']", "[class*='job-type']", "[class*='contract']",
    "[itemprop='employmentType']", "[class*='work-type']",
]

_DETAIL_REQUIREMENTS_SELECTORS = [
    "[class*='requirement']", "[class*='qualification']",
    "[class*='criteria']", "[class*='skill']",
]


class TieredExtractorV14(TieredExtractorV13):
    """v1.4 — adds pagination following and detail page enrichment."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        """Extract with pagination and detail page enrichment."""
        url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # Run base extraction (v1.3 with SPA detection, all tiers)
        jobs = await super().extract(career_page, company, html)

        if not jobs:
            return []

        # Phase 2: Pagination — fetch additional pages for more jobs
        jobs = await self._follow_pagination(url, html, jobs, career_page, company)

        # Phase 3: Detail page enrichment — visit job URLs for rich fields
        jobs = await self._enrich_from_detail_pages(jobs)

        return jobs[:MAX_JOBS_PER_PAGE]

    async def _follow_pagination(
        self, base_url: str, html: str, jobs: list[dict],
        career_page, company,
    ) -> list[dict]:
        """Detect and follow pagination links to get more jobs."""
        import httpx

        root = _parse_html(html)
        if root is None:
            return jobs

        # Find next page URL
        next_url = self._find_next_page_url(root, base_url)
        if not next_url:
            return jobs

        logger.info("v1.4 pagination: following %s", next_url)

        pages_fetched = 0
        max_pages = 5  # safety limit

        async with httpx.AsyncClient(
            timeout=10, follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        ) as client:
            current_url = next_url
            seen_urls = {base_url}

            while current_url and pages_fetched < max_pages and current_url not in seen_urls:
                seen_urls.add(current_url)
                try:
                    resp = await client.get(current_url)
                    page_html = resp.text
                    if len(page_html) < 200:
                        break

                    # Extract jobs from this page using same method
                    class _CP:
                        def __init__(s): s.url = current_url; s.id = None

                    page_jobs = await super().extract(_CP(), company, page_html)
                    if not page_jobs:
                        break

                    # Deduplicate by URL
                    existing_urls = {j["source_url"] for j in jobs}
                    new_jobs = [j for j in page_jobs if j["source_url"] not in existing_urls]
                    if not new_jobs:
                        break

                    jobs.extend(new_jobs)
                    pages_fetched += 1
                    logger.info("v1.4 pagination page %d: +%d jobs (total %d)", pages_fetched, len(new_jobs), len(jobs))

                    # Find next page on this page
                    page_root = _parse_html(page_html)
                    current_url = self._find_next_page_url(page_root, current_url) if page_root else None

                except Exception as e:
                    logger.debug("v1.4 pagination failed on %s: %s", current_url, e)
                    break

        return jobs

    def _find_next_page_url(self, root: etree._Element, base_url: str) -> Optional[str]:
        """Find the next page URL from pagination elements."""
        # Try CSS selectors
        for sel in _PAGINATION_SELECTORS:
            try:
                els = root.cssselect(sel)
                if els:
                    href = els[0].get("href")
                    if href and href != "#" and not href.startswith("javascript:"):
                        return urljoin(base_url, href)
            except Exception:
                continue

        # Try XPath selectors
        for xpath in _PAGINATION_XPATHS:
            try:
                els = root.xpath(xpath)
                if els:
                    href = els[0].get("href")
                    if href and href != "#":
                        return urljoin(base_url, href)
            except Exception:
                continue

        return None

    async def _enrich_from_detail_pages(self, jobs: list[dict]) -> list[dict]:
        """Visit individual job detail pages to extract rich fields."""
        import httpx

        # Only enrich jobs that are missing description (the main rich field)
        jobs_to_enrich = [
            (i, j) for i, j in enumerate(jobs)
            if not j.get("description") and j.get("source_url")
            and j["source_url"].startswith("http")
        ]

        if not jobs_to_enrich:
            return jobs

        # Limit to 15 detail pages to avoid being too slow
        jobs_to_enrich = jobs_to_enrich[:15]

        logger.info("v1.4 enriching %d jobs from detail pages", len(jobs_to_enrich))

        async with httpx.AsyncClient(
            timeout=8, follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        ) as client:
            import asyncio

            async def _enrich_one(idx: int, job: dict) -> tuple[int, dict]:
                """Fetch a detail page and extract rich fields."""
                try:
                    resp = await client.get(job["source_url"])
                    if resp.status_code != 200 or len(resp.text) < 200:
                        return (idx, {})

                    detail_html = resp.text
                    root = _parse_html(detail_html)
                    if root is None:
                        return (idx, {})

                    enriched = {}

                    # Description
                    for sel in _DETAIL_DESC_SELECTORS:
                        try:
                            els = root.cssselect(sel)
                            if els:
                                desc = _text(els[0])
                                if desc and len(desc) > 50:
                                    enriched["description"] = desc[:5000]
                                    break
                        except Exception:
                            continue

                    # Salary (if not already found)
                    if not job.get("salary_raw"):
                        for sel in _DETAIL_SALARY_SELECTORS:
                            try:
                                els = root.cssselect(sel)
                                if els:
                                    t = _text(els[0])
                                    if t and len(t) < 200:
                                        enriched["salary_raw"] = t
                                        break
                            except Exception:
                                continue
                        # Fallback: regex scan
                        if "salary_raw" not in enriched:
                            body_text = _text(root)
                            sal_match = _SALARY_PATTERN.search(body_text)
                            if sal_match:
                                enriched["salary_raw"] = sal_match.group(0).strip()

                    # Employment type (if not already found)
                    if not job.get("employment_type"):
                        for sel in _DETAIL_TYPE_SELECTORS:
                            try:
                                els = root.cssselect(sel)
                                if els:
                                    t = _text(els[0])
                                    if t and _JOB_TYPE_PATTERN.search(t):
                                        enriched["employment_type"] = t.strip()[:80]
                                        break
                            except Exception:
                                continue
                        # Fallback: regex scan
                        if "employment_type" not in enriched:
                            body_text = _text(root)
                            type_match = _JOB_TYPE_PATTERN.search(body_text)
                            if type_match:
                                enriched["employment_type"] = type_match.group(0).strip()

                    # Location (if not already found)
                    if not job.get("location_raw"):
                        for sel in [*_DETAIL_SALARY_SELECTORS]:  # reuse location patterns
                            pass
                        # Try location class selectors
                        for el in root.iter():
                            if not isinstance(el.tag, str):
                                continue
                            cls = (el.get("class") or "").lower()
                            if _LOCATION_CLASS_PATTERN.search(cls):
                                t = _text(el)
                                if t and 2 < len(t) < 200:
                                    enriched["location_raw"] = t
                                    break

                    return (idx, enriched)

                except Exception:
                    return (idx, {})

            # Fetch detail pages in parallel (batches of 5)
            for batch_start in range(0, len(jobs_to_enrich), 5):
                batch = jobs_to_enrich[batch_start:batch_start + 5]
                results = await asyncio.gather(*[_enrich_one(idx, job) for idx, job in batch])
                for idx, enriched in results:
                    if enriched:
                        for key, value in enriched.items():
                            if value and not jobs[idx].get(key):
                                jobs[idx][key] = value

        enriched_count = sum(1 for _, j in enumerate(jobs) if j.get("description"))
        logger.info("v1.4 enrichment done: %d/%d jobs now have descriptions", enriched_count, len(jobs))

        return jobs
