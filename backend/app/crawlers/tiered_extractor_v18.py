"""
Tiered Extraction Engine v1.8 — extends v1.6 with "crawl intelligence":

1. Cookie/Consent banner dismissal before extraction (Playwright pages)
2. Longer wait after page load (5s instead of 2s) for JS-heavy sites
3. Tier 0: API/Feed discovery — JSON-LD JobPosting, RSS feeds, common API endpoints
4. Iframe detection — fetch ATS iframes and extract from their content
5. Search form submission — trigger empty search if 0 jobs found
6. Accordion/Tab expansion — click collapsed sections with job-related labels

Inherits directly from TieredExtractorV16 (NOT v17).
"""

import json
import logging
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from lxml import etree

from app.crawlers.tiered_extractor_v16 import TieredExtractorV16
from app.crawlers.tiered_extractor import (
    _parse_html, _text, _href, _resolve_url, _detect_spa,
    _JOB_TYPE_PATTERN, _SALARY_PATTERN, _AU_LOCATIONS,
    _LOCATION_CLASS_PATTERN, _SALARY_CLASS_PATTERN,
    MAX_JOBS_PER_PAGE, MIN_JOBS_FOR_SUCCESS,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known ATS iframe domains — if an iframe src matches, fetch and extract
# from the iframe content instead of the parent page.
# ---------------------------------------------------------------------------

_ATS_IFRAME_DOMAINS = {
    "greenhouse.io", "boards.greenhouse.io",
    "lever.co", "jobs.lever.co",
    "workday.com", "myworkdayjobs.com",
    "smartrecruiters.com",
    "icims.com",
    "bamboohr.com",
    "applynow.net.au",
    "pageuppeople.com",
    "livehire.com",
    "jobvite.com",
    "teamtailor.com",
    "ashbyhq.com",
    "recruitee.com",
    "breezy.hr",
    "pinpointhq.com",
    "freshteam.com",
    "jazz.co", "applytojob.com",
}

# ---------------------------------------------------------------------------
# Common feed/API URL suffixes to probe
# ---------------------------------------------------------------------------

_FEED_SUFFIXES = [
    "/feed", "/feed/", "/jobs.xml", "/careers.xml",
    "/api/jobs", "/api/v1/jobs", "/api/careers",
    "/wp-json/wp/v2/job-listings",
    "/_next/data",  # Next.js data routes (rare but worth checking)
]

# ---------------------------------------------------------------------------
# Cookie consent button selectors (ordered by commonality)
# ---------------------------------------------------------------------------

_COOKIE_SELECTORS = [
    'button#accept', 'button.accept', '[class*=consent] button',
    'button:has-text("Accept")', 'button:has-text("Accept All")',
    'button:has-text("I agree")', 'button:has-text("OK")',
    '#cookie-accept', '.cookie-accept', '#onetrust-accept-btn-handler',
    '#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll',
    '.cc-accept', '.cc-btn.cc-allow', 'button[data-action="accept"]',
    '[aria-label="Accept cookies"]', '[aria-label="Accept all"]',
    '.cookie-banner button', '#gdpr-accept', '#accept-cookies',
]

# ---------------------------------------------------------------------------
# Accordion/tab selectors with job-related labels
# ---------------------------------------------------------------------------

_ACCORDION_SELECTORS = [
    'button[aria-expanded="false"]',
    'details:not([open]) summary',
    '[class*="accordion"] [class*="header"]',
    '[class*="collapse"] [class*="trigger"]',
    '[class*="tab"][role="tab"][aria-selected="false"]',
]

_JOB_SECTION_KEYWORDS = re.compile(
    r"job|career|vacanc|position|opening|role|hire|hiring|"
    r"opportunity|opportunities|current|available|department",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# JSON-LD helpers
# ---------------------------------------------------------------------------


def _extract_location_from_jsonld(item: dict) -> Optional[str]:
    """Extract a human-readable location string from a JobPosting JSON-LD item."""
    loc = item.get("jobLocation")
    if not loc:
        return None

    locations = loc if isinstance(loc, list) else [loc]
    parts = []
    for l in locations:
        if isinstance(l, str):
            parts.append(l)
            continue
        address = l.get("address", {})
        if isinstance(address, str):
            parts.append(address)
            continue
        if isinstance(address, dict):
            city = address.get("addressLocality", "")
            region = address.get("addressRegion", "")
            country = address.get("addressCountry", "")
            if isinstance(country, dict):
                country = country.get("name", "")
            loc_str = ", ".join(p for p in [city, region, country] if p)
            if loc_str:
                parts.append(loc_str)
    return "; ".join(parts) if parts else None


def _extract_salary_from_jsonld(item: dict) -> Optional[str]:
    """Extract a human-readable salary string from a JobPosting JSON-LD item."""
    salary = item.get("baseSalary") or item.get("estimatedSalary")
    if not salary:
        return None
    if isinstance(salary, str):
        return salary
    if isinstance(salary, dict):
        currency = salary.get("currency", "")
        value = salary.get("value", {})
        if isinstance(value, dict):
            min_val = value.get("minValue", "")
            max_val = value.get("maxValue", "")
            unit = value.get("unitText", "")
            if min_val and max_val:
                return f"{currency} {min_val}-{max_val} {unit}".strip()
            elif min_val:
                return f"{currency} {min_val} {unit}".strip()
            elif max_val:
                return f"{currency} {max_val} {unit}".strip()
        elif isinstance(value, (int, float)):
            return f"{currency} {value}".strip()
    return None


class TieredExtractorV18(TieredExtractorV16):
    """v1.8 — crawl intelligence: cookie dismissal, longer waits, Tier 0
    structured data, iframe detection, search form submission, accordion expansion."""

    # ------------------------------------------------------------------
    # Main extract() override
    # ------------------------------------------------------------------

    async def extract(self, career_page, company, html: str) -> list[dict]:
        """Extract with v1.8 crawl intelligence pre-processing."""
        url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # ── Tier 0: Try structured data (JSON-LD, RSS feeds) ──
        structured_jobs = self._extract_structured_data(html, url)
        if structured_jobs and len(structured_jobs) >= MIN_JOBS_FOR_SUCCESS:
            logger.info(
                "v1.8 Tier 0 (JSON-LD) extracted %d jobs from %s",
                len(structured_jobs), url,
            )
            return structured_jobs[:MAX_JOBS_PER_PAGE]

        # ── Iframe detection: if the page embeds an ATS iframe, fetch that ──
        iframe_html = await self._try_iframe_extraction(html, url)
        if iframe_html:
            logger.info("v1.8 using iframe content instead of parent page for %s", url)
            html = iframe_html

        # ── SPA detection + Playwright rendering (with v1.8 improvements) ──
        is_spa = _detect_spa(html)
        if not is_spa:
            is_spa = self._is_js_rendered(html, url)

        if is_spa:
            rendered = await self._render_with_playwright_v18(url)
            if rendered and len(rendered) > len(html):
                logger.info(
                    "v1.8 Playwright rendered %s (%d -> %d bytes)",
                    url, len(html), len(rendered),
                )
                html = rendered
            else:
                logger.info("v1.8 SPA detected but Playwright unavailable for %s", url)

        # ── Tier 1: ATS templates (v1.2 extended set) ──
        tier1 = self._extract_tier1_v12(url, html)
        if tier1 and len(tier1) >= MIN_JOBS_FOR_SUCCESS:
            return tier1[:MAX_JOBS_PER_PAGE]

        # ── Tier 2: v1.6 heuristic with apply-button matching + vocab ──
        tier2 = self._extract_tier2_v16(url, html)
        if tier2 and len(tier2) >= MIN_JOBS_FOR_SUCCESS:
            tier2 = await self._follow_pagination(url, html, tier2, career_page, company)
            tier2 = await self._enrich_from_detail_pages(tier2)
            return tier2[:MAX_JOBS_PER_PAGE]

        # ── Tier 3: LLM (still deferred) ──
        tier3 = self._extract_tier3_llm(url, html)
        if tier3 and len(tier3) >= MIN_JOBS_FOR_SUCCESS:
            return tier3[:MAX_JOBS_PER_PAGE]

        # ── Fallback: Try RSS/API feed probing if all tiers failed ──
        feed_jobs = await self._probe_feed_endpoints(url)
        if feed_jobs and len(feed_jobs) >= MIN_JOBS_FOR_SUCCESS:
            logger.info(
                "v1.8 feed probe found %d jobs for %s",
                len(feed_jobs), url,
            )
            return feed_jobs[:MAX_JOBS_PER_PAGE]

        # Return partial results from whichever tier got closest
        for partial in (tier1, tier2):
            if partial:
                return partial[:MAX_JOBS_PER_PAGE]

        return []

    # ------------------------------------------------------------------
    # 1. Cookie/Consent Banner Dismissal
    # ------------------------------------------------------------------

    @staticmethod
    async def _dismiss_cookie_banners(page) -> bool:
        """Click common cookie consent buttons on a Playwright page.

        Returns True if a banner was dismissed, False otherwise.
        """
        for selector in _COOKIE_SELECTORS:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=1000):
                    await btn.click()
                    await page.wait_for_timeout(1000)
                    logger.debug("v1.8 dismissed cookie banner via: %s", selector)
                    return True
            except Exception:
                continue
        return False

    # ------------------------------------------------------------------
    # 2. Longer Wait + Cookie Dismissal + Accordion Expansion
    # ------------------------------------------------------------------

    async def _render_with_playwright_v18(self, url: str) -> Optional[str]:
        """Render with Playwright — 5s wait (up from 2s), cookie dismissal,
        accordion expansion."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            return None

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                )
                try:
                    await page.goto(url, wait_until="networkidle", timeout=30000)

                    # v1.8: Longer initial wait for JS-heavy sites
                    await page.wait_for_timeout(5000)

                    # v1.8: Dismiss cookie/consent banners
                    await self._dismiss_cookie_banners(page)

                    # v1.8: Expand accordions/tabs with job-related content
                    await self._expand_accordions(page)

                    # v1.8: If very few visible text, try submitting search form
                    await self._try_search_form_submission(page)

                    # Final short wait for any expansions to render
                    await page.wait_for_timeout(1000)

                    return await page.content()

                except Exception as e:
                    logger.debug("v1.8 Playwright failed for %s: %s", url, e)
                    return None
                finally:
                    await browser.close()

        except Exception:
            return None

    # ------------------------------------------------------------------
    # 3. Tier 0: Structured Data Extraction (JSON-LD)
    # ------------------------------------------------------------------

    def _extract_structured_data(self, html: str, url: str) -> list[dict]:
        """Extract jobs from JSON-LD JobPosting schema if present."""
        jobs: list[dict] = []

        for match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html,
            re.DOTALL | re.IGNORECASE,
        ):
            try:
                raw = match.group(1).strip()
                data = json.loads(raw)
                items = data if isinstance(data, list) else [data]

                for item in items:
                    if not isinstance(item, dict):
                        continue

                    # Handle @graph wrapper
                    if "@graph" in item:
                        graph_items = item["@graph"]
                        if isinstance(graph_items, list):
                            items.extend(graph_items)
                        continue

                    item_type = item.get("@type", "")
                    # Handle both "JobPosting" and ["JobPosting"]
                    if isinstance(item_type, list):
                        item_type = item_type[0] if item_type else ""

                    if item_type != "JobPosting":
                        continue

                    title = item.get("title", "") or item.get("name", "")
                    if not title or len(title) < 3:
                        continue

                    source_url = item.get("url", "") or item.get("sameAs", "") or url
                    if source_url and not source_url.startswith("http"):
                        source_url = urljoin(url, source_url)

                    location_raw = _extract_location_from_jsonld(item)
                    salary_raw = _extract_salary_from_jsonld(item)

                    employment_type = item.get("employmentType", "")
                    if isinstance(employment_type, list):
                        employment_type = ", ".join(employment_type)

                    description = item.get("description", "") or ""
                    # Strip HTML tags from description if present
                    if "<" in description:
                        try:
                            desc_root = _parse_html(description)
                            if desc_root is not None:
                                description = _text(desc_root)
                        except Exception:
                            pass
                    description = description[:2000] if description else None

                    jobs.append({
                        "title": title.strip(),
                        "source_url": source_url or url,
                        "location_raw": location_raw,
                        "description": description,
                        "salary_raw": salary_raw,
                        "employment_type": employment_type or None,
                        "extraction_method": "tier0_jsonld",
                        "extraction_confidence": 0.95,
                    })

            except (json.JSONDecodeError, ValueError, TypeError):
                continue

        # Deduplicate by title + URL
        seen = set()
        unique_jobs = []
        for job in jobs:
            key = (job["title"].lower(), job["source_url"])
            if key not in seen:
                seen.add(key)
                unique_jobs.append(job)

        return unique_jobs

    # ------------------------------------------------------------------
    # 4. Iframe Detection
    # ------------------------------------------------------------------

    async def _try_iframe_extraction(self, html: str, url: str) -> Optional[str]:
        """Check for iframes pointing to known ATS domains. If found, fetch
        the iframe's src URL and return its HTML for extraction."""
        root = _parse_html(html)
        if root is None:
            return None

        for iframe in root.iter("iframe"):
            src = iframe.get("src", "")
            if not src:
                continue

            # Resolve relative URLs
            full_src = urljoin(url, src)

            try:
                parsed = urlparse(full_src)
                domain = parsed.hostname or ""
            except Exception:
                continue

            # Check if the iframe domain matches a known ATS
            is_ats = any(
                domain == ats_domain or domain.endswith("." + ats_domain)
                for ats_domain in _ATS_IFRAME_DOMAINS
            )
            if not is_ats:
                continue

            logger.info("v1.8 found ATS iframe: %s -> %s", url, full_src)

            try:
                async with httpx.AsyncClient(
                    timeout=15,
                    follow_redirects=True,
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36"
                        ),
                    },
                ) as client:
                    resp = await client.get(full_src)
                    if resp.status_code == 200 and len(resp.text) > 500:
                        return resp.text
            except Exception as e:
                logger.debug("v1.8 iframe fetch failed for %s: %s", full_src, e)
                continue

        return None

    # ------------------------------------------------------------------
    # 5. Search Form Submission
    # ------------------------------------------------------------------

    @staticmethod
    async def _try_search_form_submission(page) -> bool:
        """If a careers page has a visible search form and shows no results,
        try submitting the form empty to trigger the full listing."""
        search_selectors = [
            'form[action*="search"] button[type="submit"]',
            'form[action*="job"] button[type="submit"]',
            'form[action*="career"] button[type="submit"]',
            'form[class*="search"] button[type="submit"]',
            'form[class*="job"] button[type="submit"]',
            'button[class*="search"]',
            'input[type="submit"][value*="Search" i]',
            'button:has-text("Search")',
            'button:has-text("Search Jobs")',
            'button:has-text("Find Jobs")',
            'button:has-text("Show All")',
            'button:has-text("View All")',
        ]

        for selector in search_selectors:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=1000):
                    await btn.click()
                    await page.wait_for_timeout(3000)
                    logger.debug("v1.8 submitted search form via: %s", selector)
                    return True
            except Exception:
                continue

        return False

    # ------------------------------------------------------------------
    # 6. Accordion/Tab Expansion
    # ------------------------------------------------------------------

    @staticmethod
    async def _expand_accordions(page) -> int:
        """Look for collapsed sections with job-related labels and click
        to expand them. Returns count of sections expanded."""
        expanded = 0

        for selector in _ACCORDION_SELECTORS:
            try:
                elements = page.locator(selector)
                count = await elements.count()

                for i in range(min(count, 10)):  # safety limit
                    try:
                        el = elements.nth(i)
                        text = await el.text_content()
                        if text and _JOB_SECTION_KEYWORDS.search(text):
                            if await el.is_visible(timeout=500):
                                await el.click()
                                expanded += 1
                                await page.wait_for_timeout(500)
                    except Exception:
                        continue

            except Exception:
                continue

        if expanded:
            logger.debug("v1.8 expanded %d accordion/tab sections", expanded)

        return expanded

    # ------------------------------------------------------------------
    # Feed/API endpoint probing (fallback when all tiers fail)
    # ------------------------------------------------------------------

    async def _probe_feed_endpoints(self, url: str) -> list[dict]:
        """Probe common feed/API endpoints for job data. This is a last resort
        when HTML extraction fails — the site may expose jobs via an API."""
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        jobs: list[dict] = []

        try:
            async with httpx.AsyncClient(
                timeout=8,
                follow_redirects=True,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36"
                    ),
                    "Accept": "application/json, application/xml, text/xml, */*",
                },
            ) as client:
                for suffix in _FEED_SUFFIXES:
                    probe_url = base + suffix
                    try:
                        resp = await client.get(probe_url)
                        if resp.status_code != 200:
                            continue

                        content_type = resp.headers.get("content-type", "")
                        body = resp.text.strip()
                        if not body or len(body) < 50:
                            continue

                        # Try JSON parsing
                        if "json" in content_type or body.startswith(("{", "[")):
                            feed_jobs = self._parse_json_feed(body, probe_url)
                            if feed_jobs:
                                jobs.extend(feed_jobs)
                                break

                        # Try XML/RSS parsing
                        if "xml" in content_type or body.startswith("<?xml"):
                            feed_jobs = self._parse_xml_feed(body, probe_url)
                            if feed_jobs:
                                jobs.extend(feed_jobs)
                                break

                    except Exception:
                        continue

        except Exception as e:
            logger.debug("v1.8 feed probing failed for %s: %s", url, e)

        return jobs

    def _parse_json_feed(self, body: str, source_url: str) -> list[dict]:
        """Parse a JSON response that might contain job listings."""
        jobs: list[dict] = []

        try:
            data = json.loads(body)
        except (json.JSONDecodeError, ValueError):
            return []

        # Handle common JSON shapes: list of objects, or {data: [...]} / {jobs: [...]}
        items: list[dict] = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in ("data", "jobs", "results", "items", "positions", "listings",
                        "openings", "vacancies", "job_listings"):
                if key in data and isinstance(data[key], list):
                    items = data[key]
                    break

        for item in items[:MAX_JOBS_PER_PAGE]:
            if not isinstance(item, dict):
                continue

            title = (
                item.get("title", "")
                or item.get("name", "")
                or item.get("job_title", "")
                or item.get("position", "")
            )
            if not title or len(title) < 3:
                continue

            job_url = (
                item.get("url", "")
                or item.get("link", "")
                or item.get("apply_url", "")
                or item.get("href", "")
            )
            if job_url and not job_url.startswith("http"):
                job_url = urljoin(source_url, job_url)

            location = (
                item.get("location", "")
                or item.get("city", "")
                or item.get("office", "")
            )
            if isinstance(location, dict):
                location = location.get("name", "") or location.get("city", "")

            salary = item.get("salary", "") or item.get("compensation", "")
            if isinstance(salary, dict):
                salary = salary.get("text", "") or salary.get("range", "")

            emp_type = (
                item.get("employment_type", "")
                or item.get("type", "")
                or item.get("contract_type", "")
            )

            description = item.get("description", "") or item.get("summary", "")
            if description and len(description) > 2000:
                description = description[:2000]

            jobs.append({
                "title": str(title).strip(),
                "source_url": job_url or source_url,
                "location_raw": str(location).strip() or None,
                "description": str(description).strip() or None,
                "salary_raw": str(salary).strip() or None,
                "employment_type": str(emp_type).strip() or None,
                "extraction_method": "tier0_json_feed",
                "extraction_confidence": 0.88,
            })

        return jobs

    def _parse_xml_feed(self, body: str, source_url: str) -> list[dict]:
        """Parse an XML/RSS feed that might contain job listings."""
        jobs: list[dict] = []

        try:
            root = etree.fromstring(body.encode("utf-8", errors="replace"))
        except Exception:
            return []

        # Handle RSS <item> elements
        # Strip namespaces for easier querying
        for el in root.iter():
            if isinstance(el.tag, str) and "}" in el.tag:
                el.tag = el.tag.split("}", 1)[1]

        items = root.findall(".//item")
        if not items:
            items = root.findall(".//entry")  # Atom feeds

        for item in items[:MAX_JOBS_PER_PAGE]:
            title_el = item.find("title")
            title = title_el.text.strip() if title_el is not None and title_el.text else ""
            if not title or len(title) < 3:
                continue

            link_el = item.find("link")
            link = ""
            if link_el is not None:
                link = link_el.text or link_el.get("href", "") or ""
            link = link.strip()
            if link and not link.startswith("http"):
                link = urljoin(source_url, link)

            desc_el = item.find("description") or item.find("content") or item.find("summary")
            description = ""
            if desc_el is not None and desc_el.text:
                description = desc_el.text.strip()[:2000]

            jobs.append({
                "title": title,
                "source_url": link or source_url,
                "location_raw": None,
                "description": description or None,
                "salary_raw": None,
                "employment_type": None,
                "extraction_method": "tier0_rss_feed",
                "extraction_confidence": 0.85,
            })

        return jobs

    # ------------------------------------------------------------------
    # RSS link discovery from HTML <link> tags
    # ------------------------------------------------------------------

    def _find_rss_links(self, html: str, base_url: str) -> list[str]:
        """Find RSS/Atom feed links declared in HTML <link> tags."""
        links: list[str] = []
        for match in re.finditer(
            r'<link[^>]*rel=["\']alternate["\'][^>]*type=["\']application/'
            r'(?:rss|atom)\+xml["\'][^>]*href=["\']([^"\']+)["\']',
            html,
            re.IGNORECASE,
        ):
            href = match.group(1).strip()
            if href:
                links.append(urljoin(base_url, href))

        # Also check reversed attribute order
        for match in re.finditer(
            r'<link[^>]*href=["\']([^"\']+)["\'][^>]*type=["\']application/'
            r'(?:rss|atom)\+xml["\']',
            html,
            re.IGNORECASE,
        ):
            href = match.group(1).strip()
            full_url = urljoin(base_url, href)
            if full_url not in links:
                links.append(full_url)

        return links
