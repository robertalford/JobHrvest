"""
Tiered Extraction Engine v1.9 — combines the best of v1.7 (Codex extraction
improvements) and v1.8 (crawl intelligence) into a single extractor.

Inherits directly from TieredExtractorV16 (NOT v17 or v18).

From v1.7 (proven at 77% accuracy):
  - Multi-candidate container sweep (top 25) + bucket aggregation
  - Global repeated-row harvesting for job-class-labelled rows
  - Heading-section fallback for text-heavy vacancy pages
  - Strict jobset validation, scoring, and deduplication
  - Title normalization and job-signal detection

From v1.8 (crawl intelligence, selective):
  - Tier 0: JSON-LD JobPosting structured data extraction
  - Cookie/consent banner dismissal during Playwright render
  - Iframe ATS widget detection and content fetching
  - 5-second Playwright wait for JS-heavy sites

NOT copied from v1.8 (unproven / regression risk):
  - Search form submission
  - Accordion/tab expansion
"""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from lxml import etree

from app.crawlers.tiered_extractor_v16 import TieredExtractorV16, _title_has_job_noun
from app.crawlers.tiered_extractor import (
    _parse_html,
    _text,
    _href,
    _resolve_url,
    _get_el_classes,
    _is_valid_title,
    _detect_spa,
    _JOB_URL_PATTERN,
    _JOB_TYPE_PATTERN,
    _AU_LOCATIONS,
    MAX_JOBS_PER_PAGE,
    MIN_JOBS_FOR_SUCCESS,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# v1.7 patterns — title validation, normalization, row matching
# ---------------------------------------------------------------------------

_TITLE_HINT_PATTERN_V17 = re.compile(
    r"\b(?:job|jobs|career|careers|vacanc(?:y|ies)|opening|openings|"
    r"position|positions|role|roles|internship|intern|apprentice|"
    r"manager|engineer|developer|officer|specialist|assistant|"
    r"analyst|consultant|coordinator|executive|technician|designer|"
    r"administrator|recruit(?:er|ment)?|"
    r"akuntan|asisten|psikolog(?:i)?|fotografer|staf|pegawai|karyawan|lowongan)\b",
    re.IGNORECASE,
)

_CONTEXT_HINT_PATTERN_V17 = re.compile(
    r"\b(?:apply|deadline|closing|location|salary|compensation|"
    r"full[\s-]?time|part[\s-]?time|contract|permanent|temporary|"
    r"casual|remote|hybrid|qualifications?|requirements?)\b",
    re.IGNORECASE,
)

_REJECT_TITLE_PATTERN_V17 = re.compile(
    r"^(?:"
    r"powered\s+by|"
    r"info\s+lengkap|"
    r"current\s+vacancies|"
    r"open\s+roles?|"
    r"read\s+more|"
    r"show\s+all\s+jobs?|"
    r"show\s+advanced|"
    r"size\s*&\s*fit|"
    r"shipping\s*&\s*returns|"
    r"cart|checkout|wishlist|"
    r"saved\s+jobs?|job\s+alerts?|profile\s+details|work\s+preferences|account\s+settings|"
    r"job\s+seekers?|blogs?|"
    r"first\s+name|last\s+name|email|phone|message|"
    r"nature\s+needs\s+your\s+support|"
    r"bisa\s+kamu\s+baca\s+di\s+sini.*|"
    r"alamat\s+kantor|"
    r"main\s+menu"
    r")$",
    re.IGNORECASE,
)

_TRAILING_META_SPLIT_V17 = re.compile(
    r"(?:\bdeadline\s*:|\bclosing\s+date\b|\blocation\s*:|\bemployment\s+type\s*:|"
    r"\bpermanent\b|\btemporary\b|\bcontract\b|\bcasual\b|\bfull[\s-]?time\b|\bpart[\s-]?time\b)",
    re.IGNORECASE,
)

_CAMEL_LOCATION_SPLIT_V17 = re.compile(
    r"^(.{3,100}?)([a-z])([A-Z][A-Za-z\.-]+(?:[,\s]+[A-Z][A-Za-z\.-]+)*)$"
)

_ROW_CLASS_STRONG_PATTERN_V17 = re.compile(
    r"job|position|vacanc|opening|posting|recruit|career|lowongan|karir|karier",
    re.IGNORECASE,
)

_CTA_TITLE_PATTERN_V17 = re.compile(
    r"^(?:become|learn|discover|protect|shop|share|follow|read|view|download|"
    r"contact|about|what\s+we|how\s+we|our\s+(?:team|culture|story|community))\b",
    re.IGNORECASE,
)

_JOB_URL_HINT_PATTERN_V17 = re.compile(
    r"/(?:job|jobs|career|careers|position|positions|vacanc|opening|openings|"
    r"role|roles|apply|recruit|search|lowongan|karir|karier|vacature|empleo|trabajo|"
    r"portal\.na|/p/)",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V17 = re.compile(
    r"/(?:about|contact|news|blog|report|investor|ir|privacy|terms|cookie|"
    r"shop|store|donate|support|resource|event|story|team|culture|values)(?:/|$)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# v1.8 patterns — ATS iframe domains, cookie consent selectors
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
# JSON-LD helpers (from v1.8)
# ---------------------------------------------------------------------------


def _extract_location_from_jsonld(item: dict) -> Optional[str]:
    """Extract a human-readable location string from a JobPosting JSON-LD item."""
    loc = item.get("jobLocation")
    if not loc:
        return None

    locations = loc if isinstance(loc, list) else [loc]
    parts: list[str] = []
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


# ===========================================================================
# TieredExtractorV19 — unified v1.7 + v1.8 extraction
# ===========================================================================


class TieredExtractorV19(TieredExtractorV16):
    """v1.9 — combines v1.7 proven heuristic improvements with v1.8 crawl
    intelligence (JSON-LD, cookie dismissal, iframe detection)."""

    # ------------------------------------------------------------------
    # Main extract() override
    # ------------------------------------------------------------------

    async def extract(self, career_page, company, html: str) -> list[dict]:
        """Extract jobs using the v1.9 unified pipeline."""
        url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # ── SPA detection (from v1.1) ──
        if _detect_spa(html):
            rendered = await self._render_with_playwright_v19(url)
            if rendered and len(rendered) > len(html):
                logger.info(
                    "v1.9 Playwright rendered %s (%d -> %d bytes)",
                    url, len(html), len(rendered),
                )
                html = rendered

        # ── Tier 0: JSON-LD structured data (from v1.8) ──
        structured = self._extract_structured_data(html, url)
        if structured and len(structured) >= MIN_JOBS_FOR_SUCCESS:
            logger.info(
                "v1.9 Tier 0 (JSON-LD) extracted %d jobs from %s",
                len(structured), url,
            )
            return structured[:MAX_JOBS_PER_PAGE]

        # ── Iframe detection (from v1.8) ──
        iframe_html = await self._try_iframe_extraction(html, url)
        if iframe_html:
            logger.info("v1.9 using iframe content instead of parent page for %s", url)
            html = iframe_html

        # ── Tier 1: ATS templates (from v1.6 parent) ──
        tier1 = self._extract_tier1_ats(url, html)
        if tier1 and len(tier1) >= MIN_JOBS_FOR_SUCCESS:
            logger.info(
                "v1.9 Tier 1 (ATS) extracted %d jobs from %s",
                len(tier1), url,
            )
            return tier1[:MAX_JOBS_PER_PAGE]

        # ── Tier 2: v1.7's improved heuristic (candidate sweep + repeated rows) ──
        tier2 = self._extract_tier2_v19(url, html)
        if tier2 and len(tier2) >= MIN_JOBS_FOR_SUCCESS:
            logger.info(
                "v1.9 Tier 2 (v17 heuristic) extracted %d jobs from %s",
                len(tier2), url,
            )
            return tier2[:MAX_JOBS_PER_PAGE]

        # ── Fallback to parent's tier 2 (v1.3 heuristic via v1.6) ──
        tier2_parent = self._extract_tier2_v16(url, html)
        if tier2_parent and len(tier2_parent) >= MIN_JOBS_FOR_SUCCESS:
            logger.info(
                "v1.9 Tier 2 (v16 fallback) extracted %d jobs from %s",
                len(tier2_parent), url,
            )
            return tier2_parent[:MAX_JOBS_PER_PAGE]

        # Return best partial results
        for partial in (tier1, tier2, tier2_parent):
            if partial:
                return partial[:MAX_JOBS_PER_PAGE]

        return []

    # ------------------------------------------------------------------
    # Playwright rendering with 5s wait + cookie dismissal (v1.8/v1.9)
    # ------------------------------------------------------------------

    async def _render_with_playwright_v19(self, url: str) -> Optional[str]:
        """Render with Playwright — 5s wait, cookie dismissal.
        No search form submission or accordion expansion (unproven)."""
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

                    # v1.9: 5-second wait for JS-heavy sites
                    await page.wait_for_timeout(5000)

                    # v1.9: Dismiss cookie/consent banners
                    await self._dismiss_cookie_banners(page)

                    # Short wait for any banner dismissal to settle
                    await page.wait_for_timeout(1000)

                    return await page.content()

                except Exception as e:
                    logger.debug("v1.9 Playwright failed for %s: %s", url, e)
                    return None
                finally:
                    await browser.close()

        except Exception:
            return None

    # ------------------------------------------------------------------
    # Cookie banner dismissal (from v1.8)
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
                    logger.debug("v1.9 dismissed cookie banner via: %s", selector)
                    return True
            except Exception:
                continue
        return False

    # ------------------------------------------------------------------
    # Tier 0: JSON-LD structured data extraction (from v1.8)
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
        seen: set[tuple[str, str]] = set()
        unique_jobs: list[dict] = []
        for job in jobs:
            key = (job["title"].lower(), job["source_url"])
            if key not in seen:
                seen.add(key)
                unique_jobs.append(job)

        return unique_jobs

    # ------------------------------------------------------------------
    # Iframe ATS detection (from v1.8)
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

            full_src = urljoin(url, src)

            try:
                parsed = urlparse(full_src)
                domain = parsed.hostname or ""
            except Exception:
                continue

            is_ats = any(
                domain == ats_domain or domain.endswith("." + ats_domain)
                for ats_domain in _ATS_IFRAME_DOMAINS
            )
            if not is_ats:
                continue

            logger.info("v1.9 found ATS iframe: %s -> %s", url, full_src)

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
                logger.debug("v1.9 iframe fetch failed for %s: %s", full_src, e)
                continue

        return None

    # ------------------------------------------------------------------
    # Tier 2 v1.9: Multi-strategy heuristic from v1.7
    # ------------------------------------------------------------------

    def _extract_tier2_v19(self, url: str, html: str) -> Optional[list[dict]]:
        """v1.9 Tier 2 — multi-strategy candidate selection from v1.7."""
        root = _parse_html(html)
        if root is None:
            return None

        is_elementor = "elementor" in html[:5000].lower()
        page_apply_count = self._count_page_apply_buttons(root)

        candidates: list[tuple[str, list[dict]]] = []

        # Strategy 1: v1.6 base extraction as one candidate source
        base_jobs = super()._extract_tier2_v16(url, html)
        if base_jobs:
            candidates.append(("v16_base", self._dedupe_jobs(base_jobs)))

        # Strategy 2: Wider candidate container sweep (top 25)
        sweep_jobs = self._extract_from_candidate_sweep(
            root, url, is_elementor, page_apply_count,
        )
        if sweep_jobs:
            candidates.append(("container_sweep", sweep_jobs))

        # Strategy 3: Global repeated-row harvesting
        row_jobs = self._extract_from_repeated_rows(root, url)
        if row_jobs:
            candidates.append(("row_harvest", row_jobs))

        # Strategy 4: Heading-section fallback
        heading_jobs = self._extract_from_heading_sections(root, url)
        if heading_jobs:
            candidates.append(("heading_sections", heading_jobs))

        best = self._pick_best_jobset(candidates, url)
        if best:
            return best

        return None

    # ------------------------------------------------------------------
    # v1.7 candidate sweep — evaluate top 25 containers + merge buckets
    # ------------------------------------------------------------------

    def _extract_from_candidate_sweep(
        self,
        root: etree._Element,
        url: str,
        is_elementor: bool,
        page_apply_count: int,
    ) -> Optional[list[dict]]:
        """Evaluate a wider candidate set and merge compatible containers."""
        candidates = self._score_containers_v16(root, url, is_elementor, page_apply_count)
        if not candidates:
            return None

        candidates.sort(key=lambda c: (c[1], c[2]), reverse=True)

        bucketed_jobs: dict[str, list[dict]] = defaultdict(list)

        for el, score, _child_count in candidates[:25]:
            tag = (el.tag or "").lower() if isinstance(el.tag, str) else ""
            if tag in {"a", "span", "p", "h1", "h2", "h3", "h4", "h5", "h6"}:
                continue

            jobs = self._extract_jobs_v15(el, url, score)
            if not jobs:
                continue

            jobs = self._dedupe_jobs(jobs)
            key = self._container_bucket_key(el)
            bucketed_jobs[key].extend(jobs)

        if not bucketed_jobs:
            return None

        best_jobs: Optional[list[dict]] = None
        best_score = -1.0

        for _, jobs in bucketed_jobs.items():
            deduped = self._dedupe_jobs(jobs)
            if not self._passes_jobset_validation(deduped, url):
                continue
            score = self._jobset_score(deduped, url)
            if score > best_score:
                best_score = score
                best_jobs = deduped

        return best_jobs

    # ------------------------------------------------------------------
    # v1.7 repeated-row harvesting
    # ------------------------------------------------------------------

    def _extract_from_repeated_rows(
        self, root: etree._Element, url: str,
    ) -> Optional[list[dict]]:
        """Global row harvest for repeated job-like row classes across the page."""
        row_buckets: dict[str, list[etree._Element]] = defaultdict(list)

        for el in root.iter():
            if not isinstance(el.tag, str):
                continue

            tag = el.tag.lower()
            if tag not in {"li", "div", "tr", "article", "section"}:
                continue

            classes = _get_el_classes(el)
            if not classes:
                continue
            if not _ROW_CLASS_STRONG_PATTERN_V17.search(classes):
                continue

            tokens = classes.split()
            row_token = next(
                (t for t in tokens if _ROW_CLASS_STRONG_PATTERN_V17.search(t)),
                tokens[0],
            )
            key = f"{tag}:{row_token}"
            row_buckets[key].append(el)

        best_jobs: Optional[list[dict]] = None
        best_score = -1.0

        for _, rows in row_buckets.items():
            if len(rows) < 3 or len(rows) > MAX_JOBS_PER_PAGE:
                continue

            jobs: list[dict] = []
            for row in rows:
                job = self._extract_heuristic_job_v19(row, url, container_score=14)
                if job:
                    jobs.append(job)

            jobs = self._dedupe_jobs(jobs)
            if not self._passes_jobset_validation(jobs, url):
                continue

            score = self._jobset_score(jobs, url)
            if score > best_score:
                best_score = score
                best_jobs = jobs

        return best_jobs

    # ------------------------------------------------------------------
    # v1.7 heading-section fallback
    # ------------------------------------------------------------------

    def _extract_from_heading_sections(
        self, root: etree._Element, url: str,
    ) -> Optional[list[dict]]:
        """Fallback for pages where vacancies are represented as heading sections."""
        headings = root.xpath(
            "//main//h2 | //main//h3 | //main//h4 | "
            "//article//h2 | //article//h3 | //article//h4"
        )
        if not headings:
            headings = root.xpath("//h2 | //h3 | //h4")

        jobs: list[dict] = []
        for h in headings:
            raw_title = _text(h)
            title = self._normalize_title_v17(raw_title)
            if not self._is_valid_title_v19(title):
                continue

            ancestor_classes = self._collect_ancestor_classes(h, depth=4)
            has_content_ancestor = bool(
                re.search(
                    r"content|prose|article|entry|post|body|career|vacanc|job",
                    ancestor_classes,
                )
            )
            if not has_content_ancestor:
                continue

            parent_text = _text(h.getparent())[:700] if h.getparent() is not None else ""
            has_title_signal = self._title_has_job_signal(title)
            has_context_signal = bool(_CONTEXT_HINT_PATTERN_V17.search(parent_text))
            if not (has_title_signal or has_context_signal):
                continue

            link_href = _href(h)
            if not link_href:
                sibling_links = h.xpath("following-sibling::a[1]")
                if sibling_links:
                    link_href = sibling_links[0].get("href")

            source_url = _resolve_url(link_href, url) or url

            location = None
            loc_match = _AU_LOCATIONS.search(parent_text)
            if loc_match:
                location = loc_match.group(0)

            employment_type = None
            type_match = _JOB_TYPE_PATTERN.search(parent_text)
            if type_match:
                employment_type = type_match.group(0)

            jobs.append({
                "title": title,
                "source_url": source_url,
                "location_raw": location,
                "salary_raw": None,
                "employment_type": employment_type,
                "description": None,
                "extraction_method": "tier2_heading_sections_v19",
                "extraction_confidence": 0.62,
            })

        jobs = self._dedupe_jobs(jobs)
        if not self._passes_jobset_validation(jobs, url):
            return None
        return jobs

    # ------------------------------------------------------------------
    # v1.7 job extraction with title normalization
    # ------------------------------------------------------------------

    def _extract_heuristic_job_v19(
        self, row: etree._Element, base_url: str, container_score: int,
    ) -> Optional[dict]:
        """v1.9 post-processes v1.5 row extraction with v1.7 title normalization."""
        job = super()._extract_heuristic_job_v15(row, base_url, container_score)
        if not job:
            return None

        normalized = self._normalize_title_v17(job.get("title", ""))
        if not self._is_valid_title_v19(normalized):
            return None

        job["title"] = normalized
        job["extraction_method"] = "tier2_heuristic_v19"

        # If the row has a better explicit link than what base extraction found
        if job.get("source_url") == base_url:
            for a_el in row.iter("a"):
                href = a_el.get("href")
                candidate = _resolve_url(href, base_url)
                if not candidate:
                    continue
                if _JOB_URL_PATTERN.search(candidate) or candidate != base_url:
                    job["source_url"] = candidate
                    break

        return job

    # ------------------------------------------------------------------
    # v1.7 title validation
    # ------------------------------------------------------------------

    def _is_valid_title_v19(self, title: str) -> bool:
        """v1.9 title validation: v1.5 checks + v1.7 broader non-job rejection."""
        if not TieredExtractorV16._is_valid_title_v15(title):
            return False

        t = (title or "").strip()
        if not t:
            return False

        lower = t.lower()

        if "%header_" in lower or "%label_" in lower:
            return False

        if _REJECT_TITLE_PATTERN_V17.match(lower):
            return False

        if _CTA_TITLE_PATTERN_V17.search(lower) and not _TITLE_HINT_PATTERN_V17.search(lower):
            return False

        if len(t) > 90 and not _TITLE_HINT_PATTERN_V17.search(lower):
            return False

        words = t.split()
        if len(words) > 12:
            return False
        if len(words) > 8 and not _title_has_job_noun(t):
            return False

        if len(t) > 60 and " " not in t:
            return False

        return True

    # ------------------------------------------------------------------
    # v1.7 title normalization
    # ------------------------------------------------------------------

    def _normalize_title_v17(self, title: str) -> str:
        """Normalize extracted titles by stripping appended metadata noise."""
        if not title:
            return ""

        t = " ".join(title.replace("\u00a0", " ").split())

        t = t.replace("%HEADER_", "").replace("%LABEL_", "")
        t = " ".join(t.split())

        meta_match = _TRAILING_META_SPLIT_V17.search(t)
        if meta_match and meta_match.start() > 6:
            t = t[: meta_match.start()].strip(" -|:\u2013")

        camel_match = _CAMEL_LOCATION_SPLIT_V17.match(t)
        if camel_match:
            left, pivot, right = camel_match.groups()
            if 3 <= len(right) <= 60 and not self._title_has_job_signal(right):
                t = f"{left}{pivot}".strip()

        tail_loc = re.match(
            r"^(.{4,120})([A-Z][A-Za-z\.-]+,\s*[A-Z][A-Za-z\.-]+)$", t,
        )
        if tail_loc:
            left, right = tail_loc.groups()
            if self._title_has_job_signal(left) and not self._title_has_job_signal(right):
                t = left.strip()

        loc_suffix = re.match(
            r"^(.{4,100})\s+([A-Z][A-Za-z\.\- ]+,\s*[A-Z][A-Za-z\.\- ]+)$", t,
        )
        if loc_suffix:
            left, right = loc_suffix.groups()
            if (
                len(right.split()) <= 4
                and self._title_has_job_signal(left)
                and not self._title_has_job_signal(right)
            ):
                t = left.strip()

        parts = re.split(r"\s{2,}|\n+|\t+|\s[\-|\u2013|\u2022]\s", t)
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if len(part) < 4:
                continue
            if self._title_has_job_signal(part) or _is_valid_title(part):
                t = part
                break

        t = " ".join(t.strip(" |:-\u2013\u2022").split())

        return t

    # ------------------------------------------------------------------
    # v1.7 jobset selection, validation, scoring
    # ------------------------------------------------------------------

    def _pick_best_jobset(
        self, jobsets: list[tuple[str, list[dict]]], page_url: str,
    ) -> Optional[list[dict]]:
        """Choose the highest-quality jobset across extraction strategies."""
        best: Optional[list[dict]] = None
        best_score = -1.0

        fallback: Optional[list[dict]] = None
        fallback_score = -1.0

        for label, jobs in jobsets:
            deduped = self._dedupe_jobs(jobs)
            if len(deduped) < MIN_JOBS_FOR_SUCCESS:
                continue

            pruned = [
                j for j in deduped
                if self._title_has_job_signal(j.get("title", ""))
                or self._is_job_like_url(j, page_url)
            ]
            if len(pruned) >= MIN_JOBS_FOR_SUCCESS:
                deduped = pruned

            score = self._jobset_score(deduped, page_url)

            if self._passes_jobset_validation(deduped, page_url):
                if score > best_score:
                    best_score = score
                    best = deduped
            else:
                if score > fallback_score:
                    fallback_score = score
                    fallback = deduped

            logger.debug(
                "v1.9 candidate %s: %d jobs, score=%.2f, valid=%s",
                label,
                len(deduped),
                score,
                self._passes_jobset_validation(deduped, page_url),
            )

        if best:
            return best[:MAX_JOBS_PER_PAGE]

        # Conservative fallback only when we still have clear job-like signals
        if fallback and self._job_signal_count(fallback) >= 2:
            return fallback[:MAX_JOBS_PER_PAGE]

        return None

    def _passes_jobset_validation(self, jobs: list[dict], page_url: str) -> bool:
        """Reject low-quality sets while preserving small real-job pages."""
        if len(jobs) < MIN_JOBS_FOR_SUCCESS:
            return False

        titles = [self._normalize_title_v17(j.get("title", "")) for j in jobs]
        valid_titles = [t for t in titles if self._is_valid_title_v19(t)]
        if len(valid_titles) < MIN_JOBS_FOR_SUCCESS:
            return False

        unique_titles = len({t.lower() for t in valid_titles})
        if unique_titles < max(2, int(len(valid_titles) * 0.6)):
            return False

        reject_hits = sum(
            1 for t in valid_titles if _REJECT_TITLE_PATTERN_V17.match(t.lower())
        )
        if reject_hits >= max(1, int(len(valid_titles) * 0.4)):
            return False

        cta_hits = sum(
            1
            for t in valid_titles
            if _CTA_TITLE_PATTERN_V17.search(t) and not self._title_has_job_signal(t)
        )
        if cta_hits >= max(1, int(len(valid_titles) * 0.35)):
            return False

        job_signal_hits = self._job_signal_count(jobs)
        job_url_hits = sum(1 for j in jobs if self._is_job_like_url(j, page_url))

        if len(valid_titles) <= 3:
            return job_signal_hits >= 1 or job_url_hits >= 2

        if job_url_hits == 0:
            if len(valid_titles) <= 4 and job_signal_hits >= 1:
                return True
            return len(valid_titles) <= 6 and job_signal_hits >= max(
                2, int(len(valid_titles) * 0.5)
            )

        if len(valid_titles) > 6 and job_url_hits < max(
            2, int(len(valid_titles) * 0.3)
        ):
            return False

        return (
            job_signal_hits >= max(1, int(len(valid_titles) * 0.25))
            or job_url_hits >= max(2, int(len(valid_titles) * 0.4))
        )

    def _jobset_score(self, jobs: list[dict], page_url: str) -> float:
        """Score a jobset by volume, title quality, and URL quality."""
        if not jobs:
            return 0.0

        count = len(jobs)
        job_signals = self._job_signal_count(jobs)
        linked_hits = sum(1 for j in jobs if self._is_job_like_url(j, page_url))
        unique_urls = len(
            {(j.get("source_url") or "").strip() for j in jobs if j.get("source_url")}
        )

        score = count * 4.0
        score += job_signals * 2.5
        score += linked_hits * 1.5
        score += min(unique_urls, count)

        if count <= 3 and job_signals == 0 and linked_hits == 0:
            score -= 8.0

        return score

    def _job_signal_count(self, jobs: list[dict]) -> int:
        """Count jobs whose title contains a job-signal word."""
        return sum(1 for j in jobs if self._title_has_job_signal(j.get("title", "")))

    @staticmethod
    def _is_job_like_url(job: dict, page_url: str) -> bool:
        """Check whether a job's source_url looks like a real job detail link."""
        src = (job.get("source_url") or "").strip()
        if not src or src == page_url:
            return False

        try:
            parsed = urlparse(src)
        except Exception:
            return False

        path = (parsed.path or "").lower()
        query = (parsed.query or "").lower()

        if _NON_JOB_URL_PATTERN_V17.search(path):
            return False

        if _JOB_URL_HINT_PATTERN_V17.search(path):
            return True

        if "search=" in query or ("job" in query and "id=" in query):
            return True

        if re.search(r"/p/[^/]{4,}", path):
            return True

        return False

    # ------------------------------------------------------------------
    # v1.7 deduplication
    # ------------------------------------------------------------------

    def _dedupe_jobs(self, jobs: list[dict]) -> list[dict]:
        """Deduplicate jobs by normalized title + URL."""
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for job in jobs:
            title = self._normalize_title_v17(job.get("title", ""))
            if not self._is_valid_title_v19(title):
                continue

            source_url = (job.get("source_url") or "").strip()
            key = (title.lower(), source_url.lower())
            if key in seen:
                continue

            seen.add(key)
            cloned = dict(job)
            cloned["title"] = title
            deduped.append(cloned)

        return deduped[:MAX_JOBS_PER_PAGE]

    # ------------------------------------------------------------------
    # v1.7 job-signal detection
    # ------------------------------------------------------------------

    def _title_has_job_signal(self, title: str) -> bool:
        """Check if title contains a job-noun or broader job-hint word."""
        if not title:
            return False
        if _title_has_job_noun(title):
            return True
        return bool(_TITLE_HINT_PATTERN_V17.search(title))

    # ------------------------------------------------------------------
    # v1.7 container bucket key
    # ------------------------------------------------------------------

    @staticmethod
    def _container_bucket_key(el: etree._Element) -> str:
        """Build a bucket key for grouping sibling containers."""
        tag = (el.tag or "").lower() if isinstance(el.tag, str) else "el"
        classes = _get_el_classes(el).split()
        if not classes:
            return tag

        important = [
            c for c in classes
            if any(
                k in c
                for k in (
                    "job", "career", "vacan", "position",
                    "opening", "listing", "search",
                )
            )
        ]
        seed = important[0] if important else classes[0]
        return f"{tag}:{seed}"

    # ------------------------------------------------------------------
    # v1.7 ancestor class collection
    # ------------------------------------------------------------------

    @staticmethod
    def _collect_ancestor_classes(el: etree._Element, depth: int = 4) -> str:
        """Collect class names from ancestor elements up to `depth` levels."""
        classes: list[str] = []
        node = el
        for _ in range(depth):
            node = node.getparent()
            if node is None or not isinstance(node.tag, str):
                break
            cls = (node.get("class") or "").strip()
            if cls:
                classes.append(cls.lower())
        return " ".join(classes)
