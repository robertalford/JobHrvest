"""
Tiered Extraction Engine v6.3 — URL hint support, increased extraction volume,
and improved pagination handling.

Changes from v6.2:
  1. Accept URL hint via career_page.hint_url — skip discovery if provided.
  2. Increased extraction volume: if parent v1.6 returns < 5 jobs but page
     has many job-like links, also try DOM fallbacks and merge results.
  3. Better pagination: detect class*=pagination / class*=pager elements,
     follow numbered page links (not just "next page"), extract from each.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional
from urllib.parse import urlparse, urljoin

import httpx
from lxml import etree

from app.crawlers.tiered_extractor_v62 import TieredExtractorV62
from app.crawlers.tiered_extractor_v60 import (
    TieredExtractorV60,
    _detect_ats_platform,
    _APPLY_CONTEXT,
)
from app.crawlers.tiered_extractor_v16 import _title_has_job_noun
from app.crawlers.tiered_extractor import (
    _parse_html,
    _text,
    _resolve_url,
    MAX_JOBS_PER_PAGE,
    MIN_JOBS_FOR_SUCCESS,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Job-like link detection for extraction volume boost
# ---------------------------------------------------------------------------

_JOB_LINK_PATTERN = re.compile(
    r"/(?:job|career|position|opening|vacanc|rolle|stelle|posting|opportunity)",
    re.IGNORECASE,
)

# Pagination container detection
_PAGINATION_CLASS_PATTERN = re.compile(
    r"pagination|pager|page-nav|page-numbers|paginate|paging",
    re.IGNORECASE,
)


class TieredExtractorV63(TieredExtractorV62):
    """v6.3 extractor: URL hint support, increased extraction volume,
    improved pagination handling."""

    # ==================================================================
    # Change 1: Accept URL hint via career_page.hint_url
    # ==================================================================

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # If a hint_url is available, use it directly (skip discovery)
        hint_url = getattr(career_page, "hint_url", None)
        if hint_url:
            logger.info("v6.3 using hint URL: %s (original: %s)", hint_url, url)
            # Override the career_page URL with the hint
            if hasattr(career_page, "url"):
                career_page.url = hint_url
            url = hint_url

            # Fetch fresh HTML for the hint URL if it differs from the original
            if html and hint_url != url:
                hint_html = await self._fetch_hint_url(hint_url)
                if hint_html:
                    html = hint_html
            elif not html:
                hint_html = await self._fetch_hint_url(hint_url)
                if hint_html:
                    html = hint_html

        working_html = html or ""

        # --- Change 1 from v6.2: Force Playwright for SPA/JS sites ---
        working_html = await self._maybe_render_spa(url, working_html)

        ats_platform = _detect_ats_platform(url, working_html)

        # --- Parent v1.6 extraction with timeout ---
        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(
                super(TieredExtractorV60, self).extract(career_page, company, working_html),
                timeout=15.0,
            )
        except asyncio.TimeoutError:
            logger.warning("v6.3 parent extractor timeout for %s", url)
        except Exception:
            logger.exception("v6.3 parent extractor failed for %s", url)
        parent_jobs = self._dedupe(parent_jobs or [], url)

        # Phase 2: Structured data extraction
        root = _parse_html(working_html)
        candidates: list[tuple[str, list[dict]]] = []
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        structured_jobs = self._extract_structured_jobs(working_html, url)
        if structured_jobs:
            candidates.append(("structured_jsonld", structured_jobs))

        script_jobs = self._extract_jobs_from_state_scripts(working_html, url)
        if script_jobs:
            candidates.append(("state_script", script_jobs))

        # Phase 3: Dedicated ATS extractors (15s timeout)
        if ats_platform:
            try:
                ats_jobs = await asyncio.wait_for(
                    self._extract_ats_specific(ats_platform, url, working_html),
                    timeout=15.0,
                )
            except asyncio.TimeoutError:
                logger.warning("v6.3 ATS %s timeout for %s", ats_platform, url)
                ats_jobs = []
            except Exception:
                logger.exception("v6.3 ATS %s failed for %s", ats_platform, url)
                ats_jobs = []
            if ats_jobs:
                candidates.append((f"ats_{ats_platform}", ats_jobs))

        # Phase 4: DOM fallbacks — CHANGE 2: Lower threshold + count job links
        best_so_far = max((len(jobs) for _, jobs in candidates), default=0)
        page_job_link_count = self._count_page_job_links(root) if root is not None else 0

        # Run fallbacks if we have few results OR if the page has many job links
        # but we only extracted a few (missed jobs)
        run_fallbacks = (
            best_so_far < 3
            or (best_so_far < 5 and page_job_link_count > best_so_far * 2)
        )

        if run_fallbacks and root is not None:
            link_jobs = self._extract_from_job_links(root, url)
            if link_jobs:
                candidates.append(("job_links", link_jobs))

            accordion_jobs = self._extract_from_accordion_sections(root, url)
            if accordion_jobs:
                candidates.append(("accordion", accordion_jobs))

            elementor_jobs = self._extract_from_elementor_sections(root, url)
            if elementor_jobs:
                candidates.append(("elementor", elementor_jobs))

            content_block_jobs = self._extract_from_content_blocks(root, url)
            if content_block_jobs:
                candidates.append(("content_blocks", content_block_jobs))

            heading_jobs = self._extract_from_heading_rows(root, url)
            if heading_jobs:
                candidates.append(("heading_rows", heading_jobs))

            row_jobs = self._extract_from_repeating_rows(root, url)
            if row_jobs:
                candidates.append(("repeating_rows", row_jobs))

        # Pick best candidate set
        best_label, best_jobs = self._pick_best_jobset(candidates, url)
        if not best_jobs:
            return []

        # --- Change 2: Merge fallback results if parent returned few jobs ---
        if best_so_far < 5 and page_job_link_count > best_so_far * 2:
            best_jobs = self._merge_candidate_jobs(candidates, best_label, best_jobs, url)

        # --- Change 3: Follow pagination links ---
        if len(best_jobs) < MAX_JOBS_PER_PAGE and root is not None:
            try:
                paginated_jobs = await asyncio.wait_for(
                    self._follow_pagination_v63(url, working_html, best_jobs, career_page, company),
                    timeout=20.0,
                )
                if paginated_jobs and len(paginated_jobs) > len(best_jobs):
                    best_jobs = paginated_jobs
            except asyncio.TimeoutError:
                logger.warning("v6.3 pagination timeout for %s", url)
            except Exception:
                logger.exception("v6.3 pagination failed for %s", url)

        # Parallel detail page enrichment (from v6.2)
        needs_enrichment = any(
            (not j.get("location_raw") or not j.get("description"))
            for j in best_jobs
        )
        if (
            needs_enrichment
            and len(best_jobs) >= MIN_JOBS_FOR_SUCCESS
            and any(self._is_job_like_url(j.get("source_url") or "") for j in best_jobs)
        ):
            try:
                best_jobs = await asyncio.wait_for(
                    self._enrich_parallel(best_jobs),
                    timeout=25.0,
                )
            except asyncio.TimeoutError:
                logger.warning("v6.3 enrichment timeout for %s", url)
            except Exception:
                logger.exception("v6.3 enrichment failed for %s", url)
            best_jobs = self._dedupe(best_jobs, url)

        return best_jobs[:MAX_JOBS_PER_PAGE]

    # ==================================================================
    # Change 1 helper: Fetch HTML from a hint URL
    # ==================================================================

    async def _fetch_hint_url(self, hint_url: str) -> Optional[str]:
        """Fetch HTML from a hint URL."""
        try:
            async with httpx.AsyncClient(
                timeout=12,
                follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            ) as client:
                resp = await client.get(hint_url)
                if resp.status_code == 200 and len(resp.text or "") > 200:
                    return resp.text
        except Exception:
            logger.warning("v6.3 hint URL fetch failed: %s", hint_url)
        return None

    # ==================================================================
    # Change 2: Count job-like links on page
    # ==================================================================

    @staticmethod
    def _count_page_job_links(root: Optional[etree._Element]) -> int:
        """Count links on the page that look like job detail URLs."""
        if root is None:
            return 0

        count = 0
        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href:
                continue
            if _JOB_LINK_PATTERN.search(href):
                count += 1
        return count

    # ==================================================================
    # Change 2: Merge results from multiple candidate sets
    # ==================================================================

    def _merge_candidate_jobs(
        self,
        candidates: list[tuple[str, list[dict]]],
        best_label: str,
        best_jobs: list[dict],
        page_url: str,
    ) -> list[dict]:
        """Merge jobs from all candidate sets, deduplicating by URL.

        Used when the best set has few jobs but the page has many job links,
        suggesting jobs are spread across multiple extraction methods.
        """
        seen_urls: set[str] = set()
        merged: list[dict] = []

        # Add best jobs first (highest priority)
        for job in best_jobs:
            src = (job.get("source_url") or "").rstrip("/").lower()
            if src and src not in seen_urls:
                seen_urls.add(src)
                merged.append(job)
            elif not src:
                merged.append(job)

        # Add jobs from other candidate sets
        for label, jobs in candidates:
            if label == best_label:
                continue
            for job in jobs:
                src = (job.get("source_url") or "").rstrip("/").lower()
                if src and src not in seen_urls:
                    seen_urls.add(src)
                    merged.append(job)

        return self._dedupe(merged, page_url)[:MAX_JOBS_PER_PAGE]

    # ==================================================================
    # Change 3: Improved pagination handling
    # ==================================================================

    async def _follow_pagination_v63(
        self,
        base_url: str,
        html: str,
        jobs: list[dict],
        career_page,
        company,
    ) -> list[dict]:
        """Detect and follow pagination links, including numbered page links
        and pager elements (not just "next page" links).

        Handles:
        - class*=pagination / class*=pager containers with numbered links
        - Standard next-page links
        - Tab/category navigation that reveals more jobs
        """
        root = _parse_html(html)
        if root is None:
            return jobs

        # Find pagination URLs from pager/pagination containers
        pagination_urls = self._find_pagination_urls(root, base_url)

        if not pagination_urls:
            return jobs

        logger.info(
            "v6.3 pagination: found %d page links for %s",
            len(pagination_urls), base_url,
        )

        all_jobs = list(jobs)
        existing_urls = {(j.get("source_url") or "").rstrip("/").lower() for j in all_jobs}
        pages_fetched = 0
        max_pages = 8  # safety limit

        async with httpx.AsyncClient(
            timeout=10,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        ) as client:
            for page_url in pagination_urls:
                if pages_fetched >= max_pages:
                    break
                if len(all_jobs) >= MAX_JOBS_PER_PAGE:
                    break

                try:
                    resp = await client.get(page_url)
                    if resp.status_code != 200:
                        continue
                    page_html = resp.text or ""
                    if len(page_html) < 200:
                        continue

                    # SPA rendering if needed
                    page_html = await self._maybe_render_spa(page_url, page_html)

                    # Extract using parent v1.6
                    class _CP:
                        def __init__(s):
                            s.url = page_url
                            s.id = None

                    page_jobs: list[dict] = []
                    try:
                        page_jobs = await asyncio.wait_for(
                            super(TieredExtractorV60, self).extract(_CP(), company, page_html),
                            timeout=10.0,
                        )
                    except (asyncio.TimeoutError, Exception):
                        pass

                    if not page_jobs:
                        # Try DOM fallbacks on pagination page
                        page_root = _parse_html(page_html)
                        if page_root is not None:
                            page_jobs = self._extract_from_job_links(page_root, page_url)
                            if not page_jobs:
                                page_jobs = self._extract_from_repeating_rows(page_root, page_url)

                    if not page_jobs:
                        continue

                    # Deduplicate against existing jobs
                    new_jobs = []
                    for j in page_jobs:
                        src = (j.get("source_url") or "").rstrip("/").lower()
                        if src and src not in existing_urls:
                            existing_urls.add(src)
                            new_jobs.append(j)

                    if not new_jobs:
                        # No new jobs on this page, likely reached the end
                        break

                    all_jobs.extend(new_jobs)
                    pages_fetched += 1
                    logger.info(
                        "v6.3 pagination page %d: +%d jobs (total %d)",
                        pages_fetched, len(new_jobs), len(all_jobs),
                    )

                except Exception as e:
                    logger.debug("v6.3 pagination failed on %s: %s", page_url, e)
                    continue

        return all_jobs

    def _find_pagination_urls(self, root: etree._Element, base_url: str) -> list[str]:
        """Find all pagination URLs from the page.

        Looks for:
        1. Elements with class containing pagination/pager and their child links
        2. Standard next-page link patterns
        3. Numbered page links (page=2, page=3, etc.)
        """
        urls: list[str] = []
        seen: set[str] = {base_url.rstrip("/").lower()}

        # Strategy 1: Find pagination/pager containers and extract their links
        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            classes = (el.get("class") or "").lower()
            role = (el.get("role") or "").lower()

            is_pagination = (
                _PAGINATION_CLASS_PATTERN.search(classes)
                or role == "navigation"
                and _PAGINATION_CLASS_PATTERN.search(classes + " " + (el.get("aria-label") or ""))
            )

            if not is_pagination:
                continue

            for a_el in el.iter("a"):
                href = (a_el.get("href") or "").strip()
                if not href or href == "#" or href.startswith("javascript:"):
                    continue

                resolved = _resolve_url(href, base_url)
                if not resolved:
                    continue

                normalized = resolved.rstrip("/").lower()
                if normalized not in seen:
                    seen.add(normalized)
                    urls.append(resolved)

        # Strategy 2: Find links with page-number patterns in href
        if not urls:
            page_pattern = re.compile(
                r"[?&]page=\d+|/page/\d+|[?&]p=\d+|[?&]offset=\d+|[?&]start=\d+",
                re.IGNORECASE,
            )
            for a_el in root.iter("a"):
                href = (a_el.get("href") or "").strip()
                if not href or href == "#" or href.startswith("javascript:"):
                    continue
                if page_pattern.search(href):
                    resolved = _resolve_url(href, base_url)
                    if not resolved:
                        continue
                    normalized = resolved.rstrip("/").lower()
                    if normalized not in seen:
                        seen.add(normalized)
                        urls.append(resolved)

        # Strategy 3: Look for "Next" / ">" / ">>" links as a final fallback
        if not urls:
            next_pattern = re.compile(
                r"^(?:next|volgende|siguiente|nächste|suivant|>|>>|›|»)\s*$",
                re.IGNORECASE,
            )
            for a_el in root.iter("a"):
                href = (a_el.get("href") or "").strip()
                if not href or href == "#" or href.startswith("javascript:"):
                    continue

                link_text = (_text(a_el) or "").strip()
                aria_label = (a_el.get("aria-label") or "").strip().lower()
                rel = (a_el.get("rel") or "").lower()

                is_next = (
                    next_pattern.match(link_text)
                    or "next" in aria_label
                    or rel == "next"
                )

                if is_next:
                    resolved = _resolve_url(href, base_url)
                    if resolved:
                        normalized = resolved.rstrip("/").lower()
                        if normalized not in seen:
                            seen.add(normalized)
                            urls.append(resolved)

        return urls
