"""
Tiered Extraction Engine v6.4 — Fast hint-aware extraction with bounded enrichment.

Inherits from v6.2, NOT v6.3 (skips v6.3's slow pagination).

Changes from v6.2:
  1. URL hint support (simplified from v6.3): check career_page.hint_url,
     fetch that URL. If static extraction returns 0 jobs, try Playwright
     rendering on the same URL before giving up (handles JS-rendered sites).
  2. No pagination following — v6.3's pagination was too slow (minutes vs
     seconds, hitting the 60s watchdog).
  3. Bounded detail enrichment: enrich up to 15 jobs in parallel (semaphore=3)
     with a 15s total timeout. For each job missing location or description,
     fetch the detail URL and try JSON-LD JobPosting, then parent's methods.
  4. Lower extraction threshold: run DOM fallbacks if best candidate set has
     < 3 jobs (catches more without expensive pagination).
  5. Phase budgets: parent v1.6 = 18s, ATS = 12s, enrichment = 15s.
     Total fits in 60s with margin.
"""

from __future__ import annotations

import asyncio
import json
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


class TieredExtractorV64(TieredExtractorV62):
    """v6.4 extractor: fast hint-aware extraction, bounded enrichment,
    no pagination (skips v6.3's slow approach)."""

    # ==================================================================
    # Main extraction — overrides v6.2's extract()
    # ==================================================================

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # --- Change 1: URL hint support ---
        hint_url = getattr(career_page, "hint_url", None)
        hint_used = False
        if hint_url:
            logger.info("v6.4 using hint URL: %s (original: %s)", hint_url, url)
            hint_html = await self._fetch_hint_url_v64(hint_url)
            if hint_html:
                # Use hint URL and its HTML
                if hasattr(career_page, "url"):
                    career_page.url = hint_url
                url = hint_url
                html = hint_html
                hint_used = True

        working_html = html or ""

        # --- SPA rendering (from v6.2) ---
        working_html = await self._maybe_render_spa(url, working_html)

        ats_platform = _detect_ats_platform(url, working_html)

        # --- Phase 1: Parent v1.6 extraction (18s budget) ---
        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(
                super(TieredExtractorV60, self).extract(career_page, company, working_html),
                timeout=18.0,
            )
        except asyncio.TimeoutError:
            logger.warning("v6.4 parent extractor timeout for %s", url)
        except Exception:
            logger.exception("v6.4 parent extractor failed for %s", url)
        parent_jobs = self._dedupe(parent_jobs or [], url)

        # --- Change 1 continued: If hint URL + static extraction got 0 jobs,
        # try Playwright rendering (JS-rendered site) ---
        if hint_used and not parent_jobs and not self._has_structured_jobs(working_html):
            logger.info(
                "v6.4 hint URL returned 0 jobs statically, trying Playwright: %s",
                url,
            )
            rendered_html = await self._render_with_playwright_v13(url)
            if rendered_html and len(rendered_html) > len(working_html) // 2:
                working_html = rendered_html
                # Re-run parent extraction on rendered HTML
                try:
                    parent_jobs = await asyncio.wait_for(
                        super(TieredExtractorV60, self).extract(
                            career_page, company, working_html
                        ),
                        timeout=18.0,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "v6.4 parent extractor timeout (Playwright) for %s", url
                    )
                except Exception:
                    logger.exception(
                        "v6.4 parent extractor failed (Playwright) for %s", url
                    )
                parent_jobs = self._dedupe(parent_jobs or [], url)

        # --- Phase 2: Structured data extraction (fast, no timeout needed) ---
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

        # --- Phase 3: Dedicated ATS extractors (12s budget) ---
        if ats_platform:
            try:
                ats_jobs = await asyncio.wait_for(
                    self._extract_ats_specific(ats_platform, url, working_html),
                    timeout=12.0,
                )
            except asyncio.TimeoutError:
                logger.warning("v6.4 ATS %s timeout for %s", ats_platform, url)
                ats_jobs = []
            except Exception:
                logger.exception("v6.4 ATS %s failed for %s", ats_platform, url)
                ats_jobs = []
            if ats_jobs:
                candidates.append((f"ats_{ats_platform}", ats_jobs))

        # --- Phase 4: DOM fallbacks (Change 4: threshold < 3) ---
        best_so_far = max((len(jobs) for _, jobs in candidates), default=0)
        if best_so_far < 3 and root is not None:
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

        # --- Change 2: NO pagination following (v6.3 was too slow) ---

        # --- Change 3: Bounded detail enrichment (15 jobs max, semaphore=3, 15s) ---
        needs_enrichment = any(
            (not j.get("location_raw") or not j.get("description"))
            for j in best_jobs
        )
        if (
            needs_enrichment
            and len(best_jobs) >= MIN_JOBS_FOR_SUCCESS
            and any(
                self._is_job_like_url(j.get("source_url") or "") for j in best_jobs
            )
        ):
            try:
                best_jobs = await asyncio.wait_for(
                    self._enrich_bounded_v64(best_jobs),
                    timeout=15.0,
                )
            except asyncio.TimeoutError:
                logger.warning("v6.4 enrichment timeout for %s", url)
            except Exception:
                logger.exception("v6.4 enrichment failed for %s", url)
            best_jobs = self._dedupe(best_jobs, url)

        return best_jobs[:MAX_JOBS_PER_PAGE]

    # ==================================================================
    # Change 1: Fetch hint URL HTML
    # ==================================================================

    async def _fetch_hint_url_v64(self, hint_url: str) -> Optional[str]:
        """Fetch HTML from a hint URL."""
        try:
            async with httpx.AsyncClient(
                timeout=12,
                follow_redirects=True,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36"
                    )
                },
            ) as client:
                resp = await client.get(hint_url)
                if resp.status_code == 200 and len(resp.text or "") > 200:
                    return resp.text
        except Exception:
            logger.warning("v6.4 hint URL fetch failed: %s", hint_url)
        return None

    # ==================================================================
    # Change 1 helper: Quick check for structured jobs in HTML
    # ==================================================================

    @staticmethod
    def _has_structured_jobs(html: str) -> bool:
        """Quick check whether the HTML has JSON-LD JobPosting or state script jobs."""
        if not html:
            return False
        lower = html[:100_000].lower()
        return '"jobposting"' in lower or '"job_posting"' in lower

    # ==================================================================
    # Change 3: Bounded parallel enrichment (max 15 jobs, semaphore=3)
    # ==================================================================

    async def _enrich_bounded_v64(self, jobs: list[dict]) -> list[dict]:
        """Enrich up to 15 jobs missing location or description in parallel.

        Uses semaphore=3 for bounded concurrency and 15s total timeout
        (enforced by caller). For each job, tries:
          1. JSON-LD JobPosting on the detail page
          2. Parent's meta tag extraction
          3. Parent's main content extraction
        """
        semaphore = asyncio.Semaphore(3)
        max_enrich = 15

        # Partition: jobs needing enrichment (up to max_enrich) vs. already complete
        to_enrich: list[tuple[int, dict]] = []
        for i, job in enumerate(jobs):
            if len(to_enrich) >= max_enrich:
                break
            if not job.get("location_raw") or not job.get("description"):
                detail_url = job.get("source_url") or ""
                if detail_url and self._is_job_like_url(detail_url):
                    to_enrich.append((i, job))

        if not to_enrich:
            return jobs

        async def _enrich_one(job: dict) -> dict:
            detail_url = job.get("source_url") or ""
            if not detail_url:
                return job

            async with semaphore:
                try:
                    async with httpx.AsyncClient(
                        timeout=8,
                        follow_redirects=True,
                        headers={"User-Agent": "Mozilla/5.0"},
                    ) as client:
                        resp = await client.get(detail_url)
                        if resp.status_code != 200:
                            return job
                        detail_html = resp.text or ""
                except Exception:
                    return job

            if not detail_html:
                return job

            # Priority 1: JSON-LD JobPosting schema
            enriched = self._enrich_from_jsonld(job, detail_html, detail_url)
            if enriched:
                return enriched

            # Priority 2: Meta tags
            enriched = self._enrich_from_meta(job, detail_html)
            if enriched:
                return enriched

            # Priority 3: Main content area text
            enriched = self._enrich_from_main_content(job, detail_html)
            return enriched

        tasks = [_enrich_one(job) for _, job in to_enrich]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Merge enriched results back into the jobs list
        enriched_jobs = list(jobs)
        for (idx, _original), result in zip(to_enrich, results):
            if isinstance(result, Exception):
                continue  # keep original
            enriched_jobs[idx] = result

        return enriched_jobs
