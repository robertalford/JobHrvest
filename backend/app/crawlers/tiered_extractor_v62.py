"""
Tiered Extraction Engine v6.2 — Targeted improvements over v6.1.

Changes from v6.1:
  1. Forced Playwright rendering for SPA/JS-heavy sites (React, Vue, Next.js).
  2. Heading + content block pattern detector for long-form job pages.
  3. Parallel detail page enrichment (all jobs, semaphore=5, 25s timeout).
  4. Fix AcquireTM handler: stay on listing page, parse table/card structure.
  5. Reduced parent v1.6 timeout from 20s to 15s for faster discovery.
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

from app.crawlers.tiered_extractor_v61 import TieredExtractorV61
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
# SPA detection markers
# ---------------------------------------------------------------------------

_SPA_MARKERS = [
    'id="root"></div>',
    'id="app"></div>',
    "v-cloak",
    "__NEXT_DATA__",
    "__remixContext",
]


class TieredExtractorV62(TieredExtractorV61):
    """v6.2 extractor: SPA rendering, content block detection, parallel enrichment,
    AcquireTM fix, faster discovery timeout."""

    # ==================================================================
    # Change 1 + 3 + 5 + 6: Overridden extract() with SPA detection,
    # parallel enrichment, content block fallback, reduced timeout
    # ==================================================================

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        # --- Change 1: Force Playwright for SPA/JS sites ---
        working_html = await self._maybe_render_spa(url, working_html)

        ats_platform = _detect_ats_platform(url, working_html)

        # --- Change 6: Reduced parent v1.6 timeout from 20s to 15s ---
        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(
                super(TieredExtractorV60, self).extract(career_page, company, working_html),
                timeout=15.0,
            )
        except asyncio.TimeoutError:
            logger.warning("v6.2 parent extractor timeout for %s", url)
        except Exception:
            logger.exception("v6.2 parent extractor failed for %s", url)
        parent_jobs = self._dedupe(parent_jobs or [], url)

        # Phase 2: Structured data extraction (always runs -- fast, no timeout needed)
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
                logger.warning("v6.2 ATS %s timeout for %s", ats_platform, url)
                ats_jobs = []
            except Exception:
                logger.exception("v6.2 ATS %s failed for %s", ats_platform, url)
                ats_jobs = []
            if ats_jobs:
                candidates.append((f"ats_{ats_platform}", ats_jobs))

        # Phase 4: DOM fallbacks (only if we don't have good results yet)
        best_so_far = max((len(jobs) for _, jobs in candidates), default=0)
        if best_so_far < 3 and root is not None:
            link_jobs = self._extract_from_job_links(root, url)
            if link_jobs:
                candidates.append(("job_links", link_jobs))

            accordion_jobs = self._extract_from_accordion_sections(root, url)
            if accordion_jobs:
                candidates.append(("accordion", accordion_jobs))

            # Elementor fallback (from v6.1)
            elementor_jobs = self._extract_from_elementor_sections(root, url)
            if elementor_jobs:
                candidates.append(("elementor", elementor_jobs))

            # --- Change 5: Content block pattern detector ---
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

        # --- Change 3: Parallel detail page enrichment (ALL jobs, semaphore=5, 25s) ---
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
                logger.warning("v6.2 enrichment timeout for %s", url)
            except Exception:
                logger.exception("v6.2 enrichment failed for %s", url)
            best_jobs = self._dedupe(best_jobs, url)

        return best_jobs[:MAX_JOBS_PER_PAGE]

    # ==================================================================
    # Change 1: SPA detection and forced Playwright rendering
    # ==================================================================

    async def _maybe_render_spa(self, url: str, html: str) -> str:
        """If the HTML looks like an unrendered SPA, force Playwright rendering."""
        if not html:
            return html

        # Check body text length vs HTML size
        root = _parse_html(html)
        body_text = ""
        if root is not None:
            body_els = root.xpath("//body")
            if body_els:
                body_text = _text(body_els[0])

        is_spa = False

        # Condition 1: body text < 500 chars BUT HTML size > 5KB
        if len(body_text) < 500 and len(html) > 5000:
            is_spa = True

        # Condition 2: contains framework markers
        if not is_spa:
            html_lower = html[:50000]  # only scan first 50KB for markers
            for marker in _SPA_MARKERS:
                if marker in html_lower:
                    is_spa = True
                    break

        if not is_spa:
            return html

        logger.info("v6.2 SPA detected for %s, forcing Playwright render", url)
        rendered = await self._render_with_playwright_v13(url)
        if rendered and len(rendered) > len(html) // 2:
            return rendered

        return html

    # ==================================================================
    # Change 3: Parallel detail page enrichment
    # ==================================================================

    async def _enrich_parallel(self, jobs: list[dict]) -> list[dict]:
        """Enrich ALL jobs missing location or description in parallel (semaphore=5)."""
        semaphore = asyncio.Semaphore(5)

        async def _enrich_one(job: dict) -> dict:
            if job.get("location_raw") and job.get("description"):
                return job

            detail_url = job.get("source_url") or ""
            if not detail_url or not self._is_job_like_url(detail_url):
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

            # Priority 3: Main content area text (markdownified, first 2000 chars)
            enriched = self._enrich_from_main_content(job, detail_html)
            return enriched

        tasks = [_enrich_one(j) for j in jobs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        enriched_jobs = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                enriched_jobs.append(jobs[i])
            else:
                enriched_jobs.append(result)

        return enriched_jobs

    def _enrich_from_jsonld(self, job: dict, html: str, page_url: str) -> Optional[dict]:
        """Try to enrich job from JSON-LD JobPosting on the detail page."""
        for match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html, re.IGNORECASE | re.DOTALL,
        ):
            raw = (match.group(1) or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue

            # Unwrap @graph arrays
            if isinstance(data, dict) and "@graph" in data:
                data = data["@graph"]
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and str(item.get("@type", "")).lower() == "jobposting":
                        data = item
                        break
                else:
                    continue

            if not isinstance(data, dict):
                continue
            if str(data.get("@type", "")).lower() != "jobposting":
                continue

            updated = dict(job)
            if not updated.get("location_raw"):
                loc = data.get("jobLocation")
                if isinstance(loc, dict):
                    addr = loc.get("address")
                    if isinstance(addr, dict):
                        parts = [
                            addr.get("addressLocality", ""),
                            addr.get("addressRegion", ""),
                            addr.get("addressCountry", ""),
                        ]
                        loc_str = ", ".join(p.strip() for p in parts if p and p.strip())
                        if loc_str:
                            updated["location_raw"] = loc_str
                    elif isinstance(addr, str) and addr.strip():
                        updated["location_raw"] = addr.strip()
                elif isinstance(loc, list) and loc:
                    first = loc[0]
                    if isinstance(first, dict):
                        addr = first.get("address")
                        if isinstance(addr, dict):
                            parts = [
                                addr.get("addressLocality", ""),
                                addr.get("addressRegion", ""),
                                addr.get("addressCountry", ""),
                            ]
                            loc_str = ", ".join(p.strip() for p in parts if p and p.strip())
                            if loc_str:
                                updated["location_raw"] = loc_str
                elif isinstance(loc, str) and loc.strip():
                    updated["location_raw"] = loc.strip()

            if not updated.get("description"):
                desc = data.get("description", "")
                if isinstance(desc, str) and desc.strip():
                    # Strip HTML if present
                    if "<" in desc:
                        p = _parse_html(desc)
                        if p is not None:
                            desc = _text(p)
                    updated["description"] = desc.strip()[:5000]

            if not updated.get("employment_type"):
                emp = data.get("employmentType", "")
                if isinstance(emp, str) and emp.strip():
                    updated["employment_type"] = emp.strip()
                elif isinstance(emp, list) and emp:
                    updated["employment_type"] = ", ".join(str(e) for e in emp)

            return updated

        return None

    def _enrich_from_meta(self, job: dict, html: str) -> Optional[dict]:
        """Try to enrich job from meta tags (og:description, description)."""
        root = _parse_html(html)
        if root is None:
            return None

        updated = dict(job)
        changed = False

        if not updated.get("description"):
            for xpath in [
                '//meta[@property="og:description"]/@content',
                '//meta[@name="description"]/@content',
                '//meta[@name="Description"]/@content',
            ]:
                vals = root.xpath(xpath)
                if vals and str(vals[0]).strip():
                    desc = str(vals[0]).strip()
                    if len(desc) > 50:
                        updated["description"] = desc[:5000]
                        changed = True
                        break

        if not updated.get("location_raw"):
            for xpath in [
                '//meta[@name="geo.placename"]/@content',
                '//meta[@name="geo.region"]/@content',
            ]:
                vals = root.xpath(xpath)
                if vals and str(vals[0]).strip():
                    updated["location_raw"] = str(vals[0]).strip()
                    changed = True
                    break

        return updated if changed else None

    def _enrich_from_main_content(self, job: dict, html: str) -> dict:
        """Enrich from main content area text, first 2000 chars."""
        if job.get("description"):
            return job

        root = _parse_html(html)
        if root is None:
            return job

        # Find main content area
        main_els = root.xpath(
            "//main|//article|//*[@role='main']|"
            "//*[contains(@class,'content')]|//*[contains(@class,'job-detail')]|"
            "//*[contains(@class,'job-description')]"
        )
        if main_els:
            content_text = _text(main_els[0])
        else:
            body = root.xpath("//body")
            content_text = _text(body[0]) if body else ""

        if content_text and len(content_text) > 80:
            updated = dict(job)
            updated["description"] = content_text[:2000]
            return updated

        return job

    # ==================================================================
    # Change 4: Fix AcquireTM handler -- stay on listing page
    # ==================================================================

    def _extract_acquiretm(self, url: str, html: str) -> list[dict]:
        """AcquireTM: parse job listings from the listing page (home.aspx).
        Bug fix: always use the listing URL, parse the table/card structure
        on THAT page rather than navigating to detail pages."""
        if not html:
            return []

        root = _parse_html(html)
        if root is None:
            return []

        jobs: list[dict] = []

        # AcquireTM listing pages (home.aspx) have a table or list of jobs.
        # Each row has a link to job_details_clean.aspx?id=N.
        # Extract title from the link text, URL from the href.

        # Strategy 1: Find links pointing to job_details_clean.aspx
        detail_links = root.xpath(
            "//a[contains(@href,'job_details') or contains(@href,'Job_Details') or "
            "contains(@href,'JobDetails') or contains(@href,'jobdetails')]"
        )

        if detail_links:
            for link_el in detail_links[:MAX_JOBS_PER_PAGE]:
                href = (link_el.get("href") or "").strip()
                title = self._normalize_title(_text(link_el))
                if not title or not self._is_valid_title_v60(title):
                    continue

                source_url = _resolve_url(href, url) if href else url

                # Try to find location/type in the same row (parent tr or containing div)
                row = link_el.getparent()
                if row is not None and row.tag != "tr":
                    row = row.getparent()
                row_text = _text(row) if row is not None else ""

                jobs.append({
                    "title": title,
                    "source_url": source_url or url,
                    "location_raw": self._extract_location(row_text) if row_text else None,
                    "salary_raw": self._extract_salary(row_text) if row_text else None,
                    "employment_type": self._extract_type(row_text) if row_text else None,
                    "description": None,
                    "extraction_method": "ats_acquiretm_v62",
                    "extraction_confidence": 0.85,
                })
            if jobs:
                return self._dedupe(jobs, url)

        # Strategy 2: Table rows with links (broader fallback, staying on listing page)
        rows = root.xpath(
            "//table//tr[.//a[@href]]"
        )
        for row in rows[:MAX_JOBS_PER_PAGE]:
            links = row.xpath(".//a[@href]")
            if not links:
                continue

            # Use first link's text as title
            title = self._normalize_title(_text(links[0]))
            if not self._is_valid_title_v60(title):
                continue

            href = (links[0].get("href") or "").strip()
            source_url = _resolve_url(href, url) if href else url

            row_text = _text(row)
            jobs.append({
                "title": title,
                "source_url": source_url or url,
                "location_raw": self._extract_location(row_text),
                "salary_raw": self._extract_salary(row_text),
                "employment_type": self._extract_type(row_text),
                "description": None,
                "extraction_method": "ats_acquiretm_v62",
                "extraction_confidence": 0.82,
            })

        # Strategy 3: Card-based layouts (same as parent but without navigation)
        if not jobs:
            cards = root.xpath(
                "//*[contains(@class,'job-row') or contains(@class,'job-listing') or "
                "contains(@class,'job-item') or contains(@class,'posting')]"
            )
            for card in cards[:MAX_JOBS_PER_PAGE]:
                heading = card.xpath(".//h1|.//h2|.//h3|.//h4|.//*[contains(@class,'title')]")
                if heading:
                    title = self._normalize_title(_text(heading[0]))
                else:
                    card_links = card.xpath(".//a[@href]")
                    title = self._normalize_title(_text(card_links[0])) if card_links else ""

                if not self._is_valid_title_v60(title):
                    continue

                link = card.xpath(".//a[@href]")
                href = link[0].get("href") if link else None
                source_url = _resolve_url(href, url) if href else url
                card_text = _text(card)

                jobs.append({
                    "title": title,
                    "source_url": source_url or url,
                    "location_raw": self._extract_location(card_text),
                    "salary_raw": self._extract_salary(card_text),
                    "employment_type": self._extract_type(card_text),
                    "description": card_text[:5000] if len(card_text) > 60 else None,
                    "extraction_method": "ats_acquiretm_v62",
                    "extraction_confidence": 0.80,
                })

        return self._dedupe(jobs, url)

    # ==================================================================
    # Change 5: Heading + Content Block Pattern Detector
    # ==================================================================

    def _extract_from_content_blocks(self, root: etree._Element, page_url: str) -> list[dict]:
        """Extract jobs from long-form pages where each job is displayed as a
        heading (h2/h3/h4) followed by content blocks (paragraphs, lists).

        Pattern:
          <h2>Job Title</h2>
          <p>Responsibilities:</p>
          <ul>...</ul>
          <p>Requirements:</p>
          <ul>...</ul>

        Each heading = one job. Content between headings = description.
        Confirmed as job listings by presence of "Apply" links or email addresses.
        """
        jobs: list[dict] = []

        # Find all h2/h3/h4 headings in the main content area
        main_area = root.xpath(
            "//main|//article|//*[@role='main']|"
            "//*[contains(@class,'content')]|//*[contains(@class,'jobs')]|"
            "//*[contains(@class,'careers')]|//*[contains(@class,'vacancies')]"
        )
        search_root = main_area[0] if main_area else root

        headings = search_root.xpath(".//h2|.//h3|.//h4")
        if len(headings) < 2:
            return []

        # Build heading-to-content mapping
        heading_blocks: list[tuple[etree._Element, str, str]] = []
        for i, h in enumerate(headings):
            title = self._normalize_title(_text(h))
            if not title or len(title) < 3:
                continue

            # Collect content between this heading and the next
            content_parts: list[str] = []
            apply_url: Optional[str] = None
            has_apply_signal = False

            sibling = h.getnext()
            next_heading_tags = {"h1", "h2", "h3", "h4"}

            while sibling is not None:
                if sibling.tag in next_heading_tags:
                    break

                sib_text = _text(sibling)
                content_parts.append(sib_text)

                # Check for Apply links
                for a_el in sibling.iter("a"):
                    a_text = (_text(a_el) or "").strip().lower()
                    a_href = (a_el.get("href") or "").strip()
                    if "apply" in a_text or "application" in a_text:
                        has_apply_signal = True
                        if a_href and not a_href.startswith("#"):
                            apply_url = _resolve_url(a_href, page_url)
                    # Check for mailto links (email application)
                    if a_href.startswith("mailto:"):
                        has_apply_signal = True

                sibling = sibling.getnext()

            content_text = "\n".join(content_parts).strip()

            # Also check for apply link or email in the heading itself
            for a_el in h.iter("a"):
                a_text = (_text(a_el) or "").strip().lower()
                a_href = (a_el.get("href") or "").strip()
                if "apply" in a_text:
                    has_apply_signal = True
                    if a_href and not a_href.startswith("#"):
                        apply_url = _resolve_url(a_href, page_url)
                if a_href.startswith("mailto:"):
                    has_apply_signal = True

            # Check for email addresses in content text
            if not has_apply_signal and re.search(
                r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", content_text
            ):
                has_apply_signal = True

            heading_blocks.append((h, title, content_text))

            # Check for job-like content indicators
            has_duties = bool(re.search(
                r"responsibilit|requirement|qualificat|duties|experience|"
                r"key\s+skills|essential\s+criteria|desirable|"
                r"what\s+you.ll|who\s+you\s+are|about\s+the\s+role",
                content_text, re.IGNORECASE,
            ))

            # Only add if this looks like a job posting
            if not has_apply_signal and not has_duties:
                continue

            if not self._is_valid_title_v60(title):
                continue

            # Build source URL from apply link or heading link
            source_url = apply_url
            if not source_url:
                h_links = h.xpath(".//a[@href]")
                if h_links:
                    href = (h_links[0].get("href") or "").strip()
                    if href and not href.startswith("#"):
                        source_url = _resolve_url(href, page_url)
            if not source_url:
                source_url = page_url

            jobs.append({
                "title": title,
                "source_url": source_url,
                "location_raw": self._extract_location(content_text) if content_text else None,
                "salary_raw": self._extract_salary(content_text) if content_text else None,
                "employment_type": self._extract_type(content_text) if content_text else None,
                "description": content_text[:5000] if len(content_text) > 80 else None,
                "extraction_method": "tier2_content_blocks",
                "extraction_confidence": 0.70,
            })

        return self._dedupe(jobs, page_url)
