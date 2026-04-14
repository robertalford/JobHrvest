"""
Tiered Extraction Engine v6.5 — Improved title validation, title normalization,
and Manatal (careers-page.com) ATS extraction.

Inherits from TieredExtractorV16 (stable base) via V60, using V64 as reference.
Builds on V64's hint-aware extraction, bounded enrichment, and SPA rendering.

Changes from v6.4:
  1. Expanded title rejection: reject navigation labels, recruitment service labels,
     page section labels, and non-job CTA links that were passing through as false
     positives across 9+ sites.
  2. Title normalization: strip "NEW"/"New" badge suffixes and middot-separated
     metadata suffixes ("Title · Location" → "Title"). Improves title quality
     matching across 5+ sites.
  3. Manatal/careers-page.com ATS detection and DOM card extraction: parse
     .job-card elements with .jobs-title headings and /jobs/UUID links.
     Recovers ~49 jobs from 2+ previously-failed sites.
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

from app.crawlers.tiered_extractor_v64 import TieredExtractorV64
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
# Change 1: Additional title rejection patterns for common false positives
# ---------------------------------------------------------------------------

_FP_NAVIGATION_LABELS = re.compile(
    r"^(?:"
    # Recruitment agency/site navigation labels
    r"(?:for\s+)?job\s+seekers?|submit\s+a?\s*vacanc(?:y|ies)|career\s+seekers?|"
    r"recruitment\s+support|permanent\s+recruitment|temporary\s+recruitment|"
    r"find\s+a?\s*jobs?|send\s+(?:us\s+)?your\s+c\.?v\.?|what\s+we\s+recruit|"
    # Page section/category labels
    r"areas\s+we\s+recruit\s+into|meet\s+our\s+.*(?:panel|team)|"
    r"all\s+group\s+compan(?:y|ies)|our\s+(?:brands?|partners?|offices?)|"
    r"culture\s+we\s+are\s+\w+|"
    # Legal/policy labels
    r"(?:google\s+)?data\s+polic(?:y|ies)|do\s+not\s+sell\s+my\s+info(?:rmation)?.*|"
    r"cookie\s+(?:settings?|preferences?)|"
    # Job listing meta labels (not jobs themselves)
    r"job\s+qualifications?|job\s+descriptions?|job\s+requirements?|"
    r"online\s+jobs?|interactive\s+job\s+map|all\s+job\s+types?\s*(?:\(\d+\))?|"
    # ATS/platform navigation
    r"(?:prosegur|change)\s+(?:change\s+)?(?:australia|new\s+zealand|group)|"
    r"the\s+\w+\s+group|"
    # Document/framework labels (not jobs)
    r"\w+\s+(?:core\s+)?capability\s+framework.*|"
    r"\w+\s+applicant\s+pack.*|"
    r"opens?\s+in\s+new\s+window"
    r")$",
    re.IGNORECASE,
)

# Phone number with pipe/separator (e.g. "1300 272 019 | Email")
_PHONE_PIPE_PATTERN = re.compile(
    r"^(?:\+?\d[\d\s().-]{4,})\s*\|",
)

# Titles that are just duration/filter labels
_FILTER_LABEL = re.compile(
    r"^(?:long\s*[>]\s*\d+\s*mos?|short\s*[<]\s*\d+\s*mos?|"
    r"full\s*[-\s]?time|part\s*[-\s]?time|contract|casual|"
    r"permanent|temporary)$",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Change 2: Title normalization patterns
# ---------------------------------------------------------------------------

# "NEW" badge at end of title (e.g. "Assistant HR ManagerNEW", "Title New")
_NEW_BADGE = re.compile(r"(?:NEW|New|new)$")

# Middot-separated metadata suffix (e.g. "Title · Location" or "Title · Department")
_MIDDOT_SUFFIX = re.compile(r"\s*[·•]\s+[A-Z][A-Za-z\s,]+$")

# "SEEK. All applications must..." type trailing text
_SEEK_TAIL = re.compile(r"\s*SEEK\.?\s+All\s+applications?\s+.*$", re.IGNORECASE)


class TieredExtractorV65(TieredExtractorV64):
    """v6.5 extractor: improved title validation, title normalization,
    and Manatal/careers-page.com ATS support."""

    # ==================================================================
    # Main extraction — extends v6.4's extract()
    # ==================================================================

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        # --- Change 3: Manatal/careers-page.com detection ---
        if self._is_manatal_page(url, working_html):
            manatal_jobs = self._extract_manatal_cards(working_html, url)
            if manatal_jobs:
                logger.info(
                    "v6.5 Manatal extraction found %d jobs for %s",
                    len(manatal_jobs), url,
                )
                # Still run parent extraction and merge/pick best
                parent_result = await super().extract(career_page, company, html)
                if len(parent_result) > len(manatal_jobs):
                    return parent_result
                return manatal_jobs

        # Run parent v6.4 extraction pipeline
        return await super().extract(career_page, company, html)

    # ==================================================================
    # Change 1: Override title validation to add new rejection patterns
    # ==================================================================

    def _is_valid_title_v60(self, title: str) -> bool:
        """Enhanced title validation: v6.0 base + v6.5 FP rejection."""
        # Run parent validation first
        if not super()._is_valid_title_v60(title):
            return False

        t = title.strip()

        # v6.5: reject navigation/section/policy labels
        if _FP_NAVIGATION_LABELS.match(t):
            return False

        # v6.5: reject phone + pipe patterns
        if _PHONE_PIPE_PATTERN.match(t):
            return False

        # v6.5: reject pure filter/duration labels
        if _FILTER_LABEL.match(t):
            return False

        return True

    # ==================================================================
    # Change 2: Override title normalization to clean badge/metadata
    # ==================================================================

    def _normalize_title(self, title: str) -> str:
        """Enhanced title normalization: parent + v6.5 badge/metadata cleanup."""
        t = super()._normalize_title(title)
        if not t:
            return t

        # Strip "NEW" badge suffix (e.g. "Assistant HR ManagerNEW" → "Assistant HR Manager")
        t = _NEW_BADGE.sub("", t).rstrip()

        # Strip "SEEK. All applications..." trailing text
        t = _SEEK_TAIL.sub("", t).rstrip()

        # Strip middot-separated metadata suffix if the prefix is already a valid title
        # Only do this if the part before middot has 2+ words (to avoid stripping real title parts)
        m = _MIDDOT_SUFFIX.search(t)
        if m:
            prefix = t[: m.start()].strip()
            if len(prefix.split()) >= 2:
                t = prefix

        return t.strip()

    # ==================================================================
    # Change 3: Manatal/careers-page.com detection and extraction
    # ==================================================================

    @staticmethod
    def _is_manatal_page(url: str, html: str) -> bool:
        """Detect Manatal careers-page.com platform."""
        lower_url = url.lower()
        if "careers-page.com" in lower_url:
            return True
        # Also detect via Manatal branding in HTML
        if html and "manatal" in html[:10000].lower():
            if "job-card" in html[:50000]:
                return True
        return False

    def _extract_manatal_cards(self, html: str, url: str) -> list[dict]:
        """Extract jobs from Manatal careers-page.com job cards.

        Manatal renders server-side HTML with:
        - .job-card containers
        - .jobs-title for job title text
        - .job-title-link <a> elements with /jobs/UUID href
        - Location text near fa-map-marker icons
        - Description in .jobs-description-container
        """
        root = _parse_html(html)
        if root is None:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()

        # Find job cards
        cards = root.xpath(
            "//*[contains(@class,'job-card')]"
        )
        if not cards:
            # Fallback: any container with /jobs/ links
            cards = root.xpath(
                "//div[.//a[contains(@href,'/jobs/')]]"
                "[.//h1 or .//h2 or .//h3 or .//h4 or .//*[contains(@class,'title')]]"
            )

        for card in cards[:MAX_JOBS_PER_PAGE]:
            # Extract title
            title_el = card.xpath(
                ".//*[contains(@class,'jobs-title')]"
                "|.//h1|.//h2|.//h3|.//h4"
                "|.//*[contains(@class,'title')]"
            )
            if not title_el:
                continue

            raw_title = _text(title_el[0])
            title = self._normalize_title(raw_title)
            if not self._is_valid_title_v60(title):
                continue

            # Extract link
            link_el = card.xpath(
                ".//a[contains(@href,'/jobs/')]"
                "|.//a[contains(@class,'job-title-link')]"
            )
            href = link_el[0].get("href") if link_el else None
            source_url = _resolve_url(href, url) if href else url

            # Dedupe by URL
            if source_url in seen_urls:
                continue
            seen_urls.add(source_url)

            # Extract location (near map marker icon or in location class)
            location = None
            loc_candidates = card.xpath(
                ".//*[contains(@class,'location')]"
                "|.//*[contains(@class,'fa-map')]/.."
                "|.//*[contains(@class,'fa-location')]/.."
            )
            for loc_el in loc_candidates:
                loc_text = _text(loc_el).strip()
                if loc_text and len(loc_text) < 100:
                    location = loc_text
                    break

            # Extract description
            description = None
            desc_el = card.xpath(
                ".//*[contains(@class,'description')]"
                "|.//*[contains(@class,'jobs-content')]"
            )
            for de in desc_el:
                desc_text = _text(de).strip()
                if desc_text and len(desc_text) > 30:
                    description = desc_text[:500]
                    break

            jobs.append({
                "title": title,
                "source_url": source_url,
                "location_raw": location,
                "description": description,
                "extraction_method": "ats_manatal",
                "extraction_confidence": 0.88,
            })

        return jobs
