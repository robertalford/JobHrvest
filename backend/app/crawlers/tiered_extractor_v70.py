"""
Tiered Extraction Engine v7.0 — linked-card completeness + metadata depth.

Builds on v6.9 with four focused improvements:
1. Better linked-card title extraction so role text is not glued with location
   metadata (for example "DevOps EngineerVijayawada").
2. Strong-path short-title acceptance for compact real roles on detail URLs
   (for example "Sales", "MERNSTACK") while keeping Type 1 guards strict.
3. Pagination URL collection expanded for Teamtailor-style `show_more` links.
4. Bounded post-path enrichment for fast ATS/card paths so description/location
   depth is recovered even when those paths return before the main v6.4 pipeline.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v67 import _CARD_PAGINATION_HINT, _WEAK_ROLE_HINT_V67
from app.crawlers.tiered_extractor_v69 import TieredExtractorV69

logger = logging.getLogger(__name__)

_V70_NON_JOB_HEADING = re.compile(
    r"^(?:"
    r"working\s+with\s+us|"
    r"show(?:\s+\d+)?\s+more|"
    r"load(?:\s+\d+)?\s+more|"
    r"view(?:\s+\d+)?\s+more|"
    r"see(?:\s+\d+)?\s+more|"
    r"older\s+entries|newer\s+entries"
    r")$",
    re.IGNORECASE,
)

_V70_DETAIL_PATH_HINT = re.compile(
    r"(?:"
    r"/job-detail/[^/?#]{2,}|"
    r"/jobs?/[^/?#]{3,}|"
    r"/career/[^/?#]{3,}|"
    r"[?&](?:jobid|jobadid|adid|vacancyid|ajid)=\w+"
    r")",
    re.IGNORECASE,
)

_V70_LOCATION_CLASS_HINT = re.compile(
    r"(?:location|city|region|office|metadata|body__secondary)",
    re.IGNORECASE,
)

_V70_SHORT_ROLE_ALLOWLIST = {
    "sales",
    "finance",
    "marketing",
    "hr",
    "it",
    "nurse",
    "teacher",
    "chef",
    "driver",
    "cashier",
    "internship",
}

_V70_UPPERCASE_TOKEN = re.compile(r"^[A-Z][A-Z0-9+&./-]{2,20}$")

_V70_EARLY_PATH_METHOD_PREFIXES = (
    "tier2_linked_cards_v67",
    "ats_jobvite_table_v68",
    "ats_wp_job_openings_v66",
    "ats_jobs2web_",
)


class TieredExtractorV70(TieredExtractorV69):
    """v7.0 extractor: linked-card coverage + bounded depth recovery."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        jobs = await super().extract(career_page, company, html)
        if not jobs:
            return []

        if not self._should_enrich_fast_path_v70(jobs):
            return jobs[:MAX_JOBS_PER_PAGE]

        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        try:
            jobs = await asyncio.wait_for(self._enrich_bounded_v64(jobs), timeout=12.0)
            jobs = self._dedupe(jobs, page_url)
        except asyncio.TimeoutError:
            logger.warning("v7.0 fast-path enrichment timeout for %s", page_url)
        except Exception:
            logger.exception("v7.0 fast-path enrichment failed for %s", page_url)

        return jobs[:MAX_JOBS_PER_PAGE]

    def _is_valid_title_v60(self, title: str) -> bool:
        if not super()._is_valid_title_v60(title):
            return False
        return not _V70_NON_JOB_HEADING.match((title or "").strip())

    def _extract_linked_job_cards_v67(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        anchors = root.xpath("//a[@href and not(starts-with(@href,'#')) and not(starts-with(@href,'javascript:'))]")
        if not anchors:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()

        for a_el in anchors[:900]:
            href = (a_el.get("href") or "").strip()
            if not href:
                continue
            source_url = _resolve_url(href, page_url) or page_url
            if source_url in seen_urls:
                continue

            has_strong_job_path = self._has_strong_card_detail_url_v70(source_url, page_url)
            if not has_strong_job_path and not self._is_job_like_url(source_url):
                continue

            title = self._extract_card_title_v67(a_el)
            if not title:
                continue
            if not self._is_valid_card_title_v67(title, has_strong_job_path):
                continue

            seen_urls.add(source_url)
            context_text = " ".join((_text(a_el) or "").split())
            short_desc = None
            if len(context_text) >= 120 and context_text.lower() != title.lower():
                short_desc = context_text[:5000]

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_card_location_v67(a_el, title),
                    "description": short_desc,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "tier2_linked_cards_v67",
                    "extraction_confidence": 0.8,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _is_valid_card_title_v67(self, title: str, has_strong_job_path: bool) -> bool:
        if self._is_valid_title_v60(title):
            return True
        if not has_strong_job_path:
            return False

        t = (title or "").strip()
        if not t or _V70_NON_JOB_HEADING.match(t):
            return False
        if not re.search(r"[A-Za-z]", t):
            return False

        words = t.split()
        if len(words) > 6:
            return False

        if len(words) == 1:
            token = re.sub(r"[^A-Za-z0-9+&./-]", "", words[0])
            if not token:
                return False
            if token.lower() in _V70_SHORT_ROLE_ALLOWLIST:
                return True
            return bool(_V70_UPPERCASE_TOKEN.match(token))

        if len(words) <= 4 and _WEAK_ROLE_HINT_V67.search(t):
            return True

        return len(words) <= 4

    def _extract_card_title_v67(self, a_el) -> Optional[str]:
        title_nodes = a_el.xpath(
            ".//h1|.//h2|.//h3|.//h4|"
            ".//p[contains(@class,'body--medium') or contains(@class,'sub-title') "
            "or contains(@class,'text-2xl') or contains(@class,'text-3xl') "
            "or contains(@class,'text-4xl') or contains(@class,'text-5xl') "
            "or contains(@class,'text-6xl') or contains(@class,'text-7xl')]|"
            ".//span[contains(@class,'sub-title') or contains(@class,'job-title') "
            "or contains(@class,'position-title') or contains(@class,'role-title')]|"
            ".//*[contains(@class,'job-title') or contains(@class,'position-title') "
            "or contains(@class,'role-title') or contains(@class,'jobs-title') "
            "or contains(@class,'title')]"
        )

        for node in title_nodes[:10]:
            classes = (node.get("class") or "").strip()
            if _V70_LOCATION_CLASS_HINT.search(classes):
                continue
            raw = _text(node)
            if not raw:
                continue
            raw = re.sub(r"\s+\bnew\b\s*$", "", raw, flags=re.IGNORECASE).strip()
            title = self._normalize_title(raw)
            if not title:
                continue
            if len(title) > 140:
                continue
            if _V70_NON_JOB_HEADING.match(title):
                continue
            return title

        # Fallback: pick the first meaningful text fragment from the anchor.
        for piece in a_el.itertext():
            raw_piece = " ".join((piece or "").split())
            if not raw_piece:
                continue
            if raw_piece.lower() in {"new", "apply", "apply now"}:
                continue
            title = self._normalize_title(raw_piece)
            if not title:
                continue
            if len(title) > 90:
                continue
            if _V70_NON_JOB_HEADING.match(title):
                continue
            return title

        return None

    def _pagination_urls_v67(self, html: str, page_url: str) -> list[str]:
        root = _parse_html(html)
        if root is None:
            return []

        page_nav_links = root.xpath(
            "//nav[contains(translate(@aria-label,'PAGINATION','pagination'),'pagination') "
            "or contains(@class,'pagination') or contains(@class,'pager') "
            "or contains(@class,'nav-links')]//a[@href]"
            "|//div[contains(@class,'pagination') or contains(@class,'pager') "
            "or contains(@class,'nav-links')]//a[@href]"
            "|//div[@id='show_more_button']//a[@href]"
            "|//a[contains(@href,'show_more') and @href]"
            "|//a[@rel='next' and @href]"
        )

        candidates: list[str] = []
        page_host = (urlparse(page_url).netloc or "").lower()

        for a_el in page_nav_links:
            href = (a_el.get("href") or "").strip()
            if not href:
                continue
            full_url = urljoin(page_url, href)
            parsed = urlparse(full_url)
            if (parsed.netloc or "").lower() != page_host:
                continue
            if full_url.rstrip("/") == page_url.rstrip("/"):
                continue

            lower_url = full_url.lower()
            if not _CARD_PAGINATION_HINT.search(full_url) and "show_more" not in lower_url:
                continue
            candidates.append(full_url)

        deduped: list[str] = []
        seen: set[str] = set()
        for url in candidates:
            if url in seen:
                continue
            seen.add(url)
            deduped.append(url)
            if len(deduped) >= 3:
                break
        return deduped

    def _should_enrich_fast_path_v70(self, jobs: list[dict]) -> bool:
        if len(jobs) < 2:
            return False

        methods = {str(j.get("extraction_method") or "") for j in jobs}
        if not any(
            any(method.startswith(prefix) for prefix in _V70_EARLY_PATH_METHOD_PREFIXES)
            for method in methods
        ):
            return False

        missing_desc = sum(1 for j in jobs if not j.get("description"))
        if missing_desc < max(1, int(len(jobs) * 0.5)):
            return False

        detailish = sum(1 for j in jobs if self._is_job_like_url(str(j.get("source_url") or "")))
        return detailish >= max(1, int(len(jobs) * 0.5))

    def _has_strong_card_detail_url_v70(self, source_url: str, page_url: str) -> bool:
        lower = (source_url or "").lower()
        if _V70_DETAIL_PATH_HINT.search(lower):
            return True

        # Same-host detail URL with query-id signals can be high confidence even
        # when paths are non-standard.
        parsed = urlparse(source_url or "")
        page_host = (urlparse(page_url or "").netloc or "").lower()
        if page_host and (parsed.netloc or "").lower() != page_host:
            return False
        return bool(re.search(r"[?&](?:id|job|position|vacancy)=\w+", lower))
