"""
Tiered Extraction Engine v7.9 — linked-card precision + numeric-detail recovery.

Strategy:
1. Reject date-like/listing-filter labels as job titles to reduce Type-1 noise.
2. Reject query-driven listing/filter URLs (for example `/jobs?jobtype=...`) as
   non-detail URLs unless explicit detail identifiers are present.
3. Add a focused numeric-detail fallback for legacy `/jobs/<id>/...` tables where
   strict title-noun validation can under-capture real roles.
"""

from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

from app.crawlers.tiered_extractor import _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v74 import _V73_NAV_TITLE, _V73_NON_JOB_HEADING
from app.crawlers.tiered_extractor_v78 import TieredExtractorV78

_V79_DATE_ONLY_TITLE = re.compile(
    r"^(?:"
    r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|"
    r"\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{2,4}|"
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2}(?:,\s*\d{2,4})?"
    r")$",
    re.IGNORECASE,
)
_V79_FILTER_TITLE = re.compile(
    r"^(?:job\s+index|jobs?\s+near(?:\s+\w+){0,4}|search\s+jobs?)$",
    re.IGNORECASE,
)
_V79_NUMERIC_JOB_DETAIL_PATH = re.compile(r"/jobs/\d{2,}(?:/|$)", re.IGNORECASE)
_V79_LISTING_QUERY_KEYS = {
    "jobtype",
    "district",
    "province",
    "region",
    "businesstype",
    "industrial",
    "disabledperson",
    "mst",
    "salary",
    "keyword",
    "q",
    "sort",
    "page",
    "event-section",
    "event-page",
}
_V79_DETAIL_QUERY_KEYS = {
    "jobid",
    "job_id",
    "requisitionid",
    "positionid",
    "vacancyid",
    "jobadid",
    "adid",
    "ajid",
    "id",
}


class TieredExtractorV79(TieredExtractorV78):
    """v7.9 extractor: linked-card precision tightening with legacy numeric fallback."""

    def _is_valid_title_v60(self, title: str) -> bool:
        if not super()._is_valid_title_v60(title):
            return False
        t = (title or "").strip()
        if _V79_DATE_ONLY_TITLE.match(t):
            return False
        if _V79_FILTER_TITLE.match(t):
            return False
        return True

    def _is_non_job_url(self, src: str) -> bool:
        if super()._is_non_job_url(src):
            return True

        parsed = urlparse(src or "")
        path = (parsed.path or "").lower()
        if "/jobindex" in path:
            return True

        query = parsed.query or ""
        if not query:
            return False

        keys = {k.lower() for k in parse_qs(query, keep_blank_values=True) if k}
        if not keys:
            return False
        if keys & _V79_DETAIL_QUERY_KEYS:
            return False

        if path.rstrip("/").endswith("/jobs") and keys & _V79_LISTING_QUERY_KEYS:
            return True
        return ("event-section" in keys) or ("event-page" in keys)

    def _extract_linked_job_cards_v67(self, html: str, page_url: str) -> list[dict]:
        base_jobs = super()._extract_linked_job_cards_v67(html, page_url)
        seen_urls = {
            str(job.get("source_url") or "").split("#", 1)[0]
            for job in base_jobs
            if str(job.get("source_url") or "").strip()
        }

        extras = self._extract_numeric_job_path_cards_v79(html, page_url, seen_urls)
        combined = base_jobs + extras if extras else base_jobs

        # On large legacy tables, non-numeric links are usually editorial/sidebar noise.
        numeric_count = 0
        for job in combined:
            src = str(job.get("source_url") or "")
            if _V79_NUMERIC_JOB_DETAIL_PATH.search(urlparse(src).path or ""):
                numeric_count += 1
        if numeric_count >= 20:
            combined = [
                job
                for job in combined
                if _V79_NUMERIC_JOB_DETAIL_PATH.search(urlparse(str(job.get("source_url") or "")).path or "")
            ]

        return self._dedupe_basic_v66(combined)

    def _extract_numeric_job_path_cards_v79(
        self,
        html: str,
        page_url: str,
        seen_urls: set[str],
    ) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        page_host = (urlparse(page_url).netloc or "").lower()
        jobs: list[dict] = []

        anchors = root.xpath("//a[@href and not(starts-with(@href,'#')) and not(starts-with(@href,'javascript:'))]")
        for a_el in anchors[:1600]:
            href = (a_el.get("href") or "").strip()
            if not href:
                continue

            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url or source_url in seen_urls:
                continue
            if self._is_non_job_url(source_url):
                continue

            parsed = urlparse(source_url)
            if (parsed.netloc or "").lower() != page_host:
                continue
            if not _V79_NUMERIC_JOB_DETAIL_PATH.search(parsed.path or ""):
                continue
            if source_url.rstrip("/") == page_url.rstrip("/"):
                continue

            raw_title = _text(a_el) or (a_el.get("title") or "")
            title = self._normalize_title(raw_title)
            if not title:
                continue
            if not self._is_valid_title_v60(title) and not self._is_reasonable_numeric_detail_title_v79(title):
                continue

            has_strong = self._has_strong_card_detail_url_v73(source_url, page_url)
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, has_strong):
                continue

            seen_urls.add(source_url)
            context_text = " ".join((_text(a_el) or "").split())
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_card_location_v67(a_el, title),
                    "description": context_text[:5000] if len(context_text) >= 120 else None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "tier2_numeric_job_paths_v79",
                    "extraction_confidence": 0.86,
                }
            )
        return jobs

    def _is_reasonable_numeric_detail_title_v79(self, title: str) -> bool:
        t = self._normalize_title(title)
        if not t:
            return False
        if _V79_DATE_ONLY_TITLE.match(t) or _V79_FILTER_TITLE.match(t):
            return False
        if _V73_NAV_TITLE.match(t) or _V73_NON_JOB_HEADING.match(t):
            return False

        words = t.split()
        if len(words) < 2 or len(words) > 12:
            return False
        if not re.search(r"[A-Za-z]", t):
            return False
        return not re.search(r"[{}<>]", t)
