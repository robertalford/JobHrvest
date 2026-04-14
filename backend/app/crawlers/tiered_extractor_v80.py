"""
Tiered Extraction Engine v8.0 — strict query-table recovery + text hygiene.

Strategy:
1. Recover static career tables that use query-detail links (`?slug=...`) and are
   missed by generic linked-card path hints.
2. Keep specialized hyphenated role suffixes when base normalization trims them.
3. Suppress metadata/CSS-heavy description noise and repair common mojibake text.
"""

from __future__ import annotations

import html as html_mod
import re
from typing import Any
from urllib.parse import parse_qs, urlparse

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v74 import _V73_NAV_TITLE, _V73_NON_JOB_HEADING
from app.crawlers.tiered_extractor_v79 import TieredExtractorV79

_V80_CAREER_PATH_HINT = re.compile(r"/(?:career|careers|jobs?|vacanc|openings?|opportunit)", re.IGNORECASE)

_V80_DETAIL_QUERY_KEYS = {
    "slug",
    "job",
    "jobid",
    "job_id",
    "position",
    "positionid",
    "vacancy",
    "vacancyid",
    "posting",
    "id",
    "jid",
}
_V80_LISTING_QUERY_KEYS = {"q", "search", "keyword", "category", "department", "sort", "page"}

_V80_SHORT_ACRONYM_TITLE = re.compile(r"^[A-Z]{2,5}$")
_V80_TABLE_ACTION_HINT = re.compile(r"\b(?:view|apply|details?|read\s+more)\b", re.IGNORECASE)
_V80_TABLE_NON_LOCATION = re.compile(r"^(?:view|apply|details?|read\s+more)$", re.IGNORECASE)
_V80_LANGUAGE_ONLY = re.compile(r"^(?:english|indonesian|bahasa|thai|vietnamese|mandarin)$", re.IGNORECASE)
_V80_TABLE_ROLE_HINT = re.compile(
    r"\b(?:manager|engineer|assistant|chef|cook|attendant|security|coordinator|waiter|"
    r"supervisor|officer|analyst|technician|executive)\b",
    re.IGNORECASE,
)

_V80_HYPHEN_META_SUFFIX = re.compile(
    r"(?:"
    r"\b(?:remote|hybrid|on[\s-]?site|full[\s-]?time|part[\s-]?time|contract|casual|temporary|"
    r"permanent|internship|posted|closing|deadline|job\s+ref|salary|location)\b|"
    r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\b|"
    r"\b(?:nsw|vic|qld|wa|sa|tas|nt|act|australia|new\s+zealand|singapore|malaysia)\b"
    r")",
    re.IGNORECASE,
)
_V80_ROLE_SUFFIX_HINT = re.compile(
    r"\b(?:service|consulting|operations?|technology|engineering|finance|accounting|"
    r"security|compliance|digital|data|platform|oracle|infrastructure)\b",
    re.IGNORECASE,
)

_V80_STYLE_BLOCK = re.compile(r"(?is)\.[\w-]+\s*\{[^{}]{20,2000}\}")
_V80_CSS_VAR = re.compile(r"(?i)--[a-z0-9-]+\s*:\s*[^;]{1,120};")
_V80_DATE_TOKEN = re.compile(
    r"(?:"
    r"\b\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\b|"
    r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2}(?:,\s*\d{4})?\b|"
    r"\b\d{1,2}/\d{1,2}/\d{2,4}\b"
    r")",
    re.IGNORECASE,
)
_V80_META_ONLY_HINT = re.compile(
    r"\b(?:salary|workspace|job\s+type|posted|closing|location|icon|acst|aest|gmt)\b",
    re.IGNORECASE,
)
_V80_DESC_SENTENCE_HINT = re.compile(
    r"\b(?:responsib|requirement|experience|candidate|about\s+the\s+role|you\s+will|"
    r"we\s+are|key\s+duties|qualifications?)\b",
    re.IGNORECASE,
)

_V80_MOJIBAKE_MAP = {
    "â\x80\x99": "'",
    "â\x80\x98": "'",
    "â\x80\x93": "-",
    "â\x80\x94": "-",
    "â\x80\x9c": '"',
    "â\x80\x9d": '"',
    "â\x80¦": "...",
    "â\x80¢": "-",
}


class TieredExtractorV80(TieredExtractorV79):
    """v8.0 extractor: query-table recovery with strict quality gates."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        table_jobs = self._extract_query_table_jobs_v80(working_html, page_url)
        if len(table_jobs) >= 3 and self._passes_jobset_validation(table_jobs, page_url):
            finalized = self._postprocess_jobs_v73(table_jobs, working_html, page_url)
            return self._clean_jobs_v73(finalized)[:MAX_JOBS_PER_PAGE]

        jobs = await super().extract(career_page, company, html)
        if jobs:
            return jobs

        # Timeout-safe static recovery for pages where parent orchestration times out.
        recovered = self._extract_timeout_recovery_v80(working_html, page_url)
        if len(recovered) >= 3 and self._passes_jobset_validation(recovered, page_url):
            finalized = self._postprocess_jobs_v73(recovered, working_html, page_url)
            return self._clean_jobs_v73(finalized)[:MAX_JOBS_PER_PAGE]

        return jobs

    def _has_strong_card_detail_url_v73(self, source_url: str, page_url: str) -> bool:
        if super()._has_strong_card_detail_url_v73(source_url, page_url):
            return True

        parsed = urlparse(source_url or "")
        page_path = (urlparse(page_url or "").path or "/").lower()
        if not _V80_CAREER_PATH_HINT.search(page_path):
            return False

        query = parse_qs(parsed.query or "", keep_blank_values=True)
        if not query:
            return False
        keys = {k.lower() for k in query if k}
        if not (keys & _V80_DETAIL_QUERY_KEYS):
            return False
        if (keys & _V80_LISTING_QUERY_KEYS) and not (keys & _V80_DETAIL_QUERY_KEYS):
            return False

        for key in keys & _V80_DETAIL_QUERY_KEYS:
            values = query.get(key, [])
            for value in values:
                if self._is_strong_query_detail_value_v80(key, value):
                    return True
        return False

    @staticmethod
    def _is_strong_query_detail_value_v80(key: str, value: str) -> bool:
        token = (value or "").strip().lower()
        if not token:
            return False
        if key in {"id", "jobid", "job_id", "positionid", "vacancyid", "jid"}:
            return bool(re.match(r"^[a-z0-9-]{2,32}$", token))
        if key in {"slug", "job", "position", "vacancy", "posting"}:
            if re.search(r"[a-z]{3,}-\d{1,6}$", token):
                return True
            if re.search(r"(?:manager|engineer|officer|assistant|chef|analyst|coordinator)", token):
                return True
        return False

    def _extract_query_table_jobs_v80(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        table_nodes = root.xpath("//table[.//tr[.//td//a[@href]]]")
        if not table_nodes:
            return []

        best: list[dict] = []
        for table in table_nodes[:20]:
            rows = table.xpath(".//tr[.//td//a[@href]]")
            if len(rows) < 3:
                continue

            jobs: list[dict] = []
            seen_urls: set[str] = set()
            for row in rows[:600]:
                cells = row.xpath("./td")
                if len(cells) < 2:
                    continue

                anchors = cells[0].xpath(".//a[@href]")
                if not anchors:
                    continue

                href = (anchors[0].get("href") or "").strip()
                source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
                if not source_url or source_url in seen_urls:
                    continue
                if self._is_non_job_url(source_url):
                    continue
                if not self._has_strong_card_detail_url_v73(source_url, page_url):
                    continue

                title = self._normalize_title(_text(anchors[0]) or (anchors[0].get("title") or ""))
                if not title:
                    continue
                if not self._is_valid_title_v60(title):
                    if not self._is_valid_table_row_title_v80(title, row, source_url, page_url):
                        continue
                if self._is_obvious_non_job_card_v73(title, source_url, page_url, True):
                    continue

                row_text = " ".join((_text(row) or "").split())
                description = row_text[:5000] if len(row_text) >= 90 and row_text.lower() != title.lower() else None

                seen_urls.add(source_url)
                jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": self._extract_table_location_v80(cells, title),
                        "description": description,
                        "salary_raw": None,
                        "employment_type": None,
                        "extraction_method": "tier2_query_table_rows_v80",
                        "extraction_confidence": 0.88,
                    }
                )

            if len(jobs) >= 3:
                strong = sum(1 for j in jobs if self._has_strong_card_detail_url_v73(str(j.get("source_url") or ""), page_url))
                if strong >= max(3, int(len(jobs) * 0.7)) and len(jobs) > len(best):
                    best = jobs

        return self._dedupe_basic_v66(best)

    def _extract_timeout_recovery_v80(self, html: str, page_url: str) -> list[dict]:
        if not html or len(html) < 1500:
            return []
        path = (urlparse(page_url).path or "/").lower()
        if not _V80_CAREER_PATH_HINT.search(path) and "jobs-list-table" not in html.lower():
            return []

        raw_jobs = self._extract_tier2_v16(page_url, html) or []
        if len(raw_jobs) < 3:
            return []

        filtered: list[dict] = []
        for job in raw_jobs:
            title = self._normalize_title(str(job.get("title") or ""))
            source_url = str(job.get("source_url") or "").split("#", 1)[0]
            if not title or not source_url:
                continue
            has_strong = self._has_strong_card_detail_url_v73(source_url, page_url) or self._is_job_like_url(source_url)
            if not has_strong:
                continue
            if not self._is_valid_title_v60(title):
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, has_strong):
                continue
            updated = dict(job)
            updated["title"] = title
            updated["source_url"] = source_url
            filtered.append(updated)
        return self._dedupe_basic_v66(filtered)

    def _is_valid_table_row_title_v80(self, title: str, row, source_url: str, page_url: str) -> bool:
        t = (title or "").strip()
        if _V73_NAV_TITLE.match(t) or _V73_NON_JOB_HEADING.match(t):
            return False
        if not self._has_strong_card_detail_url_v73(source_url, page_url):
            return False
        row_text = " ".join((_text(row) or "").split())
        if not _V80_TABLE_ACTION_HINT.search(row_text):
            return False

        if _V80_SHORT_ACRONYM_TITLE.match(t):
            return True

        words = t.split()
        if len(words) < 2 or len(words) > 12:
            return False
        return bool(_V80_TABLE_ROLE_HINT.search(t))

    @staticmethod
    def _extract_table_location_v80(cells, title: str) -> str | None:
        for cell in cells[1:4]:
            text = " ".join((_text(cell) or "").split()).strip(" ,|-")
            if not text:
                continue
            if text.lower() == (title or "").lower():
                continue
            if len(text) > 120:
                continue
            if _V80_TABLE_NON_LOCATION.match(text):
                continue
            if _V80_LANGUAGE_ONLY.match(text):
                continue
            return text
        return None

    def _normalize_title(self, title: str) -> str:
        normalized = super()._normalize_title(title)
        return self._restore_hyphen_specialization_v80(title, normalized)

    def _restore_hyphen_specialization_v80(self, raw_title: str, normalized_title: str) -> str:
        raw = html_mod.unescape(" ".join(str(raw_title or "").replace("\xa0", " ").split())).strip()
        current = (normalized_title or "").strip()
        if not raw or not current or " - " not in raw:
            return current

        parts = [p.strip(" |:-") for p in raw.split(" - ") if p.strip(" |:-")]
        if len(parts) < 2:
            return current
        prefix = parts[0]
        suffix = " - ".join(parts[1:]).strip()
        if not suffix:
            return current

        if current.lower() != prefix.lower():
            return current
        if _V80_HYPHEN_META_SUFFIX.search(suffix):
            return current
        if len(suffix.split()) > 7:
            return current
        if not re.search(r"[A-Za-z]", suffix):
            return current
        if not ("&" in suffix or "/" in suffix or _V80_ROLE_SUFFIX_HINT.search(suffix)):
            return current

        candidate = f"{prefix} - {suffix}".strip()
        if len(candidate.split()) > 14:
            return current
        if _V73_NON_JOB_HEADING.match(candidate) or _V73_NAV_TITLE.match(candidate):
            return current
        return candidate

    def _clean_description_v73(self, value: Any):
        cleaned = super()._clean_description_v73(value)
        if not cleaned:
            return cleaned

        text = cleaned
        for bad, good in _V80_MOJIBAKE_MAP.items():
            text = text.replace(bad, good)

        text = _V80_STYLE_BLOCK.sub(" ", text)
        text = _V80_CSS_VAR.sub(" ", text)
        text = re.sub(r"\s{2,}", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()

        if self._description_is_noise_v75(text):
            return None
        return text[:5000] if text else None

    def _description_is_noise_v75(self, value: Any) -> bool:
        if super()._description_is_noise_v75(value):
            return True
        if not isinstance(value, str):
            return False

        text = html_mod.unescape(" ".join(value.replace("\xa0", " ").split()))
        if not text:
            return False
        low = text.lower()

        if "--wix-" in low or ".comp-" in low:
            return True
        if text.count("{") >= 2 and text.count(";") >= 6:
            return True

        date_hits = len(_V80_DATE_TOKEN.findall(low))
        if len(text) <= 140 and date_hits >= 2 and not re.search(r"[.!?]", text):
            return True
        if len(text.split()) <= 22 and _V80_META_ONLY_HINT.search(low) and not _V80_DESC_SENTENCE_HINT.search(low):
            return True
        return False
