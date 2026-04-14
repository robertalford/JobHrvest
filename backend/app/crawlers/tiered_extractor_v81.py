"""
Tiered Extraction Engine v8.1 — structured-row recovery for multilingual boards.

Strategy:
1. Recover table listings where role title and detail link live in different cells.
2. Add dedicated Gupy row extraction to avoid losing multilingual titles to generic
   jobset scoring gates.
3. Add dedicated JobMonster (`noo_job`) row extraction so archive pages prefer
   real vacancy rows over menu/service links.
"""

from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urlparse

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v74 import _V73_NAV_TITLE, _V73_NON_JOB_HEADING
from app.crawlers.tiered_extractor_v80 import (
    _V80_CAREER_PATH_HINT,
    _V80_LANGUAGE_ONLY,
    _V80_SHORT_ACRONYM_TITLE,
    _V80_TABLE_ACTION_HINT,
    _V80_TABLE_NON_LOCATION,
    _V80_TABLE_ROLE_HINT,
    TieredExtractorV80,
)

_V81_NUMERIC_DETAIL_PATH = re.compile(
    r"/(?:career|careers|jobs?|vacanc(?:y|ies)|opening(?:s)?)/detail/\d{1,9}(?:/|$)",
    re.IGNORECASE,
)
_V81_GENERIC_NON_ROLE_TITLE = re.compile(r"^(?:job\s+details?|job\s+checks?)$", re.IGNORECASE)
_V81_JOB_CHECK_URL = re.compile(r"/job[-_]?checks?/?$", re.IGNORECASE)

_V81_GUPY_HOST = re.compile(r"(?:^|\.)gupy\.io$", re.IGNORECASE)
_V81_GUPY_MARKER = re.compile(r"job-list__listitem|jobBoardSource=gupy_public_page", re.IGNORECASE)
_V81_GUPY_DETAIL_PATH = re.compile(r"/jobs/\d{4,}(?:/|$)", re.IGNORECASE)
_V81_EMPLOYMENT_HINT = re.compile(
    r"\b(?:efetivo|tempor[aá]rio|est[aá]gio|pj|clt|internship|contract|full[\s-]?time|part[\s-]?time)\b",
    re.IGNORECASE,
)

_V81_JOBMONSTER_MARKER = re.compile(r"\bnoo_job\b|jobmonster", re.IGNORECASE)
_V81_JOBMONSTER_DETAIL_PATH = re.compile(r"/jobs/[^/?#]{3,}(?:/|$)", re.IGNORECASE)


class TieredExtractorV81(TieredExtractorV80):
    """v8.1 extractor: structured row parsers for table, Gupy, and JobMonster."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        table_jobs = self._extract_query_table_jobs_v80(working_html, page_url)
        if self._passes_structured_row_jobset_v81(table_jobs, page_url):
            return self._finalize_structured_jobs_v81(table_jobs, working_html, page_url)

        gupy_jobs = self._extract_gupy_jobs_v81(working_html, page_url)
        if self._passes_structured_row_jobset_v81(gupy_jobs, page_url):
            return self._finalize_structured_jobs_v81(gupy_jobs, working_html, page_url)

        jobmonster_jobs = self._extract_jobmonster_jobs_v81(working_html, page_url)
        if self._passes_structured_row_jobset_v81(jobmonster_jobs, page_url):
            return self._finalize_structured_jobs_v81(jobmonster_jobs, working_html, page_url)

        return await super().extract(career_page, company, html)

    def _finalize_structured_jobs_v81(self, jobs: list[dict], html: str, page_url: str) -> list[dict]:
        finalized = self._postprocess_jobs_v73(jobs, html, page_url)
        return self._clean_jobs_v73(finalized)[:MAX_JOBS_PER_PAGE]

    def _is_valid_title_v60(self, title: str) -> bool:
        if not super()._is_valid_title_v60(title):
            return False
        return not _V81_GENERIC_NON_ROLE_TITLE.match((title or "").strip())

    def _is_non_job_url(self, src: str) -> bool:
        if super()._is_non_job_url(src):
            return True
        path = (urlparse(src or "").path or "").lower()
        return bool(_V81_JOB_CHECK_URL.search(path))

    def _has_strong_card_detail_url_v73(self, source_url: str, page_url: str) -> bool:
        if super()._has_strong_card_detail_url_v73(source_url, page_url):
            return True
        path = (urlparse(source_url or "").path or "").lower()
        return bool(_V81_NUMERIC_DETAIL_PATH.search(path))

    def _passes_structured_row_jobset_v81(self, jobs: list[dict], page_url: str) -> bool:
        if len(jobs) < 3:
            return False

        strong_urls = 0
        valid_titles: list[str] = []
        for job in jobs:
            title = self._normalize_title(str(job.get("title") or ""))
            source_url = str(job.get("source_url") or "").split("#", 1)[0]
            if not title or not source_url:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue
            if _V73_NAV_TITLE.match(title) or _V73_NON_JOB_HEADING.match(title):
                continue
            if self._is_non_job_url(source_url):
                continue

            has_strong = self._has_strong_card_detail_url_v73(source_url, page_url) or self._is_job_like_url(source_url)
            if not has_strong:
                continue

            valid_titles.append(title)
            strong_urls += 1

        if len(valid_titles) < 3:
            return False

        unique_ratio = len({t.lower() for t in valid_titles}) / max(1, len(valid_titles))
        if unique_ratio < 0.6:
            return False

        return strong_urls >= max(3, int(len(valid_titles) * 0.7))

    def _is_reasonable_structured_title_v81(self, title: str) -> bool:
        t = (title or "").strip()
        if not t:
            return False
        if _V73_NAV_TITLE.match(t) or _V73_NON_JOB_HEADING.match(t):
            return False
        if _V81_GENERIC_NON_ROLE_TITLE.match(t):
            return False
        if _V80_SHORT_ACRONYM_TITLE.match(t):
            return True
        if re.search(r"\b(?:view|apply|details?|read\s+more|click\s+here)\b", t, re.IGNORECASE):
            return False
        words = t.split()
        if len(words) < 2 or len(words) > 12:
            return False
        return bool(re.search(r"[A-Za-z]", t))

    def _extract_query_table_jobs_v80(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        table_nodes = root.xpath("//table[.//tr[.//td]]")
        if not table_nodes:
            return []

        best: list[dict] = []
        for table in table_nodes[:20]:
            rows = table.xpath(".//tr[.//td]")
            if len(rows) < 3:
                continue

            jobs: list[dict] = []
            seen_urls: set[str] = set()

            for row in rows[:700]:
                cells = row.xpath("./td")
                if len(cells) < 2:
                    continue

                source_url = self._pick_table_detail_url_v81(row, page_url, seen_urls)
                if not source_url:
                    continue

                title = self._extract_table_title_v81(row, cells)
                if not title:
                    continue
                if not self._is_valid_title_v60(title):
                    if not (
                        self._is_valid_table_row_title_v80(title, row, source_url, page_url)
                        or self._is_reasonable_structured_title_v81(title)
                    ):
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
                        "extraction_method": "tier2_query_table_rows_v81",
                        "extraction_confidence": 0.9,
                    }
                )

            if len(jobs) > len(best):
                best = jobs

        return self._dedupe_basic_v66(best)

    def _pick_table_detail_url_v81(self, row, page_url: str, seen_urls: set[str]) -> Optional[str]:
        anchors = row.xpath(".//a[@href]")
        if not anchors:
            return None

        def score(a_el) -> int:
            href = (a_el.get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url or source_url in seen_urls or self._is_non_job_url(source_url):
                return -1
            if source_url.rstrip("/") == page_url.rstrip("/"):
                return -1

            text = " ".join((_text(a_el) or "").split()).strip()
            score_val = 0
            if self._has_strong_card_detail_url_v73(source_url, page_url):
                score_val += 4
            elif self._is_job_like_url(source_url):
                score_val += 2

            if _V80_TABLE_ACTION_HINT.search(text):
                score_val += 2
            if len(text) <= 2:
                score_val -= 1
            return score_val

        best_anchor = max(anchors, key=score)
        if score(best_anchor) < 2:
            return None
        href = (best_anchor.get("href") or "").strip()
        return (_resolve_url(href, page_url) or "").split("#", 1)[0]

    def _extract_table_title_v81(self, row, cells) -> str:
        title = self._normalize_title(_text(cells[0]) or "")
        if title and not _V80_TABLE_NON_LOCATION.match(title):
            return title

        for a_el in row.xpath(".//a[@href]"):
            raw = self._normalize_title(_text(a_el) or (a_el.get("title") or ""))
            if not raw or _V80_TABLE_NON_LOCATION.match(raw):
                continue
            return raw
        return ""

    def _extract_gupy_jobs_v81(self, html: str, page_url: str) -> list[dict]:
        host = (urlparse(page_url).netloc or "").lower()
        if not _V81_GUPY_HOST.search(host) and not _V81_GUPY_MARKER.search(html or ""):
            return []

        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath("//ul[@data-testid='job-list__list']/li[.//a[@href]]|//li[@data-testid='job-list__listitem']")
        if len(rows) < 3:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()
        for row in rows[:700]:
            links = row.xpath(".//a[@href]")
            if not links:
                continue
            href = (links[0].get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url or source_url in seen_urls or self._is_non_job_url(source_url):
                continue
            if not _V81_GUPY_DETAIL_PATH.search(urlparse(source_url).path or ""):
                continue

            leaf_nodes = links[0].xpath(".//div[not(*) and normalize-space()]|.//span[not(*) and normalize-space()]")
            leaf_texts = [" ".join((_text(node) or "").split()) for node in leaf_nodes if " ".join((_text(node) or "").split())]
            raw_title = leaf_texts[0] if leaf_texts else (_text(links[0]) or "")
            title = self._normalize_title(raw_title)
            if not title:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue

            location = None
            if len(leaf_texts) >= 2:
                loc = leaf_texts[1].strip(" ,|-")
                if 2 <= len(loc) <= 120 and not _V80_LANGUAGE_ONLY.match(loc):
                    location = loc

            employment_type = None
            if len(leaf_texts) >= 3 and _V81_EMPLOYMENT_HINT.search(leaf_texts[2]):
                employment_type = leaf_texts[2][:80]

            seen_urls.add(source_url)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": None,
                    "salary_raw": None,
                    "employment_type": employment_type,
                    "extraction_method": "ats_gupy_rows_v81",
                    "extraction_confidence": 0.93,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _extract_jobmonster_jobs_v81(self, html: str, page_url: str) -> list[dict]:
        if not _V81_JOBMONSTER_MARKER.search(html or ""):
            return []

        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath("//article[contains(@class,'noo_job') and (.//a[@href] or @data-url)]")
        if len(rows) < 3:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()
        for row in rows[:MAX_JOBS_PER_PAGE * 3]:
            data_url = (row.get("data-url") or "").strip()
            source_url = (_resolve_url(data_url, page_url) or "").split("#", 1)[0] if data_url else ""

            title_nodes = row.xpath(".//h1//a[@href]|.//h2//a[@href]|.//h3//a[@href]|.//h4//a[@href]")
            title_node = title_nodes[0] if title_nodes else None
            if title_node is None:
                continue

            if not source_url:
                href = (title_node.get("href") or "").strip()
                source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url or source_url in seen_urls:
                continue
            if not _V81_JOBMONSTER_DETAIL_PATH.search(urlparse(source_url).path or ""):
                continue
            if self._is_non_job_url(source_url):
                continue

            title = self._normalize_title(_text(title_node) or "")
            if not title:
                continue
            if not self._is_valid_title_v60(title):
                if not (
                    self._is_reasonable_structured_title_v81(title)
                    or self._is_reasonable_compact_jobmonster_title_v81(title)
                ):
                    continue

            if _V73_NAV_TITLE.match(title) or _V73_NON_JOB_HEADING.match(title):
                continue

            location = None
            loc_nodes = row.xpath(".//*[contains(@class,'job-location')]//em|.//*[contains(@class,'job-location')]//a")
            for node in loc_nodes[:4]:
                loc = " ".join((_text(node) or "").split()).strip(" ,|-")
                if 2 <= len(loc) <= 120 and loc.lower() != title.lower():
                    location = loc
                    break

            description = self._extract_row_description_v73(row, title)
            seen_urls.add(source_url)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": description,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "ats_jobmonster_rows_v81",
                    "extraction_confidence": 0.91,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _is_reasonable_compact_jobmonster_title_v81(self, title: str) -> bool:
        token = (title or "").strip()
        if not token:
            return False
        if " " in token:
            return False
        if len(token) < 4 or len(token) > 28:
            return False
        if not re.match(r"^[A-Za-z][A-Za-z+&./-]*$", token):
            return False
        low = token.lower()
        return low not in {"jobs", "careers", "career", "apply", "details", "search"}
