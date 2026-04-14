"""
Tiered Extraction Engine v8.6 - precision recovery + shell/page2 coverage.

Strategy:
1. Preserve valid structured titles during final dedupe when URL evidence is strong.
2. Keep structured Homerun vacancies that look like real detail postings.
3. Expand Jobmonster (`noo_job`) listings across bounded `paged=` pagination.
4. Trigger JS rendering for module-root app shells that evade legacy SPA checks.
"""

from __future__ import annotations

import re
from urllib.parse import parse_qs, urljoin, urlparse

import httpx

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url
from app.crawlers.tiered_extractor_v85 import TieredExtractorV85, _V85_MULTI_OPENING_PATH

_V86_MODULE_ROOT = re.compile(r"id=[\"'](?:r-root|root|app)[\"']", re.IGNORECASE)
_V86_MODULE_SCRIPT = re.compile(
    r"<script[^>]+type=[\"']module[\"'][^>]+src=[\"'][^\"']*/assets/[^\"']+\.js",
    re.IGNORECASE,
)
_V86_CAREER_HINT = re.compile(r"(?:careers?|jobs?|vacanc(?:y|ies)|openings?)", re.IGNORECASE)
_V86_JOBMONSTER_PAGED = re.compile(r"(?:[?&]paged=(\d{1,3})|/page/(\d{1,3})(?:/|$))", re.IGNORECASE)
_V86_JOBMONSTER_DESC_TAIL = re.compile(
    r"\b(?:send\s+to\s+friend|save|share|view\s+more|quick\s+view)\b.*$",
    re.IGNORECASE,
)
_V86_JOBMONSTER_META_ONLY = re.compile(
    r"^(?:long|less|full|part|contract|temporary|casual)\b.*(?:\d{4}|"
    r"january|february|march|april|may|june|july|august|"
    r"september|october|november|december)\b",
    re.IGNORECASE,
)


class TieredExtractorV86(TieredExtractorV85):
    """v8.6 extractor: restore dropped structured rows and JS-shell coverage."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        role_jobs = self._extract_role_city_rows_v85(working_html, page_url)
        if self._passes_role_city_jobset_v85(role_jobs, page_url):
            return self._finalize_high_volume_jobs_v84(role_jobs, working_html, page_url)

        if self._is_module_shell_v86(working_html, page_url):
            rendered = await self._render_with_playwright_v13(page_url)
            if rendered and len(rendered) > len(working_html):
                working_html = rendered

        jobs = await super().extract(career_page, company, working_html)

        if self._is_jobmonster_result_v86(jobs):
            jobs = self._clean_jobmonster_descriptions_v86(jobs)
            seed_rows = self._extract_jobmonster_jobs_v86(working_html, page_url)
            expanded_rows = await self._expand_jobmonster_pages_v86(working_html, page_url, seed_rows)
            if len(expanded_rows) > len(jobs):
                jobs = self._merge_jobmonster_superset_v86(jobs, expanded_rows)

        if len(jobs) == 1 and self._is_generic_listing_single_title_v85(str(jobs[0].get("title") or "")):
            jobs = []

        if jobs:
            return jobs

        open_state_jobs = self._extract_open_state_rows_v85(working_html, page_url)
        if self._passes_open_state_jobset_v85(open_state_jobs, page_url):
            return self._finalize_high_volume_jobs_v84(open_state_jobs, working_html, page_url)

        return jobs

    def _is_module_shell_v86(self, html: str, page_url: str) -> bool:
        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return False

        lower = (html or "").lower()
        if not _V86_MODULE_ROOT.search(lower):
            return False
        if not _V86_MODULE_SCRIPT.search(lower):
            return False

        if not (_V86_CAREER_HINT.search(page_url or "") or _V86_CAREER_HINT.search(lower[:200000])):
            return False

        root = _parse_html(html)
        if root is None:
            return False

        detailish = 0
        for node in root.xpath("//a[@href]")[:450]:
            href = (node.get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url:
                continue
            if self._is_non_job_url(source_url):
                continue
            if self._has_strong_card_detail_url_v73(source_url, page_url) or self._is_job_like_url(source_url):
                detailish += 1
                if detailish >= 2:
                    return False

        return True

    def _drop_obvious_non_jobs_v73(self, jobs: list[dict], page_url: str = "") -> list[dict]:
        preserved: list[dict] = []
        remainder: list[dict] = []

        for job in jobs:
            method = str(job.get("extraction_method") or "")
            title = self._normalize_title(str(job.get("title") or ""))
            source_url = str(job.get("source_url") or "").split("#", 1)[0]

            if method.startswith("ats_homerun_state_") and title and source_url:
                if self._is_valid_title_v60(title) and not self._is_non_job_url(source_url):
                    path_parts = [p for p in (urlparse(source_url).path or "").split("/") if p]
                    if len(path_parts) >= 2:
                        preserved.append(job)
                        continue

            remainder.append(job)

        filtered = super()._drop_obvious_non_jobs_v73(remainder, page_url)
        return self._dedupe_title_url_location_v84(preserved + filtered, limit=MAX_JOBS_PER_PAGE)

    def _is_non_job_url(self, src: str) -> bool:
        if not super()._is_non_job_url(src):
            return False

        path = (urlparse(src or "").path or "").lower()
        # Keep account-function role URLs (for example, "accounts-payable-specialist")
        # while still blocking generic account/profile portals.
        if "/jobdetail/" in path and re.search(r"/jobdetail/[^/]*account[^/]*", path):
            return False
        if re.search(r"/jobs?/(?:[^/]+/)?[^/]*account[^/]*/?$", path):
            return False
        return True

    def _dedupe(self, jobs: list[dict], page_url: str) -> list[dict]:
        deduped: list[dict] = []
        seen: set[tuple[str, str, str]] = set()

        for job in jobs:
            title = self._normalize_title(job.get("title", ""))
            if not title:
                continue

            source_url = str(job.get("source_url") or "").strip()
            if not source_url:
                source_url = page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]
            if self._is_non_job_url(source_url):
                continue

            valid_title = self._is_valid_title_v60(title)
            if not valid_title:
                if not self._is_reasonable_structured_title_v81(title):
                    continue
                has_strong = self._has_strong_card_detail_url_v73(source_url, page_url) or self._is_job_like_url(source_url)
                if not has_strong:
                    continue

            location = str(job.get("location_raw") or "").strip().lower()
            source_path = (urlparse(source_url).path or "").lower()
            keep_location_variant = bool(location) and bool(_V85_MULTI_OPENING_PATH.search(source_path))
            key = (title.lower(), source_url.lower(), location if keep_location_variant else "")
            if key in seen:
                continue
            seen.add(key)

            cloned = dict(job)
            cloned["title"] = title
            cloned["source_url"] = source_url
            deduped.append(cloned)

            if len(deduped) >= MAX_JOBS_PER_PAGE:
                break

        return deduped

    def _should_enrich_fast_path_v73(self, jobs: list[dict], page_url: str) -> bool:
        if super()._should_enrich_fast_path_v73(jobs, page_url):
            return True
        if len(jobs) < 2 or len(jobs) > 25:
            return False

        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return False

        methods = {str(j.get("extraction_method") or "") for j in jobs}
        return any(m.startswith("ats_jobmonster_rows_v81") for m in methods)

    @staticmethod
    def _is_jobmonster_result_v86(jobs: list[dict]) -> bool:
        if not jobs:
            return False
        methods = {str(j.get("extraction_method") or "") for j in jobs}
        return bool(methods) and all(m.startswith("ats_jobmonster_rows_v81") for m in methods)

    def _extract_jobmonster_jobs_v86(self, html: str, page_url: str) -> list[dict]:
        jobs = list(super()._extract_jobmonster_jobs_v81(html, page_url))
        return self._clean_jobmonster_descriptions_v86(jobs)

    def _clean_jobmonster_descriptions_v86(self, jobs: list[dict]) -> list[dict]:
        cleaned: list[dict] = []
        for job in jobs:
            updated = dict(job)
            updated["description"] = self._clean_jobmonster_row_description_v86(
                updated.get("description"),
                str(updated.get("title") or ""),
            )
            cleaned.append(updated)
        return cleaned

    def _clean_jobmonster_row_description_v86(self, value, title: str):
        if not isinstance(value, str):
            return None

        text = " ".join(value.split()).strip()
        if not text:
            return None

        text = _V86_JOBMONSTER_DESC_TAIL.sub("", text).strip(" ,|-")
        if not text:
            return None
        if text.lower() == (title or "").strip().lower():
            return None
        if _V86_JOBMONSTER_META_ONLY.match(text):
            return None
        return text[:5000]

    def _collect_jobmonster_pagination_urls_v86(self, html: str, page_url: str) -> list[str]:
        root = _parse_html(html)
        if root is None:
            return []

        page_host = (urlparse(page_url).netloc or "").lower()
        by_page: dict[int, str] = {}

        nodes = root.xpath(
            "//a[contains(@class,'page-numbers') and @href]"
            "|//a[contains(@class,'next') and contains(@class,'page-numbers') and @href]"
        )

        for node in nodes[:120]:
            href = (node.get("href") or "").strip()
            if not href:
                continue
            full = urljoin(page_url, href).split("#", 1)[0]
            parsed = urlparse(full)
            if (parsed.netloc or "").lower() != page_host:
                continue

            page_num = 0
            query = parse_qs(parsed.query or "", keep_blank_values=True)
            try:
                page_num = int((query.get("paged") or ["0"])[0])
            except Exception:
                page_num = 0
            if page_num <= 1:
                match = _V86_JOBMONSTER_PAGED.search(full)
                if not match:
                    continue
                try:
                    page_num = int((match.group(1) or match.group(2) or "0"))
                except Exception:
                    page_num = 0
            if page_num <= 1:
                continue

            by_page[page_num] = full

        return [by_page[k] for k in sorted(by_page)[:4]]

    async def _expand_jobmonster_pages_v86(self, html: str, page_url: str, seed_jobs: list[dict]) -> list[dict]:
        if len(seed_jobs) < 3:
            return self._dedupe_title_url_location_v84(seed_jobs, limit=MAX_JOBS_PER_PAGE)

        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return self._dedupe_title_url_location_v84(seed_jobs, limit=MAX_JOBS_PER_PAGE)

        next_urls = self._collect_jobmonster_pagination_urls_v86(html, page_url)
        if not next_urls:
            return self._dedupe_title_url_location_v84(seed_jobs, limit=MAX_JOBS_PER_PAGE)

        merged = list(seed_jobs)
        async with httpx.AsyncClient(timeout=4.8, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
            for target in next_urls:
                if len(merged) >= MAX_JOBS_PER_PAGE * 2:
                    break
                try:
                    resp = await client.get(target)
                except Exception:
                    continue
                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 400:
                    continue
                merged.extend(self._extract_jobmonster_jobs_v86(body, str(resp.url)))

        return self._dedupe_title_url_location_v84(merged, limit=MAX_JOBS_PER_PAGE * 2)

    def _merge_jobmonster_superset_v86(self, current_jobs: list[dict], expanded_rows: list[dict]) -> list[dict]:
        merged: dict[tuple[str, str], dict] = {}
        ordered: list[dict] = []

        def _key(job: dict) -> tuple[str, str]:
            title = self._normalize_title(str(job.get("title") or "")).lower()
            source = str(job.get("source_url") or "").split("#", 1)[0].lower()
            return title, source

        for job in current_jobs:
            key = _key(job)
            if not key[0] or not key[1]:
                continue
            cloned = dict(job)
            merged[key] = cloned
            ordered.append(cloned)

        for row_job in expanded_rows:
            key = _key(row_job)
            if not key[0] or not key[1]:
                continue
            existing = merged.get(key)
            if existing is None:
                cloned = dict(row_job)
                merged[key] = cloned
                ordered.append(cloned)
                continue
            if not existing.get("location_raw") and row_job.get("location_raw"):
                existing["location_raw"] = row_job.get("location_raw")
            if not existing.get("description") and row_job.get("description"):
                existing["description"] = row_job.get("description")

        return self._dedupe_title_url_location_v84(ordered, limit=MAX_JOBS_PER_PAGE)
