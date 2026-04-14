"""
Tiered Extraction Engine v8.7 - anchor-safe precision + ATS volume recovery.

Strategy:
1. Preserve trusted same-page anchor vacancies (Elementor/open-state) through
   non-job filtering and dedupe.
2. Recover careers-page.com job-card listings with bounded pagination follow-up.
3. Recover Workday inline `jsondata` payload jobs before noisy linked-card fallback.
4. Keep v8.6's Jobmonster and module-shell safeguards.
"""

from __future__ import annotations

import json
import re
from urllib.parse import parse_qs, urljoin, urlparse

import httpx

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url, _text
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
_V87_AUDIENCE_TITLE = re.compile(
    r"^(?:"
    r"graduates?\s+and\s+non[-\s]?graduates?|"
    r"internship\s+and\s+student\s+opportunit(?:y|ies)|"
    r"rewards?\s+at\s+.+"
    r")$",
    re.IGNORECASE,
)
_V87_ANCHOR_HINT = re.compile(r"(?:elementor-tab|accordion|job|role|vacanc|position|opening|employment)", re.IGNORECASE)
_V87_CAREER_PATH = re.compile(r"(?:careers?|jobs?|vacanc|employment|opening)", re.IGNORECASE)
_V87_CP_HOST = re.compile(r"(?:^|\.)careers-page\.com$", re.IGNORECASE)
_V87_CP_CARD_MARKER = re.compile(r"\bjob-card\b", re.IGNORECASE)
_V87_CP_UUID_PATH = re.compile(r"/jobs/[a-f0-9-]{8,}(?:/|$)", re.IGNORECASE)
_V87_CP_PAGE_Q = re.compile(r"[?&]page=(\d{1,4})\b", re.IGNORECASE)
_V87_WORKDAY_MARKER = re.compile(r"(?:bootstrapTable|id=[\"']wdresults[\"']|jobreqid)", re.IGNORECASE)
_V87_WORKDAY_JSON_BLOCK = re.compile(r"var\s+jsondata\s*=\s*(\[\s*\{.*?\}\s*\])\s*;", re.IGNORECASE | re.DOTALL)
_V87_QUERY_ID_DETAIL = re.compile(r"/(?:job[-_]?detail|position|vacancy)[^?#]*\?id=\d{4,}", re.IGNORECASE)


class TieredExtractorV87(TieredExtractorV85):
    """v8.7 extractor: recover ATS volume while preserving precision guards."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        role_jobs = self._extract_role_city_rows_v85(working_html, page_url)
        if self._passes_role_city_jobset_v85(role_jobs, page_url):
            return self._finalize_high_volume_jobs_v84(role_jobs, working_html, page_url)

        workday_jobs = self._extract_workday_inline_json_jobs_v87(working_html, page_url)
        if self._passes_structured_row_jobset_v81(workday_jobs, page_url):
            return self._finalize_high_volume_jobs_v84(workday_jobs, working_html, page_url)

        cp_jobs = self._extract_careers_page_rows_v87(working_html, page_url)
        if self._passes_structured_row_jobset_v81(cp_jobs, page_url):
            expanded_cp_jobs = await self._expand_careers_page_rows_v87(working_html, page_url, cp_jobs)
            if self._passes_structured_row_jobset_v81(expanded_cp_jobs, page_url):
                return self._finalize_high_volume_jobs_v84(expanded_cp_jobs, working_html, page_url)
            return self._finalize_high_volume_jobs_v84(cp_jobs, working_html, page_url)

        query_id_rows = self._extract_query_id_role_rows_v87(working_html, page_url)
        if len(query_id_rows) >= 5 and self._passes_structured_row_jobset_v81(query_id_rows, page_url):
            return self._finalize_high_volume_jobs_v84(query_id_rows, working_html, page_url)

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

        if len(query_id_rows) >= 3 and self._passes_jobset_validation(query_id_rows, page_url):
            return self._finalize_high_volume_jobs_v84(query_id_rows, working_html, page_url)

        return jobs

    def _is_valid_title_v60(self, title: str) -> bool:
        if not super()._is_valid_title_v60(title):
            return False
        return not _V87_AUDIENCE_TITLE.match((title or "").strip())

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
            source_url = str(job.get("source_url") or "")
            source_base = source_url.split("#", 1)[0]

            if method.startswith("ats_homerun_state_") and title and source_base:
                if self._is_valid_title_v60(title) and not self._is_non_job_url(source_base):
                    path_parts = [p for p in (urlparse(source_url).path or "").split("/") if p]
                    if len(path_parts) >= 2:
                        preserved.append(job)
                        continue

            if self._is_preserved_same_page_job_v87(job, page_url):
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
            keep_same_page_anchor = self._is_trusted_same_page_anchor_v87(source_url, page_url, title)
            if "#" in source_url and not keep_same_page_anchor:
                source_url = source_url.split("#", 1)[0]
            if self._is_non_job_url(source_url) and not keep_same_page_anchor:
                continue

            valid_title = self._is_valid_title_v60(title)
            if not valid_title:
                if not self._is_reasonable_structured_title_v81(title):
                    continue
                has_strong = (
                    keep_same_page_anchor
                    or self._has_strong_card_detail_url_v73(source_url, page_url)
                    or self._is_job_like_url(source_url)
                )
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

    def _is_preserved_same_page_job_v87(self, job: dict, page_url: str) -> bool:
        method = str(job.get("extraction_method") or "")
        if method not in {"tier2_elementor_accordion_rows_v83", "tier2_open_state_rows_v85"}:
            return False

        title = self._normalize_title(str(job.get("title") or ""))
        if not title:
            return False
        if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
            return False

        source_url = str(job.get("source_url") or "").strip()
        return self._is_trusted_same_page_anchor_v87(source_url, page_url, title)

    def _is_trusted_same_page_anchor_v87(self, source_url: str, page_url: str, title: str = "") -> bool:
        source = (source_url or "").strip()
        if "#" not in source:
            return False

        source_base, anchor = source.split("#", 1)
        source_base = source_base.rstrip("/")
        page_base = (page_url or "").split("#", 1)[0].rstrip("/")
        if not source_base or not page_base or source_base != page_base:
            return False
        if not anchor or not _V87_ANCHOR_HINT.search(anchor):
            return False

        if _V87_CAREER_PATH.search(urlparse(page_url).path or ""):
            return True
        return bool(title and (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)))

    def _extract_careers_page_rows_v87(self, html: str, page_url: str) -> list[dict]:
        host = (urlparse(page_url).netloc or "").lower()
        lower_html = (html or "")[:400000].lower()
        if not _V87_CP_HOST.search(host):
            if not (_V87_CP_CARD_MARKER.search(lower_html) and "/jobs/" in lower_html):
                return []

        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath("//article[contains(@class,'job-card')]|//div[contains(@class,'job-card')]")
        if len(rows) < 6:
            return []

        jobs: list[dict] = []
        for row in rows[:1200]:
            link_nodes = row.xpath(".//a[contains(@href,'/jobs/') and @href]")
            if not link_nodes:
                continue

            href = (link_nodes[0].get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url or self._is_non_job_url(source_url):
                continue
            if not _V87_CP_UUID_PATH.search(urlparse(source_url).path or ""):
                continue

            title_node = row.xpath(".//*[contains(@class,'job-title')][1]")
            raw_title = ""
            if title_node:
                raw_title = " ".join(" ".join(title_node[0].xpath(".//text()")).split())
            if not raw_title:
                raw_title = " ".join(" ".join(link_nodes[0].xpath(".//text()")).split())

            title = self._normalize_title(raw_title)
            if not title:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, True):
                continue

            location = None
            for node in row.xpath(".//ul/li/span|.//ul/li[not(.//a)]")[:4]:
                loc = " ".join((_text(node) or "").split()).strip(" ,|-")
                if not loc or loc == "-" or len(loc) > 140:
                    continue
                if loc.lower() == title.lower():
                    continue
                location = loc
                break

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "ats_careers_page_rows_v87",
                    "extraction_confidence": 0.94,
                }
            )

        deduped: dict[tuple[str, str], dict] = {}
        ordered: list[dict] = []
        for job in jobs:
            key = (str(job.get("title") or "").lower(), str(job.get("source_url") or "").lower())
            if not key[0] or not key[1]:
                continue
            existing = deduped.get(key)
            if existing is None:
                cloned = dict(job)
                deduped[key] = cloned
                ordered.append(cloned)
                continue
            if not existing.get("location_raw") and job.get("location_raw"):
                existing["location_raw"] = job.get("location_raw")

        return ordered[: MAX_JOBS_PER_PAGE * 2]

    def _collect_careers_page_pagination_urls_v87(self, html: str, page_url: str) -> list[str]:
        root = _parse_html(html)
        if root is None:
            return []

        page_host = (urlparse(page_url).netloc or "").lower()
        by_page: dict[int, str] = {}
        nodes = root.xpath("//div[contains(@class,'pagination')]//a[@href]")
        for node in nodes[:80]:
            href = (node.get("href") or "").strip()
            if not href:
                continue
            full = urljoin(page_url, href).split("#", 1)[0]
            parsed = urlparse(full)
            if (parsed.netloc or "").lower() != page_host:
                continue

            match = _V87_CP_PAGE_Q.search(full)
            if not match:
                continue
            try:
                page_num = int(match.group(1))
            except Exception:
                continue
            if page_num <= 1:
                continue
            by_page[page_num] = full

        return [by_page[k] for k in sorted(by_page)[:2]]

    async def _expand_careers_page_rows_v87(self, html: str, page_url: str, seed_jobs: list[dict]) -> list[dict]:
        if len(seed_jobs) < 8:
            return self._dedupe_title_url_location_v84(seed_jobs, limit=MAX_JOBS_PER_PAGE * 2)

        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return self._dedupe_title_url_location_v84(seed_jobs, limit=MAX_JOBS_PER_PAGE * 2)

        next_urls = self._collect_careers_page_pagination_urls_v87(html, page_url)
        if not next_urls:
            return self._dedupe_title_url_location_v84(seed_jobs, limit=MAX_JOBS_PER_PAGE * 2)

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
                if resp.status_code != 200 or len(body) < 600:
                    continue
                merged.extend(self._extract_careers_page_rows_v87(body, str(resp.url)))

        return self._dedupe_title_url_location_v84(merged, limit=MAX_JOBS_PER_PAGE * 2)

    def _extract_workday_inline_json_jobs_v87(self, html: str, page_url: str) -> list[dict]:
        if not _V87_WORKDAY_MARKER.search((html or "")[:500000]):
            return []

        payload = None
        for match in _V87_WORKDAY_JSON_BLOCK.finditer(html or ""):
            candidate = (match.group(1) or "").strip()
            if len(candidate) < 200 or len(candidate) > 2000000:
                continue
            try:
                parsed = json.loads(candidate)
            except Exception:
                continue
            if isinstance(parsed, list) and parsed:
                payload = parsed
                break
        if not payload:
            return []

        jobs: list[dict] = []
        for item in payload[:3000]:
            if not isinstance(item, dict):
                continue

            raw_title = ""
            for key in ("title", "job_title", "name", "position"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    raw_title = value.strip()
                    break
            title = self._normalize_title(raw_title)
            if not title:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue

            source_url = ""
            for key in ("apply", "url", "jobUrl", "job_url", "detail_url"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    source_url = value.strip()
                    break
            source_url = (_resolve_url(source_url, page_url) or "").split("#", 1)[0]
            if not source_url or self._is_non_job_url(source_url):
                continue

            has_strong = (
                self._has_strong_card_detail_url_v73(source_url, page_url)
                or self._is_job_like_url(source_url)
                or "myworkdayjobs.com" in source_url.lower()
            )
            if not has_strong:
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, has_strong):
                continue

            location = None
            for key in ("location", "region"):
                value = item.get(key)
                if isinstance(value, str):
                    cleaned = value.strip(" ,|-")
                    if cleaned and cleaned.lower() != title.lower():
                        location = cleaned[:140]
                        break

            employment_type = None
            for key in ("timetype", "jobtype", "workertype"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    employment_type = value.strip()[:80]
                    break

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": None,
                    "salary_raw": None,
                    "employment_type": employment_type,
                    "extraction_method": "ats_workday_inline_json_v87",
                    "extraction_confidence": 0.95,
                }
            )

        cap = self._workday_inline_page_cap_v87(html)
        limit = cap if cap else MAX_JOBS_PER_PAGE * 2
        return self._dedupe_title_url_location_v84(jobs, limit=limit)

    @staticmethod
    def _workday_inline_page_cap_v87(html: str) -> int | None:
        if not html:
            return None
        match = re.search(
            r"id=[\"']wdresults[\"'][^>]*\bdata-page-size=[\"'](\d{1,3})[\"']",
            html,
            re.IGNORECASE,
        )
        if not match:
            return None
        try:
            size = int(match.group(1))
        except Exception:
            return None
        if 5 <= size <= 50:
            return size
        return None

    def _extract_query_id_role_rows_v87(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        jobs: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for link in root.xpath("//a[@href]")[:1600]:
            href = (link.get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url:
                continue
            if not _V87_QUERY_ID_DETAIL.search(source_url):
                continue
            if self._is_non_job_url(source_url):
                continue

            cards = link.xpath("ancestor::div[contains(@class,'mb-4')][1]|ancestor::article[1]|ancestor::li[1]")
            card = cards[0] if cards else link.getparent()
            if card is None:
                continue

            title_nodes = card.xpath(".//*[contains(@class,'job-title')][1]")
            if not title_nodes:
                continue
            title = self._normalize_title(" ".join((_text(title_nodes[0]) or "").split()))
            if not title:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, True):
                continue

            location = None
            for node in card.xpath(".//div[not(.//a) and normalize-space()]")[:8]:
                text = " ".join((_text(node) or "").split()).strip(" ,|-")
                if not text or text.lower() == title.lower():
                    continue
                if len(text) > 140:
                    continue
                if "," in text and re.search(r"[A-Za-z]", text):
                    location = text
                    break

            key = (title.lower(), source_url.lower())
            if key in seen:
                continue
            seen.add(key)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "tier2_query_id_rows_v87",
                    "extraction_confidence": 0.9,
                }
            )

        return self._dedupe_title_url_location_v84(jobs, limit=MAX_JOBS_PER_PAGE * 2)

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
