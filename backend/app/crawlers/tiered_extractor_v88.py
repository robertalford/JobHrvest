"""
Tiered Extraction Engine v8.8 - ATS row recovery + bounded pagination fill.

Strategy:
1. Unify careers-page style card extraction across `careers-page.com` and
   `careerspage.io`, with bounded page expansion.
2. Add dedicated Avature SearchJobs row extraction with `jobOffset` follow-up.
3. Recover document-based vacancy rows from structured download lists.
4. Keep precision by tightening obvious non-role heading acceptance.
"""

from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

import httpx

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v87 import TieredExtractorV87, _V87_CP_UUID_PATH

_V88_OBVIOUS_NON_ROLE_TITLE = re.compile(r"^(?:the\s+job|from\s+the\s+.+\s+desk)$", re.IGNORECASE)

_V88_CP_HOST = re.compile(r"(?:^|\.)careers-page\.com$", re.IGNORECASE)
_V88_CPIO_HOST = re.compile(r"(?:^|\.)careerspage\.io$", re.IGNORECASE)
_V88_CP_CARD_MARKER = re.compile(r"\bjob-card\b", re.IGNORECASE)
_V88_CPIO_CARD_MARKER = re.compile(r"\bjob-item\b", re.IGNORECASE)
_V88_CP_GENERIC_DETAIL = re.compile(r"/jobs?/[A-Za-z0-9-]{6,}(?:/|$)", re.IGNORECASE)
_V88_CP_KO_DETAIL = re.compile(r"/[a-z0-9][a-z0-9-]{2,}-k[o0]\d{2,}(?:/|$)", re.IGNORECASE)
_V88_PAGE_QUERY_NUM = re.compile(r"[?&](?:page|paged)=(\d{1,4})\b", re.IGNORECASE)
_V88_PAGE_PATH_NUM = re.compile(r"/page/(\d{1,4})(?:/|$)", re.IGNORECASE)

_V88_AVATURE_HOST = re.compile(r"(?:^|\.)avature\.net$", re.IGNORECASE)
_V88_AVATURE_MARKER = re.compile(r"section__content__results|JobDetail|SearchJobsData", re.IGNORECASE)
_V88_AVATURE_DETAIL = re.compile(r"/careers/JobDetail/[^\s/?#]{2,}/\d{2,}", re.IGNORECASE)
_V88_AVATURE_NAV_TITLE = re.compile(
    r"^(?:careers?|talent\s+community|login|english|chinese|中文|search|reset|filters?)$",
    re.IGNORECASE,
)
_V88_AVATURE_OFFSET_Q = re.compile(r"[?&]jobOffset=(\d{1,5})\b", re.IGNORECASE)
_V88_CJK_CHAR = re.compile(r"[\u3400-\u9fff]")

_V88_DOC_LINK = re.compile(r"\.(?:pdf|docx?|rtf)(?:$|[?#])", re.IGNORECASE)
_V88_DOC_ROW_MARKER = re.compile(r"download__item|vacanc(?:y|ies)|role-profile", re.IGNORECASE)
_V88_DOC_NON_JOB = re.compile(
    r"\b(?:annual\s+report|investor|sustainability|privacy|cookie|policy|brochure|statement|results?)\b",
    re.IGNORECASE,
)
_V88_DOC_ROLE_HINT = re.compile(
    r"\b(?:assistant|administrator|officer|manager|director|engineer|analyst|coordinator|"
    r"specialist|surveyor|consultant|associate|intern|lead|head|teacher|nurse|technician|"
    r"operator|driver|recruiter|developer|designer|scientist|advisor)\b",
    re.IGNORECASE,
)
_V88_SALARY_HINT = re.compile(r"(?:[$€£]|(?:aud|usd|eur|php|sgd)\b|/\s*(?:hour|month|year))", re.IGNORECASE)


class TieredExtractorV88(TieredExtractorV87):
    """v8.8 extractor: ATS row recovery with bounded pagination backfill."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        portal_jobs = self._extract_careers_portal_rows_v88(working_html, page_url)
        if self._passes_careers_portal_jobset_v88(portal_jobs, page_url):
            expanded_portal = await self._expand_careers_portal_rows_v88(working_html, page_url, portal_jobs)
            if self._passes_careers_portal_jobset_v88(expanded_portal, page_url):
                return self._finalize_strict_rows_v88(expanded_portal, page_url)
            return self._finalize_strict_rows_v88(portal_jobs, page_url)

        avature_jobs = self._extract_avature_rows_v88(working_html, page_url)
        if self._passes_avature_jobset_v88(avature_jobs, page_url):
            expanded_avature = await self._expand_avature_rows_v88(working_html, page_url, avature_jobs)
            if self._passes_avature_jobset_v88(expanded_avature, page_url):
                return self._finalize_strict_rows_v88(expanded_avature, page_url)
            return self._finalize_strict_rows_v88(avature_jobs, page_url)

        doc_jobs = self._extract_document_vacancy_rows_v88(working_html, page_url)
        if self._passes_document_jobset_v88(doc_jobs):
            return self._finalize_strict_rows_v88(doc_jobs, page_url)

        jobs = await super().extract(career_page, company, working_html)
        jobs = await self._expand_paginated_heuristic_jobs_v88(working_html, page_url, jobs)
        return jobs

    def _is_valid_title_v60(self, title: str) -> bool:
        if not super()._is_valid_title_v60(title):
            return False
        return not _V88_OBVIOUS_NON_ROLE_TITLE.match((title or "").strip())

    def _extract_careers_portal_rows_v88(self, html: str, page_url: str) -> list[dict]:
        host = (urlparse(page_url).netloc or "").lower()
        lower_html = (html or "")[:450000].lower()
        if not (_V88_CP_HOST.search(host) or _V88_CPIO_HOST.search(host)):
            if not (_V88_CP_CARD_MARKER.search(lower_html) or _V88_CPIO_CARD_MARKER.search(lower_html)):
                return []

        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath(
            "//article[contains(@class,'job-card')]"
            "|//div[contains(@class,'job-card')]"
            "|//div[contains(@class,'job-item') and .//h3//a[@href]]"
        )
        if len(rows) < 4:
            return []

        jobs: list[dict] = []
        for row in rows[:2000]:
            link = self._pick_careers_portal_link_v88(row)
            if link is None:
                continue

            href = (link.get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url or self._is_non_job_url(source_url):
                continue
            if not self._has_careers_portal_detail_url_v88(source_url, page_url):
                continue

            raw_title = ""
            title_node = row.xpath(".//*[contains(@class,'job-title')][1]|.//h3[1]//a[1]")
            if title_node:
                raw_title = " ".join(" ".join(title_node[0].xpath(".//text()")).split())
            if not raw_title:
                raw_title = " ".join(" ".join(link.xpath(".//text()")).split())
            if not raw_title:
                raw_title = (link.get("data-job-title") or "").strip()

            title = self._normalize_title(raw_title)
            if not title:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, True):
                continue

            location = self._careers_portal_location_v88(row, title)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": None,
                    "salary_raw": None,
                    "employment_type": self._careers_portal_employment_type_v88(row),
                    "extraction_method": "ats_careers_portal_rows_v88",
                    "extraction_confidence": 0.93,
                }
            )

        return self._dedupe_title_url_v88(jobs, limit=MAX_JOBS_PER_PAGE * 2)

    @staticmethod
    def _pick_careers_portal_link_v88(row):
        links = row.xpath(
            ".//h3//a[@href]"
            "|.//a[contains(@class,'job-title-link') and @href]"
            "|.//a[(contains(@href,'/jobs/') or contains(@href,'/job/') or contains(@href,'-ko')) and @href]"
        )
        return links[0] if links else None

    def _has_careers_portal_detail_url_v88(self, source_url: str, page_url: str) -> bool:
        path = (urlparse(source_url).path or "").lower()
        if _V87_CP_UUID_PATH.search(path):
            return True
        if _V88_CP_GENERIC_DETAIL.search(path) or _V88_CP_KO_DETAIL.search(path):
            return True
        return self._has_strong_card_detail_url_v73(source_url, page_url) or self._is_job_like_url(source_url)

    def _careers_portal_location_v88(self, row, title: str) -> str | None:
        data_loc = (row.get("data-location") or "").strip(" ,|-")
        if data_loc and data_loc.lower() != title.lower() and len(data_loc) <= 140:
            return data_loc

        loc_nodes = row.xpath(
            ".//*[contains(@class,'fa-location-arrow')]/ancestor::li[1]//span[not(.//i)]"
            "|.//ul/li/span"
            "|.//*[contains(@class,'location')]"
        )
        for node in loc_nodes[:8]:
            loc = " ".join((_text(node) or "").split()).strip(" ,|-")
            if not loc or loc.lower() == title.lower() or len(loc) > 140:
                continue
            if loc.lower() in {"job details", "apply", "full time", "entry level"}:
                continue
            return loc
        return None

    @staticmethod
    def _careers_portal_employment_type_v88(row) -> str | None:
        nodes = row.xpath(".//*[contains(@class,'fa-business-time')]/ancestor::li[1]//span[not(.//i)]")
        for node in nodes[:3]:
            value = " ".join((_text(node) or "").split()).strip(" ,|-")
            if value and len(value) <= 80:
                return value
        return None

    def _passes_careers_portal_jobset_v88(self, jobs: list[dict], page_url: str) -> bool:
        if len(jobs) < 5:
            return False

        valid = 0
        strong = 0
        titles: list[str] = []
        for job in jobs:
            title = self._normalize_title(str(job.get("title") or ""))
            source_url = str(job.get("source_url") or "")
            if not title or not source_url:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue
            if self._is_non_job_url(source_url):
                continue
            has_strong = self._has_careers_portal_detail_url_v88(source_url, page_url)
            if not has_strong:
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, has_strong):
                continue
            valid += 1
            strong += 1
            titles.append(title)

        if valid < 5:
            return False
        unique_ratio = len({t.lower() for t in titles}) / max(1, len(titles))
        if unique_ratio < 0.6:
            return False
        return strong >= max(4, int(valid * 0.7))

    def _collect_page_param_urls_v88(self, html: str, page_url: str, max_pages: int = 2) -> list[str]:
        root = _parse_html(html)
        if root is None:
            return []

        page_host = (urlparse(page_url).netloc or "").lower()
        by_page: dict[int, str] = {}
        for node in root.xpath("//a[@href]")[:300]:
            href = (node.get("href") or "").strip()
            if not href:
                continue
            full = urljoin(page_url, href).split("#", 1)[0]
            parsed = urlparse(full)
            if (parsed.netloc or "").lower() != page_host:
                continue

            page_num = None
            query_match = _V88_PAGE_QUERY_NUM.search(full)
            if query_match:
                page_num = int(query_match.group(1))
            else:
                path_match = _V88_PAGE_PATH_NUM.search(parsed.path or "")
                if path_match:
                    page_num = int(path_match.group(1))

            if not page_num or page_num <= 1:
                continue
            by_page[page_num] = full

        return [by_page[k] for k in sorted(by_page)[:max_pages]]

    async def _expand_careers_portal_rows_v88(self, html: str, page_url: str, seed_jobs: list[dict]) -> list[dict]:
        if not seed_jobs:
            return []

        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return self._dedupe_title_url_v88(seed_jobs, limit=MAX_JOBS_PER_PAGE * 2)

        next_urls = self._collect_page_param_urls_v88(html, page_url, max_pages=2)
        if not next_urls:
            return self._dedupe_title_url_v88(seed_jobs, limit=MAX_JOBS_PER_PAGE * 2)

        merged = list(seed_jobs)
        async with httpx.AsyncClient(timeout=4.8, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
            for target in next_urls:
                try:
                    resp = await client.get(target)
                except Exception:
                    continue
                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 600:
                    continue
                merged.extend(self._extract_careers_portal_rows_v88(body, str(resp.url)))

        return self._dedupe_title_url_v88(merged, limit=MAX_JOBS_PER_PAGE * 2)

    def _extract_avature_rows_v88(self, html: str, page_url: str) -> list[dict]:
        host = (urlparse(page_url).netloc or "").lower()
        if not _V88_AVATURE_HOST.search(host) and not _V88_AVATURE_MARKER.search((html or "")[:240000]):
            return []

        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath(
            "//div[contains(@class,'section__content__results')]//article[.//a[contains(@href,'/JobDetail/')]]"
            "|//article[.//a[contains(@href,'/JobDetail/') and @href]]"
        )
        if len(rows) < 4:
            return []

        jobs: list[dict] = []
        for row in rows[:1200]:
            links = row.xpath(
                ".//h3[contains(@class,'article__header__text__title')]//a[@href][1]"
                "|.//a[contains(@href,'/JobDetail/')][1]"
            )
            if not links:
                continue
            link = links[0]

            href = (link.get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url or self._is_non_job_url(source_url):
                continue
            if not _V88_AVATURE_DETAIL.search(source_url):
                continue

            title = self._normalize_title(" ".join((_text(link) or "").split()))
            if not self._is_reasonable_multilingual_title_v88(title):
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, True):
                continue

            location = None
            for node in row.xpath(
                ".//span[contains(@class,'icon-address')]/parent::p[1]|.//p[contains(@class,'article__footer__text')]"
            )[:3]:
                loc = " ".join((_text(node) or "").split()).strip(" ,|-")
                if not loc or loc.lower() == title.lower() or len(loc) > 120:
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
                    "extraction_method": "ats_avature_rows_v88",
                    "extraction_confidence": 0.95,
                }
            )

        return self._dedupe_title_url_v88(jobs, limit=MAX_JOBS_PER_PAGE * 2)

    def _is_reasonable_multilingual_title_v88(self, title: str) -> bool:
        t = (title or "").strip()
        if not t:
            return False
        if self._is_valid_title_v60(t) or self._is_reasonable_structured_title_v81(t):
            return True
        if _V88_AVATURE_NAV_TITLE.match(t):
            return False
        if len(t) > 120:
            return False
        if _V88_CJK_CHAR.search(t):
            return True
        return False

    def _passes_avature_jobset_v88(self, jobs: list[dict], page_url: str) -> bool:
        if len(jobs) < 5:
            return False

        valid = 0
        strong = 0
        titles: list[str] = []
        for job in jobs:
            title = self._normalize_title(str(job.get("title") or ""))
            source_url = str(job.get("source_url") or "")
            if not title or not source_url:
                continue
            if not self._is_reasonable_multilingual_title_v88(title):
                continue
            if self._is_non_job_url(source_url):
                continue
            if not _V88_AVATURE_DETAIL.search(source_url):
                continue

            valid += 1
            strong += 1
            titles.append(title)

        if valid < 5:
            return False
        unique_ratio = len({t.lower() for t in titles}) / max(1, len(titles))
        if unique_ratio < 0.7:
            return False
        return strong >= max(4, int(valid * 0.75))

    def _collect_avature_offset_urls_v88(self, html: str, page_url: str, max_pages: int = 1) -> list[str]:
        root = _parse_html(html)
        if root is None:
            return []

        page_host = (urlparse(page_url).netloc or "").lower()
        by_offset: dict[int, str] = {}
        for node in root.xpath("//a[contains(@href,'jobOffset=') and @href]")[:120]:
            href = (node.get("href") or "").strip()
            if not href:
                continue
            full = urljoin(page_url, href).split("#", 1)[0]
            parsed = urlparse(full)
            if (parsed.netloc or "").lower() != page_host:
                continue
            match = _V88_AVATURE_OFFSET_Q.search(full)
            if not match:
                continue
            try:
                offset = int(match.group(1))
            except Exception:
                continue
            if offset <= 0:
                continue
            by_offset[offset] = full

        return [by_offset[k] for k in sorted(by_offset)[:max_pages]]

    async def _expand_avature_rows_v88(self, html: str, page_url: str, seed_jobs: list[dict]) -> list[dict]:
        if not seed_jobs:
            return []

        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return self._dedupe_title_url_v88(seed_jobs, limit=MAX_JOBS_PER_PAGE * 2)

        next_urls = self._collect_avature_offset_urls_v88(html, page_url, max_pages=1)
        if not next_urls:
            return self._dedupe_title_url_v88(seed_jobs, limit=MAX_JOBS_PER_PAGE * 2)

        merged = list(seed_jobs)
        async with httpx.AsyncClient(timeout=4.8, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
            for target in next_urls:
                try:
                    resp = await client.get(target)
                except Exception:
                    continue
                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 600:
                    continue
                merged.extend(self._extract_avature_rows_v88(body, str(resp.url)))

        return self._dedupe_title_url_v88(merged, limit=MAX_JOBS_PER_PAGE * 2)

    def _extract_document_vacancy_rows_v88(self, html: str, page_url: str) -> list[dict]:
        if not _V88_DOC_ROW_MARKER.search((html or "")[:300000]):
            return []

        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath(
            "//div[contains(@class,'download__item')]"
            "|//li[contains(@class,'download__item')]"
            "|//div[contains(@class,'vacancy') and .//a[@href]]"
        )
        if len(rows) < 3:
            return []

        jobs: list[dict] = []
        for row in rows[:1200]:
            links = row.xpath(".//a[@href]")
            if not links:
                continue
            href = (links[0].get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url or not _V88_DOC_LINK.search(source_url):
                continue

            title_nodes = row.xpath(
                ".//h3[contains(@class,'title')]"
                "|.//h3[contains(@class,'download__item-title')]"
                "|.//h2[contains(@class,'title')]"
                "|.//h2"
                "|.//h3"
            )
            raw_title = " ".join((_text(title_nodes[0]) or "").split()) if title_nodes else ""
            if not raw_title:
                raw_title = " ".join((_text(links[0]) or "").split())

            title = self._normalize_title(raw_title)
            if not title:
                continue
            if _V88_DOC_NON_JOB.search(title):
                continue
            if not self._doc_title_looks_role_v88(title):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_tail_from_title_v88(title),
                    "description": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "tier2_document_rows_v88",
                    "extraction_confidence": 0.89,
                }
            )

        return self._dedupe_title_url_v88(jobs, limit=MAX_JOBS_PER_PAGE * 2)

    def _finalize_strict_rows_v88(self, jobs: list[dict], page_url: str) -> list[dict]:
        cleaned: list[dict] = []
        for job in jobs:
            title = self._normalize_title(str(job.get("title") or ""))
            source_url = str(job.get("source_url") or "").split("#", 1)[0]
            if not title or not source_url:
                continue
            if self._is_non_job_url(source_url):
                continue

            cloned = dict(job)
            cloned["title"] = title
            cloned["source_url"] = source_url
            cleaned.append(cloned)

        return self._dedupe_title_url_v88(cleaned, limit=MAX_JOBS_PER_PAGE)

    def _dedupe_title_url_v88(self, jobs: list[dict], limit: int) -> list[dict]:
        deduped: dict[tuple[str, str], dict] = {}
        ordered: list[dict] = []
        for job in jobs:
            title = str(job.get("title") or "").strip()
            source_url = str(job.get("source_url") or "").strip()
            if not title or not source_url:
                continue

            key = (title.lower(), source_url.lower())
            existing = deduped.get(key)
            if existing is None:
                cloned = dict(job)
                deduped[key] = cloned
                ordered.append(cloned)
                if len(ordered) >= limit:
                    break
                continue

            old_loc = str(existing.get("location_raw") or "").strip()
            new_loc = str(job.get("location_raw") or "").strip()
            if not old_loc and new_loc:
                existing["location_raw"] = new_loc
                continue
            if old_loc and new_loc and _V88_SALARY_HINT.search(old_loc) and not _V88_SALARY_HINT.search(new_loc):
                existing["location_raw"] = new_loc

        return ordered

    def _doc_title_looks_role_v88(self, title: str) -> bool:
        t = (title or "").strip()
        if not t:
            return False
        if self._is_valid_title_v60(t) or self._is_reasonable_structured_title_v81(t):
            return True
        words = t.split()
        if len(words) < 2 or len(words) > 14:
            return False
        return bool(_V88_DOC_ROLE_HINT.search(t))

    @staticmethod
    def _extract_location_tail_from_title_v88(title: str) -> str | None:
        parts = [p.strip(" ,|-") for p in (title or "").split(",") if p.strip(" ,|-")]
        if len(parts) < 2:
            return None
        tail = parts[-1]
        if 2 <= len(tail) <= 40 and re.search(r"[A-Za-z]", tail):
            return tail
        if len(parts) >= 3:
            tail = ", ".join(parts[-2:])
            if 4 <= len(tail) <= 60:
                return tail
        return None

    @staticmethod
    def _passes_document_jobset_v88(jobs: list[dict]) -> bool:
        if len(jobs) < 3:
            return False
        titles = [str(j.get("title") or "").strip() for j in jobs if str(j.get("source_url") or "").strip()]
        if len(titles) < 3:
            return False
        unique_ratio = len({t.lower() for t in titles}) / max(1, len(titles))
        return unique_ratio >= 0.6

    async def _expand_paginated_heuristic_jobs_v88(self, html: str, page_url: str, jobs: list[dict]) -> list[dict]:
        if not jobs or len(jobs) >= 40:
            return jobs

        methods = {str(j.get("extraction_method") or "") for j in jobs}
        if not methods or any(m.startswith("ats_") for m in methods):
            return jobs
        if not methods.issubset({"tier2_heuristic_v16", "tier2_links", "tier2_heading_rows", "tier2_linked_cards_v67"}):
            return jobs

        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return jobs

        next_urls = self._collect_listing_pagination_urls_v88(html, page_url)
        if not next_urls:
            return jobs

        merged = list(jobs)
        async with httpx.AsyncClient(timeout=4.5, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
            for target in next_urls:
                try:
                    resp = await client.get(target)
                except Exception:
                    continue
                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 500:
                    continue

                candidate = self._extract_tier2_v16(str(resp.url), body) or []
                if not candidate:
                    continue
                candidate = self._postprocess_jobs_v73(candidate, body, str(resp.url))
                candidate = self._clean_jobs_v73(candidate)
                if not candidate:
                    continue
                merged.extend(candidate)

        deduped = self._dedupe_title_url_location_v84(merged, limit=MAX_JOBS_PER_PAGE)
        if len(deduped) <= len(jobs) + 1:
            return jobs
        if not self._passes_jobset_validation(deduped, page_url):
            return jobs
        return deduped

    def _collect_listing_pagination_urls_v88(self, html: str, page_url: str) -> list[str]:
        root = _parse_html(html)
        if root is None:
            return []

        page_host = (urlparse(page_url).netloc or "").lower()
        urls: list[str] = []
        seen: set[str] = set()
        nodes = root.xpath(
            "//a[@rel='next' and @href]"
            "|//div[contains(@class,'pagination')]//a[@href]"
            "|//nav[contains(@class,'pagination')]//a[@href]"
        )
        for node in nodes[:160]:
            href = (node.get("href") or "").strip()
            if not href:
                continue
            full = urljoin(page_url, href).split("#", 1)[0]
            parsed = urlparse(full)
            if (parsed.netloc or "").lower() != page_host:
                continue
            if full.rstrip("/") == page_url.rstrip("/"):
                continue
            if not (_V88_PAGE_QUERY_NUM.search(full) or _V88_PAGE_PATH_NUM.search(parsed.path or "")):
                continue
            if full in seen:
                continue
            seen.add(full)
            urls.append(full)
            if len(urls) >= 2:
                break

        return urls
