"""
Tiered Extraction Engine v8.4 - ATS row-volume recovery.

Strategy:
1. Add dedicated high-volume row extraction for Jobvite table listings.
2. Add dedicated TalentSoft offer-row extraction with bounded pagination merge.
3. Improve Jobs2Web shell recovery by prioritizing current search URLs and
   supporting modern `JobsList_jobCard` DOM cards.
"""

from __future__ import annotations

import re
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import httpx

from app.crawlers.tiered_extractor import _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v74 import _V73_NAV_TITLE, _V73_NON_JOB_HEADING
from app.crawlers.tiered_extractor_v83 import TieredExtractorV83

_V84_JOBVITE_HOST = re.compile(r"(?:^|\.)jobvite\.com$", re.IGNORECASE)
_V84_JOBVITE_DETAIL_PATH = re.compile(r"/[^/?#]+/job/[A-Za-z0-9_-]{5,}", re.IGNORECASE)

_V84_TALENTSOFT_HOST = re.compile(r"(?:^|\.)talent-soft\.com$", re.IGNORECASE)
_V84_TALENTSOFT_DETAIL_PATH = re.compile(r"/offre-de-emploi/emploi-[^/?#]+_[0-9]+\.aspx", re.IGNORECASE)
_V84_TS_PAGINATION_PATH = re.compile(r"[?&]page=\d{1,3}\b", re.IGNORECASE)

_V84_JOBS2WEB_REACT_CARD = re.compile(r"JobsList_jobCard", re.IGNORECASE)
_V84_JOBS2WEB_SEARCH_Q = re.compile(r"[?&]searchResultView=LIST\b", re.IGNORECASE)


class TieredExtractorV84(TieredExtractorV83):
    """v8.4 extractor: ATS row-volume recovery with safer shell fallback."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        # Jobvite listing tables can carry hundreds of valid rows.
        jobvite_jobs = self._extract_jobvite_table_rows_v84(working_html, page_url)
        if self._passes_high_volume_jobset_v84(jobvite_jobs, page_url, min_jobs=8):
            return self._finalize_high_volume_jobs_v84(jobvite_jobs, working_html, page_url)

        # TalentSoft pages expose stable `ts-offer-list-item` rows.
        talentsoft_jobs = self._extract_talentsoft_offer_rows_v84(working_html, page_url)
        if self._passes_high_volume_jobset_v84(talentsoft_jobs, page_url, min_jobs=5):
            expanded = await self._expand_talentsoft_pages_v84(working_html, page_url, talentsoft_jobs)
            if self._passes_high_volume_jobset_v84(expanded, page_url, min_jobs=5):
                return self._finalize_high_volume_jobs_v84(expanded, working_html, page_url)

        # Config-only Jobs2Web shells often time out through generic fallback chains.
        if self._is_jobs2web_shell_v66(page_url, working_html):
            jobs2web_jobs = await self._extract_jobs2web_jobs_v66(page_url, working_html)
            if len(jobs2web_jobs) >= 3 and self._passes_structured_row_jobset_v81(jobs2web_jobs, page_url):
                return self._finalize_high_volume_jobs_v84(jobs2web_jobs, working_html, page_url)
            if self._is_config_only_jobs2web_shell_v84(working_html):
                return []

        return await super().extract(career_page, company, html)

    def _finalize_high_volume_jobs_v84(self, jobs: list[dict], html: str, page_url: str) -> list[dict]:
        methods = {str(j.get("extraction_method") or "") for j in jobs}
        trusted_prefixes = ("ats_jobvite_table_v84", "ats_talentsoft_rows_v84", "ats_jobs2web_")
        skip_postprocess = bool(methods) and all(
            any(m.startswith(prefix) for prefix in trusted_prefixes) for m in methods
        )

        finalized = list(jobs) if skip_postprocess else self._postprocess_jobs_v73(jobs, html, page_url)
        finalized = self._clean_jobs_v73(finalized)
        return self._dedupe_title_url_location_v84(finalized, limit=320)

    def _passes_high_volume_jobset_v84(self, jobs: list[dict], page_url: str, min_jobs: int) -> bool:
        if len(jobs) < min_jobs:
            return False

        valid = 0
        strong_urls = 0
        titles: list[str] = []
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

            has_strong = (
                self._has_strong_card_detail_url_v73(source_url, page_url)
                or self._is_job_like_url(source_url)
                or bool(_V84_JOBVITE_DETAIL_PATH.search(urlparse(source_url).path or ""))
                or bool(_V84_TALENTSOFT_DETAIL_PATH.search(urlparse(source_url).path or ""))
            )
            if not has_strong:
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, has_strong):
                continue

            valid += 1
            titles.append(title)
            strong_urls += 1

        if valid < min_jobs:
            return False
        unique_ratio = len({t.lower() for t in titles}) / max(1, len(titles))
        if unique_ratio < 0.55:
            return False
        return strong_urls >= max(min_jobs - 1, int(valid * 0.65))

    @staticmethod
    def _dedupe_title_url_location_v84(jobs: list[dict], limit: int = 320) -> list[dict]:
        out: list[dict] = []
        seen: set[tuple[str, str, str]] = set()
        for job in jobs:
            title = str(job.get("title") or "").strip()
            source_url = str(job.get("source_url") or "").strip()
            location = str(job.get("location_raw") or "").strip()
            if not title or not source_url:
                continue
            key = (title.lower(), source_url.lower(), location.lower())
            if key in seen:
                continue
            seen.add(key)
            out.append(job)
            if len(out) >= limit:
                break
        return out

    def _extract_jobvite_table_rows_v84(self, html: str, page_url: str) -> list[dict]:
        host = (urlparse(page_url).netloc or "").lower()
        if not _V84_JOBVITE_HOST.search(host) and "jv-job-list" not in (html or ""):
            return []

        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath(
            "//table[contains(@class,'jv-job-list')]//tr[.//td[contains(@class,'jv-job-list-name')]//a[@href]]"
        )
        if len(rows) < 8:
            return []

        jobs: list[dict] = []
        for row in rows[:2200]:
            link_nodes = row.xpath(".//td[contains(@class,'jv-job-list-name')]//a[@href][1]")
            if not link_nodes:
                continue

            link = link_nodes[0]
            href = (link.get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url:
                continue
            if not _V84_JOBVITE_DETAIL_PATH.search(urlparse(source_url).path or ""):
                continue
            if self._is_non_job_url(source_url):
                continue

            title = self._normalize_title(_text(link) or (link.get("title") or ""))
            if not title:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue

            location = None
            loc_nodes = row.xpath(".//td[contains(@class,'jv-job-list-location')]")
            if loc_nodes:
                loc = " ".join((_text(loc_nodes[0]) or "").split()).strip()
                if loc and loc.lower() != title.lower():
                    location = loc[:180]

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "ats_jobvite_table_v84",
                    "extraction_confidence": 0.95,
                }
            )

        return self._dedupe_title_url_location_v84(jobs, limit=420)

    def _extract_talentsoft_offer_rows_v84(self, html: str, page_url: str) -> list[dict]:
        host = (urlparse(page_url).netloc or "").lower()
        lower_html = (html or "")[:300000].lower()
        if not _V84_TALENTSOFT_HOST.search(host) and "ts-offer-list-item" not in lower_html:
            return []

        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath(
            "//li[contains(@class,'ts-offer-list-item') and .//a[contains(@class,'ts-offer-list-item__title-link') and @href]]"
        )
        if len(rows) < 3:
            return []

        jobs: list[dict] = []
        for row in rows[:1600]:
            links = row.xpath(".//a[contains(@class,'ts-offer-list-item__title-link') and @href]")
            if not links:
                continue

            link = links[0]
            href = (link.get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url:
                continue
            if not _V84_TALENTSOFT_DETAIL_PATH.search(urlparse(source_url).path or ""):
                continue
            if self._is_non_job_url(source_url):
                continue

            title = self._normalize_title(_text(link) or (link.get("title") or ""))
            if not title:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue
            if _V73_NAV_TITLE.match(title) or _V73_NON_JOB_HEADING.match(title):
                continue

            location = None
            for node in row.xpath(".//ul[contains(@class,'ts-offer-list-item__description')]/li")[:4]:
                loc = " ".join((_text(node) or "").split()).strip(" ,|-")
                if not loc:
                    continue
                if len(loc) > 120:
                    continue
                if loc.lower() == title.lower():
                    continue
                location = loc
                break

            row_text = " ".join((_text(row) or "").split())
            description = row_text[:5000] if len(row_text) > 120 and row_text.lower() != title.lower() else None

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": description,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "ats_talentsoft_rows_v84",
                    "extraction_confidence": 0.92,
                }
            )

        return self._dedupe_title_url_location_v84(jobs, limit=320)

    def _collect_talentsoft_pagination_urls_v84(self, html: str, page_url: str) -> list[str]:
        root = _parse_html(html)
        if root is None:
            return []

        page_host = (urlparse(page_url).netloc or "").lower()
        by_page: dict[int, str] = {}
        nodes = root.xpath(
            "//a[contains(@class,'ts-ol-pagination-list-item__link') and contains(@href,'page=') and @href]"
            "|//div[contains(@class,'pagination')]//a[contains(@href,'page=') and @href]"
        )
        for node in nodes[:140]:
            href = (node.get("href") or "").strip()
            if not href:
                continue
            full = urljoin(page_url, href).split("#", 1)[0]
            parsed = urlparse(full)
            if (parsed.netloc or "").lower() != page_host:
                continue
            if not _V84_TS_PAGINATION_PATH.search(full):
                continue

            query = parse_qs(parsed.query, keep_blank_values=True)
            try:
                page_num = int((query.get("page") or ["0"])[0])
            except Exception:
                continue
            if page_num <= 1:
                continue
            by_page[page_num] = full

        return [by_page[k] for k in sorted(by_page)[:4]]

    async def _expand_talentsoft_pages_v84(self, html: str, page_url: str, seed_jobs: list[dict]) -> list[dict]:
        if len(seed_jobs) < 5:
            return self._dedupe_title_url_location_v84(seed_jobs, limit=320)

        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return self._dedupe_title_url_location_v84(seed_jobs, limit=320)

        next_urls = self._collect_talentsoft_pagination_urls_v84(html, page_url)
        if not next_urls:
            return self._dedupe_title_url_location_v84(seed_jobs, limit=320)

        merged = list(seed_jobs)
        async with httpx.AsyncClient(timeout=4.5, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
            for target in next_urls:
                if len(merged) >= 280:
                    break
                try:
                    resp = await client.get(target)
                except Exception:
                    continue
                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 400:
                    continue
                merged.extend(self._extract_talentsoft_offer_rows_v84(body, str(resp.url)))
        return self._dedupe_title_url_location_v84(merged, limit=320)

    def _is_config_only_jobs2web_shell_v84(self, html: str) -> bool:
        lower_html = (html or "")[:300000].lower()
        if "xweb/rmk-jobs-search" not in lower_html:
            return False
        if "widgetloadercomponents" not in lower_html and "jobsearch_j_id1" not in lower_html:
            return False
        if "data-testid=\"jobcard\"" in lower_html or "data-testid='jobcard'" in lower_html:
            return False
        if _V84_JOBS2WEB_REACT_CARD.search(lower_html):
            return False
        return True

    def _jobs2web_endpoint_candidates_v66(self, page_url: str, cfg: dict[str, str | None]) -> list[str]:
        candidates = list(super()._jobs2web_endpoint_candidates_v66(page_url, cfg))

        parsed = urlparse(page_url or "")
        priority: list[str] = []
        if parsed.scheme and parsed.netloc and _V84_JOBS2WEB_SEARCH_Q.search(parsed.query or ""):
            priority.append(parsed._replace(fragment="").geturl())

            query = parse_qs(parsed.query or "", keep_blank_values=True)
            if "searchResultView" not in query:
                query["searchResultView"] = ["LIST"]
            if "pageNumber" not in query:
                query["pageNumber"] = ["0"]
            if "markerViewed" not in query:
                query["markerViewed"] = [""]
            if "carouselIndex" not in query:
                query["carouselIndex"] = [""]
            if "facetFilters" not in query:
                query["facetFilters"] = ["{}"]
            if "sortBy" not in query:
                query["sortBy"] = ["date"]

            priority.append(parsed._replace(query=urlencode(query, doseq=True), fragment="").geturl())
            locale = (cfg.get("locale") or "").strip()
            if locale and "locale" not in query:
                q2 = dict(query)
                q2["locale"] = [locale]
                priority.append(parsed._replace(query=urlencode(q2, doseq=True), fragment="").geturl())

        ordered: list[str] = []
        seen: set[str] = set()
        for endpoint in priority + candidates:
            if not endpoint or endpoint in seen:
                continue
            seen.add(endpoint)
            ordered.append(endpoint)
        return ordered

    async def _extract_jobs2web_jobs_v66(self, page_url: str, html: str) -> list[dict]:
        cfg = self._extract_jobs2web_config_v66(html)
        company_id = (cfg.get("company_id") or "").strip()
        if not company_id:
            return []

        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return self._extract_jobs2web_dom_v66(html, page_url)

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/html;q=0.9,*/*;q=0.8",
        }
        if cfg.get("csrf"):
            headers["X-CSRF-Token"] = str(cfg["csrf"])

        endpoints = self._jobs2web_endpoint_candidates_v66(page_url, cfg)
        if not endpoints:
            return []

        async with httpx.AsyncClient(timeout=3.8, follow_redirects=True, headers=headers) as client:
            for endpoint in endpoints[:5]:
                try:
                    resp = await client.get(endpoint)
                except Exception:
                    continue

                if resp.status_code != 200:
                    continue

                body = resp.text or ""
                if len(body) < 100:
                    continue

                jobs: list[dict] = []
                content_type = (resp.headers.get("content-type") or "").lower()
                if "json" in content_type or body.lstrip().startswith(("{", "[")):
                    try:
                        payload = resp.json()
                    except Exception:
                        payload = None
                    if payload is not None:
                        jobs = self._extract_jobs_from_generic_json_v66(payload, endpoint, page_url)

                if not jobs:
                    jobs = self._extract_jobs2web_dom_v66(body, str(resp.url))

                jobs = self._dedupe_title_url_location_v84(jobs, limit=220)
                if len(jobs) >= 3:
                    return jobs
        return []

    def _extract_jobs2web_dom_v66(self, html: str, page_url: str) -> list[dict]:
        jobs = super()._extract_jobs2web_dom_v66(html, page_url)
        if (
            len(jobs) >= 3
            and not (_V84_JOBS2WEB_REACT_CARD.search(html or "") and any(not j.get("location_raw") for j in jobs))
        ):
            return jobs

        root = _parse_html(html)
        if root is None:
            return jobs

        rows = root.xpath(
            "//li[contains(@class,'JobsList_jobCard') and .//a[contains(@class,'jobCardTitle') and @href]]"
        )
        if len(rows) < 3:
            return jobs

        out: list[dict] = list(jobs)
        for row in rows[:700]:
            links = row.xpath(".//a[contains(@class,'jobCardTitle') and @href]")
            if not links:
                continue

            link = links[0]
            href = (link.get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url or self._is_non_job_url(source_url):
                continue

            title = self._normalize_title(_text(link) or (link.get("title") or ""))
            if not title:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue

            location = None
            for node in row.xpath(".//*[contains(@class,'jobCardFooterValue')]")[:5]:
                loc = " ".join((_text(node) or "").split()).strip(" ,|-")
                if not loc or len(loc) > 120:
                    continue
                if loc.lower() == title.lower():
                    continue
                location = loc
                break

            out.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "ats_jobs2web_dom_v84",
                    "extraction_confidence": 0.9,
                }
            )

        merged: dict[tuple[str, str], dict] = {}
        for job in out:
            title = str(job.get("title") or "").strip().lower()
            source_url = str(job.get("source_url") or "").strip().lower()
            if title and source_url:
                merged[(title, source_url)] = job

        for job in out:
            title = str(job.get("title") or "").strip().lower()
            source_url = str(job.get("source_url") or "").strip().lower()
            if not title or not source_url:
                continue
            key = (title, source_url)
            prev = merged.get(key)
            if prev is None:
                merged[key] = job
                continue
            if not prev.get("location_raw") and job.get("location_raw"):
                merged[key] = job

        return self._dedupe_title_url_location_v84(list(merged.values()), limit=220)
