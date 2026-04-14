"""
Tiered Extraction Engine v8.5 - shell recovery + multi-location row retention.

Strategy:
1. Expand Jobs2Web endpoint recovery using `ssoUrl` host variants and a small
   `/services/search/*` probe fallback.
2. Retain repeated role rows that share title+URL but have distinct locations.
3. Recover Radix/accordion `data-state="open"` rows with heading+detail-link evidence.
"""

from __future__ import annotations

import re
from urllib.parse import parse_qs, quote_plus, urlparse

import httpx

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v84 import TieredExtractorV84

_V85_EMPTY_ROLE_TEXT = re.compile(r"\bnone\s+as\s+of\s+the\s+moment\b", re.IGNORECASE)
_V85_GENERIC_SINGLE_TITLE = re.compile(
    r"^(?:careers?\s*)?(?:the\s+)?latest\s+job\s+opportunities$",
    re.IGNORECASE,
)
_V85_LOCATION_LABEL = re.compile(r"\blocation\s*:\s*([^|]{2,120})", re.IGNORECASE)
_V85_CAREER_DETAILISH_PATH = re.compile(r"/(?:about-us/)?careers?/[a-z0-9][^/?#]{3,}", re.IGNORECASE)
_V85_MULTI_OPENING_PATH = re.compile(r"/(?:career|careers|job|jobs)/[a-z0-9][^/?#]{2,}", re.IGNORECASE)


class TieredExtractorV85(TieredExtractorV84):
    """v8.5 extractor: shell endpoint recovery + row-level volume fixes."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        role_jobs = self._extract_role_city_rows_v85(working_html, page_url)
        if self._passes_role_city_jobset_v85(role_jobs, page_url):
            return self._finalize_high_volume_jobs_v84(role_jobs, working_html, page_url)

        jobs = await super().extract(career_page, company, html)
        if len(jobs) == 1 and self._is_generic_listing_single_title_v85(str(jobs[0].get("title") or "")):
            jobs = []

        if jobs:
            return jobs

        open_state_jobs = self._extract_open_state_rows_v85(working_html, page_url)
        if self._passes_open_state_jobset_v85(open_state_jobs, page_url):
            return self._finalize_high_volume_jobs_v84(open_state_jobs, working_html, page_url)

        return jobs

    @staticmethod
    def _is_generic_listing_single_title_v85(title: str) -> bool:
        return bool(_V85_GENERIC_SINGLE_TITLE.match((title or "").strip()))

    def _extract_linked_job_cards_v67(self, html: str, page_url: str) -> list[dict]:
        base_jobs = super()._extract_linked_job_cards_v67(html, page_url)
        role_jobs = self._extract_role_city_rows_v85(html, page_url)
        if not role_jobs:
            return base_jobs
        return self._dedupe_title_url_location_v84(base_jobs + role_jobs, limit=MAX_JOBS_PER_PAGE)

    def _extract_role_city_rows_v85(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath("//*[contains(concat(' ', normalize-space(@class), ' '), ' role ') and .//a[@href]]")
        if len(rows) < 3:
            return []

        jobs: list[dict] = []
        for row in rows[:900]:
            row_text = " ".join((_text(row) or "").split())
            if not row_text or _V85_EMPTY_ROLE_TEXT.search(row_text):
                continue

            links = row.xpath(".//a[@href][1]")
            if not links:
                continue
            link = links[0]

            title = self._normalize_title(_text(link) or (link.get("title") or ""))
            if not title:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue

            href = (link.get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url or self._is_non_job_url(source_url):
                continue

            has_strong = self._has_strong_card_detail_url_v73(source_url, page_url) or self._is_job_like_url(source_url)
            if not has_strong:
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, has_strong):
                continue

            location = None
            for node in row.xpath(".//*[contains(@class,'city') or contains(@class,'location')]")[:4]:
                loc = " ".join((_text(node) or "").split()).strip(" ,|-")
                if not loc:
                    continue
                if len(loc) > 140 or loc.lower() == title.lower():
                    continue
                location = loc[:140]
                break
            if not location:
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": row_text[:5000] if len(row_text) > 90 else None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "tier2_role_rows_v85",
                    "extraction_confidence": 0.88,
                }
            )

        return self._dedupe_title_url_location_v84(jobs, limit=MAX_JOBS_PER_PAGE * 2)

    def _passes_role_city_jobset_v85(self, jobs: list[dict], page_url: str) -> bool:
        if len(jobs) < 6:
            return False

        strong_urls = 0
        titles: list[str] = []
        locations: list[str] = []
        for job in jobs:
            title = self._normalize_title(str(job.get("title") or ""))
            source_url = str(job.get("source_url") or "")
            location = str(job.get("location_raw") or "").strip()
            if not title or not source_url or not location:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue
            has_strong = self._has_strong_card_detail_url_v73(source_url, page_url) or self._is_job_like_url(source_url)
            if not has_strong:
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, has_strong):
                continue

            titles.append(title)
            locations.append(location)
            strong_urls += 1

        if len(titles) < 6:
            return False
        if len({t.lower() for t in titles}) < 3:
            return False
        if len({loc.lower() for loc in locations}) < 4:
            return False
        return strong_urls >= max(5, int(len(titles) * 0.75))

    def _extract_open_state_rows_v85(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath(
            "//*[@data-state='open' and .//a[@href] and (.//h1|.//h2|.//h3|.//h4|.//button)]"
        )
        if not rows:
            return []

        jobs: list[dict] = []
        for idx, row in enumerate(rows[:400], start=1):
            title = ""
            for node in row.xpath(".//h1|.//h2|.//h3|.//h4|.//button"):
                raw = self._normalize_title(_text(node) or "")
                if raw:
                    title = raw
                    break
            if not title:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue

            links = row.xpath(".//a[@href]")
            if not links:
                continue
            href = (links[0].get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url:
                continue

            source_path = urlparse(source_url).path or ""
            is_detailish = bool(_V85_CAREER_DETAILISH_PATH.search(source_path))
            if self._is_non_job_url(source_url) and not is_detailish:
                continue
            has_strong = (
                self._has_strong_card_detail_url_v73(source_url, page_url)
                or self._is_job_like_url(source_url)
                or is_detailish
            )
            if not has_strong:
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, has_strong):
                continue

            row_text = " ".join((_text(row) or "").split())
            location = None
            loc_match = _V85_LOCATION_LABEL.search(row_text or "")
            if loc_match:
                loc_text = loc_match.group(1)
                loc_text = re.split(
                    r"\b(?:find\s+out\s+more|apply(?:\s+now)?|read\s+more|view\s+more)\b",
                    loc_text,
                    maxsplit=1,
                    flags=re.IGNORECASE,
                )[0]
                location = loc_text.strip(" ,|-")[:140] or None

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": row_text[:5000] if len(row_text) > 80 else None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "tier2_open_state_rows_v85",
                    "extraction_confidence": 0.9,
                    "internal_id": f"open-state-{idx}",
                }
            )

        return self._dedupe_title_url_location_v84(jobs, limit=MAX_JOBS_PER_PAGE)

    def _passes_open_state_jobset_v85(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        valid = 0
        for job in jobs:
            title = self._normalize_title(str(job.get("title") or ""))
            source_url = str(job.get("source_url") or "")
            if not title or not source_url:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue
            source_path = urlparse(source_url).path or ""
            has_strong = (
                self._has_strong_card_detail_url_v73(source_url, page_url)
                or self._is_job_like_url(source_url)
                or bool(_V85_CAREER_DETAILISH_PATH.search(source_path))
            )
            if not has_strong:
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, has_strong):
                continue
            valid += 1

        if len(jobs) == 1:
            return valid == 1
        return valid >= 2

    def _dedupe(self, jobs: list[dict], page_url: str) -> list[dict]:
        deduped: list[dict] = []
        seen: set[tuple[str, str, str]] = set()

        for job in jobs:
            title = self._normalize_title(job.get("title", ""))
            if not self._is_valid_title_v60(title):
                continue

            source_url = str(job.get("source_url") or "").strip()
            if not source_url:
                source_url = page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]
            if self._is_non_job_url(source_url):
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

    def _extract_jobs2web_config_v66(self, html: str) -> dict[str, str | None]:
        cfg = dict(super()._extract_jobs2web_config_v66(html))
        cfg["sso_url"] = self._first_group(r"ssoUrl\s*:\s*['\"]([^'\"]+)['\"]", html)
        return cfg

    def _jobs2web_endpoint_candidates_v66(self, page_url: str, cfg: dict[str, str | None]) -> list[str]:
        candidates = list(super()._jobs2web_endpoint_candidates_v66(page_url, cfg))

        company_id = (cfg.get("company_id") or "").strip()
        locale = (cfg.get("locale") or "en_US").strip()
        encoded_company = quote_plus(company_id)
        encoded_locale = quote_plus(locale)

        parsed = urlparse(page_url or "")
        page_base = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""
        page_host = (parsed.netloc or "").lower()

        sso_url = (cfg.get("sso_url") or "").strip()
        sso_parsed = urlparse(sso_url)
        sso_base = f"{sso_parsed.scheme}://{sso_parsed.netloc}" if sso_parsed.scheme and sso_parsed.netloc else ""
        sso_host = (sso_parsed.netloc or "").lower()

        if company_id and sso_base:
            candidates.extend(
                [
                    f"{sso_base}/career?company={encoded_company}&career_ns=job_listing_summary&navBarLevel=JOB_SEARCH&locale={encoded_locale}",
                    f"{sso_base}/career?company={encoded_company}&career_ns=job_listing_summary&navBarLevel=JOB_SEARCH",
                    f"{sso_base}/career/jobsearch?company={encoded_company}&locale={encoded_locale}",
                ]
            )

        if page_base and "/search" in (parsed.path or "").lower():
            query = parse_qs(parsed.query or "", keep_blank_values=True)
            q = quote_plus((query.get("q") or [""])[0])
            loc = quote_plus((query.get("locationsearch") or [""])[0])
            candidates.append(f"{page_base}/search/?q={q}&locationsearch={loc}&searchResultView=LIST&locale={encoded_locale}")

        def _score(endpoint: str) -> int:
            p = urlparse(endpoint)
            host = (p.netloc or "").lower()
            path = (p.path or "").lower()
            low = endpoint.lower()

            score = 0
            if host == page_host and "/search/" in path:
                score += 230
            if sso_host and host == sso_host and "/career" in path:
                score += 130
            if "searchresultview=list" in low:
                score += 85
            if "/career/jobsearch" in path:
                score += 70
            if "company=" in low:
                score += 28
            if "pagenumber=" in low:
                score += 15
            if "locale=" in low:
                score += 8
            return score

        ranked = sorted(enumerate(candidates), key=lambda pair: (-_score(pair[1]), pair[0]))
        ordered: list[str] = []
        seen: set[str] = set()
        for _, endpoint in ranked:
            if not endpoint or endpoint in seen:
                continue
            seen.add(endpoint)
            ordered.append(endpoint)
        return ordered

    async def _extract_jobs2web_jobs_v66(self, page_url: str, html: str) -> list[dict]:
        jobs = await super()._extract_jobs2web_jobs_v66(page_url, html)
        if len(jobs) >= 3:
            return jobs

        cfg = self._extract_jobs2web_config_v66(html)
        if not (cfg.get("company_id") or "").strip():
            return jobs

        service_jobs = await self._probe_jobs2web_service_endpoints_v85(page_url, cfg)
        if len(service_jobs) >= 3:
            return self._dedupe_title_url_location_v84(service_jobs, limit=MAX_JOBS_PER_PAGE)
        return jobs

    async def _probe_jobs2web_service_endpoints_v85(
        self,
        page_url: str,
        cfg: dict[str, str | None],
    ) -> list[dict]:
        parsed = urlparse(page_url or "")
        if not parsed.scheme or not parsed.netloc:
            return []
        if (parsed.netloc or "").lower() in {"example.com", "localhost", "127.0.0.1"}:
            return []

        base = f"{parsed.scheme}://{parsed.netloc}"
        endpoints = [
            f"{base}/services/search/jobs",
            f"{base}/services/search/results",
            f"{base}/services/search/job",
        ]

        query = parse_qs(parsed.query or "", keep_blank_values=True)
        payload = {
            "q": (query.get("q") or [""])[0],
            "locationsearch": (query.get("locationsearch") or [""])[0],
            "pageNumber": (query.get("pageNumber") or ["0"])[0],
            "sortBy": (query.get("sortBy") or ["date"])[0],
            "facetFilters": (query.get("facetFilters") or ["{}"])[0],
            "locale": (cfg.get("locale") or "en_US"),
        }

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
        }
        if cfg.get("csrf"):
            headers["X-CSRF-Token"] = str(cfg["csrf"])

        async with httpx.AsyncClient(timeout=3.8, follow_redirects=True, headers=headers) as client:
            for endpoint in endpoints:
                for method in ("get", "post"):
                    try:
                        if method == "get":
                            resp = await client.get(endpoint, params=payload)
                        else:
                            resp = await client.post(endpoint, data=payload)
                    except Exception:
                        continue

                    if resp.status_code != 200:
                        continue

                    body = resp.text or ""
                    if len(body) < 40:
                        continue

                    parsed_payload = None
                    if "json" in (resp.headers.get("content-type") or "").lower() or body.lstrip().startswith(("{", "[")):
                        try:
                            parsed_payload = resp.json()
                        except Exception:
                            parsed_payload = None

                    extracted: list[dict] = []
                    if parsed_payload is not None:
                        extracted = self._extract_jobs_from_generic_json_v66(parsed_payload, endpoint, page_url)
                    if not extracted:
                        extracted = self._extract_jobs2web_dom_v66(body, str(resp.url))

                    extracted = self._dedupe_title_url_location_v84(extracted, limit=MAX_JOBS_PER_PAGE)
                    if len(extracted) >= 3:
                        return extracted

        return []
