"""
Tiered Extraction Engine v6.6 — timeout-safe static recovery and ATS coverage.

Builds on v6.5 with three targeted improvements:
1. Fast static high-volume extraction for dense server-rendered job lists
   (prevents timeout cascades from pagination/detail enrichment on large pages).
2. Dedicated WordPress wp-job-openings (AWSM) extractor.
3. SAP Jobs2Web app-shell fallback using on-page company/api hints.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional
from urllib.parse import urlparse, quote_plus

import httpx

from app.crawlers.tiered_extractor import _parse_html, _text, _resolve_url, MAX_JOBS_PER_PAGE
from app.crawlers.tiered_extractor_v65 import TieredExtractorV65, _FP_NAVIGATION_LABELS

logger = logging.getLogger(__name__)

_POSTED_TAIL = re.compile(r"\s+posted\s*:\s*.*$", re.IGNORECASE)
_EXPERIENCE_TAIL = re.compile(r"\s+\d+\s*[-–]\s*\d+\s*years?.*$", re.IGNORECASE)
_RANGE_TAIL = re.compile(r"\s+\d+\s*[-–]\s*\d+\b.*$", re.IGNORECASE)

_AWSM_ROLE_HINT = re.compile(
    r"\b(?:secretarial|secretary|assistant|officer|executive|manager|engineer|"
    r"developer|analyst|coordinator|specialist|technician|operator|driver|"
    r"cashier|clerk|supervisor)\b",
    re.IGNORECASE,
)

_JOBS2WEB_MARKER = re.compile(r"(?:xweb/rmk-jobs-search|j2w\.searchresultsunify)", re.IGNORECASE)


class TieredExtractorV66(TieredExtractorV65):
    """v6.6 extractor: fast static path + AWSM + Jobs2Web fallback."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        # Fast-path for dense static listing pages: avoid v1.6 pagination/detail fetch timeout chains.
        fast_jobs = self._extract_fast_static_jobs_v66(working_html, url)
        if fast_jobs:
            logger.info("v6.6 fast static path found %d jobs for %s", len(fast_jobs), url)
            return fast_jobs[:MAX_JOBS_PER_PAGE]

        # Dedicated wp-job-openings parser (common WordPress job plugin).
        awsm_jobs = self._extract_wp_job_openings_v66(working_html, url)
        if awsm_jobs:
            logger.info("v6.6 AWSM extraction found %d jobs for %s", len(awsm_jobs), url)
            return awsm_jobs[:MAX_JOBS_PER_PAGE]

        # Config-only SAP Jobs2Web shells have no server-rendered rows.
        if self._is_jobs2web_shell_v66(url, working_html):
            jobs2web_jobs = await self._extract_jobs2web_jobs_v66(url, working_html)
            if jobs2web_jobs:
                logger.info(
                    "v6.6 Jobs2Web fallback found %d jobs for %s",
                    len(jobs2web_jobs),
                    url,
                )
                return jobs2web_jobs[:MAX_JOBS_PER_PAGE]

        return await super().extract(career_page, company, html)

    def _normalize_title(self, title: str) -> str:
        t = super()._normalize_title(title)
        if not t:
            return t

        t = _POSTED_TAIL.sub("", t).strip()

        # Remove trailing experience/location tails commonly appended in card titles.
        m = _EXPERIENCE_TAIL.search(t)
        if m:
            prefix = t[: m.start()].strip()
            if len(prefix.split()) >= 2:
                t = prefix
        else:
            m = _RANGE_TAIL.search(t)
            if m:
                prefix = t[: m.start()].strip()
                if len(prefix.split()) >= 1:
                    t = prefix

        return t.strip()

    def _extract_fast_static_jobs_v66(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        # Gate tightly to avoid triggering on generic content pages.
        dense_rows = root.xpath(
            "//*[contains(@class,'list-group-item') and .//a[contains(@href,'/job/')]]"
        )
        if len(dense_rows) < 10:
            return []

        jobs = self._extract_tier2_v16(page_url, html) or []
        if len(jobs) < 10:
            return []

        strong_url_hits = 0
        for j in jobs:
            src = (j.get("source_url") or "").lower()
            if "/job/" in src or self._is_job_like_url(src):
                strong_url_hits += 1

        if strong_url_hits < max(6, int(len(jobs) * 0.6)):
            return []

        return self._dedupe_basic_v66(jobs)

    def _extract_wp_job_openings_v66(self, html: str, page_url: str) -> list[dict]:
        if "awsm-job" not in (html or "") and "wp-job-openings" not in (html or ""):
            return []

        root = _parse_html(html)
        if root is None:
            return []

        anchors = root.xpath("//a[contains(@class,'awsm-job-item')]")
        if not anchors:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()

        for a_el in anchors[:MAX_JOBS_PER_PAGE]:
            title_els = a_el.xpath(
                ".//*[contains(@class,'awsm-job-post-title')]|.//h1|.//h2|.//h3|.//h4"
            )
            raw_title = _text(title_els[0]) if title_els else _text(a_el)
            title = self._normalize_title(raw_title)
            if not self._is_valid_awsm_title_v66(title):
                continue

            href = (a_el.get("href") or "").strip()
            source_url = _resolve_url(href, page_url) if href else page_url
            source_path = urlparse(source_url).path.lower()
            if not self._is_job_like_url(source_url) and "/career/" not in source_path:
                continue

            if source_url in seen_urls:
                continue
            seen_urls.add(source_url)

            loc_els = a_el.xpath(
                ".//*[contains(@class,'awsm-job-specification-location')]"
                "|.//*[contains(@class,'awsm-job-specification-term')]"
            )
            location = None
            for loc_el in loc_els:
                loc_text = _text(loc_el).strip()
                if 2 <= len(loc_text) <= 120 and not re.search(r"more\s+details", loc_text, re.I):
                    location = loc_text
                    break

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "ats_wp_job_openings_v66",
                    "extraction_confidence": 0.86,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _is_valid_awsm_title_v66(self, title: str) -> bool:
        if self._is_valid_title_v60(title):
            return True

        t = (title or "").strip()
        if not t or _FP_NAVIGATION_LABELS.match(t):
            return False

        words = t.split()
        if len(words) > 6 or len(words) < 1:
            return False

        return bool(_AWSM_ROLE_HINT.search(t))

    @staticmethod
    def _is_jobs2web_shell_v66(url: str, html: str) -> bool:
        lower_html = (html or "")[:150000].lower()
        lower_url = (url or "").lower()

        if "jobs2web" in lower_url:
            return True

        if not _JOBS2WEB_MARKER.search(lower_html):
            return False

        return "companyid:" in lower_html or "ssocompanyid" in lower_html

    @staticmethod
    def _first_group(pattern: str, text: str) -> Optional[str]:
        m = re.search(pattern, text, re.IGNORECASE)
        if not m:
            return None
        v = (m.group(1) or "").strip()
        return v or None

    def _extract_jobs2web_config_v66(self, html: str) -> dict[str, Optional[str]]:
        return {
            "company_id": self._first_group(r"companyId\s*:\s*['\"]([^'\"]+)['\"]", html)
            or self._first_group(r"ssoCompanyId\s*:\s*['\"]([^'\"]+)['\"]", html),
            "api_url": self._first_group(r"apiURL\s*:\s*['\"]([^'\"]+)['\"]", html),
            "locale": self._first_group(r"currentLocale\s*:\s*['\"]([^'\"]+)['\"]", html)
            or self._first_group(r"locale\s*:\s*['\"]([^'\"]+)['\"]", html),
            "csrf": self._first_group(r"var\s+CSRFToken\s*=\s*['\"]([^'\"]+)['\"]", html),
            "referrer": self._first_group(r"referrer\s*:\s*['\"]([^'\"]+)['\"]", html),
        }

    def _jobs2web_endpoint_candidates_v66(self, page_url: str, cfg: dict[str, Optional[str]]) -> list[str]:
        company_id = (cfg.get("company_id") or "").strip()
        if not company_id:
            return []

        locale = (cfg.get("locale") or "en_GB").strip()
        api_url = (cfg.get("api_url") or "").rstrip("/")
        referrer = (cfg.get("referrer") or "").strip()
        encoded_company = quote_plus(company_id)
        encoded_locale = quote_plus(locale)

        candidates: list[str] = []

        if api_url:
            candidates.extend(
                [
                    f"{api_url}/career/jobsearch?company={encoded_company}&locale={encoded_locale}",
                    f"{api_url}/career/jobsearch?company={encoded_company}&career_ns=job_listing_summary&navBarLevel=JOB_SEARCH&locale={encoded_locale}",
                    f"{api_url}/career/jobreqcareersite?company={encoded_company}&locale={encoded_locale}",
                ]
            )

            api_host = urlparse(api_url).netloc.lower()
            dc = self._first_group(r"api(\d+)\.sapsf\.com", api_host)
            if dc:
                candidates.append(
                    f"https://career{dc}.sapsf.com/career?company={encoded_company}&career_ns=job_listing_summary&navBarLevel=JOB_SEARCH&locale={encoded_locale}"
                )

        if referrer and "." in referrer:
            candidates.append(
                f"https://{referrer}/career?company={encoded_company}&career_ns=job_listing_summary&navBarLevel=JOB_SEARCH&locale={encoded_locale}"
            )

        parsed = urlparse(page_url)
        page_base = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""
        if page_base:
            candidates.extend(
                [
                    f"{page_base}/career?company={encoded_company}&career_ns=job_listing_summary&navBarLevel=JOB_SEARCH&locale={encoded_locale}",
                    f"{page_base}/search/?q=&skillsSearch=false&locale={encoded_locale}",
                ]
            )

        deduped: list[str] = []
        seen: set[str] = set()
        for c in candidates:
            if c and c not in seen:
                seen.add(c)
                deduped.append(c)
        return deduped

    async def _extract_jobs2web_jobs_v66(self, page_url: str, html: str) -> list[dict]:
        cfg = self._extract_jobs2web_config_v66(html)
        company_id = cfg.get("company_id")
        if not company_id:
            return []

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/html;q=0.9,*/*;q=0.8",
        }
        if cfg.get("csrf"):
            headers["X-CSRF-Token"] = str(cfg["csrf"])

        endpoints = self._jobs2web_endpoint_candidates_v66(page_url, cfg)
        if not endpoints:
            return []

        async with httpx.AsyncClient(timeout=4, follow_redirects=True, headers=headers) as client:
            for endpoint in endpoints[:6]:
                try:
                    resp = await client.get(endpoint)
                except Exception:
                    continue

                if resp.status_code != 200:
                    continue

                body = resp.text or ""
                if len(body) < 80:
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
                    jobs = self._extract_jobs2web_dom_v66(body, page_url)

                jobs = self._dedupe_basic_v66(jobs)
                if len(jobs) >= 3:
                    return jobs

        return []

    def _extract_jobs2web_dom_v66(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()

        cards = root.xpath("//li[@data-testid='jobCard']")
        if cards:
            for card in cards[:MAX_JOBS_PER_PAGE]:
                links = card.xpath(".//a[contains(@class,'jobCardTitle') or @data-testid='jobCardTitle']")
                if not links:
                    continue
                title = self._normalize_title(_text(links[0]))
                if not self._is_valid_title_v60(title):
                    continue

                href = (links[0].get("href") or "").strip()
                source_url = _resolve_url(href, page_url) if href else page_url
                if source_url in seen_urls:
                    continue
                seen_urls.add(source_url)

                loc_nodes = card.xpath(".//*[@data-testid='jobCardLocation']")
                location = _text(loc_nodes[0]).strip() if loc_nodes else None

                jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": location,
                        "description": None,
                        "salary_raw": None,
                        "employment_type": None,
                        "extraction_method": "ats_jobs2web_dom_v66",
                        "extraction_confidence": 0.83,
                    }
                )

        return jobs

    def _extract_jobs_from_generic_json_v66(
        self,
        payload: Any,
        source_url: str,
        page_url: str,
    ) -> list[dict]:
        jobs: list[dict] = []
        seen_urls: set[str] = set()

        stack = [payload]
        while stack and len(jobs) < MAX_JOBS_PER_PAGE:
            node = stack.pop()
            if isinstance(node, dict):
                for v in node.values():
                    if isinstance(v, (dict, list)):
                        stack.append(v)

                title = self._pick_first_v66(
                    node,
                    ["title", "jobTitle", "postingTitle", "jobReqTitle", "job_title", "positionTitle"],
                )
                raw_url = self._pick_first_v66(
                    node,
                    ["url", "jobUrl", "jobURL", "applyUrl", "externalPath", "jobPath", "detailUrl"],
                )

                if not title or not raw_url:
                    continue

                clean_title = self._normalize_title(title)
                if not self._is_valid_title_v60(clean_title):
                    continue

                resolved_url = _resolve_url(raw_url, source_url) or _resolve_url(raw_url, page_url)
                if not resolved_url:
                    continue
                if not self._is_job_like_url(resolved_url) and "/job" not in resolved_url.lower():
                    continue
                if resolved_url in seen_urls:
                    continue
                seen_urls.add(resolved_url)

                location = self._pick_first_v66(node, ["location", "city", "jobLocation", "country", "region"])
                description = self._pick_first_v66(node, ["description", "summary", "jobDescription"])

                jobs.append(
                    {
                        "title": clean_title,
                        "source_url": resolved_url,
                        "location_raw": location,
                        "description": description[:5000] if isinstance(description, str) else None,
                        "salary_raw": None,
                        "employment_type": self._pick_first_v66(node, ["employmentType", "jobType"]),
                        "extraction_method": "ats_jobs2web_api_v66",
                        "extraction_confidence": 0.85,
                    }
                )

            elif isinstance(node, list):
                for item in node:
                    if isinstance(item, (dict, list)):
                        stack.append(item)

        return jobs

    @staticmethod
    def _pick_first_v66(node: dict[str, Any], keys: list[str]) -> Optional[str]:
        for k in keys:
            v = node.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None

    @staticmethod
    def _dedupe_basic_v66(jobs: list[dict]) -> list[dict]:
        out: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for job in jobs:
            title = str(job.get("title") or "").strip()
            src = str(job.get("source_url") or "").strip()
            key = (title.lower(), src.lower())
            if not title or key in seen:
                continue
            seen.add(key)
            out.append(job)
            if len(out) >= MAX_JOBS_PER_PAGE:
                break
        return out
