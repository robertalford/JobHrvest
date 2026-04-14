"""
Tiered Extraction Engine v6.8 — ATS-first precision fixes for Jobvite and Jobs2Web.

Builds on v6.7 with three targeted improvements:
1. Dedicated Jobvite table extractor to prevent seeker-tools CTA false positives
   and improve multilingual title coverage on jv-job-list boards.
2. Broader Jobs2Web shell recovery using additional ssoUrl/ssoCompanyId config
   parsing and expanded endpoint candidates.
3. Pagination URL detection expanded to include common WordPress-style
   `div.pagination` blocks (Older Entries / page/N links).
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional
from urllib.parse import quote_plus, urljoin, urlparse

from app.crawlers.tiered_extractor import _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v67 import TieredExtractorV67, _CARD_PAGINATION_HINT

logger = logging.getLogger(__name__)

_NON_JOB_CTA_TITLE_V68 = re.compile(
    r"^(?:"
    r"sign\s+up\s+for\s+job\s+alerts?\.?|"
    r"submit\s+(?:a\s+)?general\s+application\.?|"
    r"check\s+on\s+an\s+application(?:\s+you(?:'?)ve\s+submitted)?\.?|"
    r"visit\s+our\s+(?:linkedin|youtube|facebook|instagram|twitter).*$"
    r")$",
    re.IGNORECASE,
)

_JOBVITE_HOST_HINT = re.compile(r"(?:^|\.)jobvite\.com$", re.IGNORECASE)
_JOBVITE_DETAIL_URL = re.compile(r"/job/[A-Za-z0-9_-]+", re.IGNORECASE)
_JOBVITE_NON_JOB_TITLE = re.compile(
    r"^(?:"
    r"job\s+seeker\s+tools?|"
    r"sign\s+up\s+for\s+job\s+alerts?\.?|"
    r"submit\s+(?:a\s+)?general\s+application\.?|"
    r"check\s+on\s+an\s+application.*|"
    r"visit\s+our\s+(?:linkedin|youtube|facebook|instagram|twitter).*$"
    r")",
    re.IGNORECASE,
)


class TieredExtractorV68(TieredExtractorV67):
    """v6.8 extractor: ATS-focused precision for Jobvite + Jobs2Web."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        # ATS-first guard: avoid linked-card fallback on Jobvite seeker-tools links.
        if self._is_jobvite_page_v68(url, working_html):
            jobvite_jobs = self._extract_jobvite_jobs_v68(working_html, url)
            if len(jobvite_jobs) >= 3:
                logger.info("v6.8 Jobvite extraction found %d jobs for %s", len(jobvite_jobs), url)
                return jobvite_jobs

        return await super().extract(career_page, company, html)

    def _is_valid_title_v60(self, title: str) -> bool:
        if not super()._is_valid_title_v60(title):
            return False
        return not _NON_JOB_CTA_TITLE_V68.match((title or "").strip())

    @staticmethod
    def _is_jobvite_page_v68(page_url: str, html: str) -> bool:
        host = (urlparse(page_url).netloc or "").lower()
        if _JOBVITE_HOST_HINT.search(host):
            return True
        lower_html = (html or "")[:150000].lower()
        return "jv-job-list" in lower_html and "jobvite" in lower_html

    @staticmethod
    def _is_jobvite_detail_url_v68(source_url: str) -> bool:
        lower = (source_url or "").lower()
        if not _JOBVITE_DETAIL_URL.search(lower):
            return False
        if "/jobalerts" in lower or lower.endswith("/apply") or "/apply?" in lower:
            return False
        return True

    def _is_valid_jobvite_title_v68(self, title: str) -> bool:
        t = (title or "").strip()
        if not t:
            return False
        if _JOBVITE_NON_JOB_TITLE.match(t):
            return False

        # Keep strict global validation when possible.
        if self._is_valid_title_v60(t):
            return True

        # Jobvite table rows are high-confidence ATS evidence; allow multilingual
        # role titles that fail noun dictionaries as long as they are compact.
        words = t.split()
        if len(words) > 16:
            return False
        return any(ch.isalpha() for ch in t)

    def _extract_jobvite_jobs_v68(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath("//table[contains(@class,'jv-job-list')]//tr[td[contains(@class,'jv-job-list-name')]]")
        if not rows:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()

        for row in rows:
            links = row.xpath(".//td[contains(@class,'jv-job-list-name')]//a[@href]")
            if not links:
                continue

            link = links[0]
            href = (link.get("href") or "").strip()
            if not href:
                continue
            source_url = _resolve_url(href, page_url) or page_url
            if source_url in seen_urls:
                continue
            if not self._is_jobvite_detail_url_v68(source_url):
                continue

            raw_title = (link.get("title") or "").strip() or _text(link)
            title = self._normalize_title(raw_title)
            if not self._is_valid_jobvite_title_v68(title):
                continue

            location = None
            loc_cells = row.xpath(".//td[contains(@class,'jv-job-list-location')]")
            if loc_cells:
                loc_text = " ".join((_text(loc_cells[0]) or "").split())
                if 2 <= len(loc_text) <= 180:
                    location = loc_text

            seen_urls.add(source_url)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "ats_jobvite_table_v68",
                    "extraction_confidence": 0.9,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _pagination_urls_v67(self, html: str, page_url: str) -> list[str]:
        root = _parse_html(html)
        if root is None:
            return []

        page_nav_links = root.xpath(
            "//nav[contains(translate(@aria-label,'PAGINATION','pagination'),'pagination') "
            "or contains(@class,'pagination')]//a[@href]"
            "|//a[@rel='next' and @href]"
            "|//div[contains(@class,'pagination') or contains(@class,'pager') or contains(@class,'nav-links')]//a[@href]"
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
            if not _CARD_PAGINATION_HINT.search(full_url):
                continue
            candidates.append(full_url)

        deduped: list[str] = []
        seen: set[str] = set()
        for url in candidates:
            if url in seen:
                continue
            seen.add(url)
            deduped.append(url)
            if len(deduped) >= 2:
                break
        return deduped

    def _extract_jobs2web_config_v66(self, html: str) -> dict[str, Optional[str]]:
        cfg = super()._extract_jobs2web_config_v66(html)

        sso_company_id = self._first_group(r"['\"]?ssoCompanyId['\"]?\s*:\s*['\"]([^'\"]+)['\"]", html)
        sso_url = self._first_group(r"['\"]?ssoUrl['\"]?\s*:\s*['\"]([^'\"]+)['\"]", html)

        cfg["sso_company_id"] = sso_company_id
        cfg["sso_url"] = sso_url

        if not cfg.get("company_id") and sso_company_id:
            cfg["company_id"] = sso_company_id

        return cfg

    def _jobs2web_endpoint_candidates_v66(self, page_url: str, cfg: dict[str, Optional[str]]) -> list[str]:
        candidates = list(super()._jobs2web_endpoint_candidates_v66(page_url, cfg))

        company_id = (cfg.get("company_id") or cfg.get("sso_company_id") or "").strip()
        if not company_id:
            return candidates

        locale = (cfg.get("locale") or "en_GB").strip()
        encoded_company = quote_plus(company_id)
        encoded_locale = quote_plus(locale)

        sso_url = (cfg.get("sso_url") or "").rstrip("/")
        if sso_url:
            candidates.extend(
                [
                    f"{sso_url}/career?company={encoded_company}&career_ns=job_listing_summary&navBarLevel=JOB_SEARCH&locale={encoded_locale}",
                    f"{sso_url}/career?company={encoded_company}&locale={encoded_locale}",
                    f"{sso_url}/career?company={encoded_company}",
                    f"{sso_url}/search/?q=&skillsSearch=false&locale={encoded_locale}",
                    f"{sso_url}/search/?q=&skillsSearch=false",
                ]
            )

        parsed = urlparse(page_url)
        page_base = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""
        if page_base:
            candidates.extend(
                [
                    f"{page_base}/career?company={encoded_company}&career_ns=job_listing_summary&navBarLevel=JOB_SEARCH&locale={encoded_locale}",
                    f"{page_base}/career?company={encoded_company}&locale={encoded_locale}",
                    f"{page_base}/search/?q=&skillsSearch=false",
                ]
            )

        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            deduped.append(candidate)
        return deduped

    def _extract_jobs2web_dom_v66(self, html: str, page_url: str) -> list[dict]:
        jobs = super()._extract_jobs2web_dom_v66(html, page_url)
        if jobs:
            return jobs

        root = _parse_html(html)
        if root is None:
            return []

        jobs = []
        seen_urls: set[str] = set()

        links = root.xpath(
            "//a[contains(@href,'/job/') and @href and "
            "not(contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'jobalerts')) and "
            "not(contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'/apply'))]"
        )

        for link in links[:500]:
            href = (link.get("href") or "").strip()
            source_url = _resolve_url(href, page_url) if href else None
            if not source_url or source_url in seen_urls:
                continue

            raw_title = (link.get("title") or "").strip() or _text(link)
            title = self._normalize_title(raw_title)
            if not self._is_valid_title_v60(title):
                continue

            location = None
            loc_nodes = link.xpath(
                "./ancestor::*[self::li or self::article or self::div][1]"
                "//*[contains(@data-testid,'jobCardLocation') or contains(@class,'jobCardLocation') or contains(@class,'location')]"
            )
            for node in loc_nodes[:3]:
                loc_txt = " ".join((_text(node) or "").split())
                if 2 <= len(loc_txt) <= 140 and loc_txt.lower() != title.lower():
                    location = loc_txt
                    break

            seen_urls.add(source_url)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "ats_jobs2web_dom_v68",
                    "extraction_confidence": 0.84,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _extract_jobs_from_generic_json_v66(
        self,
        payload: Any,
        source_url: str,
        page_url: str,
    ) -> list[dict]:
        jobs = super()._extract_jobs_from_generic_json_v66(payload, source_url, page_url)
        if len(jobs) >= 3:
            return jobs

        extra_jobs: list[dict] = []
        seen_urls: set[str] = {str(j.get("source_url") or "").lower() for j in jobs}

        stack: list[Any] = [payload]
        while stack and len(extra_jobs) < 500:
            node = stack.pop()
            if isinstance(node, dict):
                for v in node.values():
                    if isinstance(v, (dict, list)):
                        stack.append(v)

                title = self._pick_first_v66(
                    node,
                    [
                        "title",
                        "jobTitle",
                        "postingTitle",
                        "jobReqTitle",
                        "job_title",
                        "positionTitle",
                        "jobPostingTitle",
                        "titleText",
                    ],
                )
                raw_url = self._pick_first_v66(
                    node,
                    [
                        "url",
                        "jobUrl",
                        "jobURL",
                        "applyUrl",
                        "externalPath",
                        "jobPath",
                        "detailUrl",
                        "targetSearchUrl",
                        "jobDetailsUrl",
                        "jobDetailUrl",
                    ],
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

                key = resolved_url.lower()
                if key in seen_urls:
                    continue
                seen_urls.add(key)

                extra_jobs.append(
                    {
                        "title": clean_title,
                        "source_url": resolved_url,
                        "location_raw": self._pick_first_v66(node, ["location", "city", "jobLocation", "country", "region"]),
                        "description": None,
                        "salary_raw": None,
                        "employment_type": self._pick_first_v66(node, ["employmentType", "jobType"]),
                        "extraction_method": "ats_jobs2web_api_v68",
                        "extraction_confidence": 0.84,
                    }
                )

            elif isinstance(node, list):
                for item in node:
                    if isinstance(item, (dict, list)):
                        stack.append(item)

        if extra_jobs:
            jobs.extend(extra_jobs)

        return self._dedupe_basic_v66(jobs)
