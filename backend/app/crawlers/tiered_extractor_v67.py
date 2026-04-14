"""
Tiered Extraction Engine v6.7 — resilient card extraction and pagination recovery.

Builds on v6.6 with two broad improvements:
1. Linked-card extraction for pages where job titles live in inner heading/paragraph
   nodes and anchors carry long mixed content (prevents nav-label fallbacks).
2. Bounded pagination follow-up for linked-card listings to recover page-2+ rows
   without unbounded crawling.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional
from urllib.parse import urlparse, quote_plus, urljoin

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

_CARD_LOCATION_HINT = re.compile(r"(?:\blocation\b|\bcity\b|\bregion\b|\boffice\b)", re.IGNORECASE)
_CARD_JOB_PATH_HINT = re.compile(r"/(?:job|jobs)/", re.IGNORECASE)
_CARD_PAGINATION_HINT = re.compile(r"(?:/page/\d+|[?&](?:page|paged)=\d+)", re.IGNORECASE)
_GENERIC_NON_JOB_TITLE_V67 = re.compile(
    r"^(?:job\s*board|how\s+it\s+works|view\s+all|learn\s+more|read\s+more|search)$",
    re.IGNORECASE,
)
_WEAK_ROLE_HINT_V67 = re.compile(
    r"\b(?:operations?|specialist|assistant|advisor|coordinator|technician|"
    r"engineer|analyst|manager|officer|developer|consultant|executive|administrator)\b",
    re.IGNORECASE,
)


class TieredExtractorV67(TieredExtractorV65):
    """v6.7 extractor: v6.6 features + linked-card and pagination recovery."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        # Fast-path for dense static listing pages: avoid v1.6 pagination/detail fetch timeout chains.
        fast_jobs = self._extract_fast_static_jobs_v66(working_html, url)
        if fast_jobs:
            logger.info("v6.7 fast static path found %d jobs for %s", len(fast_jobs), url)
            return fast_jobs[:MAX_JOBS_PER_PAGE]

        # Linked-card listings often keep titles in inner heading/paragraph nodes.
        card_jobs = self._extract_linked_job_cards_v67(working_html, url)
        if card_jobs and len(card_jobs) < MAX_JOBS_PER_PAGE:
            card_jobs = await self._expand_linked_job_card_pages_v67(
                page_url=url,
                seed_html=working_html,
                seed_jobs=card_jobs,
            )
        if card_jobs and len(card_jobs) >= 3 and self._passes_jobset_validation(card_jobs, url):
            logger.info("v6.7 linked-card extraction found %d jobs for %s", len(card_jobs), url)
            return card_jobs[:MAX_JOBS_PER_PAGE]

        # Dedicated wp-job-openings parser (common WordPress job plugin).
        awsm_jobs = self._extract_wp_job_openings_v66(working_html, url)
        if awsm_jobs:
            logger.info("v6.7 AWSM extraction found %d jobs for %s", len(awsm_jobs), url)
            return awsm_jobs[:MAX_JOBS_PER_PAGE]

        # Config-only SAP Jobs2Web shells have no server-rendered rows.
        if self._is_jobs2web_shell_v66(url, working_html):
            jobs2web_jobs = await self._extract_jobs2web_jobs_v66(url, working_html)
            if jobs2web_jobs:
                logger.info(
                    "v6.7 Jobs2Web fallback found %d jobs for %s",
                    len(jobs2web_jobs),
                    url,
                )
                return jobs2web_jobs[:MAX_JOBS_PER_PAGE]

        return await super().extract(career_page, company, html)

    def _is_valid_title_v60(self, title: str) -> bool:
        if not super()._is_valid_title_v60(title):
            return False
        return not _GENERIC_NON_JOB_TITLE_V67.match((title or "").strip())

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

    def _extract_linked_job_cards_v67(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        anchors = root.xpath("//a[@href and not(starts-with(@href,'#')) and not(starts-with(@href,'javascript:'))]")
        if not anchors:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()

        for a_el in anchors[:800]:
            href = (a_el.get("href") or "").strip()
            if not href:
                continue
            source_url = _resolve_url(href, page_url) or page_url
            if source_url in seen_urls:
                continue
            has_strong_job_path = bool(_CARD_JOB_PATH_HINT.search(source_url))
            if not has_strong_job_path and not self._is_job_like_url(source_url):
                continue

            title = self._extract_card_title_v67(a_el)
            if not title:
                continue
            if not self._is_valid_card_title_v67(title, has_strong_job_path):
                continue

            seen_urls.add(source_url)
            context_text = " ".join((_text(a_el) or "").split())
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_card_location_v67(a_el, title),
                    "description": context_text[:5000] if len(context_text) > 80 else None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "tier2_linked_cards_v67",
                    "extraction_confidence": 0.8,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _is_valid_card_title_v67(self, title: str, has_strong_job_path: bool) -> bool:
        if self._is_valid_title_v60(title):
            return True
        if not has_strong_job_path:
            return False
        t = (title or "").strip()
        if not t or _GENERIC_NON_JOB_TITLE_V67.match(t):
            return False
        if len(t.split()) > 4:
            return False
        return bool(_WEAK_ROLE_HINT_V67.search(t))

    def _extract_card_title_v67(self, a_el) -> Optional[str]:
        title_nodes = a_el.xpath(
            ".//h1|.//h2|.//h3|.//h4|"
            ".//p[contains(@class,'text-2xl') or contains(@class,'text-3xl') "
            "or contains(@class,'text-4xl') or contains(@class,'text-5xl') "
            "or contains(@class,'text-6xl') or contains(@class,'text-7xl')]|"
            ".//*[contains(@class,'job-title') or contains(@class,'position-title') "
            "or contains(@class,'role-title')]|"
            ".//*[contains(@class,'title')]"
        )
        for node in title_nodes[:5]:
            raw = _text(node)
            if not raw:
                continue
            title = self._normalize_title(raw)
            if len(title) > 140:
                continue
            if _GENERIC_NON_JOB_TITLE_V67.match(title):
                continue
            if _CARD_LOCATION_HINT.search((node.get("class") or "")):
                continue
            return title

        # Fallback for minimal cards with a single-line anchor title.
        title = self._normalize_title(_text(a_el))
        if not title or len(title) > 90:
            return None
        if _GENERIC_NON_JOB_TITLE_V67.match(title):
            return None
        return title

    def _extract_card_location_v67(self, a_el, title: str) -> Optional[str]:
        # Prefer explicit location-labelled nodes.
        loc_nodes = a_el.xpath(
            ".//*[contains(@class,'location') or contains(@class,'city') or contains(@class,'region') "
            "or contains(@class,'office')]"
        )
        for node in loc_nodes[:3]:
            txt = " ".join((_text(node) or "").split())
            if 2 <= len(txt) <= 120 and txt.lower() != title.lower():
                return txt

        # Fallback: first short paragraph that is not CTA/title text.
        p_nodes = a_el.xpath(".//p")
        for node in p_nodes[:4]:
            txt = " ".join((_text(node) or "").split())
            if not txt or txt.lower() == title.lower():
                continue
            if len(txt) > 120:
                continue
            if re.search(r"\b(?:apply|learn more|read more|view details|search)\b", txt, re.IGNORECASE):
                continue
            if re.search(r"[A-Za-z]", txt):
                return txt
        return None

    def _pagination_urls_v67(self, html: str, page_url: str) -> list[str]:
        root = _parse_html(html)
        if root is None:
            return []

        page_nav_links = root.xpath(
            "//nav[contains(translate(@aria-label,'PAGINATION','pagination'),'pagination') "
            "or contains(@class,'pagination')]//a[@href]|//a[@rel='next' and @href]"
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

    async def _expand_linked_job_card_pages_v67(
        self,
        page_url: str,
        seed_html: str,
        seed_jobs: list[dict],
    ) -> list[dict]:
        if not seed_jobs:
            return []
        if len(seed_jobs) >= 40:
            return self._dedupe_basic_v66(seed_jobs)

        next_urls = self._pagination_urls_v67(seed_html, page_url)
        if not next_urls:
            return self._dedupe_basic_v66(seed_jobs)

        merged = list(seed_jobs)
        async with httpx.AsyncClient(
            timeout=4.5,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"},
        ) as client:
            for next_url in next_urls:
                try:
                    resp = await client.get(next_url)
                except Exception:
                    continue
                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 200:
                    continue
                merged.extend(self._extract_linked_job_cards_v67(body, str(resp.url)))

        return self._dedupe_basic_v66(merged)

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
