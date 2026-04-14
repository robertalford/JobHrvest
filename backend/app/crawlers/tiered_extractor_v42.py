"""
Tiered Extraction Engine v4.2 — direct from v1.6.

High-impact improvements:
1. Adds AWSM WordPress jobs extractor (`.awsm-job-listing-item`) for structured plugin layouts.
2. Recalibrates small-set validation for high-evidence detail-URL jobsets to reduce false negatives.
3. Adds martian/Next.js fast-pass endpoint probing with higher request budget for app-shell boards.
4. Suppresses bare department labels in heading extraction (e.g., "Marketing", "Sales").
5. Preserves existing Greenhouse/Oracle/API recovery paths from v4.2.
"""

from __future__ import annotations

import asyncio
import html as html_lib
import json
import logging
import re
from collections import defaultdict
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse

import httpx
from lxml import etree

from app.crawlers.tiered_extractor_v16 import TieredExtractorV16, _title_has_job_noun
from app.crawlers.tiered_extractor import (
    _AU_LOCATIONS,
    _JOB_TYPE_PATTERN,
    _SALARY_PATTERN,
    _get_el_classes,
    _parse_html,
    _resolve_url,
    _text,
    MAX_JOBS_PER_PAGE,
    MIN_JOBS_FOR_SUCCESS,
)

logger = logging.getLogger(__name__)


_ROLE_HINT_PATTERN_V38 = re.compile(
    r"\b(?:engineer|developer|manager|director|analyst|specialist|assistant|"
    r"consultant|coordinator|officer|administrator|accountant|technician|owner|"
    r"designer|architect|operator|supervisor|advisor|executive|intern(?:ship)?|"
    r"recruit(?:er|ment)?|nurse|teacher|driver|chef|chemist|mechanic|clerk|"
    r"associate|representative|agent|planner|crew|yardman|warehouse|"
    r"akuntan|konsultan|asisten|staf|staff|pegawai|karyawan|influencer|"
    r"videografer|fotografer|psikolog(?:i)?|model|sarjana|fashion)\b",
    re.IGNORECASE,
)

_TITLE_REJECT_PATTERN_V38 = re.compile(
    r"^(?:our\s+leaders?|our\s+ecosystem|our\s+values?|talent\s+stories?|"
    r"franchise\s+institute|skim\s+pembiayaan\s+francaisor|sewaan\s+premis|"
    r"join\s+our\s+team|current\s+jobs?|all\s+jobs?|job\s+openings?|"
    r"search\s+jobs?|browse\s+jobs?|view\s+all\s+jobs?|"
    r"careers?|about\s+us|our\s+culture|our\s+direction|contact|home|menu|"
    r"lowongan(?:\s+kerja)?|job\s+details?|role\s+details?|internship\s+details?|"
    r"current\s+openings?|our\s+people|our\s+team|office|alamat(?:\s+kantor)?|"
    r"read\s+more|learn\s+more|show\s+more|load\s+more|info\s+lengkap|"
    r"get\s+started|sign\s+up\s+for\s+alerts?|marketing|sales|customer\s+service|"
    r"finance|operations?|human\s+resources|hr)$",
    re.IGNORECASE,
)

_DEPARTMENT_LABEL_PATTERN_V42 = re.compile(
    r"^(?:marketing|sales|customer\s+service|finance|operations?|human\s+resources|hr|it)$",
    re.IGNORECASE,
)

_LISTING_LINK_TEXT_PATTERN_V38 = re.compile(
    r"\b(?:job\s+openings?|current\s+vacancies|join\s+our\s+team|"
    r"view\s+all\s+jobs?|search\s+jobs|browse\s+jobs|lowongan|kerjaya|"
    r"karir|loker|careers?)\b",
    re.IGNORECASE,
)

_LISTING_URL_PATTERN_V38 = re.compile(
    r"/(?:career|careers|jobs?|job-openings?|vacanc|opening|openings|position|"
    r"requisition|portal\.na|candidateportal|join-our-team|current-vacancies|"
    r"lowongan|loker|kerjaya|karir)",
    re.IGNORECASE,
)

_DETAILISH_URL_PATTERN_V38 = re.compile(
    r"(?:/jobs?/[A-Za-z0-9][^/?#]{3,}|/career/openings?/|/jobdetails(?:/|$|\?)|"
    r"PortalDetail\.na\?.*jobid=|/join-our-team/[A-Za-z0-9]{6,}|"
    r"[?&](?:jobid|job_id|requisitionid|positionid|vacancyid|jobadid|adid|ajid)=)",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V38 = re.compile(
    r"(?:/(?:privacy|terms|cookie|news|blog|about|contact|investor|"
    r"team|culture|our-culture|our-values|our-ecosystem|our-direction|"
    r"talent-story|services?|franchise|login|logout|register|account|help|"
    r"address|alamat|kantor|locations?|people|leadership)(?:/|$|[?#])|"
    r"/fRecruit__Apply(?:Register|ExpressInterest)|wp-json|/feed(?:/|$)|/rss(?:/|$)|"
    r"\.pdf(?:$|\?)|\.docx?(?:$|\?))",
    re.IGNORECASE,
)

_APPLY_EVIDENCE_PATTERN_V38 = re.compile(
    r"(?:apply|application|apply\s+now|apply\s+here|mailto:|"
    r"requirements?|qualifications?|responsibilit|closing\s+date|"
    r"full\s*time|part\s*time|contract|permanent|temporary|"
    r"how\s+to\s+apply|cara\s+melamar)",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN_V38 = re.compile(
    r"(?:job|position|vacanc|opening|requisition|career|posting|listing|accordion)",
    re.IGNORECASE,
)

_ACRONYM_TITLE_PATTERN_V38 = re.compile(r"^[A-Z][A-Z0-9&/\-\+]{1,10}$")
_ORACLE_SITE_PATTERN_V39 = re.compile(r"/sites/([A-Za-z0-9_]+)/", re.IGNORECASE)
_ORACLE_SITE_NUMBER_PATTERN_V39 = re.compile(r"siteNumber\s*[:=]\s*['\"]([A-Za-z0-9_]+)", re.IGNORECASE)
_ORACLE_API_BASE_PATTERN_V39 = re.compile(r"apiBaseUrl\s*:\s*['\"](https?://[^'\"/]+(?::\d+)?)", re.IGNORECASE)
_NEXT_DATA_PATTERN_V39 = re.compile(
    r"<script[^>]+id=['\"]__NEXT_DATA__['\"][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
_MARTIAN_HOST_HINT_PATTERN_V40 = re.compile(
    r"https?://[^\"'\s]+(?:martianlogic|myrecruitmentplus)[^\"'\s]*",
    re.IGNORECASE,
)
_MARTIAN_ENDPOINT_HINT_PATTERN_V40 = re.compile(
    r"https?://[^\"'\s]+(?:api/(?:jobs?|jobads?|job-ads|job-search|search/jobs)|"
    r"_next/data/[^\"'\s]+\.json|embed-jobs|job-ads|jobads|jobs/search)[^\"'\s]*",
    re.IGNORECASE,
)
_MARTIAN_REL_ENDPOINT_HINT_PATTERN_V40 = re.compile(
    r"(?:/api/(?:jobs?|jobads?|job-ads|job-search|search/jobs)[^\"'\s<)]*|"
    r"/_next/data/[^\"'\s]+\.json)",
    re.IGNORECASE,
)
_GENERIC_LISTING_PATH_PATTERN_V41 = re.compile(
    r"/(?:careers?|jobs?|job-openings?|openings?|vacancies?|lowongan|loker|join-our-team)(?:/)?$",
    re.IGNORECASE,
)
_GREENHOUSE_TITLE_PATTERN_V41 = re.compile(r"jobs?\s+at\s+([A-Za-z0-9& .,'/-]{2,80})", re.IGNORECASE)


class TieredExtractorV42(TieredExtractorV16):
    """v4.2 extractor with stronger small-set recovery and app-shell API probing."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        company_name = getattr(company, "name", "") if company is not None else ""
        if not company_name and isinstance(company, str):
            company_name = company
        working_html = html or ""

        recovered_html = await self._recover_short_html_v38(page_url, working_html)
        if recovered_html:
            working_html = recovered_html

        candidates: list[tuple[str, list[dict]]] = []

        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(super().extract(career_page, company, working_html), timeout=24.0)
        except asyncio.TimeoutError:
            logger.warning("v4.2 parent extractor timeout for %s", page_url)
        except Exception:
            logger.exception("v4.2 parent extractor failed for %s", page_url)

        parent_jobs = self._prepare_candidate_jobs_v38(parent_jobs or [], page_url)
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        local_jobs = self._extract_from_single_page_v38(page_url, working_html)
        if local_jobs:
            candidates.append(("local_v38", local_jobs))

        state_jobs = self._extract_state_jobs_v41(working_html, page_url)
        if state_jobs:
            candidates.append(("state_json_v41", state_jobs))

        root = _parse_html(working_html)
        if root is not None:
            salesforce_rows = self._extract_salesforce_rows_v39(root, page_url)
            if salesforce_rows:
                candidates.append(("salesforce_rows_v39", salesforce_rows))

        best_label, best_jobs, best_score = self._pick_best_candidate_v38(candidates, page_url)

        need_structured_fallbacks = (
            not best_jobs
            or len(best_jobs) < MIN_JOBS_FOR_SUCCESS
            or best_score < 11.0
            or self._looks_like_martian_shell_v39(working_html)
            or self._looks_like_oracle_shell_v39(page_url, working_html)
        )
        if need_structured_fallbacks:
            oracle_jobs = await self._extract_oracle_jobs_v39(page_url, working_html)
            if oracle_jobs:
                candidates.append(("oracle_api_v39", oracle_jobs))

            martian_jobs = await self._extract_martian_jobs_v39(page_url, working_html)
            if martian_jobs:
                candidates.append(("martian_api_v39", martian_jobs))

            best_label, best_jobs, best_score = self._pick_best_candidate_v38(candidates, page_url)

        greenhouse_jobs = await self._recover_greenhouse_jobs_v41(page_url, company_name, working_html, best_jobs)
        if greenhouse_jobs:
            candidates.append(("greenhouse_recovery_v41", greenhouse_jobs))
            best_label, best_jobs, best_score = self._pick_best_candidate_v38(candidates, page_url)

        need_subpage_follow = self._should_follow_subpages_v41(page_url, working_html, best_jobs, best_score)
        if need_subpage_follow:
            subpage_urls = self._collect_listing_subpages_v38(page_url, working_html)
            fetched = 0
            for subpage_url in subpage_urls[:12]:
                if fetched >= 6:
                    break
                sub_html = await self._fetch_html_v38(subpage_url)
                if not sub_html or len(sub_html) < 200:
                    continue
                fetched += 1
                sub_jobs = self._extract_from_single_page_v38(subpage_url, sub_html)
                if sub_jobs:
                    candidates.append((f"subpage_v38:{fetched}", sub_jobs))

                state_sub_jobs = self._extract_state_jobs_v41(sub_html, subpage_url)
                if state_sub_jobs:
                    candidates.append((f"subpage_state_v41:{fetched}", state_sub_jobs))

            best_label, best_jobs, best_score = self._pick_best_candidate_v38(candidates, page_url)

        if not best_jobs:
            return []

        if best_label != "parent_v16" and any(
            self._is_job_like_url_v38(j.get("source_url") or "")
            and (j.get("source_url") or page_url).rstrip("/") != page_url.rstrip("/")
            for j in best_jobs
        ):
            try:
                best_jobs = await asyncio.wait_for(self._enrich_from_detail_pages(best_jobs), timeout=16.0)
            except asyncio.TimeoutError:
                logger.warning("v4.2 enrichment timeout for %s", page_url)
            except Exception:
                logger.exception("v4.2 enrichment failed for %s", page_url)

        final_jobs = self._prepare_candidate_jobs_v38(best_jobs, page_url)
        return final_jobs[:MAX_JOBS_PER_PAGE]

    async def _recover_short_html_v38(self, page_url: str, html_body: str) -> Optional[str]:
        body = html_body or ""
        short_or_failed = len(body) < 300 or "FETCH FAILED" in body[:120]

        probe_urls: list[str] = []
        parsed = urlparse(page_url)
        if parsed.scheme and parsed.netloc:
            base = f"{parsed.scheme}://{parsed.netloc}"
            probe_urls.extend([
                page_url,
                page_url.rstrip("/") + "/",
            ])
            if short_or_failed or parsed.path.rstrip("/") in {"", "/career", "/careers", "/jobs"}:
                probe_urls.extend(
                    [
                        base + "/careers",
                        base + "/careers/",
                        base + "/careers/fRecruit__ApplyJobList?portal=English",
                        base + "/career/job-openings",
                        base + "/careers/join-our-team",
                        base + "/recruit/Portal.na",
                        base + "/ms/kerjaya",
                        base + "/lowongan",
                        base + "/jobs/Careers",
                        base + "/hcmUI/CandidateExperience/en/sites/CX/requisitions",
                        base + "/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions",
                    ]
                )

        if not probe_urls:
            return body

        best_html = body
        best_score = self._page_listing_score_v38(page_url, body)
        seen: set[str] = set()

        async with httpx.AsyncClient(
            timeout=7,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        ) as client:
            for candidate in probe_urls:
                norm = candidate.rstrip("/")
                if not candidate or norm in seen:
                    continue
                seen.add(norm)

                try:
                    resp = await client.get(candidate)
                except Exception:
                    continue

                text = resp.text or ""
                if resp.status_code != 200 or len(text) < 200:
                    continue
                if self._looks_non_html_payload_v38(text):
                    continue

                score = self._page_listing_score_v38(str(resp.url), text)
                if score > best_score + 0.15:
                    best_html = text
                    best_score = score

        return best_html

    async def _fetch_html_v38(self, url: str) -> Optional[str]:
        try:
            async with httpx.AsyncClient(
                timeout=7,
                follow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            ) as client:
                resp = await client.get(url)
        except Exception:
            return None

        text = resp.text or ""
        if resp.status_code != 200 or len(text) < 200 or self._looks_non_html_payload_v38(text):
            return None
        return text

    def _extract_state_jobs_v41(self, html_body: str, page_url: str) -> list[dict]:
        if not html_body or len(html_body) < 200:
            return []

        candidates: list[tuple[str, list[dict]]] = []

        for match in _NEXT_DATA_PATTERN_V39.finditer(html_body):
            payload = (match.group(1) or "").strip()
            if not payload:
                continue
            try:
                parsed = json.loads(payload)
            except Exception:
                continue
            jobs = self._extract_jobs_from_json_payload_v39(parsed, page_url)
            if jobs:
                candidates.append(("next_data_v41", jobs))

        for match in re.finditer(
            r"<script[^>]+type=['\"]application/json['\"][^>]*>(.*?)</script>",
            html_body,
            re.IGNORECASE | re.DOTALL,
        ):
            payload = (match.group(1) or "").strip()
            if len(payload) < 40 or len(payload) > 1_500_000:
                continue
            if "<" in payload[:200]:
                continue
            try:
                parsed = json.loads(payload)
            except Exception:
                continue
            jobs = self._extract_jobs_from_json_payload_v39(parsed, page_url)
            if jobs:
                candidates.append(("application_json_v41", jobs))

        _label, best_jobs, _score = self._pick_best_candidate_v38(candidates, page_url)
        return best_jobs

    def _should_follow_subpages_v41(
        self,
        page_url: str,
        html_body: str,
        best_jobs: list[dict],
        best_score: float,
    ) -> bool:
        if not best_jobs or len(best_jobs) < MIN_JOBS_FOR_SUCCESS or best_score < 11.0:
            return True

        detail_hits = sum(
            1
            for job in best_jobs
            if self._is_job_like_url_v38(job.get("source_url") or "")
        )
        role_hits = sum(
            1
            for job in best_jobs
            if self._title_has_role_signal_v38(self._normalize_title_v38(job.get("title", "")))
        )

        if len(best_jobs) <= 4 and (role_hits < max(2, int(len(best_jobs) * 0.7)) or detail_hits == 0):
            return True

        root = _parse_html(html_body)
        if root is None:
            return False

        listing_links = 0
        for a_el in root.xpath("//a[@href]"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            text = self._safe_text_v38(a_el)
            if self._is_rejected_listing_link_v38(href, text):
                continue
            if _LISTING_URL_PATTERN_V38.search(href) or _LISTING_LINK_TEXT_PATTERN_V38.search(text):
                listing_links += 1

        same_page_ratio = sum(
            1
            for job in best_jobs
            if (job.get("source_url") or page_url).rstrip("/") == page_url.rstrip("/")
        ) / max(1, len(best_jobs))
        if len(best_jobs) <= 4 and listing_links >= 3:
            return True
        if listing_links >= 4 and same_page_ratio >= 0.6:
            return True

        return False

    async def _recover_greenhouse_jobs_v41(
        self,
        page_url: str,
        company_name: str,
        html_body: str,
        best_jobs: list[dict],
    ) -> list[dict]:
        parsed = urlparse(page_url or "")
        host = (parsed.hostname or "").lower()
        if "greenhouse" not in host:
            return []

        if best_jobs and len(best_jobs) >= 6:
            detail_hits = sum(1 for j in best_jobs if self._is_job_like_url_v38(j.get("source_url") or ""))
            if detail_hits >= max(2, int(len(best_jobs) * 0.5)):
                return []

        slugs: list[str] = []
        seen: set[str] = set()

        def _add_slug(value: str) -> None:
            slug = self._slugify_v41(value)
            if not slug or slug in seen or len(slug) < 2:
                return
            seen.add(slug)
            slugs.append(slug)

        query = dict(parse_qsl(parsed.query))
        if query.get("for"):
            _add_slug(query.get("for", ""))

        if company_name:
            _add_slug(company_name)
            _add_slug(re.sub(r"\b(?:inc|corp|corporation|group|ltd|limited|company|co)\b", "", company_name, flags=re.IGNORECASE))

        title_match = re.search(r"<title>(.*?)</title>", html_body or "", re.IGNORECASE | re.DOTALL)
        if title_match:
            title_text = html_lib.unescape(title_match.group(1) or "")
            for m in _GREENHOUSE_TITLE_PATTERN_V41.finditer(title_text):
                _add_slug(m.group(1))

        for m in re.finditer(r"https?://[^\"'\s]+/([a-z0-9_-]{2,50})/jobs/[0-9]{4,}", html_body or "", re.IGNORECASE):
            _add_slug(m.group(1))

        if not slugs:
            return []

        scheme = parsed.scheme or "https"
        netloc = parsed.netloc
        probe_urls: list[str] = []
        for slug in slugs[:8]:
            probe_urls.append(f"{scheme}://{netloc}/embed/job_board?for={slug}")
            probe_urls.append(f"{scheme}://{netloc}/{slug}")
            if "job-boards.greenhouse.io" in netloc:
                probe_urls.append(f"https://boards.greenhouse.io/{slug}")

        candidates: list[tuple[str, list[dict]]] = []
        seen_urls: set[str] = set()
        fetched = 0
        for probe_url in probe_urls:
            norm = probe_url.rstrip("/")
            if not probe_url or norm in seen_urls:
                continue
            seen_urls.add(norm)
            if fetched >= 6:
                break

            probe_html = await self._fetch_html_v38(probe_url)
            if not probe_html:
                continue
            fetched += 1

            jobs = self._extract_from_single_page_v38(probe_url, probe_html)
            if jobs:
                candidates.append((f"gh_page_v41:{fetched}", jobs))

            state_jobs = self._extract_state_jobs_v41(probe_html, probe_url)
            if state_jobs:
                candidates.append((f"gh_state_v41:{fetched}", state_jobs))

        _label, best, _score = self._pick_best_candidate_v38(candidates, page_url)
        return best

    @staticmethod
    def _slugify_v41(value: str) -> str:
        text = (value or "").strip().lower()
        text = re.sub(r"[^a-z0-9\s-]", "", text)
        text = re.sub(r"[\s-]+", "-", text)
        return text.strip("-")

    def _extract_from_single_page_v38(self, page_url: str, html_body: str) -> list[dict]:
        if not html_body or len(html_body) < 200:
            return []

        candidates: list[tuple[str, list[dict]]] = []

        tier1 = self._extract_tier1_v12(page_url, html_body)
        if tier1:
            candidates.append(("tier1_v12", tier1))

        tier2 = self._extract_tier2_v16(page_url, html_body)
        if tier2:
            candidates.append(("tier2_v16", tier2))

        root = _parse_html(html_body)
        if root is None:
            label, jobs, _score = self._pick_best_candidate_v38(candidates, page_url)
            return jobs

        zoho_rows = self._extract_zoho_rows_v38(root, page_url)
        if zoho_rows:
            candidates.append(("zoho_rows_v38", zoho_rows))

        salesforce_rows = self._extract_salesforce_rows_v39(root, page_url)
        if salesforce_rows:
            candidates.append(("salesforce_rows_v39", salesforce_rows))

        awsm_rows = self._extract_awsm_rows_v42(root, page_url)
        if awsm_rows:
            candidates.append(("awsm_rows_v42", awsm_rows))

        repeated_rows = self._extract_repeating_rows_v38(root, page_url)
        if repeated_rows:
            candidates.append(("repeating_rows_v38", repeated_rows))

        elementor_cards = self._extract_elementor_cards_v38(root, page_url)
        if elementor_cards:
            candidates.append(("elementor_cards_v38", elementor_cards))

        accordion_jobs = self._extract_accordion_jobs_v38(root, page_url)
        if accordion_jobs:
            candidates.append(("accordion_jobs_v38", accordion_jobs))

        heading_rows = self._extract_heading_rows_v38(root, page_url)
        if heading_rows:
            candidates.append(("heading_rows_v38", heading_rows))

        greenhouse_posts = self._extract_greenhouse_posts_v38(root, page_url)
        if greenhouse_posts:
            candidates.append(("greenhouse_posts_v38", greenhouse_posts))

        job_links = self._extract_job_links_v38(root, page_url)
        if job_links:
            candidates.append(("job_links_v38", job_links))

        _label, jobs, _score = self._pick_best_candidate_v38(candidates, page_url)
        return jobs

    def _collect_listing_subpages_v38(self, page_url: str, html_body: str) -> list[str]:
        if not html_body or len(html_body) < 200:
            return []

        root = _parse_html(html_body)
        if root is None:
            return []

        candidates: list[tuple[str, float]] = []
        seen: set[str] = set()

        for a_el in root.xpath("//a[@href]"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            text = self._safe_text_v38(a_el)
            if self._is_rejected_listing_link_v38(href, text):
                continue

            full_url = _resolve_url(href, page_url)
            if not full_url:
                continue
            norm = full_url.rstrip("/")
            if norm in seen or norm == page_url.rstrip("/"):
                continue
            seen.add(norm)

            if not self._is_related_host_v38(page_url, full_url):
                host = (urlparse(full_url).hostname or "").lower()
                if not any(x in host for x in ("greenhouse.io", "zohorecruit", "oraclecloud.com")):
                    continue

            score = 0.0
            if _LISTING_URL_PATTERN_V38.search(full_url):
                score += 5.0
            if _LISTING_LINK_TEXT_PATTERN_V38.search(text):
                score += 4.0
            if _GENERIC_LISTING_PATH_PATTERN_V41.search(urlparse(full_url).path or ""):
                score += 2.8
            if "job-openings" in full_url.lower() or "portal.na" in full_url.lower():
                score += 2.0
            if re.search(r"\b(?:job\s+openings?|join\s+our\s+team|current\s+jobs?|lowongan\s+kerja|lihat\s+lowongan)\b", text, re.IGNORECASE):
                score += 3.2
            if re.search(r"\b(?:our\s+culture|our\s+values|our\s+direction|our\s+people|talent\s+stories?)\b", text, re.IGNORECASE):
                score -= 4.8
            if re.search(r"/(?:our-culture|our-values|talent-story|our-ecosystem)(?:/|$)", full_url, re.IGNORECASE):
                score -= 6.0
            if score > 0:
                candidates.append((full_url, score))

        candidates.sort(key=lambda x: x[1], reverse=True)
        return [url for url, _score in candidates[:12]]

    def _extract_zoho_rows_v38(self, root: etree._Element, page_url: str) -> list[dict]:
        rows = root.xpath("//tr[contains(@class,'jobDetailRow')]")
        if len(rows) < MIN_JOBS_FOR_SUCCESS:
            return []

        jobs: list[dict] = []
        for row in rows[:MAX_JOBS_PER_PAGE]:
            a_el = row.xpath(".//a[contains(@class,'jobdetail')][1]")
            if not a_el:
                continue

            title = self._normalize_title_v38(_text(a_el[0]))
            href = a_el[0].get("href") or ""
            source_url = _resolve_url(href, page_url) or page_url
            row_text = _text(row)
            tds = row.xpath("./td")
            location = _text(tds[1]) if len(tds) > 1 else None
            salary = _text(tds[4]) if len(tds) > 4 else None

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": salary,
                    "employment_type": self._extract_employment_type_v38(row_text),
                    "description": self._clean_description_v38(row_text),
                    "extraction_method": "tier2_zoho_rows_v38",
                    "extraction_confidence": 0.86,
                }
            )

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_salesforce_rows_v39(self, root: etree._Element, page_url: str) -> list[dict]:
        rows = root.xpath("//tr[contains(@class,'dataRow')]")
        if len(rows) < 2:
            return []

        jobs: list[dict] = []
        for row in rows[:MAX_JOBS_PER_PAGE]:
            link_nodes = row.xpath(
                ".//td[2]//a[@href][1] | .//a[contains(@href,'fRecruit__ApplyJob')][1]"
            )
            if not link_nodes:
                continue

            link = link_nodes[0]
            title = self._normalize_title_v38(_text(link))
            if not self._is_valid_title_v38(title):
                continue
            if not self._title_has_role_signal_v38(title):
                continue

            source_url = _resolve_url(link.get("href"), page_url) or page_url
            if self._is_non_job_url_v38(source_url):
                continue

            row_text = _text(row)
            tds = row.xpath("./td")
            location = _text(tds[3]).strip() if len(tds) > 3 else None
            if location == "\xa0":
                location = None

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": self._extract_salary_v38(row_text),
                    "employment_type": self._extract_employment_type_v38(row_text),
                    "description": self._clean_description_v38(row_text),
                    "extraction_method": "tier2_salesforce_rows_v39",
                    "extraction_confidence": 0.9,
                }
            )

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_awsm_rows_v42(self, root: etree._Element, page_url: str) -> list[dict]:
        rows = root.xpath(
            "//*[contains(@class,'awsm-job-listing-item') or contains(@class,'awsm-b-job-item')]"
        )
        if len(rows) < 1:
            return []

        jobs: list[dict] = []
        for row in rows[:MAX_JOBS_PER_PAGE]:
            title_nodes = row.xpath(
                ".//h2[contains(@class,'awsm-job-post-title')][1] | "
                ".//h2[contains(@class,'awsm-b-job-post-title')][1] | "
                ".//h2[1]"
            )
            title = self._normalize_title_v38(_text(title_nodes[0])) if title_nodes else ""
            if not self._is_valid_title_v38(title):
                continue

            link_nodes = row.xpath(
                ".//a[contains(@class,'awsm-job-item')][1] | .//h2//a[@href][1] | .//a[@href][1]"
            )
            if not link_nodes:
                continue
            source_url = _resolve_url(link_nodes[0].get("href"), page_url) or page_url
            if self._is_non_job_url_v38(source_url):
                continue

            row_text = _text(row)
            location = None
            location_nodes = row.xpath(
                ".//*[contains(@class,'job-location')]//*[contains(@class,'awsm-job-specification-term')] | "
                ".//*[contains(@class,'job-location')]"
            )
            if location_nodes:
                location = " ".join(_text(location_nodes[0]).split())[:140] or None

            detailish = self._has_strong_detail_url_v42(source_url)
            if not (self._title_has_role_signal_v38(title) or detailish):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": self._extract_salary_v38(row_text),
                    "employment_type": self._extract_employment_type_v38(row_text),
                    "description": self._clean_description_v38(row_text),
                    "extraction_method": "tier2_awsm_rows_v42",
                    "extraction_confidence": 0.84 if detailish else 0.79,
                }
            )

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    @staticmethod
    def _looks_like_oracle_shell_v39(page_url: str, html_body: str) -> bool:
        lower = (html_body or "").lower()
        return (
            "oraclecloud.com" in (page_url or "").lower()
            or "candidateexperience" in lower
            or "hcmrestapi" in lower
        )

    @staticmethod
    def _looks_like_martian_shell_v39(html_body: str) -> bool:
        lower = (html_body or "").lower()
        return any(
            token in lower
            for token in (
                "__next_data__",
                "myrecruitmentplus",
                "martianlogic",
                "clientcode",
                "recruiterid",
                "jobboardthemeid",
                "/_next/static/chunks/pages/%5bclient%5d",
            )
        )

    async def _extract_oracle_jobs_v39(self, page_url: str, html_body: str) -> list[dict]:
        if not self._looks_like_oracle_shell_v39(page_url, html_body):
            return []

        parsed = urlparse(page_url)
        if not parsed.netloc:
            return []

        api_base = f"{parsed.scheme or 'https'}://{parsed.netloc}"
        m = _ORACLE_API_BASE_PATTERN_V39.search(html_body or "")
        if m:
            api_base = m.group(1).rstrip("/")

        site_ids = self._oracle_site_ids_v39(page_url, html_body)
        if not site_ids:
            return []

        site_candidates: list[tuple[str, list[dict]]] = []
        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
                for site_id in site_ids[:10]:
                    site_jobs: list[dict] = []
                    for offset in range(0, 264, 24):
                        finder_variants = [
                            (
                                f"findReqs;siteNumber={site_id},"
                                "facetsList=LOCATIONS;WORK_LOCATIONS;TITLES;CATEGORIES;POSTING_DATES,"
                                f"limit=24,offset={offset}"
                            ),
                            f"findReqs;siteNumber={site_id},limit=24,offset={offset}",
                        ]

                        found_for_offset = False
                        for finder in finder_variants:
                            query = urlencode(
                                {
                                    "onlyData": "true",
                                    "expand": "requisitionList.secondaryLocations",
                                    "finder": finder,
                                }
                            )
                            api_url = f"{api_base}/hcmRestApi/resources/latest/recruitingCEJobRequisitions?{query}"
                            try:
                                resp = await client.get(api_url)
                            except Exception:
                                continue
                            if resp.status_code >= 400:
                                continue
                            try:
                                data = resp.json()
                            except Exception:
                                continue

                            batch = self._extract_oracle_items_v39(data, page_url, site_id)
                            if not batch:
                                continue
                            site_jobs.extend(batch)
                            found_for_offset = True
                            if len(batch) < 24:
                                break

                        if not found_for_offset:
                            break
                        if len(site_jobs) >= MAX_JOBS_PER_PAGE:
                            break

                    prepared = self._prepare_candidate_jobs_v38(site_jobs, page_url)
                    if prepared:
                        site_candidates.append((site_id, prepared))
        except Exception:
            logger.debug("v4.2 oracle probing failed for %s", page_url)

        if not site_candidates:
            return []

        site_candidates.sort(
            key=lambda item: (
                len(item[1]),
                1 if re.search(r"_[0-9]+$", item[0]) else 0,
                1 if item[0].upper().endswith("_1001") else 0,
            ),
            reverse=True,
        )
        return site_candidates[0][1]

    def _oracle_site_ids_v39(self, page_url: str, html_body: str) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []

        def _add(value: str) -> None:
            site = (value or "").strip()
            if site.lower() in {"coreassets", "allitems", "forms"}:
                return
            if not re.fullmatch(r"[A-Za-z0-9_]{2,24}", site):
                return
            if not site or site in seen:
                return
            seen.add(site)
            ordered.append(site)

        for match in _ORACLE_SITE_PATTERN_V39.finditer(page_url or ""):
            _add(match.group(1))
        for match in re.finditer(
            r"(?:<base[^>]+href=['\"][^'\"]*/sites/|CandidateExperience/en/sites/)([A-Za-z0-9_]+)",
            html_body or "",
            re.IGNORECASE,
        ):
            _add(match.group(1))
        for match in _ORACLE_SITE_NUMBER_PATTERN_V39.finditer(html_body or ""):
            _add(match.group(1))
        for match in re.finditer(r"siteNumber=([A-Za-z0-9_]+)", html_body or "", re.IGNORECASE):
            _add(match.group(1))

        query_pairs = dict(parse_qsl(urlparse(page_url or "").query))
        if query_pairs.get("siteNumber"):
            _add(query_pairs.get("siteNumber", ""))

        base_ids = list(ordered)
        for site_id in base_ids:
            if re.fullmatch(r"CX(?:_\d+)?", site_id, flags=re.IGNORECASE):
                root = site_id.split("_", 1)[0]
                _add(root)
                for suffix in ("1001", "1002", "1003"):
                    _add(f"{root}_{suffix}")

        if not ordered:
            for fallback in ("CX_1001", "CX_1002", "CX"):
                _add(fallback)

        ordered.sort(
            key=lambda site: (
                0 if re.search(r"_[0-9]+$", site) else 1,
                0 if site.upper().endswith("_1001") else 1,
                site.lower(),
            )
        )
        return ordered[:12]

    def _extract_oracle_items_v39(self, data: Any, page_url: str, site_id: str) -> list[dict]:
        rows: list[dict[str, Any]] = []

        def _add_row(value: Any) -> None:
            if isinstance(value, dict):
                rows.append(value)

        if isinstance(data, dict):
            top_reqs = data.get("requisitionList")
            if isinstance(top_reqs, list):
                for row in top_reqs:
                    _add_row(row)
            elif isinstance(top_reqs, dict):
                for key in ("items", "requisitionList", "list"):
                    nested = top_reqs.get(key)
                    if isinstance(nested, list):
                        for row in nested:
                            _add_row(row)

            items = data.get("items")
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    _add_row(item)
                    req_list = item.get("requisitionList")
                    if isinstance(req_list, list):
                        for row in req_list:
                            _add_row(row)
                    elif isinstance(req_list, dict):
                        for key in ("items", "requisitionList", "list"):
                            nested = req_list.get(key)
                            if isinstance(nested, list):
                                for row in nested:
                                    _add_row(row)

        parsed = urlparse(page_url)
        host_base = f"{parsed.scheme or 'https'}://{parsed.netloc}" if parsed.netloc else ""
        jobs: list[dict] = []
        for row in rows:
            title = self._normalize_title_v38(
                str(
                    row.get("Title")
                    or row.get("title")
                    or row.get("JobTitle")
                    or row.get("jobTitle")
                    or row.get("requisitionTitle")
                    or ""
                )
            )
            if not self._is_valid_title_v38(title):
                continue
            if not (self._title_has_role_signal_v38(title) or self._is_acronym_title_v38(title)):
                continue

            req_id = str(
                row.get("Id")
                or row.get("id")
                or row.get("RequisitionId")
                or row.get("requisitionId")
                or row.get("jobId")
                or ""
            ).strip()

            source_url = ""
            for key in ("ExternalURL", "externalUrl", "PostingUrl", "postingUrl", "jobUrl", "url"):
                value = row.get(key)
                if isinstance(value, str) and value.strip():
                    source_url = value.strip()
                    break
            source_url = _resolve_url(source_url, page_url) or page_url
            if req_id and source_url.rstrip("/") == page_url.rstrip("/") and host_base:
                source_url = f"{host_base}/hcmUI/CandidateExperience/en/sites/{site_id}/job/{req_id}"
            if self._is_non_job_url_v38(source_url):
                continue

            primary = str(row.get("PrimaryLocation") or row.get("primaryLocation") or "").strip()
            country = str(row.get("PrimaryLocationCountry") or row.get("primaryLocationCountry") or "").strip()
            location = ", ".join([p for p in (primary, country) if p]) or None

            description = self._clean_description_v38(
                str(
                    row.get("Description")
                    or row.get("description")
                    or row.get("ShortDescription")
                    or row.get("ExternalDescriptionStr")
                    or row.get("ExternalResponsibilitiesStr")
                    or row.get("ExternalQualificationsStr")
                    or ""
                )
            )

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": self._extract_salary_v38(json.dumps(row, ensure_ascii=True, default=str)),
                    "employment_type": self._extract_employment_type_v38(json.dumps(row, ensure_ascii=True, default=str)),
                    "description": description,
                    "extraction_method": "tier0_oracle_api_v39",
                    "extraction_confidence": 0.9,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return jobs

    async def _extract_martian_jobs_v39(self, page_url: str, html_body: str) -> list[dict]:
        if not self._looks_like_martian_shell_v39(html_body):
            return []

        context = self._extract_martian_context_v39(page_url, html_body)
        if not (context.get("client_code") or context.get("recruiter_id") or context.get("build_id")):
            return []

        endpoints = self._martian_probe_urls_v39(page_url, context)
        if not endpoints:
            return []

        shell_like = "__next_data__" in (html_body or "").lower() and "<div id=\"__next\"></div>" in (html_body or "").lower()
        probe_plan = self._martian_endpoint_plan_v40(endpoints, aggressive=shell_like)
        if not probe_plan:
            return []
        fast_pass = self._martian_fastpass_endpoints_v42(page_url, probe_plan)

        jobs: list[dict] = []
        request_budget = 20 if shell_like else 14
        try:
            async with httpx.AsyncClient(
                timeout=3.2,
                follow_redirects=True,
                headers={
                    "Accept": "application/json,text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                },
            ) as client:
                for endpoint in fast_pass:
                    for probe_url in self._martian_paged_variants_v40(endpoint)[:3]:
                        if request_budget <= 0:
                            break
                        request_budget -= 1
                        try:
                            resp = await asyncio.wait_for(client.get(probe_url), timeout=3.2)
                        except Exception:
                            continue
                        if resp.status_code >= 400 or not resp.text:
                            continue

                        extracted = self._extract_jobs_from_probe_payload_v40(resp.text, str(resp.url), page_url)
                        if extracted:
                            jobs.extend(extracted)
                            if len(jobs) >= MAX_JOBS_PER_PAGE:
                                break
                    if request_budget <= 0 or len(jobs) >= MAX_JOBS_PER_PAGE:
                        break
                    if len(jobs) >= MIN_JOBS_FOR_SUCCESS and request_budget <= 7:
                        break

                for endpoint in probe_plan:
                    for probe_url in self._martian_paged_variants_v40(endpoint)[:2]:
                        if request_budget <= 0:
                            break
                        request_budget -= 1
                        try:
                            resp = await asyncio.wait_for(client.get(probe_url), timeout=3.2)
                        except Exception:
                            continue
                        if resp.status_code >= 400 or not resp.text:
                            continue

                        extracted = self._extract_jobs_from_probe_payload_v40(resp.text, str(resp.url), page_url)
                        if extracted:
                            jobs.extend(extracted)
                            if len(jobs) >= MAX_JOBS_PER_PAGE:
                                break
                    if request_budget <= 0 or len(jobs) >= MAX_JOBS_PER_PAGE:
                        break
                    if len(jobs) >= MIN_JOBS_FOR_SUCCESS and request_budget <= 5:
                        break

                if request_budget > 0 and len(jobs) < MIN_JOBS_FOR_SUCCESS:
                    post_endpoints = self._martian_post_endpoints_v40(probe_plan)
                    payloads = self._martian_post_payloads_v40(context)
                    for endpoint in post_endpoints[:16]:
                        for payload in payloads[:8]:
                            if request_budget <= 0:
                                break
                            request_budget -= 1
                            try:
                                resp = await asyncio.wait_for(client.post(endpoint, json=payload), timeout=3.2)
                            except Exception:
                                continue
                            if resp.status_code >= 400 or not resp.text:
                                continue

                            extracted = self._extract_jobs_from_probe_payload_v40(resp.text, str(resp.url), page_url)
                            if extracted:
                                jobs.extend(extracted)
                                if len(jobs) >= MAX_JOBS_PER_PAGE:
                                    break
                        if request_budget <= 0 or len(jobs) >= MAX_JOBS_PER_PAGE:
                            break
        except Exception:
            logger.debug("v4.2 martian probing failed for %s", page_url)

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _martian_fastpass_endpoints_v42(self, page_url: str, endpoints: list[str]) -> list[str]:
        page_host = (urlparse(page_url or "").netloc or "").lower()
        ranked: list[tuple[int, str]] = []
        seen: set[str] = set()
        for endpoint in endpoints:
            norm = (endpoint or "").rstrip("/")
            if not norm or norm in seen:
                continue
            seen.add(norm)
            low = norm.lower()
            host = (urlparse(norm).netloc or "").lower()

            score = 0
            if host == page_host:
                score += 10
            if "/_next/data/" in low:
                score += 8
            if "/api/recruiter/" in low:
                score += 8
            elif "/api/" in low and ("search" in low or "jobs" in low or "jobads" in low):
                score += 6
            if "client=" in low or "clientcode=" in low:
                score += 4
            if "recruiterid=" in low:
                score += 3
            if "pagenumber=1" in low or "page=1" in low or "offset=0" in low:
                score += 2
            if score > 0:
                ranked.append((score, norm))

        ranked.sort(key=lambda item: item[0], reverse=True)
        return [url for _score, url in ranked[:10]]

    def _extract_martian_context_v39(self, page_url: str, html_body: str) -> dict[str, str]:
        result = {
            "client_code": "",
            "recruiter_id": "",
            "job_board_theme_id": "",
            "board_name": "",
            "build_id": "",
            "next_page": "",
            "next_query": "",
            "host_hints": "",
            "endpoint_hints": "",
        }

        match = _NEXT_DATA_PATTERN_V39.search(html_body or "")
        if match:
            raw_payload = html_lib.unescape((match.group(1) or "").strip())
            try:
                parsed = json.loads(raw_payload)
                if isinstance(parsed, dict):
                    page_props = parsed.get("props", {}).get("pageProps", {})
                    if isinstance(page_props, dict):
                        result["client_code"] = str(page_props.get("clientCode") or "").strip()
                        result["recruiter_id"] = str(page_props.get("recruiterId") or "").strip()
                        result["job_board_theme_id"] = str(page_props.get("jobBoardThemeId") or "").strip()
                        result["board_name"] = str(page_props.get("name") or "").strip()
                    result["build_id"] = str(parsed.get("buildId") or "").strip()
                    result["next_page"] = str(parsed.get("page") or "").strip()
                    query = parsed.get("query")
                    if isinstance(query, dict):
                        result["next_query"] = urlencode(
                            {str(k): str(v) for k, v in query.items() if isinstance(v, (str, int, float))}
                        )
                        if not result["client_code"]:
                            result["client_code"] = str(query.get("client") or query.get("clientCode") or "").strip()
                        if not result["recruiter_id"]:
                            result["recruiter_id"] = str(query.get("recruiterId") or "").strip()
            except Exception:
                pass

        if not result["client_code"]:
            m = re.search(r"clientCode['\"]?\s*[:=]\s*['\"]([a-z0-9_-]{2,})", html_body or "", re.IGNORECASE)
            if m:
                result["client_code"] = m.group(1)
        if not result["recruiter_id"]:
            m = re.search(r"recruiterId['\"]?\s*[:=]\s*['\"]?([0-9]{2,})", html_body or "", re.IGNORECASE)
            if m:
                result["recruiter_id"] = m.group(1)
        if not result["job_board_theme_id"]:
            m = re.search(r"jobBoardThemeId['\"]?\s*[:=]\s*['\"]?([0-9]{2,})", html_body or "", re.IGNORECASE)
            if m:
                result["job_board_theme_id"] = m.group(1)
        if not result["board_name"]:
            m = re.search(r"\"name\"\s*:\s*\"([^\"]{2,80})\"", html_body or "", re.IGNORECASE)
            if m:
                result["board_name"] = m.group(1).strip()

        if not result["client_code"]:
            parsed = urlparse(page_url)
            path_parts = [seg for seg in parsed.path.split("/") if seg]
            if path_parts:
                candidate = re.sub(r"[^a-z0-9-]", "", path_parts[-1].lower())
                if len(candidate) >= 3:
                    result["client_code"] = candidate

        host_hints: list[str] = []
        seen_hosts: set[str] = set()
        for m in _MARTIAN_HOST_HINT_PATTERN_V40.finditer(html_body or ""):
            parsed_host = urlparse(m.group(0))
            if not parsed_host.netloc:
                continue
            host = f"{parsed_host.scheme or 'https'}://{parsed_host.netloc}".rstrip("/")
            if host in seen_hosts:
                continue
            seen_hosts.add(host)
            host_hints.append(host)
        if host_hints:
            result["host_hints"] = "|".join(host_hints[:8])

        endpoint_hints: list[str] = []
        seen_endpoints: set[str] = set()
        for m in _MARTIAN_ENDPOINT_HINT_PATTERN_V40.finditer(html_body or ""):
            endpoint = m.group(0).strip().strip("\"' ")
            if not endpoint:
                continue
            norm = endpoint.rstrip("/")
            if norm in seen_endpoints:
                continue
            seen_endpoints.add(norm)
            endpoint_hints.append(endpoint)
        for m in _MARTIAN_REL_ENDPOINT_HINT_PATTERN_V40.finditer(html_body or ""):
            endpoint = _resolve_url(m.group(0), page_url) or ""
            if not endpoint:
                continue
            norm = endpoint.rstrip("/")
            if norm in seen_endpoints:
                continue
            seen_endpoints.add(norm)
            endpoint_hints.append(endpoint)
        if endpoint_hints:
            result["endpoint_hints"] = "|".join(endpoint_hints[:24])

        return result

    def _martian_probe_urls_v39(self, page_url: str, context: dict[str, str]) -> list[str]:
        parsed = urlparse(page_url or "")
        if not parsed.netloc:
            return []

        base_host = f"{parsed.scheme or 'https'}://{parsed.netloc}"
        client_code = (context.get("client_code") or "").strip()
        recruiter_id = (context.get("recruiter_id") or "").strip()
        theme_id = (context.get("job_board_theme_id") or "").strip()
        board_name = (context.get("board_name") or "").strip()
        host_hints = [h for h in (context.get("host_hints") or "").split("|") if h]

        ats_hosts = [
            "https://web.martianlogic.com",
            "https://jobs.myrecruitmentplus.com",
            "https://form.myrecruitmentplus.com",
            "https://jobs.martianlogic.com",
        ]
        base_is_ats = "martianlogic" in parsed.netloc.lower() or "myrecruitmentplus" in parsed.netloc.lower()
        hosts = [base_host, *ats_hosts, *host_hints] if base_is_ats else [*ats_hosts, *host_hints, base_host]
        if client_code:
            hosts.extend([f"https://{client_code}.myrecruitmentplus.com", f"https://{client_code}.martianlogic.com"])

        deduped_hosts: list[str] = []
        seen_hosts: set[str] = set()
        for host in hosts:
            norm = host.rstrip("/")
            if not norm or norm in seen_hosts:
                continue
            seen_hosts.add(norm)
            deduped_hosts.append(norm)
        hosts = deduped_hosts[:10]

        query_templates = [
            "pageNumber=1&pageSize=50&isActive=true",
            "page=1&pageSize=50",
            "offset=0&limit=50",
        ]
        if client_code:
            query_templates.extend(
                [
                    f"client={client_code}&pageNumber=1&pageSize=50&isActive=true",
                    f"clientCode={client_code}&pageNumber=1&pageSize=50&isActive=true",
                    f"client={client_code}&page=1&pageSize=50",
                    f"clientCode={client_code}&page=1&pageSize=50",
                ]
            )
        if recruiter_id:
            query_templates.extend(
                [
                    f"recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true",
                    f"recruiterId={recruiter_id}&page=1&pageSize=50",
                ]
            )
            if client_code:
                query_templates.extend(
                    [
                        f"client={client_code}&recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true",
                        f"clientCode={client_code}&recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true",
                    ]
                )
        if theme_id and client_code:
            query_templates.extend(
                [
                    f"client={client_code}&jobBoardThemeId={theme_id}&pageNumber=1&pageSize=50&isActive=true",
                    f"clientCode={client_code}&jobBoardThemeId={theme_id}&pageNumber=1&pageSize=50&isActive=true",
                ]
            )
        if board_name and client_code:
            encoded_name = board_name.replace(" ", "%20")
            query_templates.extend(
                [
                    f"client={client_code}&name={encoded_name}&pageNumber=1&pageSize=50&isActive=true",
                    f"clientCode={client_code}&name={encoded_name}&pageNumber=1&pageSize=50&isActive=true",
                ]
            )

        search_paths = [
            "/api/jobs/search",
            "/api/job-search",
            "/api/jobads/search",
            "/api/job-ads/search",
            "/api/jobAds/search",
            "/api/search/jobs",
            "/jobs/search",
            "/embed-jobs",
        ]
        plain_paths = [
            "/api/jobs",
            "/api/jobads",
            "/api/job-ads",
            "/api/jobAds",
            "/jobs",
            "/jobads",
            "/job-ads",
        ]
        if client_code:
            search_paths.extend(
                [
                    f"/{client_code}/jobs/search",
                    f"/{client_code}/jobads/search",
                    f"/{client_code}/job-ads/search",
                    f"/{client_code}/embed-jobs",
                ]
            )
            plain_paths.extend(
                [
                    f"/{client_code}",
                    f"/{client_code}/jobs",
                    f"/{client_code}/jobads",
                    f"/{client_code}/job-ads",
                    f"/{client_code}/latest",
                ]
            )

        urls: list[str] = []
        endpoint_hints = [x for x in (context.get("endpoint_hints") or "").split("|") if x]
        for host in hosts:
            if recruiter_id:
                for recruiter_path in (
                    f"/api/recruiter/{recruiter_id}/jobs",
                    f"/api/recruiter/{recruiter_id}/jobs/search",
                    f"/api/recruiter/{recruiter_id}/jobads",
                    f"/api/recruiter/{recruiter_id}/job-ads",
                ):
                    base = f"{host}{recruiter_path}"
                    urls.append(base)
                    urls.append(f"{base}?pageNumber=1&pageSize=50")
                    urls.append(f"{base}?page=1&pageSize=50")
                    if client_code:
                        urls.append(f"{base}?clientCode={client_code}&page=1&pageSize=50")
                        urls.append(f"{base}?client={client_code}&page=1&pageSize=50")

            for path in search_paths:
                base = f"{host}{path.rstrip('/')}"
                urls.append(base)
                for query in query_templates:
                    urls.append(f"{base}?{query}")
            for path in plain_paths:
                base = f"{host}{path.rstrip('/')}"
                urls.append(base)
                if client_code:
                    urls.append(f"{base}?client={client_code}&page=1&pageSize=50")
                    urls.append(f"{base}?clientCode={client_code}&page=1&pageSize=50")

            urls.extend(self._next_data_probe_urls_v39(host, page_url, context))

        urls.extend(endpoint_hints)

        deduped: list[str] = []
        seen: set[str] = set()
        for url in urls:
            norm = (url or "").rstrip("/")
            if not norm or norm in seen:
                continue
            seen.add(norm)
            deduped.append(url)

        deduped.sort(key=self._martian_endpoint_priority_v40, reverse=True)
        return deduped[:240]

    def _next_data_probe_urls_v39(self, host: str, page_url: str, context: dict[str, str]) -> list[str]:
        build_id = (context.get("build_id") or "").strip()
        if not build_id:
            return []

        parsed = urlparse(page_url or "")
        path = parsed.path or "/"
        norm_path = "/" + "/".join(seg for seg in path.split("/") if seg)
        if not norm_path:
            norm_path = "/"

        query_pairs = dict(parse_qsl(parsed.query))
        next_query = (context.get("next_query") or "").strip()
        if next_query:
            for key, value in parse_qsl(next_query):
                query_pairs[key] = value
        client_code = (context.get("client_code") or "").strip()
        if client_code:
            query_pairs.setdefault("client", client_code)
        encoded_query = urlencode(query_pairs)

        candidates: list[str] = [f"{host}/_next/data/{build_id}/index.json"]
        if norm_path != "/":
            candidates.append(f"{host}/_next/data/{build_id}{norm_path.rstrip('/')}.json")
            candidates.append(f"{host}/_next/data/{build_id}{norm_path.rstrip('/')}/index.json")

        dynamic_keys = [
            client_code,
            context.get("next_page", "").replace("[", "").replace("]", "").strip("/"),
            str(query_pairs.get("clientCode") or ""),
            str(query_pairs.get("slug") or ""),
            str(query_pairs.get("tenant") or ""),
        ]
        dynamic_keys.extend([seg for seg in norm_path.split("/") if seg and not seg.startswith("[")])

        seen_keys: set[str] = set()
        for key in dynamic_keys:
            cleaned = (key or "").strip("/")
            if not cleaned or cleaned in seen_keys:
                continue
            seen_keys.add(cleaned)
            candidates.append(f"{host}/_next/data/{build_id}/{cleaned}.json")
            candidates.append(f"{host}/_next/data/{build_id}/{cleaned}/index.json")

        if encoded_query:
            candidates = [f"{url}?{encoded_query}" if "?" not in url else url for url in candidates]
        return candidates

    def _extract_jobs_from_probe_payload_v40(self, body: str, response_url: str, page_url: str) -> list[dict]:
        payload = (body or "").strip()
        if not payload:
            return []

        jobs: list[dict] = []
        if payload.startswith("{") or payload.startswith("["):
            try:
                parsed = json.loads(payload)
            except Exception:
                parsed = None
            if parsed is not None:
                jobs.extend(self._extract_jobs_from_json_payload_v39(parsed, response_url))

        root = _parse_html(payload)
        if root is not None:
            try:
                anchor_count = len(root.xpath("//a[@href]"))
            except Exception:
                anchor_count = 0
            if anchor_count >= 3:
                jobs.extend(self._extract_from_single_page_v38(response_url, payload))

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    @staticmethod
    def _martian_endpoint_priority_v40(url: str) -> int:
        low = (url or "").lower()
        host = (urlparse(url).netloc or "").lower()
        score = 0
        if "martianlogic" in host or "myrecruitmentplus" in host:
            score += 8
        if "/api/recruiter/" in low:
            score += 12
        elif "/api/" in low:
            score += 9
        if "/_next/data/" in low:
            score += 8
        if "search" in low:
            score += 6
        if "jobads" in low or "job-ads" in low:
            score += 5
        elif "/jobs" in low:
            score += 3
        if "client=" in low or "clientcode=" in low:
            score += 3
        if "recruiterid=" in low:
            score += 4
        if "pagenumber=1" in low or "page=1" in low or "offset=0" in low:
            score += 2
        return score

    def _martian_endpoint_plan_v40(self, endpoints: list[str], aggressive: bool = False) -> list[str]:
        if not endpoints:
            return []

        max_total = 40 if aggressive else 26
        max_per_host = 10 if aggressive else 7
        buckets: dict[str, list[str]] = defaultdict(list)
        for endpoint in endpoints:
            host = urlparse(endpoint).netloc.lower() or "_"
            buckets[host].append(endpoint)

        ordered_hosts = sorted(
            buckets.keys(),
            key=lambda h: (0 if ("martianlogic" in h or "myrecruitmentplus" in h) else 1, h),
        )

        for host in ordered_hosts:
            ranked = sorted(buckets[host], key=self._martian_endpoint_priority_v40, reverse=True)
            buckets[host] = ranked[:max_per_host]

        plan: list[str] = []
        while len(plan) < max_total:
            added = False
            for host in ordered_hosts:
                if not buckets[host]:
                    continue
                plan.append(buckets[host].pop(0))
                added = True
                if len(plan) >= max_total:
                    break
            if not added:
                break
        return plan

    @staticmethod
    def _martian_paged_variants_v40(endpoint: str) -> list[str]:
        variants = [endpoint]
        if "pageNumber=1" in endpoint:
            variants.append(endpoint.replace("pageNumber=1", "pageNumber=2"))
            variants.append(endpoint.replace("pageNumber=1", "pageNumber=3"))
        if "page=1" in endpoint:
            variants.append(endpoint.replace("page=1", "page=2"))
            variants.append(endpoint.replace("page=1", "page=3"))
        if "offset=0" in endpoint:
            variants.append(endpoint.replace("offset=0", "offset=50"))
            variants.append(endpoint.replace("offset=0", "offset=100"))

        seen: set[str] = set()
        out: list[str] = []
        for value in variants:
            if value in seen:
                continue
            seen.add(value)
            out.append(value)
        return out[:3]

    @staticmethod
    def _martian_post_endpoints_v40(endpoints: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for endpoint in endpoints:
            low = endpoint.lower()
            if "/api/" not in low:
                continue
            if "search" not in low and "jobads" not in low and "job-ads" not in low and "/jobs" not in low:
                continue
            base = endpoint.split("?", 1)[0].rstrip("/")
            if not base or base in seen:
                continue
            seen.add(base)
            out.append(base)
        return out[:20]

    @staticmethod
    def _martian_post_payloads_v40(context: dict[str, str]) -> list[dict[str, Any]]:
        client_code = (context.get("client_code") or "").strip()
        recruiter_id = (context.get("recruiter_id") or "").strip()
        theme_id = (context.get("job_board_theme_id") or "").strip()
        board_name = (context.get("board_name") or "").strip()

        payloads: list[dict[str, Any]] = []
        if client_code:
            payloads.extend(
                [
                    {"client": client_code, "pageNumber": 1, "pageSize": 50, "isActive": True},
                    {"clientCode": client_code, "pageNumber": 1, "pageSize": 50, "isActive": True},
                    {"client": client_code, "offset": 0, "limit": 50},
                    {"clientCode": client_code, "offset": 0, "limit": 50},
                ]
            )
        else:
            payloads.append({"pageNumber": 1, "pageSize": 50, "isActive": True})

        if recruiter_id:
            payloads.append({"recruiterId": recruiter_id, "pageNumber": 1, "pageSize": 50, "isActive": True})
            if client_code:
                payloads.append(
                    {
                        "client": client_code,
                        "recruiterId": recruiter_id,
                        "pageNumber": 1,
                        "pageSize": 50,
                        "isActive": True,
                    }
                )

        if theme_id:
            payload = {"jobBoardThemeId": theme_id, "pageNumber": 1, "pageSize": 50, "isActive": True}
            if client_code:
                payload["clientCode"] = client_code
            payloads.append(payload)

        if board_name and client_code:
            payloads.append({"client": client_code, "name": board_name, "pageNumber": 1, "pageSize": 50, "isActive": True})

        return payloads

    def _extract_jobs_from_json_payload_v39(self, payload: Any, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        for row in self._iter_json_dicts_v39(payload):
            title = self._normalize_title_v38(
                str(
                    row.get("title")
                    or row.get("Title")
                    or row.get("jobTitle")
                    or row.get("JobTitle")
                    or row.get("jobAdTitle")
                    or row.get("adTitle")
                    or row.get("positionTitle")
                    or row.get("position")
                    or row.get("vacancyTitle")
                    or row.get("requisitionTitle")
                    or row.get("name")
                    or ""
                )
            )
            if not self._is_valid_title_v38(title):
                continue
            if not self._title_has_role_signal_v38(title):
                continue
            if _TITLE_REJECT_PATTERN_V38.match(title.lower()):
                continue

            source_url = ""
            for key in (
                "url",
                "jobUrl",
                "job_url",
                "applyUrl",
                "apply_url",
                "applicationFormUrl",
                "application_url",
                "externalUrl",
                "postingUrl",
                "jobPostingUrl",
                "adUrl",
                "detailsUrl",
                "vacancyUrl",
                "absolute_url",
                "link",
            ):
                value = row.get(key)
                if isinstance(value, str) and value.strip():
                    source_url = value.strip()
                    break

            req_id = str(
                row.get("jobId")
                or row.get("jobAdId")
                or row.get("adId")
                or row.get("vacancyNo")
                or row.get("requisitionId")
                or row.get("Id")
                or row.get("id")
                or ""
            ).strip()
            source_url = _resolve_url(source_url, page_url) or page_url
            if req_id and source_url.rstrip("/") == page_url.rstrip("/"):
                parsed = urlparse(page_url)
                host_base = f"{parsed.scheme or 'https'}://{parsed.netloc}" if parsed.netloc else page_url.rstrip("/")
                source_url = f"{host_base}/jobdetails?jobAdId={req_id}"
            if self._is_non_job_url_v38(source_url):
                continue

            location_value = (
                row.get("location")
                or row.get("Location")
                or row.get("primaryLocation")
                or row.get("PrimaryLocation")
                or row.get("workLocation")
                or ""
            )
            location = None
            if isinstance(location_value, dict):
                location = str(
                    location_value.get("name")
                    or location_value.get("Name")
                    or location_value.get("label")
                    or location_value.get("city")
                    or location_value.get("suburb")
                    or location_value.get("state")
                    or ""
                ).strip() or None
            elif isinstance(location_value, str):
                location = location_value.strip() or None

            description = self._clean_description_v38(
                str(
                    row.get("description")
                    or row.get("Description")
                    or row.get("summary")
                    or row.get("shortDescription")
                    or row.get("introduction")
                    or row.get("ExternalDescriptionStr")
                    or ""
                )
            )

            employment_type = str(
                row.get("employmentType")
                or row.get("jobType")
                or row.get("JobType")
                or ""
            ).strip() or None
            salary_raw = str(row.get("salary") or row.get("salaryRange") or "").strip() or None

            evidence = (
                self._is_job_like_url_v38(source_url)
                or bool(location)
                or bool(employment_type)
                or bool(salary_raw)
                or (description is not None and len(description) >= 120)
                or bool(req_id)
            )
            if not evidence:
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": salary_raw or self._extract_salary_v38(json.dumps(row, ensure_ascii=True, default=str)),
                    "employment_type": employment_type,
                    "description": description,
                    "extraction_method": "tier0_json_api_v41",
                    "extraction_confidence": 0.82 if self._is_job_like_url_v38(source_url) else 0.74,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _iter_json_dicts_v39(self, payload: Any) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        stack: list[Any] = [payload]
        while stack:
            current = stack.pop()
            if isinstance(current, dict):
                out.append(current)
                for value in current.values():
                    if isinstance(value, (dict, list)):
                        stack.append(value)
            elif isinstance(current, list):
                for value in current:
                    if isinstance(value, (dict, list)):
                        stack.append(value)
            if len(out) >= 6000:
                break
        return out

    def _extract_repeating_rows_v38(self, root: etree._Element, page_url: str) -> list[dict]:
        groups: dict[str, list[etree._Element]] = defaultdict(list)

        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag not in {"li", "div", "article", "tr", "section"}:
                continue

            classes = _get_el_classes(el)
            if not classes or not _ROW_CLASS_PATTERN_V38.search(classes):
                continue

            tokens = classes.split()
            if not tokens:
                continue
            key = f"{tag}:{tokens[0]}"
            groups[key].append(el)

        jobs: list[dict] = []

        for rows in groups.values():
            if len(rows) < 3:
                continue
            for row in rows[:MAX_JOBS_PER_PAGE]:
                job = self._extract_row_job_v38(row, page_url, "tier2_repeating_rows_v38", 0.72)
                if job:
                    jobs.append(job)

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_elementor_cards_v38(self, root: etree._Element, page_url: str) -> list[dict]:
        cards = root.xpath(
            "//*[contains(@class,'elementor-column') and contains(@class,'elementor-inner-column')]"
        )
        if len(cards) < 2:
            return []

        jobs: list[dict] = []
        for card in cards[:320]:
            heading_nodes = card.xpath(".//h2[contains(@class,'elementor-heading-title')][1]")
            if not heading_nodes:
                continue
            title = self._normalize_title_v38(_text(heading_nodes[0]))

            source_url = page_url
            for a_el in card.xpath(".//a[@href]"):
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v38(href):
                    continue
                source_url = href
                if self._is_job_like_url_v38(href):
                    break
            if not self._title_has_role_signal_v38(title):
                if not (_DEPARTMENT_LABEL_PATTERN_V42.match(title) and self._has_strong_detail_url_v42(source_url)):
                    continue

            card_text = _text(card)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v38(card, title),
                    "salary_raw": self._extract_salary_v38(card_text),
                    "employment_type": self._extract_employment_type_v38(card_text),
                    "description": self._clean_description_v38(card_text),
                    "extraction_method": "tier2_elementor_cards_v38",
                    "extraction_confidence": 0.76,
                }
            )

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_accordion_jobs_v38(self, root: etree._Element, page_url: str) -> list[dict]:
        rows = root.xpath(
            "//*[contains(@class,'accordion-item') or contains(@class,'accordion__item') or contains(@class,'collapse-item')]"
        )
        if len(rows) < 1:
            return []

        jobs: list[dict] = []
        for row in rows[:MAX_JOBS_PER_PAGE]:
            heading = row.xpath(".//h2[1] | .//h3[1] | .//button[1]")
            if not heading:
                continue

            title = self._normalize_title_v38(_text(heading[0]))
            row_text = _text(row)
            if not self._title_has_role_signal_v38(title):
                continue
            if not _APPLY_EVIDENCE_PATTERN_V38.search(row_text) and len(row_text) < 150:
                continue

            source_url = page_url
            for a_el in row.xpath(".//a[@href]"):
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v38(href):
                    continue
                source_url = href
                if self._is_job_like_url_v38(href):
                    break

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v38(row, title),
                    "salary_raw": self._extract_salary_v38(row_text),
                    "employment_type": self._extract_employment_type_v38(row_text),
                    "description": self._clean_description_v38(row_text),
                    "extraction_method": "tier2_accordion_v38",
                    "extraction_confidence": 0.72,
                }
            )

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_job_links_v38(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        for a_el in root.xpath("//a[@href]")[:5000]:
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url)
            if not source_url or self._is_non_job_url_v38(source_url):
                continue

            heading_nodes = a_el.xpath(".//h1 | .//h2 | .//h3 | .//h4 | .//h5")
            raw_title = _text(heading_nodes[0]) if heading_nodes else _text(a_el)
            title = self._normalize_title_v38(raw_title)
            if not self._is_valid_title_v38(title):
                continue

            context = a_el.getparent()
            if context is None:
                context = a_el
            context_text = _text(context)

            url_hint = self._is_job_like_url_v38(source_url)
            apply_hint = bool(_APPLY_EVIDENCE_PATTERN_V38.search(context_text))

            if not (url_hint or apply_hint):
                continue
            if not self._title_has_role_signal_v38(title) and not (self._is_acronym_title_v38(title) and url_hint):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v38(context, title),
                    "salary_raw": self._extract_salary_v38(context_text),
                    "employment_type": self._extract_employment_type_v38(context_text),
                    "description": self._clean_description_v38(context_text),
                    "extraction_method": "tier2_job_links_v38",
                    "extraction_confidence": 0.74 if url_hint else 0.67,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_heading_rows_v38(self, root: etree._Element, page_url: str) -> list[dict]:
        headings = root.xpath("//h2 | //h3 | //h4")
        if len(headings) < 2:
            return []

        jobs: list[dict] = []
        for heading in headings[:400]:
            title = self._normalize_title_v38(_text(heading))
            if not self._is_valid_title_v38(title):
                continue
            if not self._title_has_role_signal_v38(title):
                continue
            if _DEPARTMENT_LABEL_PATTERN_V42.match(title):
                continue

            container = heading.getparent()
            if container is None:
                container = heading
            row_text = _text(container)
            if len(row_text) < 20:
                continue

            source_url = page_url
            for link in container.xpath(".//a[@href]"):
                href = _resolve_url(link.get("href"), page_url)
                if not href or self._is_non_job_url_v38(href):
                    continue
                source_url = href
                if self._is_job_like_url_v38(href):
                    break

            has_apply_context = bool(_APPLY_EVIDENCE_PATTERN_V38.search(row_text))
            has_detail_url = self._is_job_like_url_v38(source_url)
            if not (has_apply_context or has_detail_url or len(row_text) >= 120):
                continue
            if len(title.split()) <= 2 and not (has_apply_context or has_detail_url):
                continue

            location = None
            for loc_el in container.xpath(".//p | .//span | .//div"):
                loc_text = " ".join(_text(loc_el).split())
                if not loc_text or loc_text == title:
                    continue
                if 2 < len(loc_text) < 120 and re.search(r"[A-Za-z]", loc_text):
                    location = loc_text
                    break

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": self._extract_salary_v38(row_text),
                    "employment_type": self._extract_employment_type_v38(row_text),
                    "description": self._clean_description_v38(row_text),
                    "extraction_method": "tier2_heading_rows_v38",
                    "extraction_confidence": 0.7 if has_detail_url else 0.66,
                }
            )

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_greenhouse_posts_v38(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        for a_el in root.xpath("//a[@href]")[:4000]:
            href = (a_el.get("href") or "").strip()
            source_url = _resolve_url(href, page_url)
            if not source_url:
                continue
            if not re.search(r"/jobs/[0-9]{4,}", source_url, re.IGNORECASE):
                continue
            if self._is_non_job_url_v38(source_url):
                continue

            row = a_el
            cursor = a_el
            for _ in range(6):
                parent = cursor.getparent()
                if parent is None:
                    break
                if "job-post" in _get_el_classes(parent).lower():
                    row = parent
                    break
                cursor = parent

            title = ""
            title_nodes = row.xpath(".//*[contains(@class,'body--medium')][1]")
            if title_nodes:
                title = self._normalize_title_v38(_text(title_nodes[0]))
            if not title:
                title = self._normalize_title_v38(_text(a_el))
            if not self._is_valid_title_v38(title):
                continue
            if not self._title_has_role_signal_v38(title):
                continue

            location = None
            location_nodes = row.xpath(".//*[contains(@class,'body--metadata')][1]")
            if location_nodes:
                location = " ".join(_text(location_nodes[0]).split())[:180]

            row_text = _text(row)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": self._extract_salary_v38(row_text),
                    "employment_type": self._extract_employment_type_v38(row_text),
                    "description": self._clean_description_v38(row_text),
                    "extraction_method": "tier2_greenhouse_posts_v38",
                    "extraction_confidence": 0.85,
                }
            )

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_row_job_v38(
        self,
        row: etree._Element,
        page_url: str,
        method: str,
        confidence: float,
    ) -> Optional[dict]:
        links = row.xpath(".//a[@href]")
        heading = row.xpath(".//h1[1] | .//h2[1] | .//h3[1] | .//h4[1]")

        title_raw = ""
        if heading:
            title_raw = _text(heading[0])
        elif links:
            title_raw = _text(links[0])
        else:
            first_cell = row.xpath(".//td[1]")
            if first_cell:
                title_raw = _text(first_cell[0])

        title = self._normalize_title_v38(title_raw)
        if not self._is_valid_title_v38(title):
            return None

        source_url = page_url
        for link in links:
            href = _resolve_url(link.get("href"), page_url)
            if not href or self._is_non_job_url_v38(href):
                continue
            source_url = href
            if self._is_job_like_url_v38(href):
                break

        row_text = _text(row)
        if not self._title_has_role_signal_v38(title):
            if not (self._is_acronym_title_v38(title) and self._is_job_like_url_v38(source_url)):
                return None

        return {
            "title": title,
            "source_url": source_url,
            "location_raw": self._extract_location_v38(row, title),
            "salary_raw": self._extract_salary_v38(row_text),
            "employment_type": self._extract_employment_type_v38(row_text),
            "description": self._clean_description_v38(row_text),
            "extraction_method": method,
            "extraction_confidence": confidence,
        }

    def _pick_best_candidate_v38(
        self,
        candidates: list[tuple[str, list[dict]]],
        page_url: str,
    ) -> tuple[str, list[dict], float]:
        if not candidates:
            return "", [], 0.0

        scored: list[tuple[str, list[dict], float]] = []
        for label, jobs in candidates:
            prepared = self._prepare_candidate_jobs_v38(jobs, page_url)
            if not prepared:
                continue
            score = self._candidate_score_v38(prepared, page_url)
            scored.append((label, prepared, score))

        if not scored:
            return "", [], 0.0

        best_label, best_jobs, best_score = max(scored, key=lambda item: item[2])

        for label, jobs, score in scored:
            if label == best_label:
                continue
            overlap = self._title_overlap_ratio_v38(jobs, best_jobs)
            if len(jobs) >= len(best_jobs) + 1 and overlap >= 0.58 and score >= best_score - 1.4:
                best_label, best_jobs, best_score = label, jobs, score

        return best_label, best_jobs[:MAX_JOBS_PER_PAGE], best_score

    def _prepare_candidate_jobs_v38(self, jobs: list[dict], page_url: str) -> list[dict]:
        cleaned: list[dict] = []

        for idx, raw in enumerate(jobs):
            title = self._normalize_title_v38(raw.get("title", ""))
            source_url = _resolve_url(raw.get("source_url"), page_url) or page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]

            if self._is_non_job_url_v38(source_url):
                continue
            if not self._is_title_acceptable_v38(title, source_url):
                continue

            desc = self._clean_description_v38(str(raw.get("description") or ""))
            cleaned.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": raw.get("location_raw") or None,
                    "salary_raw": raw.get("salary_raw") or None,
                    "employment_type": raw.get("employment_type") or self._extract_employment_type_v38(desc or ""),
                    "description": desc,
                    "extraction_method": raw.get("extraction_method") or "tier2_v38",
                    "extraction_confidence": raw.get("extraction_confidence", 0.65),
                    "_order": idx,
                }
            )

        deduped = self._dedupe_jobs_v38(cleaned, page_url)
        if not self._is_valid_jobset_v38(deduped, page_url):
            return []
        return deduped

    def _dedupe_jobs_v38(self, jobs: list[dict], page_url: str) -> list[dict]:
        by_key: dict[tuple[str, str], dict] = {}

        for job in jobs:
            title = self._normalize_title_v38(job.get("title", ""))
            url = _resolve_url(job.get("source_url"), page_url) or page_url
            key = (title.lower(), url.lower())
            existing = by_key.get(key)
            if existing is None:
                by_key[key] = job
                continue

            if self._title_quality_score_v38(title) > self._title_quality_score_v38(existing.get("title", "")):
                by_key[key] = job

        deduped = sorted(by_key.values(), key=lambda item: int(item.get("_order", 0)))
        for item in deduped:
            item.pop("_order", None)
        return deduped[:MAX_JOBS_PER_PAGE]

    def _is_valid_jobset_v38(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title_v38(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return False

        role_hits = sum(1 for t in titles if self._title_has_role_signal_v38(t))
        reject_hits = sum(1 for t in titles if _TITLE_REJECT_PATTERN_V38.match(t.lower()))
        evidence_hits = sum(1 for j in jobs if self._job_has_evidence_v38(j, page_url))
        strong_url_hits = sum(
            1 for j in jobs if self._has_strong_detail_url_v42(j.get("source_url") or "")
        )

        if reject_hits >= max(1, int(len(titles) * 0.25)):
            return False

        if len(titles) <= 4:
            required_role_hits = max(2, int(len(titles) * 0.7))
            required_evidence_hits = max(1, int(len(titles) * 0.5))
            if evidence_hits < required_evidence_hits:
                return False
            if role_hits >= required_role_hits:
                return True
            # Allow sparse-title vocab sets when most rows have strong detail URLs.
            if role_hits >= 1 and strong_url_hits >= max(2, len(titles) - 1):
                return True
            return False

        if len(titles) >= MIN_JOBS_FOR_SUCCESS:
            if role_hits < max(1, int(len(titles) * 0.6)):
                return False
            if evidence_hits < max(1, int(len(titles) * 0.3)):
                return False
            return True

        # Small sets are accepted only with strong precision.
        if role_hits < len(titles):
            return False
        return evidence_hits >= max(1, len(titles) - 1)

    def _candidate_score_v38(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        titles = [self._normalize_title_v38(j.get("title", "")) for j in jobs]
        role_hits = sum(1 for t in titles if self._title_has_role_signal_v38(t))
        reject_hits = sum(1 for t in titles if _TITLE_REJECT_PATTERN_V38.match(t.lower()))
        detail_hits = sum(1 for j in jobs if self._is_job_like_url_v38(j.get("source_url") or ""))
        evidence_hits = sum(1 for j in jobs if self._job_has_evidence_v38(j, page_url))

        score = len(jobs) * 4.8
        score += role_hits * 2.6
        score += detail_hits * 1.5
        score += evidence_hits * 1.8
        score -= reject_hits * 6.0
        return score

    def _has_strong_detail_url_v42(self, source_url: str) -> bool:
        url = (source_url or "").strip()
        if not url or self._is_non_job_url_v38(url):
            return False
        if self._is_job_like_url_v38(url):
            return True

        parsed = urlparse(url)
        path = (parsed.path or "").lower()
        if re.search(r"/jobs?/[^/?#]{4,}", path):
            leaf = path.rstrip("/").split("/")[-1]
            if leaf not in {
                "job",
                "jobs",
                "careers",
                "career",
                "openings",
                "vacancies",
                "positions",
            }:
                return True
        query = parsed.query.lower()
        if re.search(r"(?:^|&)(?:jobid|job_id|requisitionid|vacancyid|jobadid|adid|ajid)=", query):
            return True
        parts = [p for p in path.split("/") if p]
        if parts:
            leaf = parts[-1]
            if leaf not in {
                "career",
                "careers",
                "jobs",
                "job",
                "vacancies",
                "vacancy",
                "openings",
                "positions",
                "position",
                "join-our-team",
                "lowongan",
                "loker",
            } and len(leaf) >= 6:
                if "-" in leaf or len(parts) >= 2 or re.search(r"[0-9]", leaf):
                    return True
        return False

    def _job_has_evidence_v38(self, job: dict, page_url: str) -> bool:
        source_url = (job.get("source_url") or page_url).strip() or page_url
        desc = job.get("description") or ""

        if self._has_strong_detail_url_v42(source_url):
            return True
        if source_url.rstrip("/") != page_url.rstrip("/") and not self._is_non_job_url_v38(source_url):
            parsed = urlparse(source_url)
            parts = [p for p in (parsed.path or "").split("/") if p]
            if parts:
                leaf = parts[-1].lower()
                if leaf not in {
                    "career", "careers", "jobs", "job", "vacancies", "vacancy",
                    "openings", "join-our-team", "current-vacancies", "lowongan",
                    "loker", "positions", "position", "address", "alamat", "kantor",
                } and len(leaf) >= 4:
                    if re.search(r"[0-9]", leaf) or "-" in leaf or len(leaf) >= 12 or len(parts) >= 3:
                        return True
                if (
                    len(parts) >= 2
                    and not _GENERIC_LISTING_PATH_PATTERN_V41.search(parsed.path or "")
                    and any(re.search(r"[0-9]", part) for part in parts)
                ):
                    return True
        if job.get("salary_raw") or job.get("employment_type") or job.get("location_raw"):
            return True
        if _APPLY_EVIDENCE_PATTERN_V38.search(desc or ""):
            return True
        return len((desc or "").strip()) >= 180

    def _normalize_title_v38(self, title: str) -> str:
        value = html_lib.unescape((title or "").strip())
        value = re.sub(r"\s+", " ", value)
        value = value.strip(" \t\r\n-–|:;,>")
        value = re.sub(r"\s*(?:apply\s+now|apply\s+here|read\s+more|learn\s+more|info\s+lengkap)\s*$", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s*(?:deadline\s*:\s*\S+.*|closing\s+date\s*:\s*\S+.*)$", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+OR\s+.*$", "", value, flags=re.IGNORECASE)
        value = value.strip(" \t\r\n-–|:;,")
        value = re.sub(r"\.+$", "", value).strip()
        return value

    def _is_valid_title_v38(self, title: str) -> bool:
        if not title:
            return False

        t = title.strip()
        if len(t) < 4 or len(t) > 180:
            return False
        if not re.search(r"[A-Za-z]", t):
            return False
        if _TITLE_REJECT_PATTERN_V38.match(t.lower()):
            return False
        return True

    def _is_title_acceptable_v38(self, title: str, source_url: str) -> bool:
        if self._is_valid_title_v38(title):
            return True
        if _DEPARTMENT_LABEL_PATTERN_V42.match(title or "") and self._has_strong_detail_url_v42(source_url):
            return True
        return self._is_acronym_title_v38(title) and self._is_job_like_url_v38(source_url)

    def _title_has_role_signal_v38(self, title: str) -> bool:
        if not title:
            return False
        return bool(_ROLE_HINT_PATTERN_V38.search(title) or _title_has_job_noun(title) or self._is_acronym_title_v38(title))

    @staticmethod
    def _is_acronym_title_v38(title: str) -> bool:
        t = (title or "").strip()
        if not _ACRONYM_TITLE_PATTERN_V38.match(t):
            return False
        return t.lower() not in {"home", "menu", "faq", "apply"}

    def _is_job_like_url_v38(self, url: str) -> bool:
        value = (url or "").strip()
        if not value:
            return False
        if self._is_non_job_url_v38(value):
            return False
        return bool(_DETAILISH_URL_PATTERN_V38.search(value))

    @staticmethod
    def _is_non_job_url_v38(url: str) -> bool:
        value = (url or "").strip().lower()
        if not value:
            return False
        if value.startswith("mailto:") or value.startswith("tel:"):
            return True
        return bool(_NON_JOB_URL_PATTERN_V38.search(value))

    def _title_quality_score_v38(self, title: str) -> float:
        t = self._normalize_title_v38(title)
        if not t:
            return -10.0
        score = 0.0
        score += 2.0 if self._title_has_role_signal_v38(t) else 0.0
        score += 1.0 if self._is_valid_title_v38(t) else 0.0
        score -= max(0.0, (len(t) - 90) / 80.0)
        return score

    def _title_overlap_ratio_v38(self, a_jobs: list[dict], b_jobs: list[dict]) -> float:
        a_titles = {self._normalize_title_v38(j.get("title", "")).lower() for j in a_jobs if j.get("title")}
        b_titles = {self._normalize_title_v38(j.get("title", "")).lower() for j in b_jobs if j.get("title")}
        if not a_titles or not b_titles:
            return 0.0
        return len(a_titles & b_titles) / max(1, min(len(a_titles), len(b_titles)))

    def _page_listing_score_v38(self, page_url: str, html_body: str) -> float:
        if not html_body or len(html_body) < 200:
            return -20.0
        lower = html_body.lower()
        score = 0.0

        score += min(lower.count("apply now"), 10)
        score += min(lower.count("job"), 10)
        score += min(lower.count("career"), 6)

        if _LISTING_URL_PATTERN_V38.search(page_url or ""):
            score += 3.0
        if "portal.na" in (page_url or "").lower():
            score += 5.0

        root = _parse_html(html_body)
        if root is not None:
            listing_links = 0
            role_links = 0
            for a_el in root.xpath("//a[@href]"):
                href = (a_el.get("href") or "").strip()
                text = self._safe_text_v38(a_el)
                if _LISTING_URL_PATTERN_V38.search(href) or _LISTING_LINK_TEXT_PATTERN_V38.search(text):
                    listing_links += 1
                if _ROLE_HINT_PATTERN_V38.search(text):
                    role_links += 1
            score += min(listing_links * 1.3, 18.0)
            score += min(role_links * 2.0, 18.0)

        return score

    @staticmethod
    def _looks_non_html_payload_v38(body: Optional[str]) -> bool:
        if not body:
            return True
        sample = body[:900].lstrip()
        if sample.startswith("%PDF-"):
            return True
        low = sample.lower()
        if (low.startswith("{") or low.startswith("[")) and "<html" not in low[:300]:
            return True
        return False

    @staticmethod
    def _is_related_host_v38(parent_url: str, child_url: str) -> bool:
        p = urlparse(parent_url).hostname or ""
        c = urlparse(child_url).hostname or ""
        if not p or not c:
            return False
        if p == c:
            return True

        p_parts = p.rsplit(".", 2)
        c_parts = c.rsplit(".", 2)
        p_base = ".".join(p_parts[-2:]) if len(p_parts) >= 2 else p
        c_base = ".".join(c_parts[-2:]) if len(c_parts) >= 2 else c
        return p_base == c_base

    @staticmethod
    def _is_rejected_listing_link_v38(href: str, text: str) -> bool:
        href_l = (href or "").lower()
        text_l = (text or "").lower()
        if _NON_JOB_URL_PATTERN_V38.search(href_l):
            return True
        if re.search(r"\b(?:our\s+culture|our\s+values|talent\s+stories?|our\s+ecosystem)\b", text_l):
            return True
        return False

    @staticmethod
    def _safe_text_v38(el: etree._Element) -> str:
        try:
            txt = el.text_content()
            if txt:
                return " ".join(txt.split())
        except Exception:
            pass
        try:
            txt = etree.tostring(el, method="text", encoding="unicode")
            return " ".join((txt or "").split())
        except Exception:
            return ""

    def _clean_description_v38(self, description: str) -> Optional[str]:
        text = " ".join((description or "").split())
        if not text:
            return None
        cut = re.search(r"\b(?:how\s+to\s+apply|cara\s+melamar|application\s+process)\b", text, re.IGNORECASE)
        if cut:
            text = text[: cut.start()].strip()
        if len(text) < 45:
            return None
        return text[:5000]

    def _extract_location_v38(self, row: etree._Element, title: str) -> Optional[str]:
        for el in row.iter():
            if not isinstance(el.tag, str):
                continue
            cls = _get_el_classes(el)
            if "location" in cls or "map-marker" in cls:
                loc = " ".join(_text(el).split())
                if loc and loc != title and 2 < len(loc) < 160:
                    return loc

        row_text = _text(row)
        match = _AU_LOCATIONS.search(row_text) if row_text else None
        if match:
            return match.group(0)[:120]
        return None

    @staticmethod
    def _extract_salary_v38(text: str) -> Optional[str]:
        if not text:
            return None
        match = _SALARY_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:120]
        return None

    @staticmethod
    def _extract_employment_type_v38(text: str) -> Optional[str]:
        if not text:
            return None
        match = _JOB_TYPE_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:80]
        return None
