"""
Tiered Extraction Engine v4.6 — direct from v1.6.

Focused, lower-complexity improvements:
1. Shell-aware short-page recovery with targeted listing probes.
2. Tier-0 structured extraction (JSON-LD + embedded state JSON).
3. Deterministic Martian/MyRecruitmentPlus recovery (same-path query matrix,
   API/_next-data probes, and strict payload parsing).
4. Utility-card extraction for generic ATS rows (e.g. p-4/mx-4 card lists).
5. Teaser-aware detail enrichment refresh for richer metadata depth.
6. Light traversal for high-signal listing subpages and ATS iframes.
"""

from __future__ import annotations

import asyncio
import html as html_lib
import json
import logging
import re
from collections import defaultdict
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode, urlparse

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


_ROLE_HINT_PATTERN_V46 = re.compile(
    r"\b(?:engineer|developer|manager|director|analyst|specialist|assistant|"
    r"consultant|coordinator|officer|administrator|accountant|technician|"
    r"designer|architect|operator|supervisor|advisor|executive|intern(?:ship)?|"
    r"recruit(?:er)?|nurse|teacher|driver|chef|chemist|mechanic|associate|"
    r"representative|agent|planner|liaison|akuntan|konsultan|asisten|pegawai|"
    r"karyawan|influencer|videografer|fotografer|psikolog(?:i)?|model|sarjana|"
    r"fashion|full\s*stack|fullstack|devops|owner|customer\s+service)\b",
    re.IGNORECASE,
)

_TITLE_REJECT_PATTERN_V46 = re.compile(
    r"^(?:join\s+our\s+team|current\s+jobs?|all\s+jobs?|job\s+openings?|"
    r"search\s+jobs?|browse\s+jobs?|view\s+all\s+jobs?|careers?|"
    r"about\s+us|our\s+culture|our\s+values?|our\s+ecosystem|"
    r"contact|home|menu|read\s+more|learn\s+more|show\s+more|load\s+more|"
    r"info\s+lengkap|apply(?:\s+now)?|job\s+details?|role\s+details?|"
    r"lowongan(?:\s+kerja(?:\s+[a-z]+)?)?)$",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V46 = re.compile(
    r"(?:/(?:privacy|terms|cookie|news|blog|about|contact|investor|team|culture|"
    r"our-culture|our-values|our-ecosystem|services?|leadership|people|login|"
    r"logout|register|account|help|support|wp-json|feed|rss)(?:/|$|[?#])|"
    r"\.pdf(?:$|\?)|\.docx?(?:$|\?))",
    re.IGNORECASE,
)

_DETAILISH_URL_PATTERN_V46 = re.compile(
    r"(?:/jobs?/[^/?#]{4,}|/career/openings?/|/jobdetails(?:/|$|\?)|"
    r"PortalDetail\.na\?.*jobid=|[?&](?:jobid|job_id|requisitionid|positionid|"
    r"vacancyid|jobadid|adid|ajid)=)",
    re.IGNORECASE,
)

_APPLY_EVIDENCE_PATTERN_V46 = re.compile(
    r"(?:apply|application|apply\s+now|apply\s+here|requirements?|"
    r"qualifications?|responsibilit|closing\s+date|full\s*time|part\s*time|"
    r"contract|permanent|temporary|how\s+to\s+apply|cara\s+melamar)",
    re.IGNORECASE,
)

_LISTING_LINK_TEXT_PATTERN_V46 = re.compile(
    r"\b(?:job\s+openings?|current\s+vacancies|join\s+our\s+team|view\s+all\s+jobs?|"
    r"search\s+jobs|browse\s+jobs|lowongan|kerjaya|karir|loker|careers?)\b",
    re.IGNORECASE,
)

_LISTING_URL_PATTERN_V46 = re.compile(
    r"/(?:careers?|jobs?|job-openings?|openings?|vacancies?|position|requisition|"
    r"portal\.na|candidateportal|join-our-team|lowongan|loker|kerjaya|karir)",
    re.IGNORECASE,
)

_MARTIAN_SHELL_PATTERN_V46 = re.compile(
    r"(?:myrecruitmentplus|martianlogic|clientcode|recruiterid|jobboardthemeid|__NEXT_DATA__)",
    re.IGNORECASE,
)

_NEXT_DATA_PATTERN_V46 = re.compile(
    r"<script[^>]+id=['\"]__NEXT_DATA__['\"][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)

_HTML_FRAGMENT_MARKER_PATTERN_V46 = re.compile(
    r"(?:\bp-4\b|\bmx-4\b|cursor-pointer|apply\s+now|job-card|elementor-column|"
    r"info\s+lengkap)",
    re.IGNORECASE,
)

_STATUS_SUFFIX_PATTERN_V46 = re.compile(
    r"(?:\s*[-|]?\s*(?:just\s+posted|posted\s+(?:today|yesterday)|"
    r"\d+\s+(?:minutes?|hours?|days?|weeks?)\s+ago|new(?:\s+role)?))\s*!?\s*$",
    re.IGNORECASE,
)

_MARKETING_TITLE_FRAGMENT_V46 = re.compile(
    r"\b(?:tell\s+us|learn\s+more|read\s+more|meet\s+our|discover|submit|"
    r"our\s+services|our\s+team|our\s+culture)\b",
    re.IGNORECASE,
)

_SCRIPT_SRC_PATTERN_V46 = re.compile(
    r"<script[^>]+src=['\"]([^\"']+)['\"][^>]*>",
    re.IGNORECASE,
)

_CTA_CARD_LINK_TEXT_PATTERN_V46 = re.compile(
    r"^(?:learn\s+more|read\s+more|view\s+details?|info\s+lengkap|apply(?:\s+now)?|"
    r"lihat\s+detail|selengkapnya)$",
    re.IGNORECASE,
)

_IFRAME_LISTING_PATTERN_V46 = re.compile(
    r"(?:greenhouse|lever|smartrecruiters|workday|myworkdayjobs|icims|"
    r"myrecruitmentplus|martianlogic|job|career|vacanc|opening)",
    re.IGNORECASE,
)

_DETAIL_DESC_SELECTORS_V46 = (
    "article",
    ".job-description",
    ".job__description",
    ".description",
    ".entry-content",
    "main",
)


class TieredExtractorV46(TieredExtractorV16):
    """v4.6 extractor with v1.6-first fallback arbitration and focused shell recovery."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        recovered_html = await self._recover_short_html_v46(page_url, working_html)
        if recovered_html:
            working_html = recovered_html

        candidates: list[tuple[str, list[dict]]] = []

        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(super().extract(career_page, company, working_html), timeout=24.0)
        except asyncio.TimeoutError:
            logger.warning("v4.6 parent extractor timeout for %s", page_url)
        except Exception:
            logger.exception("v4.6 parent extractor failed for %s", page_url)

        parent_jobs = self._prepare_candidate_jobs_v46(parent_jobs or [], page_url)
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        local_jobs = self._extract_local_jobs_v46(page_url, working_html)
        if local_jobs:
            candidates.append(("local_v46", local_jobs))

        structured_jobs = self._extract_state_and_jsonld_jobs_v46(working_html, page_url)
        if structured_jobs:
            candidates.append(("structured_v46", structured_jobs))

        best_label, best_jobs, best_score = self._pick_best_candidate_v46(candidates, page_url)

        if self._needs_deep_recovery_v46(page_url, working_html, best_jobs, best_score):
            martian_jobs = await self._extract_martian_jobs_v46(page_url, working_html)
            if martian_jobs:
                candidates.append(("martian_v46", martian_jobs))

            rss_jobs = await self._extract_rss_jobs_v46(page_url, working_html)
            if rss_jobs:
                candidates.append(("rss_v46", rss_jobs))

            subpage_jobs = await self._follow_listing_subpages_v46(page_url, working_html)
            if subpage_jobs:
                candidates.append(("subpage_follow_v46", subpage_jobs))

            iframe_jobs = await self._extract_iframe_jobs_v46(page_url, working_html)
            if iframe_jobs:
                candidates.append(("iframe_v46", iframe_jobs))

            best_label, best_jobs, best_score = self._pick_best_candidate_v46(candidates, page_url)

        if not best_jobs:
            return []

        if self._should_enrich_detail_pages_v46(best_jobs, page_url):
            try:
                best_jobs = await asyncio.wait_for(self._enrich_from_detail_pages(best_jobs), timeout=16.0)
            except asyncio.TimeoutError:
                logger.warning("v4.6 enrichment timeout for %s", page_url)
            except Exception:
                logger.exception("v4.6 enrichment failed for %s", page_url)

        final_jobs = self._prepare_candidate_jobs_v46(best_jobs, page_url)
        return final_jobs[:MAX_JOBS_PER_PAGE]

    async def _recover_short_html_v46(self, page_url: str, html_body: str) -> Optional[str]:
        body = html_body or ""
        shell_like = self._looks_like_shell_v46(page_url, body)
        short_or_failed = len(body) < 300 or "FETCH FAILED" in body[:120]
        if not (shell_like or short_or_failed):
            return body

        parsed = urlparse(page_url)
        if not parsed.scheme or not parsed.netloc:
            return body

        base = f"{parsed.scheme}://{parsed.netloc}"
        probes = [
            page_url,
            page_url.rstrip("/") + "/",
            f"{base}/careers",
            f"{base}/careers/",
            f"{base}/jobs",
            f"{base}/jobs/",
            f"{base}/career/job-openings",
            f"{base}/careers/join-our-team",
            f"{base}/lowongan",
        ]

        best_html = body
        best_score = self._page_listing_score_v46(page_url, body)
        seen: set[str] = set()

        async with httpx.AsyncClient(
            timeout=7,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        ) as client:
            for probe in probes:
                norm = probe.rstrip("/")
                if not probe or norm in seen:
                    continue
                seen.add(norm)
                try:
                    resp = await client.get(probe)
                except Exception:
                    continue

                text = resp.text or ""
                if resp.status_code != 200 or len(text) < 200:
                    continue
                if self._looks_non_html_payload_v46(text):
                    continue

                score = self._page_listing_score_v46(str(resp.url), text)
                if score > best_score + 0.2:
                    best_html = text
                    best_score = score

        return best_html

    def _extract_local_jobs_v46(self, page_url: str, html_body: str) -> list[dict]:
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
            _label, jobs, _score = self._pick_best_candidate_v46(candidates, page_url)
            return jobs

        utility_rows = self._extract_utility_card_rows_v46(root, page_url)
        if utility_rows:
            candidates.append(("utility_rows_v46", utility_rows))

        elementor_cards = self._extract_elementor_cards_v46(root, page_url)
        if elementor_cards:
            candidates.append(("elementor_cards_v46", elementor_cards))

        cta_cards = self._extract_heading_cta_cards_v46(root, page_url)
        if cta_cards:
            candidates.append(("heading_cta_v46", cta_cards))

        greenhouse_posts = self._extract_greenhouse_posts_v46(root, page_url)
        if greenhouse_posts:
            candidates.append(("greenhouse_posts_v46", greenhouse_posts))

        job_links = self._extract_job_links_v46(root, page_url)
        if job_links:
            candidates.append(("job_links_v46", job_links))

        _label, jobs, _score = self._pick_best_candidate_v46(candidates, page_url)
        return jobs

    def _extract_state_and_jsonld_jobs_v46(self, html_body: str, page_url: str) -> list[dict]:
        if not html_body or len(html_body) < 120:
            return []

        candidates: list[tuple[str, list[dict]]] = []

        state_jobs: list[dict] = []
        for match in _NEXT_DATA_PATTERN_V46.finditer(html_body):
            payload = (match.group(1) or "").strip()
            if not payload:
                continue
            try:
                parsed = json.loads(payload)
            except Exception:
                continue
            state_jobs.extend(self._extract_jobs_from_json_payload_v46(parsed, page_url, "tier0_next_data_v46"))

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
            state_jobs.extend(self._extract_jobs_from_json_payload_v46(parsed, page_url, "tier0_state_json_v46"))

        if state_jobs:
            candidates.append(("state_json_v46", state_jobs))

        jsonld_jobs: list[dict] = []
        for match in re.finditer(
            r"<script[^>]+type=['\"]application/ld\+json['\"][^>]*>(.*?)</script>",
            html_body,
            re.IGNORECASE | re.DOTALL,
        ):
            payload = html_lib.unescape((match.group(1) or "").strip())
            if not payload or payload.startswith("<"):
                continue
            try:
                parsed = json.loads(payload)
            except Exception:
                continue

            for node in self._iter_json_dicts_v46(parsed):
                type_field = node.get("@type")
                if isinstance(type_field, list):
                    types = {str(v).lower() for v in type_field}
                    is_job = "jobposting" in types
                else:
                    is_job = str(type_field or "").lower() == "jobposting"
                if not is_job:
                    continue

                title = self._normalize_title_v46(str(node.get("title") or node.get("name") or ""))
                if not self._is_title_acceptable_v46(title, str(node.get("url") or "")):
                    continue

                source_url = _resolve_url(
                    node.get("url") or node.get("sameAs") or node.get("applyUrl"),
                    page_url,
                ) or page_url
                if self._is_non_job_url_v46(source_url):
                    continue

                location = self._extract_location_from_json_v46(node)
                description = self._clean_description_v46(
                    str(node.get("description") or node.get("responsibilities") or node.get("qualifications") or "")
                )
                salary_raw = self._extract_salary_v46(json.dumps(node, ensure_ascii=True, default=str))
                employment_type = str(node.get("employmentType") or "").strip() or None

                evidence = (
                    self._is_job_like_url_v46(source_url)
                    or bool(location)
                    or bool(salary_raw)
                    or bool(employment_type)
                    or (description is not None and len(description) >= 140)
                )
                if not evidence:
                    continue

                jsonld_jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": location,
                        "salary_raw": salary_raw,
                        "employment_type": employment_type,
                        "description": description,
                        "extraction_method": "tier0_jsonld_v46",
                        "extraction_confidence": 0.84 if self._is_job_like_url_v46(source_url) else 0.78,
                    }
                )

        if jsonld_jobs:
            candidates.append(("jsonld_v46", jsonld_jobs))

        _label, best_jobs, _score = self._pick_best_candidate_v46(candidates, page_url)
        return best_jobs

    async def _extract_martian_jobs_v46(self, page_url: str, html_body: str) -> list[dict]:
        if not self._looks_like_martian_shell_v46(html_body):
            return []

        context = self._extract_martian_context_v46(page_url, html_body)
        if not (context.get("client_code") or context.get("recruiter_id") or context.get("build_id")):
            return []

        endpoints = self._martian_probe_urls_v46(page_url, context)
        if not endpoints:
            return []

        jobs: list[dict] = []
        request_budget = 20
        seen: set[str] = set()

        async with httpx.AsyncClient(
            timeout=3.2,
            follow_redirects=True,
            headers={
                "Accept": "application/json,text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Referer": page_url,
            },
        ) as client:
            for endpoint in endpoints[:28]:
                if request_budget <= 0:
                    break
                norm = endpoint.rstrip("/")
                if not endpoint or norm in seen:
                    continue
                seen.add(norm)
                request_budget -= 1
                try:
                    resp = await asyncio.wait_for(client.get(endpoint), timeout=3.3)
                except Exception:
                    continue
                if resp.status_code >= 400 or not resp.text:
                    continue

                extracted = self._extract_jobs_from_probe_payload_v46(resp.text, str(resp.url), page_url)
                if extracted:
                    jobs.extend(extracted)
                    if len(jobs) >= MAX_JOBS_PER_PAGE:
                        break

            if len(jobs) < MIN_JOBS_FOR_SUCCESS and request_budget > 0:
                post_endpoints = self._martian_post_endpoints_v46(endpoints)
                payloads = self._martian_post_payloads_v46(context)
                for endpoint in post_endpoints[:8]:
                    for payload in payloads[:5]:
                        if request_budget <= 0:
                            break
                        request_budget -= 1
                        try:
                            resp = await asyncio.wait_for(client.post(endpoint, json=payload), timeout=3.3)
                        except Exception:
                            continue
                        if resp.status_code >= 400 or not resp.text:
                            continue
                        extracted = self._extract_jobs_from_probe_payload_v46(resp.text, str(resp.url), page_url)
                        if extracted:
                            jobs.extend(extracted)
                            if len(jobs) >= MAX_JOBS_PER_PAGE:
                                break
                    if request_budget <= 0 or len(jobs) >= MAX_JOBS_PER_PAGE:
                        break

        return self._prepare_candidate_jobs_v46(jobs, page_url)

    async def _extract_rss_jobs_v46(self, page_url: str, html_body: str) -> list[dict]:
        if not html_body:
            return []

        feed_urls: list[str] = []
        for m in re.finditer(
            r"<link[^>]+rel=['\"]alternate['\"][^>]+type=['\"]application/rss\+xml['\"][^>]+href=['\"]([^\"']+)['\"]",
            html_body,
            re.IGNORECASE,
        ):
            feed = _resolve_url((m.group(1) or "").strip(), page_url)
            if feed:
                feed_urls.append(feed)

        if not feed_urls:
            return []

        jobs: list[dict] = []
        seen: set[str] = set()

        async with httpx.AsyncClient(
            timeout=6,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
            },
        ) as client:
            for feed_url in feed_urls[:3]:
                norm = feed_url.rstrip("/")
                if norm in seen:
                    continue
                seen.add(norm)

                try:
                    resp = await client.get(feed_url)
                except Exception:
                    continue
                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 200:
                    continue

                root = _parse_html(body)
                if root is None:
                    continue

                rows = root.xpath("//item | //entry")
                if not rows:
                    continue

                for row in rows[:MAX_JOBS_PER_PAGE]:
                    title = self._normalize_title_v46(
                        " ".join(row.xpath("./title//text()") or []).strip()
                    )
                    if not self._is_title_acceptable_v46(title, ""):
                        continue

                    link = " ".join(row.xpath("./link//text()") or []).strip()
                    if not link:
                        hrefs = row.xpath("./link/@href")
                        if hrefs:
                            link = str(hrefs[0]).strip()
                    source_url = _resolve_url(link, page_url) or page_url
                    if self._is_non_job_url_v46(source_url):
                        continue

                    desc = " ".join(row.xpath("./description//text() | ./summary//text()") or []).strip()
                    description = self._clean_description_v46(desc)
                    if not (self._is_job_like_url_v46(source_url) or (description and len(description) >= 120)):
                        continue

                    jobs.append(
                        {
                            "title": title,
                            "source_url": source_url,
                            "location_raw": None,
                            "salary_raw": self._extract_salary_v46(desc),
                            "employment_type": self._extract_employment_type_v46(desc),
                            "description": description,
                            "extraction_method": "tier0_rss_v46",
                            "extraction_confidence": 0.76,
                        }
                    )

        return self._prepare_candidate_jobs_v46(jobs, page_url)

    async def _follow_listing_subpages_v46(self, page_url: str, html_body: str) -> list[dict]:
        root = _parse_html(html_body)
        if root is None:
            return []

        candidates: list[tuple[str, float]] = []
        seen: set[str] = set()

        for a_el in root.xpath("//a[@href]"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            text = self._safe_text_v46(a_el)
            full_url = _resolve_url(href, page_url)
            if not full_url or full_url.rstrip("/") == page_url.rstrip("/"):
                continue
            if self._is_non_job_url_v46(full_url):
                continue

            norm = full_url.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)

            score = 0.0
            if _LISTING_URL_PATTERN_V46.search(full_url):
                score += 4.0
            if _LISTING_LINK_TEXT_PATTERN_V46.search(text):
                score += 3.2
            if score > 0:
                candidates.append((full_url, score))

        if not candidates:
            return []

        candidates.sort(key=lambda x: x[1], reverse=True)

        jobsets: list[tuple[str, list[dict]]] = []
        fetched = 0
        for subpage_url, _score in candidates[:10]:
            if fetched >= 4:
                break
            sub_html = await self._fetch_html_v46(subpage_url)
            if not sub_html:
                continue
            fetched += 1

            local_jobs = self._extract_local_jobs_v46(subpage_url, sub_html)
            if local_jobs:
                jobsets.append((f"sub_local_{fetched}", local_jobs))

            structured = self._extract_state_and_jsonld_jobs_v46(sub_html, subpage_url)
            if structured:
                jobsets.append((f"sub_structured_{fetched}", structured))

        _label, best_jobs, _score = self._pick_best_candidate_v46(jobsets, page_url)
        return best_jobs

    async def _extract_iframe_jobs_v46(self, page_url: str, html_body: str) -> list[dict]:
        root = _parse_html(html_body)
        if root is None:
            return []

        iframe_urls: list[str] = []
        seen: set[str] = set()
        for iframe in root.xpath("//iframe[@src]"):
            src = (iframe.get("src") or "").strip()
            full_url = _resolve_url(src, page_url)
            if not full_url:
                continue
            if not _IFRAME_LISTING_PATTERN_V46.search(full_url):
                continue
            norm = full_url.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
            iframe_urls.append(full_url)

        if not iframe_urls:
            return []

        jobsets: list[tuple[str, list[dict]]] = []
        fetched = 0
        for iframe_url in iframe_urls[:4]:
            iframe_html = await self._fetch_html_v46(iframe_url)
            if not iframe_html:
                continue
            fetched += 1

            local_jobs = self._extract_local_jobs_v46(iframe_url, iframe_html)
            if local_jobs:
                jobsets.append((f"iframe_local_{fetched}", local_jobs))

            structured = self._extract_state_and_jsonld_jobs_v46(iframe_html, iframe_url)
            if structured:
                jobsets.append((f"iframe_structured_{fetched}", structured))

        _label, best_jobs, _score = self._pick_best_candidate_v46(jobsets, page_url)
        return best_jobs

    async def _fetch_html_v46(self, url: str) -> Optional[str]:
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

        body = resp.text or ""
        if resp.status_code != 200 or len(body) < 200 or self._looks_non_html_payload_v46(body):
            return None
        return body

    def _extract_jobs_from_probe_payload_v46(self, body: str, response_url: str, page_url: str) -> list[dict]:
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
                jobs.extend(self._extract_jobs_from_json_payload_v46(parsed, response_url, "tier0_probe_json_v46"))

        root = _parse_html(payload)
        if root is not None:
            anchor_count = len(root.xpath("//a[@href]"))
            marker_hit = bool(_HTML_FRAGMENT_MARKER_PATTERN_V46.search(payload[:12000]))
            if anchor_count >= 2 or marker_hit:
                jobs.extend(self._extract_local_jobs_v46(response_url, payload))

        return self._prepare_candidate_jobs_v46(jobs, page_url)

    def _extract_jobs_from_json_payload_v46(self, payload: Any, page_url: str, method: str) -> list[dict]:
        jobs: list[dict] = []

        for row in self._iter_json_dicts_v46(payload):
            title = self._normalize_title_v46(
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
            if not self._is_valid_title_v46(title):
                continue
            if not self._title_has_role_signal_v46(title):
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

            if self._is_non_job_url_v46(source_url):
                continue

            location = self._extract_location_from_json_v46(row)
            description = self._clean_description_v46(
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
            salary_raw = self._extract_salary_v46(json.dumps(row, ensure_ascii=True, default=str))

            evidence = (
                self._is_job_like_url_v46(source_url)
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
                    "salary_raw": salary_raw,
                    "employment_type": employment_type,
                    "description": description,
                    "extraction_method": method,
                    "extraction_confidence": 0.82 if self._is_job_like_url_v46(source_url) else 0.74,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return self._prepare_candidate_jobs_v46(jobs, page_url)

    def _extract_martian_context_v46(self, page_url: str, html_body: str) -> dict[str, str]:
        context = {
            "client_code": "",
            "recruiter_id": "",
            "job_board_theme_id": "",
            "board_name": "",
            "build_id": "",
            "next_page": "",
            "next_query": "",
        }

        match = _NEXT_DATA_PATTERN_V46.search(html_body or "")
        if match:
            raw_payload = html_lib.unescape((match.group(1) or "").strip())
            try:
                parsed = json.loads(raw_payload)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                page_props = parsed.get("props", {}).get("pageProps", {})
                if isinstance(page_props, dict):
                    context["client_code"] = str(page_props.get("clientCode") or "").strip()
                    context["recruiter_id"] = str(page_props.get("recruiterId") or "").strip()
                    context["job_board_theme_id"] = str(page_props.get("jobBoardThemeId") or "").strip()
                    context["board_name"] = str(page_props.get("name") or "").strip()

                context["build_id"] = str(parsed.get("buildId") or "").strip()
                context["next_page"] = str(parsed.get("page") or "").strip()
                query = parsed.get("query")
                if isinstance(query, dict):
                    context["next_query"] = urlencode(
                        {str(k): str(v) for k, v in query.items() if isinstance(v, (str, int, float))}
                    )
                    if not context["client_code"]:
                        context["client_code"] = str(query.get("client") or query.get("clientCode") or "").strip()

        if not context["client_code"]:
            m = re.search(r"clientCode['\"]?\s*[:=]\s*['\"]([a-z0-9_-]{2,})", html_body or "", re.IGNORECASE)
            if m:
                context["client_code"] = m.group(1)

        if not context["recruiter_id"]:
            m = re.search(r"recruiterId['\"]?\s*[:=]\s*['\"]?([0-9]{2,})", html_body or "", re.IGNORECASE)
            if m:
                context["recruiter_id"] = m.group(1)

        if not context["job_board_theme_id"]:
            m = re.search(r"jobBoardThemeId['\"]?\s*[:=]\s*['\"]?([0-9]{2,})", html_body or "", re.IGNORECASE)
            if m:
                context["job_board_theme_id"] = m.group(1)

        if not context["client_code"]:
            parsed = urlparse(page_url)
            parts = [seg for seg in parsed.path.split("/") if seg]
            if parts:
                candidate = re.sub(r"[^a-z0-9-]", "", parts[-1].lower())
                if len(candidate) >= 3:
                    context["client_code"] = candidate

        return context

    def _martian_probe_urls_v46(self, page_url: str, context: dict[str, str]) -> list[str]:
        parsed = urlparse(page_url or "")
        if not parsed.netloc:
            return []

        client_code = (context.get("client_code") or "").strip()
        recruiter_id = (context.get("recruiter_id") or "").strip()
        theme_id = (context.get("job_board_theme_id") or "").strip()

        page_host = f"{parsed.scheme or 'https'}://{parsed.netloc}".rstrip("/")
        ats_hosts = [
            page_host,
            "https://web.martianlogic.com",
            "https://jobs.myrecruitmentplus.com",
            "https://jobs.martianlogic.com",
            "https://form.myrecruitmentplus.com",
        ]
        if client_code:
            ats_hosts.extend([
                f"https://{client_code}.myrecruitmentplus.com",
                f"https://{client_code}.martianlogic.com",
            ])

        hosts: list[str] = []
        seen_hosts: set[str] = set()
        for host in ats_hosts:
            norm = host.rstrip("/")
            if not norm or norm in seen_hosts:
                continue
            seen_hosts.add(norm)
            hosts.append(norm)

        path = "/" + "/".join(seg for seg in (parsed.path or "").split("/") if seg)
        path = path.rstrip("/")

        query_templates = [
            "pageNumber=1&pageSize=50&isActive=true",
            "pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            "page=1&pageSize=50",
            "offset=0&limit=50",
            "search=",
        ]
        if client_code:
            query_templates.extend(
                [
                    f"client={client_code}&pageNumber=1&pageSize=50&isActive=true",
                    f"clientCode={client_code}&pageNumber=1&pageSize=50&isActive=true",
                    f"client={client_code}&search=",
                    f"clientCode={client_code}&search=",
                ]
            )
        if recruiter_id:
            query_templates.extend(
                [
                    f"recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true",
                    f"recruiterId={recruiter_id}&search=",
                ]
            )
        if recruiter_id and client_code:
            query_templates.extend(
                [
                    f"client={client_code}&recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true",
                    f"clientCode={client_code}&recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true",
                ]
            )
        if theme_id and client_code:
            query_templates.append(
                f"clientCode={client_code}&jobBoardThemeId={theme_id}&pageNumber=1&pageSize=50&isActive=true"
            )

        probe_urls: list[str] = []

        for host in hosts:
            for api_path in (
                "/api/jobs/search",
                "/api/job-search",
                "/api/jobads/search",
                "/api/job-ads/search",
                "/api/search/jobs",
                "/jobs/search",
                "/embed-jobs",
            ):
                base = f"{host}{api_path}"
                probe_urls.append(base)
                for q in query_templates:
                    probe_urls.append(f"{base}?{q}")

            if recruiter_id:
                for recruiter_path in (
                    f"/api/recruiter/{recruiter_id}/jobs/search",
                    f"/api/recruiter/{recruiter_id}/jobads",
                    f"/api/recruiter/{recruiter_id}/job-ads",
                ):
                    base = f"{host}{recruiter_path}"
                    probe_urls.append(base)
                    for q in query_templates:
                        probe_urls.append(f"{base}?{q}")

            base_candidates = [host]
            if path:
                base_candidates.append(f"{host}{path}")
            if client_code:
                base_candidates.append(f"{host}/{client_code}")

            for base in base_candidates:
                probe_urls.append(base)
                for q in query_templates:
                    probe_urls.append(f"{base}?{q}")

            probe_urls.extend(self._next_data_probe_urls_v46(host, page_url, context))

        prioritized = sorted(
            {u.rstrip("/"): u for u in probe_urls if u}.values(),
            key=lambda u: self._martian_endpoint_priority_v46(u, parsed.netloc.lower(), path),
            reverse=True,
        )
        return prioritized[:52]

    def _next_data_probe_urls_v46(self, host: str, page_url: str, context: dict[str, str]) -> list[str]:
        build_id = (context.get("build_id") or "").strip()
        if not build_id:
            return []

        parsed = urlparse(page_url)
        path = "/" + "/".join(seg for seg in (parsed.path or "").split("/") if seg)
        if not path:
            path = "/"

        query_pairs = dict(parse_qsl(parsed.query))
        next_query = (context.get("next_query") or "").strip()
        if next_query:
            for key, value in parse_qsl(next_query):
                query_pairs[key] = value

        client_code = (context.get("client_code") or "").strip()
        if client_code:
            query_pairs.setdefault("client", client_code)
        encoded_query = urlencode(query_pairs)

        candidates = [f"{host}/_next/data/{build_id}/index.json"]
        if path != "/":
            candidates.append(f"{host}/_next/data/{build_id}{path}.json")
            candidates.append(f"{host}/_next/data/{build_id}{path}/index.json")

        dynamic = [client_code, context.get("next_page", "").replace("[", "").replace("]", "").strip("/")]
        for key in dynamic:
            cleaned = (key or "").strip("/")
            if not cleaned:
                continue
            candidates.append(f"{host}/_next/data/{build_id}/{cleaned}.json")
            candidates.append(f"{host}/_next/data/{build_id}/{cleaned}/index.json")

        if encoded_query:
            candidates = [f"{u}?{encoded_query}" for u in candidates]
        return candidates

    def _martian_endpoint_priority_v46(self, url: str, page_host: str, page_path: str) -> int:
        low = (url or "").lower()
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").rstrip("/")

        score = 0
        if host == page_host:
            score += 18
        if page_path and path == page_path:
            score += 10
        if "/_next/data/" in low:
            score += 12
        if "/api/recruiter/" in low:
            score += 10
        elif "/api/" in low:
            score += 8
        if "search" in low:
            score += 7
        if "client=" in low or "clientcode=" in low:
            score += 5
        if "recruiterid=" in low:
            score += 4
        if "pagenumber=1" in low or "page=1" in low or "offset=0" in low:
            score += 2
        if any(x in host for x in ("martianlogic", "myrecruitmentplus")):
            score += 3
        return score

    @staticmethod
    def _martian_post_endpoints_v46(endpoints: list[str]) -> list[str]:
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
        return out[:18]

    @staticmethod
    def _martian_post_payloads_v46(context: dict[str, str]) -> list[dict[str, Any]]:
        client_code = (context.get("client_code") or "").strip()
        recruiter_id = (context.get("recruiter_id") or "").strip()
        theme_id = (context.get("job_board_theme_id") or "").strip()

        payloads: list[dict[str, Any]] = []
        if client_code:
            payloads.extend(
                [
                    {"client": client_code, "pageNumber": 1, "pageSize": 50, "isActive": True},
                    {"clientCode": client_code, "pageNumber": 1, "pageSize": 50, "isActive": True},
                    {"client": client_code, "search": ""},
                    {"clientCode": client_code, "search": ""},
                ]
            )
        else:
            payloads.append({"pageNumber": 1, "pageSize": 50, "isActive": True})

        if recruiter_id:
            payloads.append({"recruiterId": recruiter_id, "pageNumber": 1, "pageSize": 50, "isActive": True})
            if client_code:
                payloads.append(
                    {
                        "clientCode": client_code,
                        "recruiterId": recruiter_id,
                        "pageNumber": 1,
                        "pageSize": 50,
                        "isActive": True,
                    }
                )

        if theme_id and client_code:
            payloads.append(
                {
                    "clientCode": client_code,
                    "jobBoardThemeId": theme_id,
                    "pageNumber": 1,
                    "pageSize": 50,
                    "isActive": True,
                }
            )

        return payloads

    def _extract_utility_card_rows_v46(self, root: etree._Element, page_url: str) -> list[dict]:
        rows = root.xpath(
            "//div[contains(concat(' ', normalize-space(@class), ' '), ' p-4 ') "
            "or contains(concat(' ', normalize-space(@class), ' '), ' mx-4 ')]"
        )
        if len(rows) < 2:
            return []

        jobs: list[dict] = []
        for row in rows[:320]:
            links = row.xpath(".//a[@href]")
            if not links:
                continue

            heading_nodes = row.xpath(
                ".//h1[1] | .//h2[1] | .//h3[1] | "
                ".//div[contains(@class,'cursor-pointer')][1]"
            )
            title_raw = _text(heading_nodes[0]) if heading_nodes else _text(links[0])
            title = self._normalize_title_v46(title_raw)
            if not self._is_valid_title_v46(title):
                continue
            if not self._title_has_role_signal_v46(title):
                continue

            source_url = page_url
            for a_el in links:
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v46(href):
                    continue
                source_url = href
                if self._is_job_like_url_v46(href):
                    break

            row_text = _text(row)
            has_apply = bool(_APPLY_EVIDENCE_PATTERN_V46.search(row_text))
            if not (self._is_job_like_url_v46(source_url) or has_apply or len(row_text) >= 120):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v46(row, title),
                    "salary_raw": self._extract_salary_v46(row_text),
                    "employment_type": self._extract_employment_type_v46(row_text),
                    "description": self._clean_description_v46(row_text),
                    "extraction_method": "tier2_utility_cards_v46",
                    "extraction_confidence": 0.79 if self._is_job_like_url_v46(source_url) else 0.73,
                }
            )

        prepared = self._prepare_candidate_jobs_v46(jobs, page_url)
        return prepared if len(prepared) >= 2 else []

    def _extract_elementor_cards_v46(self, root: etree._Element, page_url: str) -> list[dict]:
        cards = root.xpath(
            "//*[contains(@class,'elementor-column') and contains(@class,'elementor-inner-column')]"
        )
        if len(cards) < 2:
            return []

        jobs: list[dict] = []
        for card in cards[:360]:
            heading_nodes = card.xpath(".//h2[contains(@class,'elementor-heading-title')][1]")
            if not heading_nodes:
                continue
            title = self._normalize_title_v46(_text(heading_nodes[0]))
            if not self._is_valid_title_v46(title):
                continue
            if not self._title_has_role_signal_v46(title):
                continue

            source_url = page_url
            for a_el in card.xpath(".//a[@href]"):
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v46(href):
                    continue
                source_url = href
                if self._is_job_like_url_v46(href):
                    break

            card_text = _text(card)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v46(card, title),
                    "salary_raw": self._extract_salary_v46(card_text),
                    "employment_type": self._extract_employment_type_v46(card_text),
                    "description": self._clean_description_v46(card_text),
                    "extraction_method": "tier2_elementor_cards_v46",
                    "extraction_confidence": 0.76,
                }
            )

        return self._prepare_candidate_jobs_v46(jobs, page_url)

    def _extract_heading_cta_cards_v46(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        for a_el in root.xpath("//a[@href]")[:5000]:
            link_text = " ".join(_text(a_el).split())
            if not _CTA_CARD_LINK_TEXT_PATTERN_V46.match(link_text):
                continue

            source_url = _resolve_url(a_el.get("href"), page_url) or ""
            if not source_url or self._is_non_job_url_v46(source_url):
                continue

            row = a_el
            best_row = a_el
            for _ in range(6):
                parent = row.getparent()
                if parent is None:
                    break
                row = parent
                heading_count = len(row.xpath(".//h1|.//h2|.//h3|.//h4"))
                link_count = len(row.xpath(".//a[@href]"))
                if heading_count >= 1 and 1 <= link_count <= 12:
                    best_row = row
                    break

            heading_nodes = best_row.xpath(".//h1[1] | .//h2[1] | .//h3[1] | .//h4[1]")
            if not heading_nodes:
                continue

            title = self._normalize_title_v46(_text(heading_nodes[0]))
            if not self._is_valid_title_v46(title):
                continue
            if not self._title_has_role_signal_v46(title):
                continue

            row_text = " ".join(_text(best_row).split())[:2600]
            has_apply = bool(_APPLY_EVIDENCE_PATTERN_V46.search(row_text))
            if not (has_apply or self._is_job_like_url_v46(source_url) or len(row_text) >= 95):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v46(best_row, title),
                    "salary_raw": self._extract_salary_v46(row_text),
                    "employment_type": self._extract_employment_type_v46(row_text),
                    "description": self._clean_description_v46(row_text),
                    "extraction_method": "tier2_heading_cta_cards_v46",
                    "extraction_confidence": 0.77 if self._is_job_like_url_v46(source_url) else 0.72,
                }
            )

        prepared = self._prepare_candidate_jobs_v46(jobs, page_url)
        if len(prepared) < 2:
            return []
        role_hits = sum(1 for j in prepared if self._title_has_role_signal_v46(j.get("title", "")))
        if role_hits < max(2, int(len(prepared) * 0.65)):
            return []
        return prepared

    def _extract_greenhouse_posts_v46(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        for a_el in root.xpath("//a[@href]")[:5000]:
            href = (a_el.get("href") or "").strip()
            source_url = _resolve_url(href, page_url)
            if not source_url:
                continue
            if not re.search(r"/jobs/[0-9]{4,}", source_url, re.IGNORECASE):
                continue
            if self._is_non_job_url_v46(source_url):
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

            title_nodes = row.xpath(".//*[contains(@class,'body--medium')][1]")
            title = self._normalize_title_v46(_text(title_nodes[0])) if title_nodes else self._normalize_title_v46(_text(a_el))
            if not self._is_title_acceptable_v46(title, source_url):
                continue

            location_nodes = row.xpath(".//*[contains(@class,'body--metadata')][1]")
            location = " ".join(_text(location_nodes[0]).split())[:180] if location_nodes else None

            row_text = _text(row)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": self._extract_salary_v46(row_text),
                    "employment_type": self._extract_employment_type_v46(row_text),
                    "description": self._clean_description_v46(row_text),
                    "extraction_method": "tier2_greenhouse_posts_v46",
                    "extraction_confidence": 0.85,
                }
            )

        return self._prepare_candidate_jobs_v46(jobs, page_url)

    def _extract_job_links_v46(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        for a_el in root.xpath("//a[@href]")[:5000]:
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url)
            if not source_url or self._is_non_job_url_v46(source_url):
                continue

            heading_nodes = a_el.xpath(".//h1 | .//h2 | .//h3 | .//h4 | .//h5")
            raw_title = _text(heading_nodes[0]) if heading_nodes else _text(a_el)
            title = self._normalize_title_v46(raw_title)
            if not self._is_valid_title_v46(title):
                continue
            if not self._title_has_role_signal_v46(title):
                continue

            context = a_el.getparent()
            if context is None:
                context = a_el
            context_text = _text(context)
            if len(title.split()) > 10 and not self._is_job_like_url_v46(source_url):
                continue
            if _LISTING_LINK_TEXT_PATTERN_V46.search(title) and not self._is_job_like_url_v46(source_url):
                continue
            if not (
                self._is_job_like_url_v46(source_url)
                or bool(_APPLY_EVIDENCE_PATTERN_V46.search(context_text))
                or (source_url.rstrip("/") != page_url.rstrip("/") and len(context_text) >= 80)
            ):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v46(context, title),
                    "salary_raw": self._extract_salary_v46(context_text),
                    "employment_type": self._extract_employment_type_v46(context_text),
                    "description": self._clean_description_v46(context_text),
                    "extraction_method": "tier2_job_links_v46",
                    "extraction_confidence": 0.73 if self._is_job_like_url_v46(source_url) else 0.67,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return self._prepare_candidate_jobs_v46(jobs, page_url)

    def _pick_best_candidate_v46(
        self,
        candidates: list[tuple[str, list[dict]]],
        page_url: str,
    ) -> tuple[str, list[dict], float]:
        if not candidates:
            return "", [], 0.0

        scored: list[tuple[str, list[dict], float]] = []
        for label, jobs in candidates:
            prepared = self._prepare_candidate_jobs_v46(jobs, page_url)
            if not prepared:
                continue
            score = self._candidate_score_v46(prepared, page_url)
            scored.append((label, prepared, score))

        if not scored:
            return "", [], 0.0

        best_label, best_jobs, best_score = max(scored, key=lambda item: item[2])

        for label, jobs, score in scored:
            if label == best_label:
                continue
            overlap = self._title_overlap_ratio_v46(jobs, best_jobs)
            if len(jobs) >= len(best_jobs) + 1 and overlap >= 0.6 and score >= best_score - 1.2:
                best_label, best_jobs, best_score = label, jobs, score

        return best_label, best_jobs[:MAX_JOBS_PER_PAGE], best_score

    def _prepare_candidate_jobs_v46(self, jobs: list[dict], page_url: str) -> list[dict]:
        cleaned: list[dict] = []

        for idx, raw in enumerate(jobs):
            title = self._normalize_title_v46(raw.get("title", ""))
            source_url = _resolve_url(raw.get("source_url"), page_url) or page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]

            if self._is_non_job_url_v46(source_url):
                continue
            if not self._is_title_acceptable_v46(title, source_url):
                continue

            desc = self._clean_description_v46(str(raw.get("description") or ""))
            cleaned.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": raw.get("location_raw") or None,
                    "salary_raw": raw.get("salary_raw") or None,
                    "employment_type": raw.get("employment_type") or self._extract_employment_type_v46(desc or ""),
                    "description": desc,
                    "extraction_method": raw.get("extraction_method") or "tier2_v46",
                    "extraction_confidence": raw.get("extraction_confidence", 0.65),
                    "_order": idx,
                }
            )

        deduped = self._dedupe_jobs_v46(cleaned, page_url)
        if not self._is_valid_jobset_v46(deduped, page_url):
            return []
        return deduped

    def _dedupe_jobs_v46(self, jobs: list[dict], page_url: str) -> list[dict]:
        by_key: dict[tuple[str, str], dict] = {}
        by_url: dict[str, dict] = {}

        for job in jobs:
            title = self._normalize_title_v46(job.get("title", ""))
            url = _resolve_url(job.get("source_url"), page_url) or page_url
            key = (title.lower(), url.lower())
            existing = by_key.get(key)
            if existing is None or self._title_quality_score_v46(title) > self._title_quality_score_v46(existing.get("title", "")):
                by_key[key] = job

        for job in by_key.values():
            norm_url = (_resolve_url(job.get("source_url"), page_url) or page_url).rstrip("/").lower()
            current = by_url.get(norm_url)
            if current is None:
                by_url[norm_url] = job
                continue
            if self._title_quality_score_v46(job.get("title", "")) > self._title_quality_score_v46(current.get("title", "")):
                by_url[norm_url] = job

        deduped = sorted(by_url.values(), key=lambda item: int(item.get("_order", 0)))
        for item in deduped:
            item.pop("_order", None)
        return deduped[:MAX_JOBS_PER_PAGE]

    def _is_valid_jobset_v46(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title_v46(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return False

        role_hits = sum(1 for t in titles if self._title_has_role_signal_v46(t))
        reject_hits = sum(1 for t in titles if _TITLE_REJECT_PATTERN_V46.match(t.lower()))
        evidence_hits = sum(1 for j in jobs if self._job_has_evidence_v46(j, page_url))

        if reject_hits >= max(1, int(len(titles) * 0.22)):
            return False

        if len(titles) <= 2:
            return role_hits == len(titles) and evidence_hits >= 1

        if len(titles) <= 4:
            if role_hits < max(2, int(len(titles) * 0.7)):
                return False
            return evidence_hits >= max(1, int(len(titles) * 0.5))

        if role_hits < max(1, int(len(titles) * 0.6)):
            return False
        return evidence_hits >= max(1, int(len(titles) * 0.3))

    def _candidate_score_v46(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        titles = [self._normalize_title_v46(j.get("title", "")) for j in jobs]
        role_hits = sum(1 for t in titles if self._title_has_role_signal_v46(t))
        reject_hits = sum(1 for t in titles if _TITLE_REJECT_PATTERN_V46.match(t.lower()))
        detail_hits = sum(1 for j in jobs if self._is_job_like_url_v46(j.get("source_url") or ""))
        evidence_hits = sum(1 for j in jobs if self._job_has_evidence_v46(j, page_url))

        score = len(jobs) * 4.6
        score += role_hits * 2.8
        score += detail_hits * 1.7
        score += evidence_hits * 1.6
        score -= reject_hits * 6.0
        return score

    def _job_has_evidence_v46(self, job: dict, page_url: str) -> bool:
        source_url = (job.get("source_url") or page_url).strip() or page_url
        desc = str(job.get("description") or "")

        if self._is_job_like_url_v46(source_url):
            return True

        if source_url.rstrip("/") != page_url.rstrip("/") and not self._is_non_job_url_v46(source_url):
            parsed = urlparse(source_url)
            parts = [p for p in (parsed.path or "").split("/") if p]
            if parts:
                leaf = parts[-1].lower()
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
                }:
                    if re.search(r"[0-9]", leaf) or "-" in leaf or len(leaf) >= 10 or len(parts) >= 2:
                        return True

        if job.get("salary_raw") or job.get("employment_type") or job.get("location_raw"):
            return True
        if _APPLY_EVIDENCE_PATTERN_V46.search(desc):
            return True
        return len(desc.strip()) >= 180

    def _normalize_title_v46(self, title: str) -> str:
        value = html_lib.unescape((title or "").strip())
        value = re.sub(r"\s+", " ", value)
        value = re.sub(r"([A-Za-z])((?:Just|Posted)\b)", r"\1 \2", value)
        value = value.strip(" \t\r\n-–|:;,>")
        value = re.sub(
            r"\s*(?:apply\s+now|apply\s+here|read\s+more|learn\s+more|info\s+lengkap)\s*$",
            "",
            value,
            flags=re.IGNORECASE,
        )
        value = re.sub(r"\s*(?:deadline\s*:\s*\S+.*|closing\s+date\s*:\s*\S+.*)$", "", value, flags=re.IGNORECASE)
        value = _STATUS_SUFFIX_PATTERN_V46.sub("", value)
        value = value.strip(" \t\r\n-–|:;,")
        value = re.sub(r"\.+$", "", value).strip()
        return value

    def _is_valid_title_v46(self, title: str) -> bool:
        if not title:
            return False

        t = title.strip()
        if len(t) < 4 or len(t) > 180:
            return False
        if not re.search(r"[A-Za-z]", t):
            return False
        if len(t.split()) > 14:
            return False
        if _TITLE_REJECT_PATTERN_V46.match(t.lower()):
            return False
        if _MARKETING_TITLE_FRAGMENT_V46.search(t):
            return False
        return True

    def _is_title_acceptable_v46(self, title: str, source_url: str) -> bool:
        if not self._is_valid_title_v46(title):
            return self._is_acronym_title_v46(title) and self._is_job_like_url_v46(source_url)

        if self._title_has_role_signal_v46(title):
            return True

        if self._is_job_like_url_v46(source_url):
            # Allow uncommon role wording only when URL strongly indicates a detail page.
            return len(title.split()) <= 8

        return False

    def _title_has_role_signal_v46(self, title: str) -> bool:
        if not title:
            return False
        return bool(_ROLE_HINT_PATTERN_V46.search(title) or _title_has_job_noun(title) or self._is_acronym_title_v46(title))

    @staticmethod
    def _is_acronym_title_v46(title: str) -> bool:
        t = (title or "").strip()
        if not re.fullmatch(r"[A-Z][A-Z0-9&/\-\+]{1,10}", t):
            return False
        return t.lower() not in {"home", "menu", "faq", "apply"}

    def _is_job_like_url_v46(self, url: str) -> bool:
        value = (url or "").strip()
        if not value or self._is_non_job_url_v46(value):
            return False
        if _DETAILISH_URL_PATTERN_V46.search(value):
            return True

        parsed = urlparse(value)
        path = (parsed.path or "").lower().rstrip("/")
        parts = [p for p in path.split("/") if p]
        if not parts:
            return False

        leaf = parts[-1]
        if re.fullmatch(r"\d{4,}", leaf):
            parent = parts[-2] if len(parts) >= 2 else ""
            if parent not in {
                "news",
                "blog",
                "about",
                "contact",
                "page",
                "category",
                "tag",
                "privacy",
                "terms",
            }:
                return True

        query = parsed.query.lower()
        if re.search(r"(?:^|&)(?:jobid|job_id|requisitionid|vacancyid|jobadid|adid|ajid)=", query):
            return True

        return False

    @staticmethod
    def _is_non_job_url_v46(url: str) -> bool:
        value = (url or "").strip().lower()
        if not value:
            return False
        if value.startswith("mailto:") or value.startswith("tel:"):
            return True
        return bool(_NON_JOB_URL_PATTERN_V46.search(value))

    @staticmethod
    def _iter_json_dicts_v46(payload: Any) -> list[dict[str, Any]]:
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
            if len(out) >= 7000:
                break
        return out

    def _extract_location_from_json_v46(self, node: dict[str, Any]) -> Optional[str]:
        value = (
            node.get("location")
            or node.get("Location")
            or node.get("primaryLocation")
            or node.get("PrimaryLocation")
            or node.get("workLocation")
            or node.get("jobLocation")
            or ""
        )

        if isinstance(value, str):
            return value.strip() or None
        if isinstance(value, dict):
            if "address" in value and isinstance(value["address"], dict):
                addr = value["address"]
                loc = ", ".join(
                    p
                    for p in (
                        str(addr.get("addressLocality") or "").strip(),
                        str(addr.get("addressRegion") or "").strip(),
                        str(addr.get("addressCountry") or "").strip(),
                    )
                    if p
                )
                if loc:
                    return loc
            for key in ("name", "Name", "label", "city", "suburb", "state"):
                raw = str(value.get(key) or "").strip()
                if raw:
                    return raw
        if isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item.strip():
                    return item.strip()
                if isinstance(item, dict):
                    nested = self._extract_location_from_json_v46(item)
                    if nested:
                        return nested
        return None

    async def _enrich_from_detail_pages(self, jobs: list[dict]) -> list[dict]:
        jobs_to_enrich = [
            (i, j)
            for i, j in enumerate(jobs)
            if self._needs_detail_refresh_v46(j)
            and j.get("source_url")
            and str(j.get("source_url")).startswith("http")
            and not self._is_non_job_url_v46(str(j.get("source_url") or ""))
        ]

        if not jobs_to_enrich:
            return jobs

        jobs_to_enrich = jobs_to_enrich[:18]

        async with httpx.AsyncClient(
            timeout=7,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        ) as client:

            async def _enrich_one(idx: int, job: dict) -> tuple[int, dict]:
                try:
                    resp = await client.get(str(job.get("source_url") or ""))
                except Exception:
                    return idx, {}

                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 200 or self._looks_non_html_payload_v46(body):
                    return idx, {}

                root = _parse_html(body)
                if root is None:
                    return idx, {}

                enriched: dict[str, Any] = {}

                description = self._extract_detail_description_v46(root)
                if description:
                    enriched["description"] = description

                location = self._extract_detail_location_v46(root)
                if location:
                    enriched["location_raw"] = location

                employment_type = self._extract_detail_employment_type_v46(root)
                if employment_type:
                    enriched["employment_type"] = employment_type

                salary = self._extract_salary_v46(_text(root))
                if salary:
                    enriched["salary_raw"] = salary

                return idx, enriched

            for batch_start in range(0, len(jobs_to_enrich), 4):
                batch = jobs_to_enrich[batch_start: batch_start + 4]
                try:
                    results = await asyncio.wait_for(
                        asyncio.gather(*[_enrich_one(idx, job) for idx, job in batch]),
                        timeout=5.2,
                    )
                except asyncio.TimeoutError:
                    continue

                for idx, enriched in results:
                    if not enriched:
                        continue
                    current = jobs[idx]
                    for key, value in enriched.items():
                        if not value:
                            continue
                        if key == "description":
                            current_desc = str(current.get("description") or "")
                            if not current_desc or len(value) > len(current_desc) + 80:
                                current[key] = value
                        elif not current.get(key):
                            current[key] = value

        return jobs

    def _extract_detail_description_v46(self, root: etree._Element) -> Optional[str]:
        for selector in _DETAIL_DESC_SELECTORS_V46:
            try:
                nodes = root.cssselect(selector)
            except Exception:
                nodes = []
            for node in nodes[:2]:
                text = self._clean_description_v46(_text(node))
                if text and len(text) >= 140:
                    return text

        for node in root.xpath("//article | //main")[:2]:
            text = self._clean_description_v46(_text(node))
            if text and len(text) >= 140:
                return text

        return None

    def _extract_detail_location_v46(self, root: etree._Element) -> Optional[str]:
        icon_xpaths = [
            "//*[contains(@class,'fa-map-marker')]/ancestor::*[1]/following-sibling::*[1]",
            "//h3[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'location')]/following-sibling::*[1]",
            "//*[contains(@class,'location')][1]",
        ]
        for xp in icon_xpaths:
            for node in root.xpath(xp)[:4]:
                loc = " ".join(_text(node).split())
                if 2 < len(loc) < 180 and re.search(r"[A-Za-z]", loc):
                    return loc

        body = _text(root)
        m = _AU_LOCATIONS.search(body or "")
        if m:
            return m.group(0)[:120]
        return None

    def _extract_detail_employment_type_v46(self, root: etree._Element) -> Optional[str]:
        xpaths = [
            "//*[contains(@class,'fa-clock')]/ancestor::*[1]/following-sibling::*[1]",
            "//h3[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'employment')]/following-sibling::*[1]",
        ]
        for xp in xpaths:
            for node in root.xpath(xp)[:4]:
                t = " ".join(_text(node).split())
                if t and _JOB_TYPE_PATTERN.search(t):
                    return _JOB_TYPE_PATTERN.search(t).group(0).strip()[:80]

        body = _text(root)
        m = _JOB_TYPE_PATTERN.search(body or "")
        if m:
            return m.group(0).strip()[:80]
        return None

    def _should_enrich_detail_pages_v46(self, jobs: list[dict], page_url: str) -> bool:
        off_page = [
            j
            for j in jobs
            if (j.get("source_url") or "").startswith("http")
            and (j.get("source_url") or page_url).rstrip("/") != page_url.rstrip("/")
            and not self._is_non_job_url_v46(j.get("source_url") or "")
        ]
        if not off_page:
            return False

        detail_hits = sum(1 for j in off_page if self._is_job_like_url_v46(j.get("source_url") or ""))
        refresh_hits = sum(1 for j in off_page if self._needs_detail_refresh_v46(j))

        if detail_hits >= 1:
            return True
        if refresh_hits >= 2:
            return True
        return len(off_page) >= 5

    def _needs_detail_refresh_v46(self, job: dict) -> bool:
        desc = str(job.get("description") or "").strip()
        if not desc or len(desc) < 170:
            return True
        if re.search(r"\b(?:apply\s+now|read\s+more|learn\s+more|info\s+lengkap)\b", desc, re.IGNORECASE):
            return True
        if not job.get("location_raw") or not job.get("employment_type"):
            return True
        return False

    def _needs_deep_recovery_v46(
        self,
        page_url: str,
        html_body: str,
        best_jobs: list[dict],
        best_score: float,
    ) -> bool:
        if not best_jobs:
            return True
        if len(best_jobs) < MIN_JOBS_FOR_SUCCESS:
            return True
        if best_score < 11.0:
            return True
        if self._looks_like_shell_v46(page_url, html_body):
            return True
        role_hits = sum(1 for j in best_jobs if self._title_has_role_signal_v46(j.get("title", "")))
        return role_hits < max(2, int(len(best_jobs) * 0.6))

    def _looks_like_shell_v46(self, page_url: str, html_body: str) -> bool:
        lower = (html_body or "").lower()
        if "<div id=\"__next\"></div>" in lower and "__next_data__" in lower:
            return True
        if len(lower) < 350:
            return True
        if _MARTIAN_SHELL_PATTERN_V46.search(lower):
            return True
        if "oraclecloud.com" in (page_url or "").lower() and "candidateexperience" in lower:
            return True
        return False

    @staticmethod
    def _looks_like_martian_shell_v46(html_body: str) -> bool:
        return bool(_MARTIAN_SHELL_PATTERN_V46.search(html_body or ""))

    def _page_listing_score_v46(self, page_url: str, html_body: str) -> float:
        if not html_body or len(html_body) < 200:
            return -20.0

        lower = html_body.lower()
        score = 0.0
        score += min(lower.count("apply now"), 10)
        score += min(lower.count("job"), 10)
        score += min(lower.count("career"), 6)
        if _LISTING_URL_PATTERN_V46.search(page_url or ""):
            score += 3.0

        root = _parse_html(html_body)
        if root is not None:
            listing_links = 0
            role_links = 0
            for a_el in root.xpath("//a[@href]"):
                href = (a_el.get("href") or "").strip()
                text = self._safe_text_v46(a_el)
                if _LISTING_URL_PATTERN_V46.search(href) or _LISTING_LINK_TEXT_PATTERN_V46.search(text):
                    listing_links += 1
                if _ROLE_HINT_PATTERN_V46.search(text):
                    role_links += 1
            score += min(listing_links * 1.3, 18.0)
            score += min(role_links * 2.0, 18.0)

        return score

    @staticmethod
    def _safe_text_v46(el: etree._Element) -> str:
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

    def _clean_description_v46(self, description: str) -> Optional[str]:
        text = " ".join((description or "").split())
        if not text:
            return None
        cut = re.search(r"\b(?:how\s+to\s+apply|cara\s+melamar|application\s+process)\b", text, re.IGNORECASE)
        if cut:
            text = text[: cut.start()].strip()
        if len(text) < 45:
            return None
        return text[:5000]

    def _extract_location_v46(self, row: etree._Element, title: str) -> Optional[str]:
        for el in row.iter():
            if not isinstance(el.tag, str):
                continue
            cls = _get_el_classes(el)
            if "location" in cls.lower() or "map-marker" in cls.lower():
                loc = " ".join(_text(el).split())
                if loc and loc != title and 2 < len(loc) < 160:
                    return loc

        row_text = _text(row)
        match = _AU_LOCATIONS.search(row_text) if row_text else None
        if match:
            return match.group(0)[:120]
        return None

    @staticmethod
    def _extract_salary_v46(text: str) -> Optional[str]:
        if not text:
            return None
        match = _SALARY_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:120]
        return None

    @staticmethod
    def _extract_employment_type_v46(text: str) -> Optional[str]:
        if not text:
            return None
        match = _JOB_TYPE_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:80]
        return None

    def _title_quality_score_v46(self, title: str) -> float:
        t = self._normalize_title_v46(title)
        if not t:
            return -10.0
        score = 0.0
        score += 2.0 if self._title_has_role_signal_v46(t) else 0.0
        score += 1.0 if self._is_valid_title_v46(t) else 0.0
        score -= max(0.0, (len(t) - 90) / 80.0)
        return score

    def _title_overlap_ratio_v46(self, a_jobs: list[dict], b_jobs: list[dict]) -> float:
        a_titles = {self._normalize_title_v46(j.get("title", "")).lower() for j in a_jobs if j.get("title")}
        b_titles = {self._normalize_title_v46(j.get("title", "")).lower() for j in b_jobs if j.get("title")}
        if not a_titles or not b_titles:
            return 0.0
        return len(a_titles & b_titles) / max(1, min(len(a_titles), len(b_titles)))

    @staticmethod
    def _looks_non_html_payload_v46(body: Optional[str]) -> bool:
        if not body:
            return True
        sample = body[:900].lstrip()
        if sample.startswith("%PDF-"):
            return True
        low = sample.lower()
        if (low.startswith("{") or low.startswith("[")) and "<html" not in low[:300]:
            return True
        return False
