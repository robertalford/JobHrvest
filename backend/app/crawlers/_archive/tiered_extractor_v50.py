"""
Tiered Extraction Engine v5.0 — direct from v1.6.

v5.0 takes a simpler, timeout-safe approach focused on high-impact general patterns:
1. Timeout-safe parent fallback arbitration (never hard-fail on a single phase timeout).
2. Tier-0 structured extraction from __NEXT_DATA__, generic JSON scripts, and JSON-LD JobPosting.
3. Fast Martian/MyRecruitmentPlus shell recovery with bounded same-host-first probing.
4. Role-gated Elementor/utility card extraction for heading+CTA career grids.
5. Bounded listing-hub traversal for multilingual careers hubs (jobs/lowongan/loker/karir).
"""

from __future__ import annotations

import asyncio
import html as html_lib
import json
import logging
import re
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


_ROLE_HINT_PATTERN_V50 = re.compile(
    r"\b(?:engineer|developer|manager|director|analyst|specialist|assistant|"
    r"consultant|coordinator|officer|administrator|accountant|technician|"
    r"designer|architect|operator|supervisor|advisor|executive|intern(?:ship)?|"
    r"recruit(?:er)?|nurse|teacher|driver|chef|chemist|mechanic|associate|"
    r"representative|agent|planner|liaison|scientist|sales|marketing|service|"
    r"customer\s+service|content|writer|crew|foreman|electrician|labourer|"
    r"akuntan|asisten|konsultan|influencer|videografer|fotografer|psikolog(?:i)?|"
    r"sarjana|fashion\s+designer|personal\s+assistant|model)\b",
    re.IGNORECASE,
)

_TITLE_REJECT_PATTERN_V50 = re.compile(
    r"^(?:join\s+our\s+team|current\s+jobs?|all\s+jobs?|job\s+openings?|"
    r"search\s+jobs?|browse\s+jobs?|view\s+all\s+jobs?|careers?|open\s+roles?|"
    r"about\s+us|our\s+culture|our\s+values?|contact|home|menu|"
    r"read\s+more|learn\s+more|show\s+more|load\s+more|"
    r"apply(?:\s+now)?|job\s+details?|role\s+details?|"
    r"job\s+alerts?|my\s+applications?|login|register|sign\s+in|"
    r"lowongan(?:\s+kerja(?:\s+[a-z]+)?)?|beranda)$",
    re.IGNORECASE,
)

_HEADING_REJECT_PATTERN_V50 = re.compile(
    r"^(?:internship\s+details|job\s+description|position\s+description|"
    r"role\s+description|our\s+team|latest\s+news)$",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V50 = re.compile(
    r"(?:/(?:privacy|terms|cookie|news|blog|about|contact|investor|team|culture|"
    r"services?|leadership|people|login|logout|register|account|help|support|"
    r"wp-json|feed|rss|author|category|tag)(?:/|$|[?#]))",
    re.IGNORECASE,
)

_DETAILISH_URL_PATTERN_V50 = re.compile(
    r"(?:/jobs?/[^/?#]{4,}|/career/openings?/|/jobdetails(?:/|$|\?)|"
    r"PortalDetail\.na\?.*jobid=|[?&](?:jobid|job_id|requisitionid|positionid|"
    r"vacancyid|jobadid|adid|ajid|id)=)",
    re.IGNORECASE,
)

_LISTING_LINK_TEXT_PATTERN_V50 = re.compile(
    r"\b(?:job\s+openings?|current\s+vacancies|join\s+our\s+team|view\s+all\s+jobs?|"
    r"search\s+jobs|browse\s+jobs|open\s+roles|lowongan|kerjaya|karir|loker|"
    r"careers?)\b",
    re.IGNORECASE,
)

_LISTING_HUB_URL_PATTERN_V50 = re.compile(
    r"/(?:careers?|jobs?|job-openings?|openings?|vacancies?|position|requisition|"
    r"join-our-team|lowongan|loker|kerjaya|karir)",
    re.IGNORECASE,
)

_APPLY_CONTEXT_PATTERN_V50 = re.compile(
    r"(?:apply|application|apply\s+now|apply\s+here|requirements?|"
    r"qualifications?|responsibilit|closing\s+date|full\s*time|part\s*time|"
    r"contract|permanent|temporary|how\s+to\s+apply|cara\s+melamar|"
    r"info\s+lengkap|lamar)",
    re.IGNORECASE,
)

_CTA_TEXT_PATTERN_V50 = re.compile(
    r"^(?:apply(?:\s+now)?|read\s+more|learn\s+more|info\s+lengkap|"
    r"job\s+details?|view\s+details?)$",
    re.IGNORECASE,
)

_MARTIAN_SHELL_PATTERN_V50 = re.compile(
    r"(?:myrecruitmentplus|martianlogic|clientcode|recruiterid|jobboardthemeid|__NEXT_DATA__)",
    re.IGNORECASE,
)

_NEXT_DATA_PATTERN_V50 = re.compile(
    r"<script[^>]+id=['\"]__NEXT_DATA__['\"][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)

_GENERIC_SLUG_TAILS_V50 = {
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
    "karir",
    "kerjaya",
}


class TieredExtractorV50(TieredExtractorV16):
    """v5.0 extractor with simplified shell recovery and card-grid extraction."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        html_body = html or ""
        started = asyncio.get_running_loop().time()

        candidates: list[tuple[str, list[dict]]] = []

        try:
            parent_jobs = await asyncio.wait_for(
                super().extract(career_page, company, html_body),
                timeout=12.0,
            )
        except asyncio.TimeoutError:
            logger.warning("v5.0 parent extractor timeout for %s", page_url)
            parent_jobs = []
        except Exception:
            logger.exception("v5.0 parent extractor failed for %s", page_url)
            parent_jobs = []

        parent_jobs = self._prepare_jobs_v50(parent_jobs, page_url)
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        local_jobs = self._extract_local_dom_jobs_v50(page_url, html_body)
        if local_jobs:
            candidates.append(("local_dom_v50", local_jobs))

        structured_jobs = self._extract_structured_jobs_v50(html_body, page_url)
        if structured_jobs:
            candidates.append(("structured_v50", structured_jobs))

        best_label, best_jobs, best_score = self._pick_best_candidate_v50(candidates, page_url)

        shell_like = self._looks_like_martian_shell_v50(page_url, html_body)
        if self._should_probe_shell_v50(best_jobs, best_score, shell_like) and self._within_budget_v50(started, 20.0):
            martian_jobs = await self._extract_martian_shell_jobs_v50(page_url, html_body)
            if martian_jobs:
                candidates.append(("martian_shell_v50", martian_jobs))

        if self._should_follow_hubs_v50(best_jobs) and self._within_budget_v50(started, 23.5):
            hub_jobs = await self._follow_listing_hubs_v50(page_url, html_body)
            if hub_jobs:
                candidates.append(("hub_follow_v50", hub_jobs))

        best_label, best_jobs, _best_score = self._pick_best_candidate_v50(candidates, page_url)
        if not best_jobs:
            return []

        final_jobs = self._prepare_jobs_v50(best_jobs, page_url)
        if self._should_enrich_v50(final_jobs, page_url) and self._within_budget_v50(started, 25.5):
            try:
                enriched = await asyncio.wait_for(self._enrich_from_detail_pages(final_jobs), timeout=6.5)
                final_jobs = self._prepare_jobs_v50(enriched, page_url)
            except asyncio.TimeoutError:
                logger.warning("v5.0 detail enrichment timeout for %s", page_url)
            except Exception:
                logger.exception("v5.0 detail enrichment failed for %s", page_url)

        logger.info("v5.0 selected %s for %s (%d jobs)", best_label, page_url, len(final_jobs))
        return final_jobs[:MAX_JOBS_PER_PAGE]

    def _extract_local_dom_jobs_v50(self, page_url: str, html_body: str) -> list[dict]:
        if not html_body or len(html_body) < 80:
            return []

        candidates: list[tuple[str, list[dict]]] = []

        tier1 = self._extract_tier1_v12(page_url, html_body)
        if tier1:
            candidates.append(("tier1", self._prepare_jobs_v50(tier1, page_url)))

        tier2 = self._extract_tier2_v16(page_url, html_body)
        if tier2:
            candidates.append(("tier2_v16", self._prepare_jobs_v50(tier2, page_url)))

        root = _parse_html(html_body)
        if root is None:
            _label, jobs, _score = self._pick_best_candidate_v50(candidates, page_url)
            return jobs

        elementor = self._extract_elementor_cards_v50(root, page_url)
        if elementor:
            candidates.append(("elementor_cards_v50", elementor))

        utility = self._extract_utility_rows_v50(root, page_url)
        if utility:
            candidates.append(("utility_rows_v50", utility))

        links = self._extract_job_links_v50(root, page_url)
        if links:
            candidates.append(("job_links_v50", links))

        _label, jobs, _score = self._pick_best_candidate_v50(candidates, page_url)
        return jobs

    def _extract_elementor_cards_v50(self, root: etree._Element, page_url: str) -> list[dict]:
        cards = root.xpath(
            "//*[contains(@class,'elementor-column') and contains(@class,'elementor-inner-column')]"
        )
        if len(cards) < 2:
            return []

        jobs: list[dict] = []
        for card in cards[:500]:
            heading_nodes = card.xpath(
                ".//h2[contains(@class,'elementor-heading-title')][1] | "
                ".//h3[contains(@class,'elementor-heading-title')][1]"
            )
            if not heading_nodes:
                continue

            title = self._normalize_title_v50(_text(heading_nodes[0]))
            if not title or _TITLE_REJECT_PATTERN_V50.match(title.lower()):
                continue

            source_url = ""
            cta_text = ""
            for a_el in card.xpath(".//a[@href]"):
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v50(href):
                    continue

                text = self._safe_text_v50(a_el)
                text_l = text.lower()
                if _LISTING_LINK_TEXT_PATTERN_V50.search(text_l):
                    continue

                source_url = href
                cta_text = text
                if self._is_job_like_url_v50(href) or _CTA_TEXT_PATTERN_V50.match(text_l):
                    break

            if not source_url:
                continue
            if not self._is_title_acceptable_v50(title, source_url):
                continue

            card_text = _text(card)
            if len(card_text) < 35 and not _APPLY_CONTEXT_PATTERN_V50.search(cta_text):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v50(card, title),
                    "salary_raw": self._extract_salary_v50(card_text),
                    "employment_type": self._extract_job_type_v50(card_text),
                    "description": self._clean_description_v50(card_text),
                    "extraction_method": "tier2_elementor_cards_v50",
                    "extraction_confidence": 0.82,
                }
            )

        return self._prepare_jobs_v50(jobs, page_url)

    def _extract_utility_rows_v50(self, root: etree._Element, page_url: str) -> list[dict]:
        rows = root.xpath(
            "//div[contains(concat(' ', normalize-space(@class), ' '), ' p-4 ') "
            "or contains(concat(' ', normalize-space(@class), ' '), ' mx-4 ') "
            "or contains(concat(' ', normalize-space(@class), ' '), ' job-card ')]"
        )
        if len(rows) < 2:
            return []

        jobs: list[dict] = []
        for row in rows[:420]:
            links = row.xpath(".//a[@href]")
            if not links:
                continue

            heading_nodes = row.xpath(
                ".//h1[1] | .//h2[1] | .//h3[1] | .//h4[1] | "
                ".//div[contains(@class,'cursor-pointer')][1]"
            )
            title = self._normalize_title_v50(_text(heading_nodes[0])) if heading_nodes else self._normalize_title_v50(_text(links[0]))
            if not title:
                continue

            source_url = ""
            for a_el in links:
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v50(href):
                    continue
                source_url = href
                if self._is_job_like_url_v50(href):
                    break

            if not source_url or not self._is_title_acceptable_v50(title, source_url):
                continue

            row_text = _text(row)
            has_apply = bool(_APPLY_CONTEXT_PATTERN_V50.search(row_text))
            if not (self._job_url_has_detail_evidence_v50(source_url, page_url) or has_apply or len(row_text) >= 120):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v50(row, title),
                    "salary_raw": self._extract_salary_v50(row_text),
                    "employment_type": self._extract_job_type_v50(row_text),
                    "description": self._clean_description_v50(row_text),
                    "extraction_method": "tier2_utility_rows_v50",
                    "extraction_confidence": 0.8,
                }
            )

        return self._prepare_jobs_v50(jobs, page_url)

    def _extract_job_links_v50(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        for a_el in root.xpath("//a[@href]")[:4200]:
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url)
            if not source_url or self._is_non_job_url_v50(source_url):
                continue
            if not self._job_url_has_detail_evidence_v50(source_url, page_url):
                continue

            heading_nodes = a_el.xpath(".//h1 | .//h2 | .//h3 | .//h4")
            raw_title = _text(heading_nodes[0]) if heading_nodes else _text(a_el)
            title = self._normalize_title_v50(raw_title)
            if not self._is_title_acceptable_v50(title, source_url):
                continue

            context = a_el
            cursor = a_el
            for _ in range(3):
                parent = cursor.getparent()
                if parent is None:
                    break
                cursor = parent
                link_count = len(cursor.xpath(".//a[@href]"))
                if 1 <= link_count <= 14:
                    context = cursor
                    break

            context_text = _text(context)
            if _LISTING_LINK_TEXT_PATTERN_V50.search(title.lower()):
                continue
            if not (self._job_url_has_detail_evidence_v50(source_url, page_url) or len(context_text) >= 100):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v50(context, title),
                    "salary_raw": self._extract_salary_v50(context_text),
                    "employment_type": self._extract_job_type_v50(context_text),
                    "description": self._clean_description_v50(context_text),
                    "extraction_method": "tier2_job_links_v50",
                    "extraction_confidence": 0.72,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return self._prepare_jobs_v50(jobs, page_url)

    def _extract_structured_jobs_v50(self, html_body: str, page_url: str) -> list[dict]:
        if not html_body or len(html_body) < 80:
            return []

        candidates: list[tuple[str, list[dict]]] = []

        state_jobs: list[dict] = []
        context: dict[str, str] = {}

        next_match = _NEXT_DATA_PATTERN_V50.search(html_body)
        if next_match:
            payload = html_lib.unescape((next_match.group(1) or "").strip())
            if payload:
                try:
                    parsed = json.loads(payload)
                except Exception:
                    parsed = None
                if parsed is not None:
                    context = self._extract_martian_context_v50(page_url, parsed)
                    state_jobs.extend(
                        self._extract_jobs_from_json_payload_v50(
                            parsed,
                            page_url,
                            "tier0_next_data_v50",
                            context=context,
                        )
                    )

        for match in re.finditer(
            r"<script[^>]+type=['\"]application/json['\"][^>]*>(.*?)</script>",
            html_body,
            re.IGNORECASE | re.DOTALL,
        ):
            payload = html_lib.unescape((match.group(1) or "").strip())
            if len(payload) < 40 or len(payload) > 1_200_000:
                continue
            if "<" in payload[:200]:
                continue
            try:
                parsed = json.loads(payload)
            except Exception:
                continue
            state_jobs.extend(
                self._extract_jobs_from_json_payload_v50(
                    parsed,
                    page_url,
                    "tier0_state_json_v50",
                    context=context,
                )
            )

        if state_jobs:
            candidates.append(("state_json_v50", self._prepare_jobs_v50(state_jobs, page_url)))

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

            for node in self._iter_json_dicts_v50(parsed):
                type_field = node.get("@type")
                is_job = False
                if isinstance(type_field, str):
                    is_job = type_field.lower() == "jobposting"
                elif isinstance(type_field, list):
                    is_job = any(str(t).lower() == "jobposting" for t in type_field)
                if not is_job:
                    continue

                title = self._normalize_title_v50(str(node.get("title") or node.get("name") or ""))
                source_url = _resolve_url(node.get("url") or node.get("sameAs") or node.get("applyUrl"), page_url) or page_url
                if not self._is_title_acceptable_v50(title, source_url):
                    continue

                location = self._extract_location_from_json_v50(node)
                description = self._clean_description_v50(
                    str(node.get("description") or node.get("responsibilities") or node.get("qualifications") or "")
                )

                jsonld_jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": location,
                        "salary_raw": self._extract_salary_v50(json.dumps(node, ensure_ascii=True, default=str)),
                        "employment_type": str(node.get("employmentType") or "").strip() or None,
                        "description": description,
                        "extraction_method": "tier0_jsonld_v50",
                        "extraction_confidence": 0.86,
                    }
                )

        if jsonld_jobs:
            candidates.append(("jsonld_v50", self._prepare_jobs_v50(jsonld_jobs, page_url)))

        _label, best_jobs, _score = self._pick_best_candidate_v50(candidates, page_url)
        return best_jobs

    async def _extract_martian_shell_jobs_v50(self, page_url: str, html_body: str) -> list[dict]:
        if not self._looks_like_martian_shell_v50(page_url, html_body):
            return []

        context = self._extract_martian_context_from_html_v50(page_url, html_body)
        if not (context.get("client_code") or context.get("recruiter_id") or context.get("build_id")):
            return []

        endpoints = self._martian_probe_urls_v50(page_url, context)
        if not endpoints:
            return []

        jobs: list[dict] = []
        budget = 12
        seen: set[str] = set()

        async with httpx.AsyncClient(
            timeout=2.6,
            follow_redirects=True,
            headers={
                "Accept": "application/json,text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Referer": page_url,
            },
        ) as client:
            for endpoint in endpoints:
                if budget <= 0:
                    break
                norm = endpoint.rstrip("/")
                if not endpoint or norm in seen:
                    continue
                seen.add(norm)
                budget -= 1

                try:
                    resp = await asyncio.wait_for(client.get(endpoint), timeout=2.6)
                except Exception:
                    continue

                if resp.status_code >= 400 or not resp.text:
                    continue

                extracted = self._extract_jobs_from_probe_payload_v50(
                    resp.text,
                    str(resp.url),
                    page_url,
                    context,
                )
                if extracted:
                    jobs.extend(extracted)
                    prepared = self._prepare_jobs_v50(jobs, page_url)
                    if len(prepared) >= MIN_JOBS_FOR_SUCCESS:
                        return prepared

            post_endpoints = [u for u in endpoints if "/api/" in u and "search" in u][:2]
            post_payloads = self._martian_post_payloads_v50(context)
            for endpoint in post_endpoints:
                if budget <= 0:
                    break
                for payload in post_payloads[:2]:
                    if budget <= 0:
                        break
                    budget -= 1
                    try:
                        resp = await asyncio.wait_for(client.post(endpoint.split("?", 1)[0], json=payload), timeout=2.6)
                    except Exception:
                        continue

                    if resp.status_code >= 400 or not resp.text:
                        continue

                    extracted = self._extract_jobs_from_probe_payload_v50(
                        resp.text,
                        str(resp.url),
                        page_url,
                        context,
                    )
                    if extracted:
                        jobs.extend(extracted)
                        prepared = self._prepare_jobs_v50(jobs, page_url)
                        if len(prepared) >= MIN_JOBS_FOR_SUCCESS:
                            return prepared

        return self._prepare_jobs_v50(jobs, page_url)

    def _extract_jobs_from_probe_payload_v50(
        self,
        body: str,
        response_url: str,
        page_url: str,
        context: dict[str, str],
    ) -> list[dict]:
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
                jobs.extend(
                    self._extract_jobs_from_json_payload_v50(
                        parsed,
                        response_url,
                        "tier0_martian_probe_v50",
                        context=context,
                    )
                )

        root = _parse_html(payload)
        if root is not None:
            marker_text = (payload[:3000] or "").lower()
            if any(marker in marker_text for marker in ("elementor", "job-card", "p-4", "mx-4", "info lengkap", "apply")):
                jobs.extend(self._extract_elementor_cards_v50(root, response_url))
                jobs.extend(self._extract_utility_rows_v50(root, response_url))
                jobs.extend(self._extract_job_links_v50(root, response_url))

        return self._prepare_jobs_v50(jobs, page_url)

    async def _follow_listing_hubs_v50(self, page_url: str, html_body: str) -> list[dict]:
        root = _parse_html(html_body)
        if root is None:
            return []

        links = self._collect_listing_hub_links_v50(root, page_url)
        if not links:
            return []

        candidates: list[tuple[str, list[dict]]] = []
        fetched = 0
        for sub_url in links[:2]:
            sub_html = await self._fetch_html_v50(sub_url)
            if not sub_html:
                continue
            fetched += 1

            local = self._extract_local_dom_jobs_v50(sub_url, sub_html)
            if local:
                candidates.append((f"hub_local_{fetched}", local))

            structured = self._extract_structured_jobs_v50(sub_html, sub_url)
            if structured:
                candidates.append((f"hub_structured_{fetched}", structured))

        _label, jobs, _score = self._pick_best_candidate_v50(candidates, page_url)
        return jobs

    def _collect_listing_hub_links_v50(self, root: etree._Element, page_url: str) -> list[str]:
        parsed_page = urlparse(page_url)
        host = (parsed_page.netloc or "").lower()

        scored: list[tuple[int, str]] = []
        seen: set[str] = set()

        for a_el in root.xpath("//a[@href]")[:1500]:
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            full_url = _resolve_url(href, page_url)
            if not full_url or self._is_non_job_url_v50(full_url):
                continue

            parsed = urlparse(full_url)
            other_host = (parsed.netloc or "").lower()
            if other_host and host and other_host != host:
                base_a = ".".join(host.split(".")[-2:])
                base_b = ".".join(other_host.split(".")[-2:])
                if base_a != base_b:
                    continue

            if full_url.rstrip("/") == page_url.rstrip("/"):
                continue

            text = self._safe_text_v50(a_el)
            score = 0
            if _LISTING_LINK_TEXT_PATTERN_V50.search(text):
                score += 10
            if _LISTING_HUB_URL_PATTERN_V50.search(parsed.path or ""):
                score += 8
            if any(tok in (parsed.path or "").lower() for tok in ("lowongan", "loker", "karir", "kerjaya")):
                score += 8
            if "?" in full_url and any(k in full_url.lower() for k in ("search", "jobs", "career")):
                score += 2

            if score <= 0:
                continue
            norm = full_url.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
            scored.append((score, full_url))

        scored.sort(key=lambda item: item[0], reverse=True)
        return [u for _, u in scored[:5]]

    async def _fetch_html_v50(self, url: str) -> Optional[str]:
        try:
            async with httpx.AsyncClient(
                timeout=4.5,
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
        if resp.status_code != 200 or len(body) < 180 or self._looks_non_html_payload_v50(body):
            return None
        return body

    def _extract_jobs_from_json_payload_v50(
        self,
        payload: Any,
        page_url: str,
        method: str,
        context: Optional[dict[str, str]] = None,
    ) -> list[dict]:
        jobs: list[dict] = []
        client_code = (context or {}).get("client_code", "")

        for row in self._iter_json_dicts_v50(payload):
            title = self._normalize_title_v50(
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
            if not title:
                continue
            if self._looks_like_taxonomy_node_v50(row, title):
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
                if client_code:
                    source_url = f"{host_base}/{client_code}/{req_id}"
                else:
                    source_url = f"{host_base}/jobdetails?jobAdId={req_id}"

            if self._is_non_job_url_v50(source_url):
                continue
            if not self._is_title_acceptable_v50(title, source_url):
                continue

            description = self._clean_description_v50(
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

            location = self._extract_location_from_json_v50(row)
            employment_type = str(row.get("employmentType") or row.get("jobType") or row.get("JobType") or "").strip() or None
            salary_raw = self._extract_salary_v50(json.dumps(row, ensure_ascii=True, default=str))

            evidence = (
                self._job_url_has_detail_evidence_v50(source_url, page_url)
                or bool(location)
                or bool(employment_type)
                or bool(salary_raw)
                or (description is not None and len(description) >= 110)
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
                    "extraction_confidence": 0.8,
                }
            )

            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return self._prepare_jobs_v50(jobs, page_url)

    def _pick_best_candidate_v50(
        self,
        candidates: list[tuple[str, list[dict]]],
        page_url: str,
    ) -> tuple[str, list[dict], float]:
        if not candidates:
            return "", [], 0.0

        scored: list[tuple[str, list[dict], float]] = []
        for label, jobs in candidates:
            prepared = self._prepare_jobs_v50(jobs, page_url)
            if not prepared:
                continue
            score = self._candidate_score_v50(prepared, page_url)
            scored.append((label, prepared, score))

        if not scored:
            return "", [], 0.0

        best_label, best_jobs, best_score = max(scored, key=lambda item: item[2])

        for label, jobs, score in scored:
            if label == best_label:
                continue
            overlap = self._title_overlap_ratio_v50(jobs, best_jobs)
            if len(jobs) >= len(best_jobs) + 1 and overlap >= 0.6 and score >= best_score - 1.1:
                best_label, best_jobs, best_score = label, jobs, score

        return best_label, best_jobs[:MAX_JOBS_PER_PAGE], best_score

    def _prepare_jobs_v50(self, jobs: list[dict], page_url: str) -> list[dict]:
        cleaned: list[dict] = []

        for idx, raw in enumerate(jobs or []):
            title = self._normalize_title_v50(str(raw.get("title") or ""))
            source_url = _resolve_url(raw.get("source_url"), page_url) or page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]

            description = self._clean_description_v50(str(raw.get("description") or ""))
            if self._is_non_job_url_v50(source_url):
                continue
            if not self._is_title_acceptable_v50(title, source_url):
                continue

            cleaned.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": raw.get("location_raw") or None,
                    "salary_raw": raw.get("salary_raw") or None,
                    "employment_type": raw.get("employment_type") or self._extract_job_type_v50(description or ""),
                    "description": description,
                    "extraction_method": raw.get("extraction_method") or "tier2_v50",
                    "extraction_confidence": raw.get("extraction_confidence", 0.66),
                    "_order": idx,
                }
            )

        deduped = self._dedupe_jobs_v50(cleaned, page_url)
        if not self._is_valid_jobset_v50(deduped, page_url):
            return []
        return deduped

    def _dedupe_jobs_v50(self, jobs: list[dict], page_url: str) -> list[dict]:
        by_key: dict[tuple[str, str], dict] = {}
        by_url: dict[str, dict] = {}
        page_norm = (page_url or "").rstrip("/").lower()

        for job in jobs:
            title = self._normalize_title_v50(job.get("title", ""))
            url = _resolve_url(job.get("source_url"), page_url) or page_url
            key = (title.lower(), url.lower())
            existing = by_key.get(key)
            if existing is None or self._title_quality_score_v50(title) > self._title_quality_score_v50(existing.get("title", "")):
                by_key[key] = job

        for job in by_key.values():
            norm_url = (_resolve_url(job.get("source_url"), page_url) or page_url).rstrip("/").lower()
            if norm_url == page_norm:
                same_page_key = f"{norm_url}#title:{self._normalize_title_v50(job.get('title', '')).lower()}"
                by_url[same_page_key] = job
                continue

            current = by_url.get(norm_url)
            if current is None:
                by_url[norm_url] = job
                continue
            if self._title_quality_score_v50(job.get("title", "")) > self._title_quality_score_v50(current.get("title", "")):
                by_url[norm_url] = job

        deduped = sorted(by_url.values(), key=lambda item: int(item.get("_order", 0)))
        for item in deduped:
            item.pop("_order", None)
        return deduped[:MAX_JOBS_PER_PAGE]

    def _is_valid_jobset_v50(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title_v50(j.get("title", "")) for j in jobs if j.get("title")]
        if not titles:
            return False

        role_hits = sum(1 for t in titles if self._title_has_role_signal_v50(t))
        reject_hits = sum(1 for t in titles if _TITLE_REJECT_PATTERN_V50.match(t.lower()))
        evidence_hits = sum(1 for j in jobs if self._job_has_evidence_v50(j, page_url))

        if reject_hits >= max(1, int(len(titles) * 0.2)):
            return False

        if len(titles) <= 2:
            return role_hits == len(titles) and evidence_hits >= 1

        if role_hits < max(2, int(len(titles) * 0.5)):
            return False

        return evidence_hits >= max(2, int(len(titles) * 0.4))

    def _candidate_score_v50(self, jobs: list[dict], page_url: str) -> float:
        titles = [self._normalize_title_v50(j.get("title", "")) for j in jobs]
        role_hits = sum(1 for t in titles if self._title_has_role_signal_v50(t))
        detail_hits = sum(1 for j in jobs if self._job_url_has_detail_evidence_v50(str(j.get("source_url") or ""), page_url))
        evidence_hits = sum(1 for j in jobs if self._job_has_evidence_v50(j, page_url))

        score = len(jobs) * 4.5
        score += role_hits * 2.6
        score += detail_hits * 2.1
        score += evidence_hits * 1.4
        return score

    def _job_has_evidence_v50(self, job: dict, page_url: str) -> bool:
        source_url = (job.get("source_url") or page_url).strip() or page_url
        desc = str(job.get("description") or "")

        if self._job_url_has_detail_evidence_v50(source_url, page_url):
            return True
        if job.get("salary_raw") or job.get("employment_type") or job.get("location_raw"):
            return True
        if _APPLY_CONTEXT_PATTERN_V50.search(desc):
            return True
        return len(desc.strip()) >= 170

    def _normalize_title_v50(self, title: str) -> str:
        value = html_lib.unescape((title or "").strip())
        value = re.sub(r"\s+", " ", value)
        value = value.strip(" \t\r\n-–|:;,>")
        value = re.sub(
            r"\s*(?:apply\s+now|apply\s+here|read\s+more|learn\s+more|info\s+lengkap)\s*$",
            "",
            value,
            flags=re.IGNORECASE,
        )
        value = re.sub(r"\s*(?:deadline\s*:\s*\S+.*|closing\s+date\s*:\s*\S+.*)$", "", value, flags=re.IGNORECASE)
        value = value.strip(" \t\r\n-–|:;,")
        value = re.sub(r"\.+$", "", value).strip()
        return value

    def _is_title_acceptable_v50(self, title: str, source_url: str) -> bool:
        if not self._is_valid_title_text_v50(title):
            return False

        if self._title_has_role_signal_v50(title):
            return True

        if self._is_job_like_url_v50(source_url) and len(title.split()) <= 8:
            return True

        return False

    def _is_valid_title_text_v50(self, title: str) -> bool:
        t = (title or "").strip()
        if not t:
            return False
        if len(t) < 3 or len(t) > 180:
            return False
        if len(t.split()) > 13:
            return False
        if not re.search(r"[A-Za-z]", t):
            return False

        lower = t.lower()
        if _TITLE_REJECT_PATTERN_V50.match(lower):
            return False
        if _HEADING_REJECT_PATTERN_V50.match(lower):
            return False

        if any(phrase == lower for phrase in ("sign in", "log in", "register", "apply now", "search jobs")):
            return False

        if not super()._is_valid_title_v16(t):
            # Allow compact single-word role titles if role evidence is strong
            # (e.g. "Influencer", "Akuntan" on Elementor card grids).
            if len(t.split()) == 1 and len(t) <= 32 and self._title_has_role_signal_v50(t):
                return True
            return False

        return True

    def _title_has_role_signal_v50(self, title: str) -> bool:
        if not title:
            return False
        return bool(_ROLE_HINT_PATTERN_V50.search(title) or _title_has_job_noun(title) or self._is_acronym_title_v50(title))

    @staticmethod
    def _is_acronym_title_v50(title: str) -> bool:
        t = (title or "").strip()
        if not re.fullmatch(r"[A-Z][A-Z0-9&/\-\+]{1,10}", t):
            return False
        return t.lower() not in {"home", "menu", "faq", "apply"}

    def _job_url_has_detail_evidence_v50(self, url: str, page_url: str) -> bool:
        value = (url or "").strip()
        if not value or self._is_non_job_url_v50(value):
            return False
        if self._is_job_like_url_v50(value):
            return True

        if page_url and value.rstrip("/") == page_url.rstrip("/"):
            return False

        parsed = urlparse(value)
        parts = [p for p in (parsed.path or "").split("/") if p]
        if not parts:
            return False

        leaf = parts[-1].lower()
        if leaf in _GENERIC_SLUG_TAILS_V50:
            return False
        if leaf in {"index", "home", "about", "contact", "news", "blog", "privacy", "terms"}:
            return False

        if len(leaf) >= 6 and ("-" in leaf or re.search(r"\d", leaf) or len(parts) >= 2):
            return True
        return len(leaf) >= 10

    def _is_job_like_url_v50(self, url: str) -> bool:
        value = (url or "").strip()
        if not value or self._is_non_job_url_v50(value):
            return False
        if _DETAILISH_URL_PATTERN_V50.search(value):
            return True

        parsed = urlparse(value)
        path = (parsed.path or "").lower().rstrip("/")
        parts = [p for p in path.split("/") if p]
        if not parts:
            return False

        leaf = parts[-1]
        if re.fullmatch(r"\d{4,}", leaf):
            parent = parts[-2] if len(parts) >= 2 else ""
            if parent not in {"news", "blog", "about", "contact", "page", "category", "tag", "privacy", "terms"}:
                return True

        query = parsed.query.lower()
        if re.search(r"(?:^|&)(?:jobid|job_id|requisitionid|vacancyid|jobadid|adid|ajid)=", query):
            return True

        return False

    @staticmethod
    def _is_non_job_url_v50(url: str) -> bool:
        value = (url or "").strip().lower()
        if not value:
            return False
        if value.startswith("mailto:") or value.startswith("tel:"):
            return True
        return bool(_NON_JOB_URL_PATTERN_V50.search(value))

    @staticmethod
    def _looks_non_html_payload_v50(body: Optional[str]) -> bool:
        if not body:
            return True
        sample = body[:900].lstrip()
        if sample.startswith("%PDF-"):
            return True
        low = sample.lower()
        if (low.startswith("{") or low.startswith("[")) and "<html" not in low[:300]:
            return True
        return False

    def _should_probe_shell_v50(self, best_jobs: list[dict], best_score: float, shell_like: bool) -> bool:
        if shell_like:
            return True
        if not best_jobs:
            return True
        if len(best_jobs) < MIN_JOBS_FOR_SUCCESS:
            return True
        return best_score < 12.0

    def _should_follow_hubs_v50(self, best_jobs: list[dict]) -> bool:
        if not best_jobs:
            return True
        return len(best_jobs) < 4

    def _looks_like_martian_shell_v50(self, page_url: str, html_body: str) -> bool:
        lower = (html_body or "").lower()
        if "<div id=\"__next\"></div>" in lower and "__next_data__" in lower:
            return True
        if len(lower) < 350:
            return True
        if _MARTIAN_SHELL_PATTERN_V50.search(lower):
            return True
        if "myrecruitmentplus" in (page_url or "").lower() or "martianlogic" in (page_url or "").lower():
            return True
        return False

    @staticmethod
    def _within_budget_v50(started: float, limit_seconds: float) -> bool:
        return (asyncio.get_running_loop().time() - started) < limit_seconds

    def _extract_location_v50(self, row: etree._Element, title: str) -> Optional[str]:
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
    def _extract_salary_v50(text: str) -> Optional[str]:
        if not text:
            return None
        match = _SALARY_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:120]
        return None

    @staticmethod
    def _extract_job_type_v50(text: str) -> Optional[str]:
        if not text:
            return None
        match = _JOB_TYPE_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:80]
        return None

    def _extract_location_from_json_v50(self, node: dict[str, Any]) -> Optional[str]:
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
                    nested = self._extract_location_from_json_v50(item)
                    if nested:
                        return nested
        return None

    @staticmethod
    def _iter_json_dicts_v50(payload: Any) -> list[dict[str, Any]]:
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

    @staticmethod
    def _looks_like_taxonomy_node_v50(node: dict[str, Any], title: str) -> bool:
        lowered = title.lower().strip()
        if _TITLE_REJECT_PATTERN_V50.match(lowered):
            return True

        keyset = {str(k).lower() for k in node.keys()}
        taxonomy_keys = {
            "department",
            "departments",
            "category",
            "categories",
            "team",
            "teams",
            "function",
            "functions",
            "office",
            "location",
            "locations",
            "count",
            "total",
            "children",
            "nodes",
        }
        evidence_keys = {
            "url",
            "joburl",
            "applyurl",
            "applicationformurl",
            "id",
            "jobid",
            "requisitionid",
            "description",
            "externaldescriptionstr",
            "summary",
        }

        if len(keyset & taxonomy_keys) >= 2 and not (keyset & evidence_keys):
            return True

        if "count" in keyset and any(k in keyset for k in ("department", "category", "team")):
            return True

        return False

    def _title_quality_score_v50(self, title: str) -> float:
        t = self._normalize_title_v50(title)
        if not t:
            return -10.0
        score = 0.0
        score += 2.0 if self._title_has_role_signal_v50(t) else 0.0
        score += 1.0 if self._is_valid_title_text_v50(t) else 0.0
        score -= max(0.0, (len(t) - 90) / 80.0)
        return score

    def _title_overlap_ratio_v50(self, a_jobs: list[dict], b_jobs: list[dict]) -> float:
        a_titles = {self._normalize_title_v50(j.get("title", "")).lower() for j in a_jobs if j.get("title")}
        b_titles = {self._normalize_title_v50(j.get("title", "")).lower() for j in b_jobs if j.get("title")}
        if not a_titles or not b_titles:
            return 0.0
        return len(a_titles & b_titles) / max(1, min(len(a_titles), len(b_titles)))

    @staticmethod
    def _safe_text_v50(el: etree._Element) -> str:
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

    def _clean_description_v50(self, description: str) -> Optional[str]:
        text = " ".join((description or "").split())
        if not text:
            return None
        cut = re.search(r"\b(?:how\s+to\s+apply|cara\s+melamar|application\s+process)\b", text, re.IGNORECASE)
        if cut:
            text = text[: cut.start()].strip()
        if len(text) < 45:
            return None
        return text[:5000]

    def _should_enrich_v50(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False
        off_page = [
            j
            for j in jobs
            if (j.get("source_url") or "").startswith("http")
            and (j.get("source_url") or page_url).rstrip("/") != page_url.rstrip("/")
            and not self._is_non_job_url_v50(j.get("source_url") or "")
        ]
        if not off_page:
            return False

        low_quality = sum(1 for j in off_page if not j.get("description") or len(str(j.get("description") or "")) < 170)
        if low_quality >= 2:
            return True

        return len(off_page) >= 5

    def _extract_martian_context_from_html_v50(self, page_url: str, html_body: str) -> dict[str, str]:
        context = {
            "client_code": "",
            "recruiter_id": "",
            "job_board_theme_id": "",
            "build_id": "",
            "next_page": "",
            "next_query": "",
        }

        match = _NEXT_DATA_PATTERN_V50.search(html_body or "")
        if match:
            raw_payload = html_lib.unescape((match.group(1) or "").strip())
            try:
                parsed = json.loads(raw_payload)
            except Exception:
                parsed = {}

            parsed_context = self._extract_martian_context_v50(page_url, parsed)
            context.update(parsed_context)

        if not context["client_code"]:
            m = re.search(r"clientCode['\"]?\s*[:=]\s*['\"]([a-z0-9_-]{2,})", html_body or "", re.IGNORECASE)
            if m:
                context["client_code"] = m.group(1)

        if not context["recruiter_id"]:
            m = re.search(r"recruiterId['\"]?\s*[:=]\s*['\"]?([0-9]{2,})", html_body or "", re.IGNORECASE)
            if m:
                context["recruiter_id"] = m.group(1)

        if not context["client_code"]:
            parsed_url = urlparse(page_url)
            parts = [seg for seg in parsed_url.path.split("/") if seg]
            if parts:
                guess = re.sub(r"[^a-z0-9-]", "", parts[-1].lower())
                if len(guess) >= 3:
                    context["client_code"] = guess

        return context

    def _extract_martian_context_v50(self, page_url: str, payload: dict[str, Any]) -> dict[str, str]:
        context = {
            "client_code": "",
            "recruiter_id": "",
            "job_board_theme_id": "",
            "build_id": "",
            "next_page": "",
            "next_query": "",
        }

        if not isinstance(payload, dict):
            return context

        page_props = payload.get("props", {}).get("pageProps", {})
        if isinstance(page_props, dict):
            context["client_code"] = str(page_props.get("clientCode") or "").strip()
            context["recruiter_id"] = str(page_props.get("recruiterId") or "").strip()
            context["job_board_theme_id"] = str(page_props.get("jobBoardThemeId") or "").strip()

        context["build_id"] = str(payload.get("buildId") or "").strip()
        context["next_page"] = str(payload.get("page") or "").strip()

        query = payload.get("query")
        if isinstance(query, dict):
            context["next_query"] = urlencode(
                {str(k): str(v) for k, v in query.items() if isinstance(v, (str, int, float))}
            )
            if not context["client_code"]:
                context["client_code"] = str(query.get("client") or query.get("clientCode") or "").strip()

        if not context["client_code"]:
            parsed = urlparse(page_url)
            parts = [seg for seg in parsed.path.split("/") if seg]
            if parts:
                context["client_code"] = parts[-1].strip()

        return context

    def _martian_probe_urls_v50(self, page_url: str, context: dict[str, str]) -> list[str]:
        parsed = urlparse(page_url or "")
        if not parsed.netloc:
            return []

        client_code = (context.get("client_code") or "").strip()
        recruiter_id = (context.get("recruiter_id") or "").strip()
        build_id = (context.get("build_id") or "").strip()

        page_host = f"{parsed.scheme or 'https'}://{parsed.netloc}".rstrip("/")

        hosts = [
            page_host,
            "https://web.martianlogic.com",
            "https://jobs.myrecruitmentplus.com",
        ]
        if client_code:
            hosts.append(f"https://{client_code}.myrecruitmentplus.com")

        query_parts = ["pageNumber=1&pageSize=50&isActive=true"]
        if client_code:
            query_parts.extend(
                [
                    f"client={client_code}&pageNumber=1&pageSize=50&isActive=true",
                    f"clientCode={client_code}&pageNumber=1&pageSize=50&isActive=true",
                    f"clientCode={client_code}&search=",
                ]
            )
        if recruiter_id:
            query_parts.extend(
                [
                    f"recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true",
                    f"recruiterId={recruiter_id}&search=",
                ]
            )

        probe_urls: list[str] = []
        for host in hosts:
            host = host.rstrip("/")

            for api_path in (
                "/api/jobs/search",
                "/api/job-search",
                "/api/jobads/search",
                "/api/job-ads/search",
                "/jobs/search",
                "/embed-jobs",
            ):
                base = f"{host}{api_path}"
                probe_urls.append(base)
                for q in query_parts:
                    probe_urls.append(f"{base}?{q}")

            if client_code:
                base = f"{host}/{client_code}"
                probe_urls.append(base)
                for q in query_parts:
                    probe_urls.append(f"{base}?{q}")

            if build_id:
                probe_urls.extend(self._next_data_probe_urls_v50(host, page_url, context))

        deduped = sorted({u.rstrip("/"): u for u in probe_urls if u}.values())
        deduped.sort(key=lambda u: self._martian_endpoint_priority_v50(u, parsed.netloc.lower()), reverse=True)
        return self._diversify_probe_hosts_v50(deduped, limit=18)

    def _next_data_probe_urls_v50(self, host: str, page_url: str, context: dict[str, str]) -> list[str]:
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
        if client_code:
            candidates.append(f"{host}/_next/data/{build_id}/{client_code}.json")
            candidates.append(f"{host}/_next/data/{build_id}/{client_code}/index.json")

        if encoded_query:
            candidates = [f"{u}?{encoded_query}" for u in candidates]
        return candidates

    @staticmethod
    def _martian_endpoint_priority_v50(url: str, page_host: str) -> int:
        low = (url or "").lower()
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()

        score = 0
        if host == page_host:
            score += 15
        if "/api/" in low:
            score += 8
        if "search" in low:
            score += 7
        if "/_next/data/" in low:
            score += 6
        if "client=" in low or "clientcode=" in low:
            score += 4
        if "recruiterid=" in low:
            score += 3
        return score

    @staticmethod
    def _diversify_probe_hosts_v50(urls: list[str], limit: int = 18) -> list[str]:
        buckets: dict[str, list[str]] = {}
        host_order: list[str] = []

        for url in urls:
            host = (urlparse(url).netloc or "").lower() or "_no_host"
            if host not in buckets:
                buckets[host] = []
                host_order.append(host)
            buckets[host].append(url)

        ordered: list[str] = []
        while host_order and len(ordered) < limit:
            next_hosts: list[str] = []
            for host in host_order:
                queue = buckets.get(host) or []
                if not queue:
                    continue
                ordered.append(queue.pop(0))
                if queue:
                    next_hosts.append(host)
                if len(ordered) >= limit:
                    break
            host_order = next_hosts

        return ordered[:limit]

    @staticmethod
    def _martian_post_payloads_v50(context: dict[str, str]) -> list[dict[str, Any]]:
        client_code = (context.get("client_code") or "").strip()
        recruiter_id = (context.get("recruiter_id") or "").strip()

        payloads: list[dict[str, Any]] = []
        if client_code:
            payloads.extend(
                [
                    {"client": client_code, "pageNumber": 1, "pageSize": 50, "isActive": True},
                    {"clientCode": client_code, "pageNumber": 1, "pageSize": 50, "isActive": True},
                ]
            )
        else:
            payloads.append({"pageNumber": 1, "pageSize": 50, "isActive": True})

        if recruiter_id:
            payloads.append({"recruiterId": recruiter_id, "pageNumber": 1, "pageSize": 50, "isActive": True})

        return payloads
