"""
Tiered Extraction Engine v4.9 — direct from v1.6.

v4.9 focuses on a simpler, timeout-safe strategy:
1. Fast local DOM extraction for Elementor/utility card layouts.
2. Strict structured-data extraction (JSON-LD + embedded JSON state).
3. Fast-pass Martian/MyRecruitmentPlus shell probing with tight request budgets.
4. Hub/iframe traversal with bounded fetches for multi-step listing pages.
5. Coverage-first selection with role/evidence validation and lightweight enrichment.
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


_ROLE_HINT_PATTERN_V49 = re.compile(
    r"\b(?:engineer|developer|manager|director|analyst|specialist|assistant|"
    r"consultant|coordinator|officer|administrator|accountant|technician|"
    r"designer|architect|operator|supervisor|advisor|executive|intern(?:ship)?|"
    r"recruit(?:er)?|nurse|teacher|driver|chef|chemist|mechanic|associate|"
    r"representative|agent|planner|liaison|akuntan|konsultan|asisten|pegawai|"
    r"karyawan|influencer|videografer|fotografer|psikolog(?:i)?|sarjana|"
    r"full\s*stack|fullstack|devops|yardman|activator|scientist|foreman|"
    r"electrician|labourer|operator|technologist|marketing|media|customer\s+service|"
    r"fashion|model|writer|content|sales|service|crew)\b",
    re.IGNORECASE,
)

_TITLE_REJECT_PATTERN_V49 = re.compile(
    r"^(?:join\s+our\s+team|current\s+jobs?|all\s+jobs?|job\s+openings?|"
    r"search\s+jobs?|browse\s+jobs?|view\s+all\s+jobs?|careers?|open\s+roles?|"
    r"about\s+us|our\s+culture|our\s+values?|our\s+ecosystem|"
    r"contact|home|menu|read\s+more|learn\s+more|show\s+more|load\s+more|"
    r"apply(?:\s+now)?|job\s+details?|role\s+details?|"
    r"job\s+alerts?|my\s+applications?|login|register|"
    r"lowongan(?:\s+kerja(?:\s+[a-z]+)?)?|beranda)$",
    re.IGNORECASE,
)

_MARKETING_FRAGMENT_PATTERN_V49 = re.compile(
    r"\b(?:tell\s+us|learn\s+more|read\s+more|meet\s+our|discover|submit|"
    r"our\s+services|our\s+team|our\s+culture|latest\s+news|"
    r"company\s+overview|working\s+at)\b",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V49 = re.compile(
    r"(?:/(?:privacy|terms|cookie|news|blog|about|contact|investor|team|culture|"
    r"our-culture|our-values|our-ecosystem|services?|leadership|people|login|"
    r"logout|register|account|help|support|wp-json|feed|rss|author|category|tag)"
    r"(?:/|$|[?#])|\.(?:pdf|docx?)(?:$|\?))",
    re.IGNORECASE,
)

_DETAILISH_URL_PATTERN_V49 = re.compile(
    r"(?:/jobs?/[^/?#]{4,}|/career/openings?/|/jobdetails(?:/|$|\?)|"
    r"PortalDetail\.na\?.*jobid=|[?&](?:jobid|job_id|requisitionid|positionid|"
    r"vacancyid|jobadid|adid|ajid|id)=)",
    re.IGNORECASE,
)

_LISTING_LINK_TEXT_PATTERN_V49 = re.compile(
    r"\b(?:job\s+openings?|current\s+vacancies|join\s+our\s+team|view\s+all\s+jobs?|"
    r"search\s+jobs|browse\s+jobs|open\s+roles|lowongan|kerjaya|karir|loker|careers?)\b",
    re.IGNORECASE,
)

_LISTING_URL_PATTERN_V49 = re.compile(
    r"/(?:careers?|jobs?|job-openings?|openings?|vacancies?|position|requisition|"
    r"portal\.na|candidateportal|join-our-team|lowongan|loker|kerjaya|karir)",
    re.IGNORECASE,
)

_APPLY_EVIDENCE_PATTERN_V49 = re.compile(
    r"(?:apply|application|apply\s+now|apply\s+here|requirements?|"
    r"qualifications?|responsibilit|closing\s+date|full\s*time|part\s*time|"
    r"contract|permanent|temporary|how\s+to\s+apply|cara\s+melamar|"
    r"info\s+lengkap|lamar)",
    re.IGNORECASE,
)

_MARTIAN_SHELL_PATTERN_V49 = re.compile(
    r"(?:myrecruitmentplus|martianlogic|clientcode|recruiterid|jobboardthemeid|__NEXT_DATA__)",
    re.IGNORECASE,
)

_NEXT_DATA_PATTERN_V49 = re.compile(
    r"<script[^>]+id=['\"]__NEXT_DATA__['\"][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)

_HEADING_BLOCK_STOP_PATTERN_V49 = re.compile(r"^h[1-4]$", re.IGNORECASE)

_HEADING_TITLE_REJECT_PATTERN_V49 = re.compile(
    r"^(?:internship\s+details|job\s+description|position\s+description|role\s+description|details)$",
    re.IGNORECASE,
)

_IFRAME_LISTING_PATTERN_V49 = re.compile(
    r"(?:greenhouse|lever|smartrecruiters|workday|myworkdayjobs|icims|"
    r"myrecruitmentplus|martianlogic|job|career|vacanc|opening)",
    re.IGNORECASE,
)

_GENERIC_SLUG_TAILS_V49 = {
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

_DETAIL_DESC_SELECTORS_V49 = (
    "article",
    ".job-description",
    ".job__description",
    ".description",
    ".entry-content",
    "main",
)


class TieredExtractorV49(TieredExtractorV16):
    """v4.9 extractor with fast-shell recovery and timeout-safe DOM-first extraction."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""
        started = asyncio.get_running_loop().time()

        candidates: list[tuple[str, list[dict]]] = []

        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(super().extract(career_page, company, working_html), timeout=14.0)
        except asyncio.TimeoutError:
            logger.warning("v4.9 parent extractor timeout for %s", page_url)
        except Exception:
            logger.exception("v4.9 parent extractor failed for %s", page_url)

        parent_jobs = self._prepare_candidate_jobs_v49(parent_jobs or [], page_url)
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        local_jobs = self._extract_local_dom_jobs_v49(page_url, working_html)
        if local_jobs:
            candidates.append(("local_v49", local_jobs))

        structured_jobs = self._extract_structured_jobs_v49(working_html, page_url)
        if structured_jobs:
            candidates.append(("structured_v49", structured_jobs))

        best_label, best_jobs, best_score = self._pick_best_candidate_v49(candidates, page_url)

        shell_like = self._looks_like_shell_v49(page_url, working_html)
        if self._should_run_deep_recovery_v49(best_jobs, best_score, shell_like):
            if shell_like and self._within_budget_v49(started, 21.5):
                martian_jobs = await self._extract_martian_jobs_v49(page_url, working_html)
                if martian_jobs:
                    candidates.append(("martian_fast_v49", martian_jobs))

                oracle_jobs = await self._extract_oracle_jobs_v49(page_url, working_html)
                if oracle_jobs:
                    candidates.append(("oracle_api_v49", oracle_jobs))

            if self._within_budget_v49(started, 23.5):
                subpage_jobs = await self._follow_listing_subpages_v49(page_url, working_html)
                if subpage_jobs:
                    candidates.append(("subpage_follow_v49", subpage_jobs))

            if self._within_budget_v49(started, 24.5):
                iframe_jobs = await self._extract_iframe_jobs_v49(page_url, working_html)
                if iframe_jobs:
                    candidates.append(("iframe_v49", iframe_jobs))

            best_label, best_jobs, best_score = self._pick_best_candidate_v49(candidates, page_url)

        if not best_jobs:
            return []

        final_jobs = self._prepare_candidate_jobs_v49(best_jobs, page_url)
        if self._should_enrich_detail_pages_v49(final_jobs, page_url, shell_like) and self._within_budget_v49(started, 26.0):
            try:
                final_jobs = await asyncio.wait_for(
                    self._enrich_from_detail_pages_v49(final_jobs, page_url),
                    timeout=8.0,
                )
            except asyncio.TimeoutError:
                logger.warning("v4.9 enrichment timeout for %s", page_url)
            except Exception:
                logger.exception("v4.9 enrichment failed for %s", page_url)

        final_jobs = self._prepare_candidate_jobs_v49(final_jobs, page_url)
        logger.info(
            "v4.9 selected %s for %s (%d jobs, score=%.2f)",
            best_label,
            page_url,
            len(final_jobs),
            best_score,
        )
        return final_jobs[:MAX_JOBS_PER_PAGE]

    def _extract_local_dom_jobs_v49(self, page_url: str, html_body: str) -> list[dict]:
        if not html_body or len(html_body) < 100:
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
            _label, jobs, _score = self._pick_best_candidate_v49(candidates, page_url)
            return jobs

        elementor_cards = self._extract_elementor_cards_v49(root, page_url)
        if elementor_cards:
            candidates.append(("elementor_cards_v49", elementor_cards))

        utility_rows = self._extract_utility_card_rows_v49(root, page_url)
        if utility_rows:
            candidates.append(("utility_rows_v49", utility_rows))

        heading_blocks = self._extract_heading_blocks_v49(root, page_url)
        if heading_blocks:
            candidates.append(("heading_blocks_v49", heading_blocks))

        job_links = self._extract_job_links_v49(root, page_url)
        if job_links:
            candidates.append(("job_links_v49", job_links))

        _label, jobs, _score = self._pick_best_candidate_v49(candidates, page_url)
        return jobs

    def _extract_structured_jobs_v49(self, html_body: str, page_url: str) -> list[dict]:
        if not html_body or len(html_body) < 80:
            return []

        candidates: list[tuple[str, list[dict]]] = []
        state_jobs: list[dict] = []

        for match in _NEXT_DATA_PATTERN_V49.finditer(html_body):
            payload = html_lib.unescape((match.group(1) or "").strip())
            if not payload:
                continue
            try:
                parsed = json.loads(payload)
            except Exception:
                continue
            state_jobs.extend(self._extract_jobs_from_json_payload_v49(parsed, page_url, "tier0_next_data_v49"))

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
            state_jobs.extend(self._extract_jobs_from_json_payload_v49(parsed, page_url, "tier0_state_json_v49"))

        if state_jobs:
            candidates.append(("state_json_v49", state_jobs))

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

            for node in self._iter_json_dicts_v49(parsed):
                type_field = node.get("@type")
                is_job = False
                if isinstance(type_field, str):
                    is_job = type_field.lower() == "jobposting"
                elif isinstance(type_field, list):
                    is_job = any(str(t).lower() == "jobposting" for t in type_field)
                if not is_job:
                    continue

                title = self._normalize_title_v49(str(node.get("title") or node.get("name") or ""))
                source_url = _resolve_url(node.get("url") or node.get("sameAs") or node.get("applyUrl"), page_url) or page_url
                if not self._is_title_acceptable_v49(title, source_url):
                    continue
                if self._is_non_job_url_v49(source_url):
                    continue

                location = self._extract_location_from_json_v49(node)
                description = self._clean_description_v49(
                    str(node.get("description") or node.get("responsibilities") or node.get("qualifications") or "")
                )

                jsonld_jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": location,
                        "salary_raw": self._extract_salary_v49(json.dumps(node, ensure_ascii=True, default=str)),
                        "employment_type": str(node.get("employmentType") or "").strip() or None,
                        "description": description,
                        "extraction_method": "tier0_jsonld_v49",
                        "extraction_confidence": 0.86,
                    }
                )

        if jsonld_jobs:
            candidates.append(("jsonld_v49", jsonld_jobs))

        _label, best_jobs, _score = self._pick_best_candidate_v49(candidates, page_url)
        return best_jobs

    async def _extract_martian_jobs_v49(self, page_url: str, html_body: str) -> list[dict]:
        if not self._looks_like_martian_shell_v49(html_body):
            return []

        context = self._extract_martian_context_v49(page_url, html_body)
        if not (context.get("client_code") or context.get("recruiter_id") or context.get("build_id")):
            return []

        endpoints = self._martian_probe_urls_v49(page_url, context)
        if not endpoints:
            return []

        jobs: list[dict] = []
        budget = 12
        seen: set[str] = set()

        async with httpx.AsyncClient(
            timeout=2.8,
            follow_redirects=True,
            headers={
                "Accept": "application/json,text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Referer": page_url,
            },
        ) as client:
            for endpoint in endpoints[:20]:
                if budget <= 0:
                    break
                norm = endpoint.rstrip("/")
                if not endpoint or norm in seen:
                    continue
                seen.add(norm)
                budget -= 1

                try:
                    resp = await asyncio.wait_for(client.get(endpoint), timeout=2.8)
                except Exception:
                    continue
                if resp.status_code >= 400 or not resp.text:
                    continue

                extracted = self._extract_jobs_from_probe_payload_v49(resp.text, str(resp.url), page_url, context)
                if extracted:
                    jobs.extend(extracted)
                    prepared = self._prepare_candidate_jobs_v49(jobs, page_url)
                    if len(prepared) >= MIN_JOBS_FOR_SUCCESS:
                        return prepared

            post_payloads = self._martian_post_payloads_v49(context)
            for endpoint in self._martian_post_endpoints_v49(endpoints)[:4]:
                for payload in post_payloads[:3]:
                    if budget <= 0:
                        break
                    budget -= 1
                    try:
                        resp = await asyncio.wait_for(client.post(endpoint, json=payload), timeout=2.8)
                    except Exception:
                        continue
                    if resp.status_code >= 400 or not resp.text:
                        continue
                    extracted = self._extract_jobs_from_probe_payload_v49(resp.text, str(resp.url), page_url, context)
                    if extracted:
                        jobs.extend(extracted)
                        prepared = self._prepare_candidate_jobs_v49(jobs, page_url)
                        if len(prepared) >= MIN_JOBS_FOR_SUCCESS:
                            return prepared
                if budget <= 0:
                    break

        return self._prepare_candidate_jobs_v49(jobs, page_url)

    def _extract_jobs_from_probe_payload_v49(
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
                jobs.extend(self._extract_jobs_from_json_payload_v49(parsed, response_url, "tier0_probe_json_v49", context))

        root = _parse_html(payload)
        if root is not None:
            anchor_count = len(root.xpath("//a[@href]"))
            if anchor_count >= 2:
                jobs.extend(self._extract_elementor_cards_v49(root, response_url))
                jobs.extend(self._extract_utility_card_rows_v49(root, response_url))
                jobs.extend(self._extract_heading_blocks_v49(root, response_url))

        return self._prepare_candidate_jobs_v49(jobs, page_url)

    def _extract_martian_context_v49(self, page_url: str, html_body: str) -> dict[str, str]:
        context = {
            "client_code": "",
            "recruiter_id": "",
            "job_board_theme_id": "",
            "build_id": "",
            "next_page": "",
            "next_query": "",
        }

        match = _NEXT_DATA_PATTERN_V49.search(html_body or "")
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

        if not context["client_code"]:
            parsed_url = urlparse(page_url)
            parts = [seg for seg in parsed_url.path.split("/") if seg]
            if parts:
                guess = re.sub(r"[^a-z0-9-]", "", parts[-1].lower())
                if len(guess) >= 3:
                    context["client_code"] = guess

        return context

    def _martian_probe_urls_v49(self, page_url: str, context: dict[str, str]) -> list[str]:
        parsed = urlparse(page_url or "")
        if not parsed.netloc:
            return []

        client_code = (context.get("client_code") or "").strip()
        recruiter_id = (context.get("recruiter_id") or "").strip()
        theme_id = (context.get("job_board_theme_id") or "").strip()

        page_host = f"{parsed.scheme or 'https'}://{parsed.netloc}".rstrip("/")
        hosts = [
            page_host,
            "https://web.martianlogic.com",
            "https://jobs.myrecruitmentplus.com",
            "https://jobs.martianlogic.com",
        ]
        if client_code:
            hosts.extend(
                [
                    f"https://{client_code}.myrecruitmentplus.com",
                    f"https://{client_code}.martianlogic.com",
                ]
            )

        q_templates = [
            "pageNumber=1&pageSize=50&isActive=true",
            "page=1&pageSize=50",
            "offset=0&limit=50",
            "search=",
        ]
        if client_code:
            q_templates.extend(
                [
                    f"client={client_code}&pageNumber=1&pageSize=50&isActive=true",
                    f"clientCode={client_code}&pageNumber=1&pageSize=50&isActive=true",
                    f"clientCode={client_code}&search=",
                ]
            )
        if recruiter_id:
            q_templates.extend(
                [
                    f"recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true",
                    f"recruiterId={recruiter_id}&search=",
                ]
            )
        if recruiter_id and client_code:
            q_templates.append(
                f"clientCode={client_code}&recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true"
            )
        if theme_id and client_code:
            q_templates.append(
                f"clientCode={client_code}&jobBoardThemeId={theme_id}&pageNumber=1&pageSize=50&isActive=true"
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
                for q in q_templates:
                    probe_urls.append(f"{base}?{q}")

            if client_code:
                base = f"{host}/{client_code}"
                probe_urls.append(base)
                for q in q_templates:
                    probe_urls.append(f"{base}?{q}")

            probe_urls.extend(self._next_data_probe_urls_v49(host, page_url, context))

        deduped = sorted({u.rstrip("/"): u for u in probe_urls if u}.values())
        deduped.sort(key=lambda u: self._martian_endpoint_priority_v49(u, parsed.netloc.lower()), reverse=True)
        return self._diversify_probe_hosts_v49(deduped, limit=42)

    def _next_data_probe_urls_v49(self, host: str, page_url: str, context: dict[str, str]) -> list[str]:
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
    def _martian_endpoint_priority_v49(url: str, page_host: str) -> int:
        low = (url or "").lower()
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()

        score = 0
        if host == page_host:
            score += 14
        if "/_next/data/" in low:
            score += 10
        if "/api/" in low:
            score += 8
        if "search" in low:
            score += 6
        if "client=" in low or "clientcode=" in low:
            score += 4
        if "recruiterid=" in low:
            score += 3
        if any(x in host for x in ("martianlogic", "myrecruitmentplus")):
            score += 2
        return score

    @staticmethod
    def _diversify_probe_hosts_v49(urls: list[str], limit: int = 42) -> list[str]:
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
    def _martian_post_endpoints_v49(endpoints: list[str]) -> list[str]:
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
        return out[:8]

    @staticmethod
    def _martian_post_payloads_v49(context: dict[str, str]) -> list[dict[str, Any]]:
        client_code = (context.get("client_code") or "").strip()
        recruiter_id = (context.get("recruiter_id") or "").strip()

        payloads: list[dict[str, Any]] = []
        if client_code:
            payloads.extend(
                [
                    {"client": client_code, "pageNumber": 1, "pageSize": 50, "isActive": True},
                    {"clientCode": client_code, "pageNumber": 1, "pageSize": 50, "isActive": True},
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

        return payloads

    async def _extract_oracle_jobs_v49(self, page_url: str, html_body: str) -> list[dict]:
        lowered_url = (page_url or "").lower()
        lowered_html = (html_body or "").lower()
        if "oraclecloud.com" not in lowered_url and "candidateexperience" not in lowered_url and "candidateexperience" not in lowered_html:
            return []

        parsed = urlparse(page_url)
        if not parsed.netloc:
            return []

        site_ids = self._oracle_site_ids_v49(page_url, html_body)
        if not site_ids:
            return []

        base = f"{parsed.scheme or 'https'}://{parsed.netloc}"
        jobs: list[dict] = []

        async with httpx.AsyncClient(
            timeout=5.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "application/json,text/plain,*/*",
                "Referer": page_url,
            },
        ) as client:
            for site_id in site_ids[:5]:
                offset = 0
                for _ in range(3):
                    api_urls = [
                        (
                            f"{base}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
                            f"?onlyData=true&expand=requisitionList.secondaryLocations"
                            f"&finder=findReqs;siteNumber={site_id},"
                            f"facetsList=LOCATIONS%3BWORK_LOCATIONS%3BTITLES%3BCATEGORIES%3BPOSTING_DATES,"
                            f"limit=24,offset={offset}"
                        ),
                        (
                            f"{base}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
                            f"?onlyData=true&finder=findReqs;siteNumber={site_id},limit=24,offset={offset}"
                        ),
                    ]

                    payload: Any = None
                    for api_url in api_urls:
                        try:
                            resp = await client.get(api_url)
                        except Exception:
                            continue
                        if resp.status_code != 200 or not resp.text:
                            continue
                        try:
                            payload = resp.json()
                        except Exception:
                            payload = None
                        if payload is not None:
                            break

                    if payload is None:
                        break

                    rows = self._oracle_rows_from_payload_v49(payload)
                    if not rows:
                        break

                    added = 0
                    for row in rows:
                        title = self._normalize_title_v49(
                            str(row.get("Title") or row.get("requisitionTitle") or row.get("title") or row.get("name") or "")
                        )
                        req_id = str(row.get("Id") or row.get("id") or row.get("requisitionId") or row.get("jobId") or "").strip()

                        source_url = page_url
                        if req_id:
                            source_url = f"{base}/hcmUI/CandidateExperience/en/sites/{site_id}/job/{req_id}"

                        if not self._is_title_acceptable_v49(title, source_url):
                            continue
                        if self._is_non_job_url_v49(source_url):
                            continue

                        location = " ".join(
                            p
                            for p in (
                                str(row.get("PrimaryLocation") or "").strip(),
                                str(row.get("PrimaryLocationCountry") or "").strip(),
                            )
                            if p
                        ) or None

                        jobs.append(
                            {
                                "title": title,
                                "source_url": source_url,
                                "location_raw": location,
                                "salary_raw": self._extract_salary_v49(json.dumps(row, ensure_ascii=True, default=str)),
                                "employment_type": str(row.get("JobType") or row.get("employmentType") or "").strip() or None,
                                "description": self._clean_description_v49(
                                    str(row.get("ExternalDescriptionStr") or row.get("description") or row.get("Summary") or "")
                                ),
                                "extraction_method": "tier0_oracle_api_v49",
                                "extraction_confidence": 0.87,
                            }
                        )
                        added += 1
                        if len(jobs) >= MAX_JOBS_PER_PAGE:
                            break

                    if added == 0 or len(rows) < 24:
                        break
                    offset += 24

                prepared = self._prepare_candidate_jobs_v49(jobs, page_url)
                if len(prepared) >= MIN_JOBS_FOR_SUCCESS:
                    return prepared

        return self._prepare_candidate_jobs_v49(jobs, page_url)

    async def _follow_listing_subpages_v49(self, page_url: str, html_body: str) -> list[dict]:
        root = _parse_html(html_body)
        if root is None:
            return []

        page_host = (urlparse(page_url).netloc or "").lower()
        candidates: list[tuple[str, float]] = []
        seen: set[str] = set()

        for a_el in root.xpath("//a[@href]")[:1200]:
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            full_url = _resolve_url(href, page_url)
            if not full_url or full_url.rstrip("/") == page_url.rstrip("/"):
                continue
            if self._is_non_job_url_v49(full_url):
                continue

            norm = full_url.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)

            parsed = urlparse(full_url)
            score = 0.0
            if parsed.netloc.lower() == page_host:
                score += 2.0
            if _LISTING_URL_PATTERN_V49.search(full_url):
                score += 4.0
            if _LISTING_LINK_TEXT_PATTERN_V49.search(self._safe_text_v49(a_el)):
                score += 3.0

            if score >= 4.0:
                candidates.append((full_url, score))

        if not candidates:
            return []

        candidates.sort(key=lambda x: x[1], reverse=True)
        jobsets: list[tuple[str, list[dict]]] = []
        fetched = 0

        for subpage_url, _score in candidates[:5]:
            if fetched >= 3:
                break
            sub_html = await self._fetch_html_v49(subpage_url)
            if not sub_html:
                continue
            fetched += 1

            local_jobs = self._extract_local_dom_jobs_v49(subpage_url, sub_html)
            if local_jobs:
                jobsets.append((f"sub_local_{fetched}", local_jobs))

            structured = self._extract_structured_jobs_v49(sub_html, subpage_url)
            if structured:
                jobsets.append((f"sub_structured_{fetched}", structured))

        _label, best_jobs, _score = self._pick_best_candidate_v49(jobsets, page_url)
        return best_jobs

    async def _extract_iframe_jobs_v49(self, page_url: str, html_body: str) -> list[dict]:
        root = _parse_html(html_body)
        if root is None:
            return []

        iframe_urls: list[str] = []
        seen: set[str] = set()
        for iframe in root.xpath("//iframe[@src]")[:25]:
            src = (iframe.get("src") or "").strip()
            full_url = _resolve_url(src, page_url)
            if not full_url:
                continue
            if not _IFRAME_LISTING_PATTERN_V49.search(full_url):
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
        for iframe_url in iframe_urls[:2]:
            iframe_html = await self._fetch_html_v49(iframe_url)
            if not iframe_html:
                continue
            fetched += 1

            local_jobs = self._extract_local_dom_jobs_v49(iframe_url, iframe_html)
            if local_jobs:
                jobsets.append((f"iframe_local_{fetched}", local_jobs))

            structured = self._extract_structured_jobs_v49(iframe_html, iframe_url)
            if structured:
                jobsets.append((f"iframe_structured_{fetched}", structured))

        _label, best_jobs, _score = self._pick_best_candidate_v49(jobsets, page_url)
        return best_jobs

    async def _fetch_html_v49(self, url: str) -> Optional[str]:
        try:
            async with httpx.AsyncClient(
                timeout=4.8,
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
        if resp.status_code != 200 or len(body) < 180 or self._looks_non_html_payload_v49(body):
            return None
        return body

    def _extract_elementor_cards_v49(self, root: etree._Element, page_url: str) -> list[dict]:
        cards = root.xpath(
            "//*[contains(@class,'elementor-column') and contains(@class,'elementor-inner-column')]"
        )
        if len(cards) < 2:
            return []

        jobs: list[dict] = []
        for card in cards[:420]:
            heading_nodes = card.xpath(
                ".//h2[contains(@class,'elementor-heading-title')][1] | "
                ".//h3[contains(@class,'elementor-heading-title')][1]"
            )
            if not heading_nodes:
                continue

            title = self._normalize_title_v49(_text(heading_nodes[0]))
            if _TITLE_REJECT_PATTERN_V49.match(title.lower()):
                continue

            source_url = ""
            cta_text = ""
            for a_el in card.xpath(".//a[@href]"):
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v49(href):
                    continue
                link_text = self._safe_text_v49(a_el)
                if _LISTING_LINK_TEXT_PATTERN_V49.search(link_text):
                    continue
                source_url = href
                cta_text = link_text
                if self._is_job_like_url_v49(href):
                    break

            if not source_url:
                continue
            if not self._is_title_acceptable_v49(title, source_url):
                continue

            card_text = _text(card)
            if len(card_text) < 35 and not _APPLY_EVIDENCE_PATTERN_V49.search(cta_text):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v49(card, title),
                    "salary_raw": self._extract_salary_v49(card_text),
                    "employment_type": self._extract_employment_type_v49(card_text),
                    "description": self._clean_description_v49(card_text),
                    "extraction_method": "tier2_elementor_cards_v49",
                    "extraction_confidence": 0.82,
                }
            )

        prepared = self._prepare_candidate_jobs_v49(jobs, page_url)
        return prepared if len(prepared) >= 2 else []

    def _extract_utility_card_rows_v49(self, root: etree._Element, page_url: str) -> list[dict]:
        rows = root.xpath(
            "//div[contains(concat(' ', normalize-space(@class), ' '), ' p-4 ') "
            "or contains(concat(' ', normalize-space(@class), ' '), ' mx-4 ') "
            "or contains(concat(' ', normalize-space(@class), ' '), ' job-card ')]"
        )
        if len(rows) < 2:
            return []

        jobs: list[dict] = []
        for row in rows[:360]:
            links = row.xpath(".//a[@href]")
            if not links:
                continue

            heading_nodes = row.xpath(
                ".//h1[1] | .//h2[1] | .//h3[1] | .//h4[1] | "
                ".//div[contains(@class,'cursor-pointer')][1]"
            )
            title = self._normalize_title_v49(_text(heading_nodes[0])) if heading_nodes else self._normalize_title_v49(_text(links[0]))
            if not title:
                continue

            source_url = ""
            for a_el in links:
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v49(href):
                    continue
                source_url = href
                if self._is_job_like_url_v49(href):
                    break

            if not source_url:
                continue
            if not self._is_title_acceptable_v49(title, source_url):
                continue

            row_text = _text(row)
            has_apply = bool(_APPLY_EVIDENCE_PATTERN_V49.search(row_text))
            if not (self._job_url_has_detail_evidence_v49(source_url, page_url) or has_apply or len(row_text) >= 120):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v49(row, title),
                    "salary_raw": self._extract_salary_v49(row_text),
                    "employment_type": self._extract_employment_type_v49(row_text),
                    "description": self._clean_description_v49(row_text),
                    "extraction_method": "tier2_utility_cards_v49",
                    "extraction_confidence": 0.8,
                }
            )

        prepared = self._prepare_candidate_jobs_v49(jobs, page_url)
        return prepared if len(prepared) >= 2 else []

    def _extract_heading_blocks_v49(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        seen_titles: set[str] = set()

        headings = root.xpath("//h2 | //h3 | //h4")
        if len(headings) < 2:
            return []

        for heading in headings[:700]:
            title = self._normalize_title_v49(_text(heading))
            if not title:
                continue
            if _TITLE_REJECT_PATTERN_V49.match(title.lower()):
                continue

            title_key = title.lower()
            if title_key in seen_titles:
                continue
            seen_titles.add(title_key)

            link_els: list[etree._Element] = [a for a in heading.xpath(".//a[@href]")]
            detail_chunks: list[str] = []

            sibling = heading.getnext()
            steps = 0
            while sibling is not None and steps < 8:
                steps += 1
                if isinstance(sibling.tag, str) and _HEADING_BLOCK_STOP_PATTERN_V49.match(sibling.tag):
                    break
                detail_chunks.append(_text(sibling))
                for a_el in sibling.xpath(".//a[@href]"):
                    link_els.append(a_el)
                sibling = sibling.getnext()

            detail_text = " ".join(chunk for chunk in detail_chunks if chunk).strip()
            description = self._clean_description_v49(detail_text)

            source_url = page_url
            has_apply_signal = bool(_APPLY_EVIDENCE_PATTERN_V49.search(detail_text))
            for a_el in link_els:
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v49(href):
                    continue
                link_text = self._safe_text_v49(a_el)
                if _LISTING_LINK_TEXT_PATTERN_V49.search(link_text):
                    continue
                source_url = href
                if self._is_job_like_url_v49(href):
                    break

            if not self._is_title_acceptable_v49(title, source_url):
                continue

            evidence = (
                self._job_url_has_detail_evidence_v49(source_url, page_url)
                or has_apply_signal
                or (description is not None and len(description) >= 180)
            )
            if not evidence:
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v49(heading.getparent() if heading.getparent() is not None else heading, title),
                    "salary_raw": self._extract_salary_v49(detail_text),
                    "employment_type": self._extract_employment_type_v49(detail_text),
                    "description": description,
                    "extraction_method": "tier2_heading_blocks_v49",
                    "extraction_confidence": 0.74,
                }
            )

        prepared = self._prepare_candidate_jobs_v49(jobs, page_url)
        return prepared if len(prepared) >= 2 else []

    def _extract_job_links_v49(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        for a_el in root.xpath("//a[@href]")[:4200]:
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url)
            if not source_url or self._is_non_job_url_v49(source_url):
                continue
            if not self._job_url_has_detail_evidence_v49(source_url, page_url):
                continue

            heading_nodes = a_el.xpath(".//h1 | .//h2 | .//h3 | .//h4 | .//h5")
            raw_title = _text(heading_nodes[0]) if heading_nodes else _text(a_el)
            title = self._normalize_title_v49(raw_title)
            if not self._is_title_acceptable_v49(title, source_url):
                continue
            if not self._title_has_role_signal_v49(title):
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
            if _LISTING_LINK_TEXT_PATTERN_V49.search(title):
                continue
            if not (self._job_url_has_detail_evidence_v49(source_url, page_url) or len(context_text) >= 95):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v49(context, title),
                    "salary_raw": self._extract_salary_v49(context_text),
                    "employment_type": self._extract_employment_type_v49(context_text),
                    "description": self._clean_description_v49(context_text),
                    "extraction_method": "tier2_job_links_v49",
                    "extraction_confidence": 0.72,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return self._prepare_candidate_jobs_v49(jobs, page_url)

    def _extract_jobs_from_json_payload_v49(
        self,
        payload: Any,
        page_url: str,
        method: str,
        context: Optional[dict[str, str]] = None,
    ) -> list[dict]:
        jobs: list[dict] = []
        client_code = (context or {}).get("client_code", "")

        for row in self._iter_json_dicts_v49(payload):
            title = self._normalize_title_v49(
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
            if _TITLE_REJECT_PATTERN_V49.match(title.lower()):
                continue
            if self._looks_like_taxonomy_node_v49(row, title):
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

            if self._is_non_job_url_v49(source_url):
                continue
            if not self._is_title_acceptable_v49(title, source_url):
                continue

            description = self._clean_description_v49(
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

            location = self._extract_location_from_json_v49(row)
            employment_type = str(row.get("employmentType") or row.get("jobType") or row.get("JobType") or "").strip() or None
            salary_raw = self._extract_salary_v49(json.dumps(row, ensure_ascii=True, default=str))

            evidence = (
                self._job_url_has_detail_evidence_v49(source_url, page_url)
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

        return self._prepare_candidate_jobs_v49(jobs, page_url)

    def _pick_best_candidate_v49(
        self,
        candidates: list[tuple[str, list[dict]]],
        page_url: str,
    ) -> tuple[str, list[dict], float]:
        if not candidates:
            return "", [], 0.0

        scored: list[tuple[str, list[dict], float]] = []
        for label, jobs in candidates:
            prepared = self._prepare_candidate_jobs_v49(jobs, page_url)
            if not prepared:
                continue
            score = self._candidate_score_v49(prepared, page_url)
            scored.append((label, prepared, score))

        if not scored:
            return "", [], 0.0

        best_label, best_jobs, best_score = max(scored, key=lambda item: item[2])

        for label, jobs, score in scored:
            if label == best_label:
                continue
            overlap = self._title_overlap_ratio_v49(jobs, best_jobs)
            if len(jobs) >= len(best_jobs) + 1 and overlap >= 0.6 and score >= best_score - 1.2:
                best_label, best_jobs, best_score = label, jobs, score

        return best_label, best_jobs[:MAX_JOBS_PER_PAGE], best_score

    def _prepare_candidate_jobs_v49(self, jobs: list[dict], page_url: str) -> list[dict]:
        cleaned: list[dict] = []

        for idx, raw in enumerate(jobs):
            title = self._normalize_title_v49(str(raw.get("title") or ""))
            source_url = _resolve_url(raw.get("source_url"), page_url) or page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]

            desc = self._clean_description_v49(str(raw.get("description") or ""))
            if self._is_non_job_url_v49(source_url):
                continue
            if not self._is_title_acceptable_v49(title, source_url):
                continue

            cleaned.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": raw.get("location_raw") or None,
                    "salary_raw": raw.get("salary_raw") or None,
                    "employment_type": raw.get("employment_type") or self._extract_employment_type_v49(desc or ""),
                    "description": desc,
                    "extraction_method": raw.get("extraction_method") or "tier2_v49",
                    "extraction_confidence": raw.get("extraction_confidence", 0.66),
                    "_order": idx,
                }
            )

        deduped = self._dedupe_jobs_v49(cleaned, page_url)
        if not self._is_valid_jobset_v49(deduped, page_url):
            return []
        return deduped

    def _dedupe_jobs_v49(self, jobs: list[dict], page_url: str) -> list[dict]:
        by_key: dict[tuple[str, str], dict] = {}
        by_url: dict[str, dict] = {}
        page_norm = (page_url or "").rstrip("/").lower()

        for job in jobs:
            title = self._normalize_title_v49(job.get("title", ""))
            url = _resolve_url(job.get("source_url"), page_url) or page_url
            key = (title.lower(), url.lower())
            existing = by_key.get(key)
            if existing is None or self._title_quality_score_v49(title) > self._title_quality_score_v49(existing.get("title", "")):
                by_key[key] = job

        for job in by_key.values():
            norm_url = (_resolve_url(job.get("source_url"), page_url) or page_url).rstrip("/").lower()
            if norm_url == page_norm:
                same_page_key = f"{norm_url}#title:{self._normalize_title_v49(job.get('title', '')).lower()}"
                by_url[same_page_key] = job
                continue

            current = by_url.get(norm_url)
            if current is None:
                by_url[norm_url] = job
                continue
            if self._title_quality_score_v49(job.get("title", "")) > self._title_quality_score_v49(current.get("title", "")):
                by_url[norm_url] = job

        deduped = sorted(by_url.values(), key=lambda item: int(item.get("_order", 0)))
        for item in deduped:
            item.pop("_order", None)
        return deduped[:MAX_JOBS_PER_PAGE]

    def _is_valid_jobset_v49(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title_v49(j.get("title", "")) for j in jobs if j.get("title")]
        if not titles:
            return False

        role_hits = sum(1 for t in titles if self._title_has_role_signal_v49(t))
        reject_hits = sum(1 for t in titles if _TITLE_REJECT_PATTERN_V49.match(t.lower()))
        evidence_hits = sum(1 for j in jobs if self._job_has_evidence_v49(j, page_url))
        detail_hits = sum(1 for j in jobs if self._job_url_has_detail_evidence_v49(str(j.get("source_url") or ""), page_url))

        if reject_hits >= max(1, int(len(titles) * 0.22)):
            return False

        if len(titles) <= 2:
            return role_hits == len(titles) and evidence_hits >= 1

        if len(titles) <= 4:
            if role_hits >= max(2, int(len(titles) * 0.6)) and evidence_hits >= max(1, int(len(titles) * 0.5)):
                return True
            return role_hits >= 1 and detail_hits >= len(titles) and evidence_hits >= 1

        if role_hits < max(2, int(len(titles) * 0.45)):
            return False

        return evidence_hits >= max(2, int(len(titles) * 0.35))

    def _candidate_score_v49(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        titles = [self._normalize_title_v49(j.get("title", "")) for j in jobs]
        role_hits = sum(1 for t in titles if self._title_has_role_signal_v49(t))
        reject_hits = sum(1 for t in titles if _TITLE_REJECT_PATTERN_V49.match(t.lower()))
        detail_hits = sum(1 for j in jobs if self._job_url_has_detail_evidence_v49(str(j.get("source_url") or ""), page_url))
        evidence_hits = sum(1 for j in jobs if self._job_has_evidence_v49(j, page_url))

        score = len(jobs) * 4.7
        score += role_hits * 2.7
        score += detail_hits * 2.0
        score += evidence_hits * 1.6
        score -= reject_hits * 6.2
        return score

    def _job_has_evidence_v49(self, job: dict, page_url: str) -> bool:
        source_url = (job.get("source_url") or page_url).strip() or page_url
        desc = str(job.get("description") or "")

        if self._job_url_has_detail_evidence_v49(source_url, page_url):
            return True

        if job.get("salary_raw") or job.get("employment_type") or job.get("location_raw"):
            return True
        if _APPLY_EVIDENCE_PATTERN_V49.search(desc):
            return True
        return len(desc.strip()) >= 170

    def _normalize_title_v49(self, title: str) -> str:
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

    def _is_valid_title_v49(self, title: str) -> bool:
        if not title:
            return False

        t = title.strip()
        if len(t) < 3 or len(t) > 180:
            return False
        if not re.search(r"[A-Za-z]", t):
            return False
        if len(t.split()) > 14:
            return False
        if _TITLE_REJECT_PATTERN_V49.match(t.lower()):
            return False
        if _HEADING_TITLE_REJECT_PATTERN_V49.match(t.lower()):
            return False
        if _MARKETING_FRAGMENT_PATTERN_V49.search(t):
            return False
        return True

    def _is_title_acceptable_v49(self, title: str, source_url: str) -> bool:
        if not self._is_valid_title_v49(title):
            return False

        if self._title_has_role_signal_v49(title):
            return True

        if self._is_job_like_url_v49(source_url) and len(title.split()) <= 8:
            return True

        return False

    def _title_has_role_signal_v49(self, title: str) -> bool:
        if not title:
            return False
        return bool(_ROLE_HINT_PATTERN_V49.search(title) or _title_has_job_noun(title) or self._is_acronym_title_v49(title))

    @staticmethod
    def _is_acronym_title_v49(title: str) -> bool:
        t = (title or "").strip()
        if not re.fullmatch(r"[A-Z][A-Z0-9&/\-\+]{1,10}", t):
            return False
        return t.lower() not in {"home", "menu", "faq", "apply"}

    def _job_url_has_detail_evidence_v49(self, url: str, page_url: str) -> bool:
        value = (url or "").strip()
        if not value or self._is_non_job_url_v49(value):
            return False
        if self._is_job_like_url_v49(value):
            return True

        if page_url and value.rstrip("/") == page_url.rstrip("/"):
            return False

        parsed = urlparse(value)
        parts = [p for p in (parsed.path or "").split("/") if p]
        if not parts:
            return False

        leaf = parts[-1].lower()
        if leaf in _GENERIC_SLUG_TAILS_V49:
            return False
        if leaf in {"index", "home", "about", "contact", "news", "blog", "privacy", "terms"}:
            return False

        if len(leaf) >= 6 and ("-" in leaf or re.search(r"\d", leaf) or len(parts) >= 2):
            return True
        return len(leaf) >= 10

    def _is_job_like_url_v49(self, url: str) -> bool:
        value = (url or "").strip()
        if not value or self._is_non_job_url_v49(value):
            return False
        if _DETAILISH_URL_PATTERN_V49.search(value):
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
    def _is_non_job_url_v49(url: str) -> bool:
        value = (url or "").strip().lower()
        if not value:
            return False
        if value.startswith("mailto:") or value.startswith("tel:"):
            return True
        return bool(_NON_JOB_URL_PATTERN_V49.search(value))

    @staticmethod
    def _looks_non_html_payload_v49(body: Optional[str]) -> bool:
        if not body:
            return True
        sample = body[:900].lstrip()
        if sample.startswith("%PDF-"):
            return True
        low = sample.lower()
        if (low.startswith("{") or low.startswith("[")) and "<html" not in low[:300]:
            return True
        return False

    def _should_run_deep_recovery_v49(self, best_jobs: list[dict], best_score: float, shell_like: bool) -> bool:
        if shell_like:
            return True
        if not best_jobs:
            return True
        if len(best_jobs) < MIN_JOBS_FOR_SUCCESS:
            return True
        return best_score < 12.0

    def _looks_like_shell_v49(self, page_url: str, html_body: str) -> bool:
        lower = (html_body or "").lower()
        if "<div id=\"__next\"></div>" in lower and "__next_data__" in lower:
            return True
        if len(lower) < 350:
            return True
        if _MARTIAN_SHELL_PATTERN_V49.search(lower):
            return True
        if "oraclecloud.com" in (page_url or "").lower() and "candidateexperience" in lower:
            return True
        return False

    @staticmethod
    def _looks_like_martian_shell_v49(html_body: str) -> bool:
        return bool(_MARTIAN_SHELL_PATTERN_V49.search(html_body or ""))

    @staticmethod
    def _within_budget_v49(started: float, limit_seconds: float) -> bool:
        return (asyncio.get_running_loop().time() - started) < limit_seconds

    def _extract_location_v49(self, row: etree._Element, title: str) -> Optional[str]:
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
    def _extract_salary_v49(text: str) -> Optional[str]:
        if not text:
            return None
        match = _SALARY_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:120]
        return None

    @staticmethod
    def _extract_employment_type_v49(text: str) -> Optional[str]:
        if not text:
            return None
        match = _JOB_TYPE_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:80]
        return None

    def _extract_location_from_json_v49(self, node: dict[str, Any]) -> Optional[str]:
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
                    nested = self._extract_location_from_json_v49(item)
                    if nested:
                        return nested
        return None

    @staticmethod
    def _iter_json_dicts_v49(payload: Any) -> list[dict[str, Any]]:
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
    def _looks_like_taxonomy_node_v49(node: dict[str, Any], title: str) -> bool:
        lowered = title.lower().strip()
        if _TITLE_REJECT_PATTERN_V49.match(lowered):
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

    def _oracle_rows_from_payload_v49(self, payload: Any) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        def _add_row(candidate: Any) -> None:
            if not isinstance(candidate, dict):
                return
            if not any(k in candidate for k in ("Title", "requisitionTitle", "title", "name")):
                return
            if not any(k in candidate for k in ("Id", "id", "requisitionId", "jobId")):
                return
            rows.append(candidate)

        if isinstance(payload, dict):
            items = payload.get("items")
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        req_list = item.get("requisitionList")
                        if isinstance(req_list, list):
                            for row in req_list:
                                _add_row(row)
                        else:
                            _add_row(item)
            else:
                _add_row(payload)
        elif isinstance(payload, list):
            for item in payload:
                _add_row(item)

        if rows:
            return rows

        for node in self._iter_json_dicts_v49(payload):
            req_list = node.get("requisitionList") if isinstance(node, dict) else None
            if isinstance(req_list, list):
                for row in req_list:
                    _add_row(row)
            else:
                _add_row(node)

        return rows

    @staticmethod
    def _oracle_site_ids_v49(page_url: str, html_body: str) -> list[str]:
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

        for match in re.finditer(r"/sites/([A-Za-z0-9_]+)/", page_url or "", re.IGNORECASE):
            _add(match.group(1))
        for match in re.finditer(
            r"(?:<base[^>]+href=['\"][^'\"]*/sites/|CandidateExperience/en/sites/)([A-Za-z0-9_]+)",
            html_body or "",
            re.IGNORECASE,
        ):
            _add(match.group(1))
        for match in re.finditer(r"siteNumber\s*[:=]\s*['\"]([A-Za-z0-9_]+)", html_body or "", re.IGNORECASE):
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
                for suffix in ("1001", "1002", "1003", "1004"):
                    _add(f"{root}_{suffix}")

        if not ordered:
            for fallback in ("CX_1001", "CX_1002", "CX"):
                _add(fallback)

        ordered.sort(
            key=lambda site: (
                0 if re.search(r"_\d+$", site) else 1,
                0 if site.upper().endswith("_1001") else 1,
                site.lower(),
            )
        )
        return ordered

    async def _enrich_from_detail_pages_v49(self, jobs: list[dict], page_url: str) -> list[dict]:
        jobs_to_enrich = [
            (i, j)
            for i, j in enumerate(jobs)
            if self._needs_detail_refresh_v49(j)
            and j.get("source_url")
            and str(j.get("source_url")).startswith("http")
            and str(j.get("source_url")).rstrip("/") != page_url.rstrip("/")
            and not self._is_non_job_url_v49(str(j.get("source_url") or ""))
        ]

        if not jobs_to_enrich:
            return jobs

        jobs_to_enrich = jobs_to_enrich[:8]

        async with httpx.AsyncClient(
            timeout=4.5,
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
                if resp.status_code != 200 or len(body) < 200 or self._looks_non_html_payload_v49(body):
                    return idx, {}

                root = _parse_html(body)
                if root is None:
                    return idx, {}

                enriched: dict[str, Any] = {}
                description = self._extract_detail_description_v49(root)
                if description:
                    enriched["description"] = description

                location = self._extract_detail_location_v49(root)
                if location:
                    enriched["location_raw"] = location

                employment_type = self._extract_detail_employment_type_v49(root)
                if employment_type:
                    enriched["employment_type"] = employment_type

                salary = self._extract_salary_v49(_text(root))
                if salary:
                    enriched["salary_raw"] = salary

                return idx, enriched

            for batch_start in range(0, len(jobs_to_enrich), 3):
                batch = jobs_to_enrich[batch_start: batch_start + 3]
                try:
                    results = await asyncio.wait_for(
                        asyncio.gather(*[_enrich_one(idx, job) for idx, job in batch]),
                        timeout=4.8,
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
                            if not current_desc or len(value) > len(current_desc) + 70:
                                current[key] = value
                        elif not current.get(key):
                            current[key] = value

        return jobs

    def _extract_detail_description_v49(self, root: etree._Element) -> Optional[str]:
        for selector in _DETAIL_DESC_SELECTORS_V49:
            try:
                nodes = root.cssselect(selector)
            except Exception:
                nodes = []
            for node in nodes[:2]:
                text = self._clean_description_v49(_text(node))
                if text and len(text) >= 130:
                    return text

        for node in root.xpath("//article | //main")[:2]:
            text = self._clean_description_v49(_text(node))
            if text and len(text) >= 130:
                return text

        return None

    def _extract_detail_location_v49(self, root: etree._Element) -> Optional[str]:
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

    def _extract_detail_employment_type_v49(self, root: etree._Element) -> Optional[str]:
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

    def _should_enrich_detail_pages_v49(self, jobs: list[dict], page_url: str, shell_like: bool) -> bool:
        if shell_like:
            return False

        off_page = [
            j
            for j in jobs
            if (j.get("source_url") or "").startswith("http")
            and (j.get("source_url") or page_url).rstrip("/") != page_url.rstrip("/")
            and not self._is_non_job_url_v49(j.get("source_url") or "")
        ]
        if not off_page:
            return False

        detail_hits = sum(1 for j in off_page if self._job_url_has_detail_evidence_v49(j.get("source_url") or "", page_url))
        refresh_hits = sum(1 for j in off_page if self._needs_detail_refresh_v49(j))

        if detail_hits >= 1:
            return True
        if refresh_hits >= 2:
            return True
        return len(off_page) >= 5

    def _needs_detail_refresh_v49(self, job: dict) -> bool:
        desc = str(job.get("description") or "").strip()
        if not desc or len(desc) < 165:
            return True
        if re.search(r"\b(?:apply\s+now|read\s+more|learn\s+more|info\s+lengkap|cara\s+melamar)\b", desc, re.IGNORECASE):
            return True
        if not job.get("location_raw") or not job.get("employment_type"):
            return True
        return False

    def _title_quality_score_v49(self, title: str) -> float:
        t = self._normalize_title_v49(title)
        if not t:
            return -10.0
        score = 0.0
        score += 2.0 if self._title_has_role_signal_v49(t) else 0.0
        score += 1.0 if self._is_valid_title_v49(t) else 0.0
        score -= max(0.0, (len(t) - 90) / 80.0)
        return score

    def _title_overlap_ratio_v49(self, a_jobs: list[dict], b_jobs: list[dict]) -> float:
        a_titles = {self._normalize_title_v49(j.get("title", "")).lower() for j in a_jobs if j.get("title")}
        b_titles = {self._normalize_title_v49(j.get("title", "")).lower() for j in b_jobs if j.get("title")}
        if not a_titles or not b_titles:
            return 0.0
        return len(a_titles & b_titles) / max(1, min(len(a_titles), len(b_titles)))

    @staticmethod
    def _safe_text_v49(el: etree._Element) -> str:
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

    def _clean_description_v49(self, description: str) -> Optional[str]:
        text = " ".join((description or "").split())
        if not text:
            return None
        cut = re.search(r"\b(?:how\s+to\s+apply|cara\s+melamar|application\s+process)\b", text, re.IGNORECASE)
        if cut:
            text = text[: cut.start()].strip()
        if len(text) < 45:
            return None
        return text[:5000]
