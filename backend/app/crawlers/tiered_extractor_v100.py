"""
V10.0 — LLM-based extractor using Codex CLI on the host.

Writes HTML to shared /storage volume, signals the host-side extraction
helper via a file-based queue, and reads back JSON results.

The extraction prompt is stored in storage/v10_extraction_prompt.md.
"""

import asyncio
import json
import hashlib
import logging
import os
import re
import time
import uuid
from html import unescape
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v16 import TieredExtractorV16

logger = logging.getLogger(__name__)

PROMPT_FILE = "/storage/v10_extraction_prompt.md"
QUEUE_DIR = "/storage/v10_queue"
RESULT_TIMEOUT = 45  # Keep below extraction phase cap to avoid phase-level timeouts.
LOCAL_CONFIDENT_JOB_COUNT = 3
LLM_MAX_HTML_CHARS = 35000

_PROMPT_CACHE: str | None = None

_JOB_DETAIL_URL_RE = re.compile(
    r"("
    r"/p/[0-9a-z]{6,}|"
    r"/jobs?/[0-9a-z][0-9a-z\-_/]{2,}|"
    r"/career/(?!jobs?$|careers?$|job-openings/?$|openings/?$)[^/?#]{3,}|"
    r"/career/openings/[^/?#]{2,}|"
    r"/vacanc(?:y|ies)/[^/?#]{2,}|"
    r"/job[-_/]?detail[s]?/[^/?#]{2,}|"
    r"/job/view/[0-9]+|"
    r"[?&](?:jobId|job_id|vacancyId|vacancy_id|requisitionId|openingId|positionId|jobAdId|adId|career_job_req_id)="
    r")",
    re.IGNORECASE,
)
_DOC_VACANCY_RE = re.compile(r"\.(?:pdf|doc|docx)(?:[?#].*)?$", re.IGNORECASE)
_LISTING_PATH_RE = re.compile(
    r"/(?:jobs?|careers?|job-search|openings?|vacancies?|positions?)/?$",
    re.IGNORECASE,
)
_DETAIL_QUERY_ID_RE = re.compile(
    r"(?:^|&)(?:id|jobid|job_id|vacancyid|vacancy_id|requisitionid|openingid|positionid|jobadid|adid|career_job_req_id)=\d{1,12}(?:&|$)",
    re.IGNORECASE,
)
_NON_JOB_TITLE_RE = re.compile(
    r"^(?:"
    r"apply(?: now)?|"
    r"search jobs?|"
    r"browse jobs?|"
    r"view (?:all )?(?:jobs?|openings?)|"
    r"job openings?|"
    r"job vacancies?|"
    r"job details?|"
    r"jobdetail|"
    r"careers?|"
    r"join us|"
    r"join our team|"
    r"working with us|"
    r"we are hiring|"
    r"our culture|"
    r"our direction"
    r")$",
    re.IGNORECASE,
)
_GENERIC_CTA_RE = re.compile(
    r"^(?:apply(?: now)?|selengkapnya|learn more|read more|view details?)$",
    re.IGNORECASE,
)
_APPLY_CTA_RE = re.compile(
    r"(?:^|\b)(?:apply(?: now)?|submit application|quick apply|view details?|read more|learn more)(?:\b|$)",
    re.IGNORECASE,
)
_NON_ROLE_HEADING_RE = re.compile(
    r"^(?:expired date|location|lokasi|job type|type|department|detail|details?)$",
    re.IGNORECASE,
)
_BLOCKED_PAGE_RE = re.compile(
    r"(?:captcha|we apologize for the inconvenience|your activity .* made us think that you are a bot)",
    re.IGNORECASE,
)
_STATE_SCRIPT_ID_RE = re.compile(r"__(?:NEXT_DATA|NUXT|INITIAL_STATE)__", re.IGNORECASE)
_GENERIC_APPLY_PATH_RE = re.compile(r"^/(?:career-job|apply|application)(?:/)?$", re.IGNORECASE)
_ROLE_WORD_RE = re.compile(
    r"\b(?:"
    r"engineer|developer|analyst|manager|director|consultant|architect|officer|assistant|"
    r"coordinator|specialist|technician|operator|driver|nurse|teacher|lecturer|professor|"
    r"animator|designer|producer|recruiter|sales|marketing|finance|accountant|intern|"
    r"internship|chef|packer|surveyor|modelling|modeling|hgv|site\s+engineer|project"
    r")\b",
    re.IGNORECASE,
)
_SAME_PAGE_META_HINT_RE = re.compile(
    r"\b(?:location|job\s*type|employment\s*type|position|full\s*time|part\s*time|internship|resume|apply|recruit(?:ment)?)\b",
    re.IGNORECASE,
)
_INLINE_LOCATION_RE = re.compile(
    r"\b(?:location|based in)\s*:\s*([A-Za-z0-9][^|;<]{1,80})",
    re.IGNORECASE,
)
_INLINE_JOB_TYPE_RE = re.compile(
    r"\b(?:job\s*type|employment\s*type|position)\s*:\s*([A-Za-z0-9][^|;<]{1,80})",
    re.IGNORECASE,
)
_NON_ROLE_LABEL_RE = re.compile(r"\b(?:department|team|office|function|category|division|business unit)\b", re.IGNORECASE)
_FETCH_JSON_CALL_RE = re.compile(r"""fetch\(\s*['"]([^'"]+?\.json(?:\?[^'"]*)?)['"]\s*\)""", re.IGNORECASE)
_MARTIAN_CLIENT_RE = re.compile(r"""clientCode['"]?\s*[:=]\s*['"]([a-z0-9_-]{2,})['"]""", re.IGNORECASE)
_MARTIAN_RECRUITER_RE = re.compile(r"""recruiterId['"]?\s*[:=]\s*['"]?([0-9]{2,})""", re.IGNORECASE)
_MARTIAN_THEME_RE = re.compile(r"""jobBoardThemeId['"]?\s*[:=]\s*['"]?([0-9]{2,})""", re.IGNORECASE)
_WORKDAY_TENANT_RE = re.compile(r'tenant:\s*"([^"]+)"', re.IGNORECASE)
_WORKDAY_SITE_RE = re.compile(r'siteId:\s*"([^"]+)"', re.IGNORECASE)
_WORKDAY_LOCALE_RE = re.compile(r'requestLocale:\s*"([^"]+)"', re.IGNORECASE)
_ICIMS_HOST_RE = re.compile(r"https?://([a-z0-9.-]+\.icims\.com)", re.IGNORECASE)
_SUCCESSFACTORS_PORTAL_RE = re.compile(r"""['"](/portalcareer\?[^'"]+)['"]""", re.IGNORECASE)
_SUCCESSFACTORS_CAREER_JOB_SEARCH_RE = re.compile(
    r"""['"](/career\?[^'"]*navBarLevel=JOB_SEARCH[^'"]*)['"]""",
    re.IGNORECASE,
)


def _load_prompt() -> str:
    """Load the extraction prompt from file."""
    global _PROMPT_CACHE
    if _PROMPT_CACHE is not None:
        return _PROMPT_CACHE
    if os.path.exists(PROMPT_FILE):
        with open(PROMPT_FILE) as f:
            _PROMPT_CACHE = f.read()
            return _PROMPT_CACHE
    _PROMPT_CACHE = "Extract all job listings from the HTML. Return JSON with a 'jobs' array."
    return _PROMPT_CACHE


def _truncate_html(html: str, max_chars: int = LLM_MAX_HTML_CHARS) -> str:
    """Truncate HTML to fit within LLM context."""
    def _preserve_json_scripts(match: re.Match) -> str:
        attrs = (match.group(1) or "").strip()
        body = (match.group(2) or "").strip()
        attrs_l = attrs.lower()
        keep = (
            "application/json" in attrs_l
            or "ld+json" in attrs_l
            or bool(_STATE_SCRIPT_ID_RE.search(attrs))
        )
        if not keep:
            return ""
        compact_body = re.sub(r"\s+", " ", body)
        if len(compact_body) > 20000:
            compact_body = compact_body[:20000] + " ..."
        return f"<script {attrs}>{compact_body}</script>"

    cleaned = re.sub(
        r"<script\b([^>]*)>(.*?)</script>",
        _preserve_json_scripts,
        html,
        flags=re.DOTALL | re.IGNORECASE,
    )
    cleaned = re.sub(r'<style[^>]*>.*?</style>', '', cleaned, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r'<!--.*?-->', '', cleaned, flags=re.DOTALL)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = re.sub(r'\s+data-[a-z-]+="[^"]*"', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'<svg[^>]*>.*?</svg>', '', cleaned, flags=re.DOTALL | re.IGNORECASE)

    if len(cleaned) <= max_chars:
        return cleaned
    half = max_chars // 2
    return cleaned[:half] + "\n<!-- truncated -->\n" + cleaned[-half:]


class TieredExtractorV100(TieredExtractorV16):
    """Hybrid extractor: local deterministic extraction first, LLM fallback second."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if career_page else ""
        company_name = company.name if company else ""
        html = html or ""

        local_jobs = self._extract_local_jobs(html, url)
        if local_jobs and len(local_jobs) >= LOCAL_CONFIDENT_JOB_COUNT:
            logger.info("v10 local extracted %d jobs from %s", len(local_jobs), url)
            return local_jobs

        # Captcha / bot-wall pages never contain extractable job data.
        if self._is_blocked_page(html):
            if local_jobs:
                return local_jobs
            logger.info("v10 blocked page detected, skipping LLM for %s", url)
            return []

        shell_jobs = await self._extract_shell_endpoint_jobs(html, url)
        seed_jobs = self._dedupe_jobs(local_jobs + shell_jobs)
        if seed_jobs and len(seed_jobs) >= LOCAL_CONFIDENT_JOB_COUNT:
            logger.info(
                "v10 shell/local extracted %d jobs from %s (local=%d, shell=%d)",
                len(seed_jobs),
                url,
                len(local_jobs),
                len(shell_jobs),
            )
            return seed_jobs

        prompt = _load_prompt()
        truncated_html = _truncate_html(html)

        # Build full prompt
        full_prompt = f"""{prompt}

## Context
- Company: {company_name}
- Page URL: {url}

## HTML Content

{truncated_html}

Now extract all jobs from this HTML. Return ONLY the JSON object, no other text."""

        llm_jobs = await self._request_llm(full_prompt, url)
        merged = self._dedupe_jobs(seed_jobs + llm_jobs)
        if merged:
            logger.info(
                "v10 extracted %d jobs from %s (local=%d, llm=%d)",
                len(merged),
                url,
                len(seed_jobs),
                len(llm_jobs),
            )
            return merged
        return []

    async def _request_llm(self, full_prompt: str, page_url: str) -> list[dict]:
        """Queue prompt for host-side worker and parse the result."""
        # Write request to queue (host-side worker picks it up)
        try:
            os.makedirs(QUEUE_DIR, exist_ok=True)
        except Exception as exc:
            logger.warning("v10: queue unavailable (%s), skipping LLM for %s", exc, page_url)
            return []
        req_id = str(uuid.uuid4())[:8]
        req_file = os.path.join(QUEUE_DIR, f"{req_id}.prompt")
        result_file = os.path.join(QUEUE_DIR, f"{req_id}.result")

        with open(req_file, "w") as f:
            f.write(full_prompt)

        # Wait for result
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self._wait_for_result(result_file, req_id, page_url)
            )
            return result
        finally:
            # Cleanup
            for f in [req_file, result_file]:
                try:
                    os.unlink(f)
                except Exception:
                    pass

    def _wait_for_result(self, result_file: str, req_id: str, page_url: str) -> list[dict]:
        """Wait for the host-side worker to produce a result."""
        start = time.time()
        while time.time() - start < RESULT_TIMEOUT:
            if os.path.exists(result_file):
                try:
                    with open(result_file) as f:
                        output = f.read()
                    return self._parse_response(output, page_url)
                except Exception as e:
                    logger.error("v10: failed to read result for %s: %s", page_url, e)
                    return []
            time.sleep(1)

        logger.warning("v10: timeout waiting for LLM result (%ds) for %s", RESULT_TIMEOUT, page_url)
        return []

    def _parse_response(self, output: str, page_url: str) -> list[dict]:
        if not output:
            return []

        try:
            data = json.loads(output)
            return self._normalize_jobs(data, page_url)
        except json.JSONDecodeError:
            pass

        json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', output, re.DOTALL)
        if json_match:
            try:
                data = json.loads(json_match.group(1))
                return self._normalize_jobs(data, page_url)
            except json.JSONDecodeError:
                pass

        for match in re.finditer(r'\{[^{}]*"jobs"\s*:\s*\[.*?\].*?\}', output, re.DOTALL):
            try:
                data = json.loads(match.group())
                return self._normalize_jobs(data, page_url)
            except json.JSONDecodeError:
                continue

        logger.warning("v10: could not parse response for %s", page_url)
        return []

    def _normalize_jobs(self, data: dict, page_url: str) -> list[dict]:
        if not isinstance(data, dict):
            return []

        jobs = data.get("jobs", [])
        if not isinstance(jobs, list):
            return []

        result = []
        for j in jobs:
            if not isinstance(j, dict):
                continue
            title = (j.get("title") or "").strip()
            if not self._is_valid_title(title):
                continue

            source_url = (j.get("source_url") or j.get("url") or "").strip()
            source_url = _resolve_url(source_url, page_url) if source_url else page_url

            result.append({
                "title": title,
                "source_url": source_url or page_url,
                "location_raw": j.get("location_raw") or j.get("location") or None,
                "salary_raw": j.get("salary_raw") or j.get("salary") or None,
                "employment_type": j.get("employment_type") or j.get("type") or None,
                "description": j.get("description") or None,
                "extraction_method": "v10_llm",
                "extraction_confidence": 0.85,
            })

        # Save wrapper
        wrapper = data.get("wrapper")
        if wrapper and isinstance(wrapper, dict):
            try:
                wrapper_dir = "/storage/v10_wrappers"
                os.makedirs(wrapper_dir, exist_ok=True)
                domain_hash = hashlib.md5(page_url.encode()).hexdigest()[:12]
                with open(os.path.join(wrapper_dir, f"{domain_hash}.json"), "w") as f:
                    json.dump({"url": page_url, "wrapper": wrapper, "job_count": len(result)}, f, indent=2)
            except Exception:
                pass

        return self._dedupe_jobs(result)

    def _extract_local_jobs(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        jobs: list[dict] = []
        jobs.extend(self._extract_jsonld_jobs(root, page_url))
        jobs.extend(self._extract_embedded_state_jobs(root, page_url))
        jobs.extend(self._extract_breezy_rows(root, page_url))
        jobs.extend(self._extract_teamtailor_rows(root, page_url))
        jobs.extend(self._extract_gupy_rows(root, page_url))
        jobs.extend(self._extract_generic_job_rows(root, page_url))
        jobs.extend(self._extract_wordpress_career_cards(root, page_url))
        jobs.extend(self._extract_group_card_rows(root, page_url))
        jobs.extend(self._extract_list_group_rows(root, page_url))
        jobs.extend(self._extract_span_metadata_cards(root, page_url))
        jobs.extend(self._extract_heading_cta_cards(root, page_url))
        jobs.extend(self._extract_split_table_cards(root, page_url))
        jobs.extend(self._extract_table_rows(root, page_url))
        if len(jobs) < 3 and not self._has_strong_detail_anchor_evidence(root, page_url):
            jobs.extend(self._extract_same_page_role_sections(root, page_url))
        jobs.extend(self._extract_blog_post_rows(root, page_url))
        # Anchor-only extraction is noisier; use it only as low-coverage recovery.
        if len(jobs) < 3:
            jobs.extend(self._extract_detail_anchors(root, page_url))
        return self._dedupe_jobs(jobs)

    def _has_strong_detail_anchor_evidence(self, root, page_url: str) -> bool:
        count = 0
        for anchor in root.xpath("//a[@href]")[:1800]:
            source_url = _resolve_url((anchor.get("href") or "").strip(), page_url)
            if self._is_probable_job_url(source_url, page_url):
                count += 1
                if count >= 3:
                    return True
        return False

    async def _extract_shell_endpoint_jobs(self, html: str, page_url: str) -> list[dict]:
        """Recover jobs from JS-shell pages that expose endpoint hints in HTML."""
        jobs: list[dict] = []
        jobs.extend(await self._extract_fetch_json_shell_jobs(html, page_url))
        jobs.extend(await self._extract_workday_shell_jobs(html, page_url))
        jobs.extend(await self._extract_martian_shell_jobs(html, page_url))
        jobs.extend(await self._extract_successfactors_shell_jobs(html, page_url))
        jobs.extend(await self._extract_icims_shell_jobs(html, page_url))
        return self._dedupe_jobs(jobs)

    async def _extract_fetch_json_shell_jobs(self, html: str, page_url: str) -> list[dict]:
        if "fetch(" not in (html or "").lower():
            return []
        root = _parse_html(html)
        if root is None:
            return []

        endpoint_urls: list[str] = []
        location_map: dict[str, str] = {}
        scripts = root.xpath("//script/text()")
        for raw in scripts[:120]:
            text = raw or ""
            for match in _FETCH_JSON_CALL_RE.finditer(text):
                endpoint = _resolve_url(match.group(1).strip(), page_url)
                if endpoint and endpoint not in endpoint_urls:
                    endpoint_urls.append(endpoint)
            if "location" in text.lower():
                location_map.update(self._extract_location_code_map(text))

        jobs: list[dict] = []
        for endpoint in endpoint_urls[:4]:
            payload = await self._fetch_json_endpoint_payload(endpoint)
            if payload is None:
                continue
            jobs.extend(self._jobs_from_endpoint_payload(payload, page_url, location_map))
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break
        return jobs

    async def _extract_workday_shell_jobs(self, html: str, page_url: str) -> list[dict]:
        lower = (html or "").lower()
        if "window.workday" not in lower and "myworkdayjobs.com" not in lower:
            return []

        tenant_match = _WORKDAY_TENANT_RE.search(html or "")
        site_match = _WORKDAY_SITE_RE.search(html or "")
        if not tenant_match or not site_match:
            return []

        tenant = tenant_match.group(1).strip()
        site_id = site_match.group(1).strip()
        if not tenant or not site_id:
            return []

        locale_match = _WORKDAY_LOCALE_RE.search(html or "")
        locale = locale_match.group(1).strip() if locale_match else ""
        base = f"{urlparse(page_url).scheme or 'https'}://{urlparse(page_url).netloc}"
        endpoint = f"{base}/wday/cxs/{tenant}/{site_id}/jobs"

        jobs: list[dict] = []
        for offset in (0, 20, 40):
            payload = await self._fetch_json_endpoint_payload(
                endpoint,
                method="POST",
                payload={"appliedFacets": {}, "limit": 20, "offset": offset, "searchText": ""},
            )
            if payload is None and offset == 0:
                payload = await self._fetch_json_endpoint_payload(endpoint)
            if payload is None:
                break
            page_jobs = self._jobs_from_workday_payload(payload, page_url, site_id, locale)
            if not page_jobs:
                break
            before = len(jobs)
            jobs.extend(page_jobs)
            jobs = self._dedupe_jobs(jobs)
            if len(jobs) == before or len(jobs) >= MAX_JOBS_PER_PAGE:
                break
        return jobs

    async def _extract_martian_shell_jobs(self, html: str, page_url: str) -> list[dict]:
        lower = (html or "").lower()
        if (
            "clientcode" not in lower
            and "recruiterid" not in lower
            and "martianlogic" not in lower
            and "myrecruitmentplus" not in lower
        ):
            return []

        context = self._extract_martian_shell_context(html, page_url)
        client_code = context.get("client_code", "")
        recruiter_id = context.get("recruiter_id", "")
        board_name = context.get("board_name", "")
        theme_id = context.get("job_board_theme_id", "")
        query_client = context.get("query_client", "")
        if not client_code and not recruiter_id:
            return []

        parsed = urlparse(page_url)
        base = f"{parsed.scheme or 'https'}://{parsed.netloc}"
        hosts = [
            base,
            "https://web.martianlogic.com",
            "https://jobs.myrecruitmentplus.com",
            "https://form.myrecruitmentplus.com",
            "https://jobs.martianlogic.com",
        ]
        if client_code:
            hosts.extend(
                [
                    f"https://{client_code}.myrecruitmentplus.com",
                    f"https://{client_code}.martianlogic.com",
                ]
            )
        seen_hosts: set[str] = set()
        normalized_hosts: list[str] = []
        for host in hosts:
            norm = host.rstrip("/")
            if not norm or norm in seen_hosts:
                continue
            seen_hosts.add(norm)
            normalized_hosts.append(norm)

        query_templates = []
        if client_code and theme_id:
            query_templates.extend(
                [
                    f"clientCode={client_code}&jobBoardThemeId={theme_id}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"client={client_code}&jobBoardThemeId={theme_id}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                ]
            )
        if client_code:
            query_templates.extend(
                [
                    f"client={client_code}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"clientCode={client_code}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"client={client_code}&page=1&perPage=50&isActive=true",
                ]
            )
        if query_client and query_client != client_code:
            query_templates.extend(
                [
                    f"client={query_client}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"clientCode={query_client}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                ]
            )
        if client_code and recruiter_id:
            query_templates.extend(
                [
                    f"client={client_code}&recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"clientCode={client_code}&recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                ]
            )
        if client_code and board_name:
            query_templates.extend(
                [
                    f"client={client_code}&name={board_name}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"clientCode={client_code}&name={board_name}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                ]
            )

        path_templates = ["/", "/jobs", "/job-ads", "/jobads", "/embed-jobs", "/api/jobs/search", "/api/job-ads/search"]
        if client_code:
            path_templates.extend(
                [
                    f"/{client_code}",
                    f"/{client_code}/",
                    f"/{client_code}/jobs",
                    f"/{client_code}/job-ads",
                    f"/{client_code}/jobads",
                    f"/{client_code}/embed-jobs",
                ]
            )

        host_probes: dict[str, list[str]] = {}
        for host in normalized_hosts:
            probes_for_host: list[str] = []
            probes_for_host.extend(self._martian_next_data_probes(host, page_url, context))
            for path in path_templates:
                probes_for_host.append(f"{host}{path}")
                for query in query_templates:
                    probes_for_host.append(f"{host}{path}?{query}")
            if recruiter_id:
                probes_for_host.extend(
                    [
                        f"{host}/api/recruiter/{recruiter_id}/jobs?pageNumber=1&pageSize=50",
                        f"{host}/api/recruiter/{recruiter_id}/job-ads?pageNumber=1&pageSize=50",
                        f"{host}/api/recruiter/{recruiter_id}/jobads?pageNumber=1&pageSize=50",
                        f"{host}/api/job-ads/search?recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true",
                    ]
                )
            deduped_host: list[str] = []
            seen_host_probe: set[str] = set()
            for probe in probes_for_host:
                if not probe or probe in seen_host_probe:
                    continue
                seen_host_probe.add(probe)
                deduped_host.append(probe)
            host_probes[host] = deduped_host

        probes = self._round_robin_probe_urls(host_probes, limit=56)

        jobs: list[dict] = []
        for probe in probes:
            payload = await self._fetch_json_endpoint_payload(probe)
            if payload is None:
                continue
            jobs.extend(self._jobs_from_endpoint_payload(payload, page_url))
            jobs.extend(self._jobs_from_state_payload(payload, page_url))
            jobs = self._dedupe_jobs(jobs)
            if len(jobs) >= 8:
                break
        return jobs

    def _extract_martian_shell_context(self, html: str, page_url: str) -> dict[str, str]:
        context: dict[str, str] = {}
        html = html or ""

        next_data_match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html,
            re.IGNORECASE | re.DOTALL,
        )
        if next_data_match:
            try:
                data = json.loads(next_data_match.group(1))
                page_props = (((data.get("props") or {}).get("pageProps") or {}) if isinstance(data, dict) else {})
                if isinstance(page_props, dict):
                    context["client_code"] = str(page_props.get("clientCode") or "").strip()
                    context["recruiter_id"] = str(page_props.get("recruiterId") or "").strip()
                    context["board_name"] = str(page_props.get("name") or "").strip()
                    context["job_board_theme_id"] = str(page_props.get("jobBoardThemeId") or "").strip()
                context["build_id"] = str(data.get("buildId") or "").strip()
                query_obj = data.get("query") if isinstance(data, dict) else None
                if isinstance(query_obj, dict):
                    context["query_client"] = str(
                        query_obj.get("client") or query_obj.get("clientCode") or query_obj.get("slug") or ""
                    ).strip()
                    query_pairs = []
                    for key, value in query_obj.items():
                        if isinstance(value, (str, int, float)) and str(value).strip():
                            query_pairs.append((str(key), str(value)))
                    if query_pairs:
                        context["next_query"] = urlencode(query_pairs)
            except Exception:
                pass

        if not context.get("client_code"):
            match = _MARTIAN_CLIENT_RE.search(html)
            if match:
                context["client_code"] = match.group(1).strip()
        if not context.get("recruiter_id"):
            match = _MARTIAN_RECRUITER_RE.search(html)
            if match:
                context["recruiter_id"] = match.group(1).strip()
        if not context.get("job_board_theme_id"):
            match = _MARTIAN_THEME_RE.search(html)
            if match:
                context["job_board_theme_id"] = match.group(1).strip()

        if not context.get("client_code"):
            path_parts = [part for part in (urlparse(page_url).path or "").split("/") if part]
            if path_parts:
                context["client_code"] = path_parts[0].strip()

        query_values = dict(parse_qsl(urlparse(page_url).query))
        if not context.get("query_client"):
            context["query_client"] = str(query_values.get("client") or query_values.get("clientCode") or "").strip()
        if not context.get("recruiter_id"):
            context["recruiter_id"] = str(query_values.get("recruiterId") or "").strip()
        if not context.get("job_board_theme_id"):
            context["job_board_theme_id"] = str(query_values.get("jobBoardThemeId") or "").strip()
        if not context.get("board_name"):
            context["board_name"] = str(query_values.get("name") or "").strip()

        return context

    def _martian_next_data_probes(self, host: str, page_url: str, context: dict[str, str]) -> list[str]:
        build_id = str(context.get("build_id") or "").strip()
        if not build_id:
            return []

        parsed = urlparse(page_url)
        path = "/" + "/".join(seg for seg in (parsed.path or "").split("/") if seg)
        if not path:
            path = "/"

        query_pairs = dict(parse_qsl(parsed.query, keep_blank_values=True))
        next_query = str(context.get("next_query") or "").strip()
        if next_query:
            for key, value in parse_qsl(next_query, keep_blank_values=True):
                query_pairs.setdefault(key, value)

        client_code = str(context.get("client_code") or "").strip()
        query_client = str(context.get("query_client") or "").strip()
        if client_code and "client" not in query_pairs and "clientCode" not in query_pairs:
            query_pairs["client"] = client_code
        elif query_client and "client" not in query_pairs and "clientCode" not in query_pairs:
            query_pairs["client"] = query_client

        candidates = [f"{host}/_next/data/{build_id}/index.json"]
        if path != "/":
            norm_path = path.rstrip("/")
            candidates.append(f"{host}/_next/data/{build_id}{norm_path}.json")
            candidates.append(f"{host}/_next/data/{build_id}{norm_path}/index.json")

        for slug in (client_code, query_client):
            cleaned = re.sub(r"[^a-z0-9_-]+", "-", slug.lower()).strip("-")
            if not cleaned:
                continue
            candidates.append(f"{host}/_next/data/{build_id}/{cleaned}.json")
            candidates.append(f"{host}/_next/data/{build_id}/{cleaned}/index.json")

        encoded_query = urlencode(query_pairs, doseq=True) if query_pairs else ""
        if encoded_query:
            candidates = [f"{candidate}?{encoded_query}" for candidate in candidates]

        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            deduped.append(candidate)
        return deduped

    def _round_robin_probe_urls(self, host_probes: dict[str, list[str]], limit: int = 56) -> list[str]:
        ordered_hosts = [host for host, probes in host_probes.items() if probes]
        output: list[str] = []
        seen: set[str] = set()
        while ordered_hosts and len(output) < limit:
            remaining_hosts: list[str] = []
            for host in ordered_hosts:
                probes = host_probes.get(host) or []
                while probes and probes[0] in seen:
                    probes.pop(0)
                if not probes:
                    continue
                probe = probes.pop(0)
                if probe in seen:
                    continue
                seen.add(probe)
                output.append(probe)
                if probes:
                    remaining_hosts.append(host)
                if len(output) >= limit:
                    break
            ordered_hosts = remaining_hosts
        return output

    async def _fetch_json_endpoint_payload(
        self,
        endpoint: str,
        method: str = "GET",
        payload: dict | None = None,
    ) -> Any | None:
        try:
            async with httpx.AsyncClient(
                timeout=5.8,
                follow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json,text/plain,*/*",
                },
            ) as client:
                if method.upper() == "POST":
                    response = await client.post(endpoint, json=payload or {})
                else:
                    response = await client.get(endpoint)
        except Exception:
            return None

        if response.status_code != 200:
            return None

        body = (response.text or "").strip()
        if not body:
            return None

        try:
            return response.json()
        except Exception:
            pass

        parsed = self._parse_state_payloads(body)
        if parsed:
            return parsed[0]

        # Some ATS probes return HTML fragments/pages instead of JSON.
        # Convert recoverable rows/cards into a compact JSON-like payload.
        local_jobs = self._extract_local_jobs(body, endpoint)
        if local_jobs:
            payload_jobs: list[dict[str, Any]] = []
            for job in local_jobs[:MAX_JOBS_PER_PAGE]:
                title = self._clean_title(str(job.get("title") or ""))
                if not self._is_valid_title(title):
                    continue
                source_url = str(job.get("source_url") or "").strip()
                if source_url and not self._is_probable_job_url(source_url, endpoint):
                    source_url = ""
                payload_jobs.append(
                    {
                        "title": title,
                        "url": source_url or self._same_page_job_url(endpoint, title, len(payload_jobs) + 1),
                        "location": job.get("location_raw"),
                        "description": job.get("description"),
                        "employmentType": job.get("employment_type"),
                    }
                )
            if payload_jobs:
                return payload_jobs
        return None

    async def _extract_successfactors_shell_jobs(self, html: str, page_url: str) -> list[dict]:
        parsed = urlparse(page_url)
        host = (parsed.netloc or "").lower()
        lower = (html or "").lower()
        if "successfactors.com" not in host and "portalcareer?" not in lower:
            return []

        candidate_urls: list[str] = []
        for match in _SUCCESSFACTORS_PORTAL_RE.finditer(html or ""):
            candidate = _resolve_url(match.group(1).strip(), page_url)
            if candidate:
                candidate_urls.append(candidate)
        for match in _SUCCESSFACTORS_CAREER_JOB_SEARCH_RE.finditer(html or ""):
            candidate = _resolve_url(match.group(1).strip(), page_url)
            if candidate:
                candidate_urls.append(candidate)

        if parsed.scheme and parsed.netloc:
            query_pairs = dict(parse_qsl(parsed.query, keep_blank_values=True))
            if query_pairs:
                query_pairs["navBarLevel"] = "JOB_SEARCH"
                query_pairs.setdefault("career_ns", "job_listing_summary")
                encoded = urlencode(query_pairs, doseq=True)
                candidate_urls.append(
                    urlunparse((parsed.scheme, parsed.netloc, "/portalcareer", "", encoded, ""))
                )
                candidate_urls.append(
                    urlunparse((parsed.scheme, parsed.netloc, parsed.path or "/career", "", encoded, ""))
                )

        deduped_urls: list[str] = []
        seen_urls: set[str] = set()
        for candidate in candidate_urls:
            if not candidate:
                continue
            cand_host = (urlparse(candidate).netloc or "").lower()
            if host and cand_host and cand_host != host:
                continue
            if candidate in seen_urls:
                continue
            seen_urls.add(candidate)
            deduped_urls.append(candidate)

        jobs: list[dict] = []
        for candidate in deduped_urls[:6]:
            payload = await self._fetch_json_endpoint_payload(candidate)
            if payload is None:
                continue
            jobs.extend(self._jobs_from_endpoint_payload(payload, candidate))
            jobs.extend(self._jobs_from_state_payload(payload, candidate))
            jobs = self._dedupe_jobs(jobs)
            if len(jobs) >= 8:
                break
        return jobs

    async def _extract_icims_shell_jobs(self, html: str, page_url: str) -> list[dict]:
        raw = unescape(html or "")
        if "icims.com" not in raw.lower():
            return []

        hosts: list[str] = []
        seen_hosts: set[str] = set()
        for match in _ICIMS_HOST_RE.finditer(raw):
            host = (match.group(1) or "").strip().lower()
            if not host or host in seen_hosts:
                continue
            seen_hosts.add(host)
            hosts.append(host)

        jobs: list[dict] = []
        for host in hosts[:3]:
            probes = [
                f"https://{host}/jobs/search?ss=1",
                f"https://{host}/jobs/search",
            ]
            for probe in probes:
                payload = await self._fetch_json_endpoint_payload(probe)
                if payload is None:
                    continue
                jobs.extend(self._jobs_from_endpoint_payload(payload, probe))
                jobs.extend(self._jobs_from_state_payload(payload, probe))
                jobs = self._dedupe_jobs(jobs)
                if len(jobs) >= 8:
                    return jobs
        return jobs

    def _extract_location_code_map(self, script_text: str) -> dict[str, str]:
        mapping: dict[str, str] = {}
        if not script_text:
            return mapping
        for match in re.finditer(r"\b([a-z]{2,6})\s*:\s*['\"]([^'\"]{2,120})['\"]", script_text, re.IGNORECASE):
            code = match.group(1).lower().strip()
            value = self._clean_text(match.group(2))
            if not code or not value:
                continue
            if len(code) <= 6 and len(value) >= 2:
                mapping[code] = value
        return mapping

    def _jobs_from_endpoint_payload(
        self,
        payload: Any,
        page_url: str,
        location_map: dict[str, str] | None = None,
    ) -> list[dict]:
        location_map = location_map or {}
        items: list[Any]
        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            items = []
            for key in ("jobs", "positions", "data", "results", "items"):
                value = payload.get(key)
                if isinstance(value, list):
                    items = value
                    break
            if not items:
                items = [payload]
        else:
            return []

        jobs: list[dict] = []
        for idx, item in enumerate(items[:2000], start=1):
            if not isinstance(item, dict):
                continue
            title = ""
            for key in ("title", "jobTitle", "name", "positionTitle", "role"):
                value = item.get(key)
                if isinstance(value, str):
                    candidate = self._clean_title(unescape(value))
                    if self._is_valid_title(candidate):
                        title = candidate
                        break
            if not title:
                continue

            source_url = ""
            for key in ("url", "absolute_url", "jobUrl", "detailUrl", "applyUrl", "apply_url"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    source_url = _resolve_url(value.strip(), page_url) or ""
                    break
            if source_url and not self._is_probable_job_url(source_url, page_url):
                source_url = ""
            if not source_url:
                source_url = self._same_page_job_url(page_url, title, idx)

            location = self._extract_location_text(item.get("location"))
            if isinstance(location, str):
                mapped = location_map.get(location.lower().strip())
                if mapped:
                    location = mapped

            description = None
            for key in ("description", "summary", "teaser"):
                value = item.get(key)
                if isinstance(value, str):
                    cleaned = self._clean_text(unescape(value))
                    if cleaned:
                        description = cleaned[:1500]
                        break

            employment_type = None
            value = item.get("jobType") or item.get("employmentType") or item.get("type")
            if isinstance(value, str):
                employment_type = self._clean_text(value) or None

            jobs.append(
                self._job(
                    title,
                    source_url,
                    location,
                    None,
                    employment_type,
                    "v10_shell_json_endpoint",
                    0.9,
                    description,
                )
            )
        return jobs

    def _jobs_from_workday_payload(
        self,
        payload: Any,
        page_url: str,
        site_id: str,
        locale: str,
    ) -> list[dict]:
        if not isinstance(payload, dict):
            return []

        postings = payload.get("jobPostings")
        if not isinstance(postings, list):
            postings = payload.get("jobs")
        if not isinstance(postings, list):
            return []

        jobs: list[dict] = []
        for item in postings[:2000]:
            if not isinstance(item, dict):
                continue

            title = self._clean_title(str(item.get("title") or item.get("bulletFields") or ""))
            if not self._is_valid_title(title):
                continue

            path = ""
            for key in ("externalPath", "url", "jobUrl"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    path = value.strip()
                    break
            if not path:
                continue

            source_url = _resolve_url(path, page_url) or ""
            if source_url and f"/{site_id}/job/" not in source_url and path.startswith("/job/"):
                locale_part = f"/{locale}" if locale else ""
                source_url = _resolve_url(f"{locale_part}/{site_id}{path}", page_url) or source_url
            if not self._is_probable_job_url(source_url, page_url):
                continue

            location = None
            for key in ("locationsText", "location", "locationName"):
                value = item.get(key)
                if isinstance(value, str):
                    cleaned = self._clean_text(value)
                    if cleaned:
                        location = cleaned
                        break
                if isinstance(value, list):
                    parts = [self._clean_text(str(v)) for v in value if str(v).strip()]
                    if parts:
                        location = ", ".join(parts[:2])
                        break

            jobs.append(
                self._job(
                    title,
                    source_url,
                    location,
                    None,
                    None,
                    "v10_shell_workday_api",
                    0.93,
                )
            )
        return jobs

    def _extract_embedded_state_jobs(self, root, page_url: str) -> list[dict]:
        """Extract jobs from embedded JSON state payloads in app-shell pages."""
        scripts = root.xpath("//script/text()")
        if not scripts:
            return []

        jobs: list[dict] = []
        for raw in scripts[:120]:
            for payload in self._parse_state_payloads(raw):
                jobs.extend(self._jobs_from_state_payload(payload, page_url))
        return jobs

    def _parse_state_payloads(self, raw: str) -> list[Any]:
        text = (raw or "").strip()
        if not text or len(text) < 2:
            return []

        payloads: list[Any] = []
        # Direct JSON payloads (for script[type=application/json]).
        if text[:1] in ("{", "["):
            try:
                payloads.append(json.loads(text))
            except Exception:
                pass
            return payloads

        # JS assignment wrappers (window.__NEXT_DATA__ = {...};).
        assignment_match = re.search(
            r"(?:window\.)?[A-Za-z0-9_$]+(?:\.[A-Za-z0-9_$]+)*\s*=\s*(\{.*\}|\[.*\])\s*;?\s*$",
            text,
            re.DOTALL,
        )
        if assignment_match:
            try:
                payloads.append(json.loads(assignment_match.group(1)))
                return payloads
            except Exception:
                pass

        # JSON.parse("...") wrappers.
        parse_match = re.search(r"JSON\.parse\((['\"])(.+?)\1\)", text, re.DOTALL)
        if parse_match:
            encoded = parse_match.group(2)
            try:
                decoded = bytes(encoded, "utf-8").decode("unicode_escape")
                payloads.append(json.loads(decoded))
                return payloads
            except Exception:
                pass
        return payloads

    def _jobs_from_state_payload(self, payload: Any, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        title_keys = ("title", "jobTitle", "name", "positionTitle", "role", "jobAdTitle", "jobName")
        url_keys = (
            "url",
            "absolute_url",
            "jobUrl",
            "jobURL",
            "detailUrl",
            "applyUrl",
            "apply_url",
            "postingUrl",
            "canonicalUrl",
            "externalPath",
            "jobPath",
            "jobDetailUrl",
            "jobAdUrl",
            "applicationFormUrl",
            "applicationUrl",
            "path",
            "slug",
        )
        location_keys = ("location", "locationName", "city", "region", "country", "jobLocation")
        type_keys = ("employmentType", "jobType", "type")
        description_keys = ("description", "summary", "teaser")

        for obj in self._walk_json(payload):
            if not isinstance(obj, dict):
                continue

            title = ""
            title_key = ""
            for key in title_keys:
                value = obj.get(key)
                if isinstance(value, str):
                    candidate = self._clean_title(value)
                    if self._is_valid_title(candidate):
                        title = candidate
                        title_key = key
                        break
            if not title:
                continue
            if _NON_ROLE_LABEL_RE.search(title) and not _ROLE_WORD_RE.search(title):
                continue

            raw_url = ""
            used_id_fallback = False
            for key in url_keys:
                value = obj.get(key)
                if isinstance(value, str) and value.strip():
                    raw_url = value.strip()
                    break
            if not raw_url:
                for id_key in ("jobAdId", "jobId", "jobID", "requisitionId", "positionId", "id"):
                    value = obj.get(id_key)
                    if isinstance(value, (int, str)) and str(value).strip().isdigit():
                        sep = "&" if "?" in page_url else "?"
                        raw_url = f"{page_url}{sep}jobAdId={str(value).strip()}"
                        used_id_fallback = True
                        break
            if not raw_url:
                continue

            key_names = " ".join(str(k) for k in obj.keys()).lower()
            has_job_key = bool(re.search(r"job|position|requisition|opening|vacanc|posting", key_names))
            if used_id_fallback and not has_job_key:
                continue
            if title_key == "name" and not has_job_key and not _ROLE_WORD_RE.search(title):
                continue

            source_url = _resolve_url(raw_url, page_url)
            if not self._is_probable_job_url(source_url, page_url):
                continue

            location: str | None = None
            for key in location_keys:
                value = obj.get(key)
                location = self._extract_location_text(value)
                if location:
                    break

            employment_type = None
            for key in type_keys:
                value = obj.get(key)
                if isinstance(value, str):
                    employment_type = self._clean_text(value) or None
                    if employment_type:
                        break

            description = None
            for key in description_keys:
                value = obj.get(key)
                if isinstance(value, str):
                    description = self._clean_text(value) or None
                    if description:
                        description = description[:1500]
                        break

            jobs.append(
                self._job(
                    title,
                    source_url,
                    location,
                    None,
                    employment_type,
                    "v10_local_state_json",
                    0.91,
                    description,
                )
            )
        return jobs

    def _extract_breezy_rows(self, root, page_url: str) -> list[dict]:
        rows = root.xpath("//li[contains(@class,'position') and .//a[contains(@href,'/p/')]]")
        jobs: list[dict] = []
        for row in rows[:1200]:
            a_nodes = row.xpath(".//a[contains(@href,'/p/')][1]")
            title_nodes = row.xpath(".//h2[1]")
            if not a_nodes or not title_nodes:
                continue
            source_url = _resolve_url((a_nodes[0].get("href") or "").strip(), page_url)
            title = self._clean_title(_text(title_nodes[0]))
            if not self._is_valid_title(title) or not self._is_probable_job_url(source_url, page_url):
                continue
            location_nodes = row.xpath(".//*[contains(@class,'location')]//span[1]")
            location = self._clean_text(_text(location_nodes[0])) if location_nodes else None
            jobs.append(self._job(title, source_url, location, None, None, "v10_local_breezy", 0.98))
        return jobs

    def _extract_teamtailor_rows(self, root, page_url: str) -> list[dict]:
        rows = root.xpath("//li[contains(@class,'w-full') and .//a[contains(@href,'/jobs/')]]")
        jobs: list[dict] = []
        for row in rows[:1200]:
            a_nodes = row.xpath(".//a[contains(@href,'/jobs/')][1]")
            if not a_nodes:
                continue
            source_url = _resolve_url((a_nodes[0].get("href") or "").strip(), page_url)
            if not source_url or not re.search(r"/jobs/\d+", source_url):
                continue
            title = self._clean_title(_text(a_nodes[0]))
            if not self._is_valid_title(title):
                continue
            location_nodes = row.xpath(".//*[contains(@class,'mt-1')]//span[last()][1]")
            location = self._clean_text(_text(location_nodes[0])) if location_nodes else None
            jobs.append(self._job(title, source_url, location, None, None, "v10_local_teamtailor", 0.97))
        return jobs

    def _extract_generic_job_rows(self, root, page_url: str) -> list[dict]:
        rows = root.xpath(
            "//*[contains(concat(' ', normalize-space(@class), ' '), ' job ') and .//a[contains(@href,'/career/openings/')]]"
        )
        jobs: list[dict] = []
        for row in rows[:1200]:
            link_nodes = row.xpath(".//a[contains(@href,'/career/openings/')][1]")
            if not link_nodes:
                continue
            source_url = _resolve_url((link_nodes[0].get("href") or "").strip(), page_url)
            if not self._is_probable_job_url(source_url, page_url):
                continue

            title_nodes = row.xpath(".//*[contains(@class,'job__name')][1]|.//h2[1]|.//h3[1]")
            title = self._clean_title(_text(title_nodes[0])) if title_nodes else self._clean_title(_text(link_nodes[0]))
            if not self._is_valid_title(title):
                continue

            location_nodes = row.xpath(".//*[contains(@class,'job__location')][1]|.//*[contains(@class,'location')][1]")
            location = self._clean_text(_text(location_nodes[0])) if location_nodes else None
            job_type_nodes = row.xpath(".//*[contains(@class,'job__type')][1]|.//*[contains(@class,'type')][1]")
            job_type = self._clean_text(_text(job_type_nodes[0])) if job_type_nodes else None
            jobs.append(self._job(title, source_url, location, None, job_type, "v10_local_job_grid", 0.95))
        return jobs

    def _extract_gupy_rows(self, root, page_url: str) -> list[dict]:
        host = (urlparse(page_url).netloc or "").lower()
        rows = root.xpath("//ul[@data-testid='job-list__list']/li[.//a[contains(@href,'/jobs/')]]")
        if not rows and "gupy.io" not in host:
            return []
        if not rows:
            rows = root.xpath("//li[.//a[contains(@href,'/jobs/') and contains(@href,'jobBoardSource=gupy')]]")

        jobs: list[dict] = []
        for row in rows[:2000]:
            link_nodes = row.xpath(".//a[contains(@href,'/jobs/')][1]")
            if not link_nodes:
                continue
            source_url = _resolve_url((link_nodes[0].get("href") or "").strip(), page_url)
            if not self._is_probable_job_url(source_url, page_url):
                continue

            title = ""
            title_nodes = row.xpath(
                ".//a[contains(@href,'/jobs/')]//div/div[1][1]"
                "|.//div[@data-testid='job-list-item-title'][1]"
                "|.//h1[1]|.//h2[1]|.//h3[1]"
            )
            for node in title_nodes[:8]:
                candidate = self._clean_title(_text(node))
                if self._is_valid_title(candidate):
                    title = candidate
                    break
            if not title:
                title = self._clean_title(_text(link_nodes[0]))
            if not self._is_valid_title(title):
                continue

            location_nodes = row.xpath(
                ".//a[contains(@href,'/jobs/')]//div/div[2][1]"
                "|.//*[contains(@data-testid,'location')][1]"
            )
            location = self._clean_text(_text(location_nodes[0])) if location_nodes else None

            job_type_nodes = row.xpath(
                ".//a[contains(@href,'/jobs/')]//div/div[3][1]"
                "|.//*[contains(@data-testid,'employment')][1]"
            )
            job_type = self._clean_text(_text(job_type_nodes[0])) if job_type_nodes else None

            jobs.append(self._job(title, source_url, location, None, job_type, "v10_local_gupy_rows", 0.95))
        return jobs

    def _extract_wordpress_career_cards(self, root, page_url: str) -> list[dict]:
        rows = root.xpath("//div[contains(@class,'col-md-6') and .//a[contains(@href,'/career/')]]")
        jobs: list[dict] = []
        for row in rows[:1500]:
            link_nodes = row.xpath(".//h4//a[contains(@href,'/career/')][1]|.//a[contains(@href,'/career/')][1]")
            if not link_nodes:
                continue
            source_url = _resolve_url((link_nodes[0].get("href") or "").strip(), page_url)
            if not self._is_probable_job_url(source_url, page_url):
                continue
            title = self._clean_title(_text(link_nodes[0]))
            if not self._is_valid_title(title):
                continue
            location_nodes = row.xpath(".//*[contains(@class,'ctf-region')]//*[contains(@class,'ctf-value')][1]")
            location = self._clean_text(_text(location_nodes[0])) if location_nodes else None
            jobs.append(self._job(title, source_url, location, None, None, "v10_local_wp_cards", 0.95))
        return jobs

    def _extract_group_card_rows(self, root, page_url: str) -> list[dict]:
        """Extract anchor-wrapped card grids with heading text in nested title nodes."""
        rows = root.xpath("//a[contains(concat(' ', normalize-space(@class), ' '), ' group ') and @href]")
        jobs: list[dict] = []
        for row in rows[:2200]:
            source_url = _resolve_url((row.get("href") or "").strip(), page_url)
            if not self._is_probable_blog_job_url(source_url, page_url):
                continue

            title_nodes = row.xpath(
                ".//p[contains(@class,'text-3xl')][1]|.//h1[1]|.//h2[1]|.//h3[1]|.//*[contains(@class,'job-title')][1]"
            )
            title = self._clean_title(_text(title_nodes[0])) if title_nodes else self._clean_title(_text(row))
            if not self._is_valid_title(title):
                continue

            location_nodes = row.xpath(
                ".//div[contains(@class,'font-serif')]//p[1]|.//*[contains(@class,'location')][1]"
            )
            location = self._clean_text(_text(location_nodes[0])) if location_nodes else None
            description = self._clean_text(_text(row))
            description = description[:1500] if description and len(description) > 40 else None
            jobs.append(self._job(title, source_url, location, None, None, "v10_local_group_cards", 0.92, description))
        return jobs

    def _extract_list_group_rows(self, root, page_url: str) -> list[dict]:
        """Extract Bootstrap/list-group style listing rows with heading links + metadata."""
        rows = root.xpath(
            "//div[contains(concat(' ', normalize-space(@class), ' '), ' list-group-item ')]"
            "[.//h1//a[@href] or .//h2//a[@href] or .//h3//a[@href]]"
        )
        jobs: list[dict] = []
        for row in rows[:2500]:
            link_nodes = row.xpath(".//h1//a[@href][1] | .//h2//a[@href][1] | .//h3//a[@href][1] | .//a[@href][1]")
            if not link_nodes:
                continue
            source_url = _resolve_url((link_nodes[0].get("href") or "").strip(), page_url)
            if not self._is_probable_job_url(source_url, page_url):
                continue

            title = self._clean_title(_text(link_nodes[0]))
            if not self._is_valid_title(title):
                continue

            location_nodes = row.xpath(
                ".//*[contains(@class,'priority-data')]//a[position()=2][1]"
                "|.//*[contains(@class,'location')][1]"
            )
            location = self._clean_text(_text(location_nodes[0])) if location_nodes else None

            description = self._clean_text(_text(row))
            description = description[:1500] if description and len(description) > 40 else None
            jobs.append(self._job(title, source_url, location, None, None, "v10_local_list_group", 0.92, description))
        return jobs

    def _extract_span_metadata_cards(self, root, page_url: str) -> list[dict]:
        """Extract card anchors where role metadata is split into nested span fields."""
        rows = root.xpath(
            "//*[contains(concat(' ', normalize-space(@class), ' '), ' career-list ') and .//a[@href]]"
            "|//*[contains(concat(' ', normalize-space(@class), ' '), ' job-card ') and .//a[@href]]"
        )
        jobs: list[dict] = []
        for row in rows[:2000]:
            link_nodes = row.xpath(".//a[@href][1]")
            if not link_nodes:
                continue
            source_url = _resolve_url((link_nodes[0].get("href") or "").strip(), page_url)
            if not self._is_probable_job_url(source_url, page_url):
                continue

            title_nodes = row.xpath(".//span[contains(@class,'sub-title')][1]|.//h1[1]|.//h2[1]|.//h3[1]")
            title = self._clean_title(_text(title_nodes[0])) if title_nodes else self._clean_title(_text(link_nodes[0]))
            if not self._is_valid_title(title):
                continue

            location_nodes = row.xpath(".//span[contains(@class,'job-location')][1]|.//*[contains(@class,'location')][1]")
            location = self._clean_text(_text(location_nodes[0])) if location_nodes else None

            experience_nodes = row.xpath(".//span[contains(@class,'experience')][1]|.//span[contains(@class,'job-experience')][1]")
            posted_nodes = row.xpath(".//span[contains(@class,'job-clock')][1]|.//span[contains(@class,'posted')][1]")
            description_parts = []
            if experience_nodes:
                description_parts.append(self._clean_text(_text(experience_nodes[0])))
            if posted_nodes:
                description_parts.append(self._clean_text(_text(posted_nodes[0])))
            description = " | ".join(part for part in description_parts if part) or None

            jobs.append(self._job(title, source_url, location, None, None, "v10_local_span_card", 0.9, description))
        return jobs

    def _extract_heading_cta_cards(self, root, page_url: str) -> list[dict]:
        """Extract cards where heading title and CTA/detail URL are split across sibling nodes."""
        rows = root.xpath(
            "//div[contains(@class,'elementor-widget-wrap') and (.//h1|.//h2|.//h3|.//h4) and .//a[@href]]"
            "|//div[contains(@class,'card') and (.//h1|.//h2|.//h3|.//h4) and .//a[@href]]"
        )
        jobs: list[dict] = []
        for row in rows[:3000]:
            heading_nodes = row.xpath(
                ".//*[contains(@class,'heading-title')][1]|.//h1[1]|.//h2[1]|.//h3[1]|.//h4[1]"
            )
            if not heading_nodes:
                continue
            title = self._clean_title(_text(heading_nodes[0]))
            if not self._is_valid_title(title) or _NON_ROLE_HEADING_RE.match(title):
                continue

            link_nodes = row.xpath(".//a[@href]")
            source_url = None
            for link in link_nodes[:12]:
                href = _resolve_url((link.get("href") or "").strip(), page_url)
                link_text = self._clean_text(_text(link))
                if self._is_probable_job_url(href, page_url):
                    source_url = href
                    break
                if self._is_apply_context_url(href, link_text, page_url):
                    source_url = href
                    break
            if not source_url:
                continue

            location_nodes = row.xpath(".//*[contains(@class,'icon-list-text')][1]|.//*[contains(@class,'location')][1]")
            location = self._clean_text(_text(location_nodes[0])) if location_nodes else None
            description = self._clean_text(_text(row))
            description = description[:1200] if description and len(description) > 40 else None
            jobs.append(self._job(title, source_url, location, None, None, "v10_local_heading_cta", 0.9, description))
        return jobs

    def _extract_same_page_role_sections(self, root, page_url: str) -> list[dict]:
        """Extract role sections where headings + metadata exist but no detail link."""
        headings = root.xpath("//h1|//h2|//h3|//h4")
        jobs: list[dict] = []
        seen_titles: set[str] = set()
        for idx, heading in enumerate(headings[:2200], start=1):
            if heading.xpath("ancestor::nav|ancestor::header|ancestor::footer"):
                continue

            title = self._clean_title(unescape(_text(heading)))
            if not self._is_valid_title(title):
                continue
            if _NON_JOB_TITLE_RE.match(title) or _NON_ROLE_HEADING_RE.match(title):
                continue
            if title.lower() in seen_titles:
                continue
            if not _ROLE_WORD_RE.search(title):
                continue

            container = None
            body_text = ""
            for ancestor in heading.iterancestors():
                if not isinstance(ancestor.tag, str):
                    continue
                tag = ancestor.tag.lower()
                if tag == "body":
                    break
                if tag not in {"section", "article", "div", "li"}:
                    continue
                classes = (ancestor.get("class") or "").lower()
                if any(x in classes for x in ("menu", "nav", "footer", "header", "breadcrumb")):
                    continue
                text = self._clean_text(unescape(_text(ancestor)))
                if len(text) < 60 or len(text) > 2600:
                    continue
                if not _SAME_PAGE_META_HINT_RE.search(text):
                    continue
                container = ancestor
                body_text = text
                break
            if container is None:
                continue

            location = self._extract_inline_location(body_text, title)
            employment_type = self._extract_inline_job_type(body_text)
            source_url = self._same_page_job_url(page_url, title, idx)
            description = body_text[:1500] if len(body_text) > 40 else None

            jobs.append(
                self._job(
                    title,
                    source_url,
                    location,
                    None,
                    employment_type,
                    "v10_local_same_page_section",
                    0.84,
                    description,
                )
            )
            seen_titles.add(title.lower())
        return jobs

    def _extract_blog_post_rows(self, root, page_url: str) -> list[dict]:
        """Extract WordPress/Divi-style post lists used as job listing feeds."""
        rows = root.xpath(
            "//article[contains(@class,'post') and .//*[contains(@class,'entry-title')]//a[@href]]"
        )
        jobs: list[dict] = []
        for row in rows[:1800]:
            link_nodes = row.xpath(
                ".//*[contains(@class,'entry-title')]//a[@href][1]"
                "|.//h1//a[@href][1]|.//h2//a[@href][1]|.//h3//a[@href][1]"
            )
            if not link_nodes:
                continue
            link = link_nodes[0]
            source_url = _resolve_url((link.get("href") or "").strip(), page_url)
            if not self._is_probable_blog_job_url(source_url, page_url):
                continue

            title = self._clean_title(unescape(_text(link)))
            if not self._is_valid_title(title):
                continue
            if not _ROLE_WORD_RE.search(title) and not _ROLE_WORD_RE.search(source_url or ""):
                continue

            location = None
            title_loc = re.search(r"\s+[–-]\s+([A-Za-z][A-Za-z ,.&'-]{1,60})$", title)
            if title_loc:
                location = self._clean_text(title_loc.group(1)) or None

            description = self._clean_text(unescape(_text(row)))
            description = description[:1500] if description and len(description) > 40 else None
            jobs.append(self._job(title, source_url, location, None, None, "v10_local_blog_post", 0.9, description))
        return jobs

    def _extract_table_rows(self, root, page_url: str) -> list[dict]:
        rows = root.xpath("//tr[.//a[@href]]")
        jobs: list[dict] = []
        for row in rows[:2000]:
            link_nodes = row.xpath(".//a[@href][1]")
            if not link_nodes:
                continue
            href = (link_nodes[0].get("href") or "").strip()
            source_url = _resolve_url(href, page_url)
            if not source_url:
                continue
            if not self._is_probable_job_url(source_url, page_url) and not _DOC_VACANCY_RE.search(source_url):
                continue

            title_nodes = row.xpath(
                ".//*[contains(@class,'body--medium')][1]|.//h1[1]|.//h2[1]|.//h3[1]|.//h4[1]|.//strong[1]"
            )
            title = self._clean_title(_text(title_nodes[0])) if title_nodes else self._clean_title(_text(link_nodes[0]))
            if not self._is_valid_title(title):
                title = self._clean_title(_text(link_nodes[0]))
            if not self._is_valid_title(title):
                continue

            location_nodes = row.xpath(
                "./td[2][1]"
                "|.//*[contains(@class,'body__secondary')][1]"
                "|.//*[contains(@class,'body--metadata')][1]"
                "|.//*[contains(@class,'location')][1]"
            )
            location = self._clean_text(_text(location_nodes[0])) if location_nodes else None
            description = self._clean_text(_text(row))
            description = description[:1500] if description and len(description) > 40 else None
            jobs.append(self._job(title, source_url, location, None, None, "v10_local_table", 0.9, description))
        return jobs

    def _extract_split_table_cards(self, root, page_url: str) -> list[dict]:
        """
        Recover card layouts where the title and detail link are on different table rows.
        Example pattern: title in one <tr>, generic CTA (Apply/Selengkapnya) with detail URL in another.
        """
        tables = root.xpath("//table[.//a[@href] and (.//h1|.//h2|.//h3|.//h4)]")
        jobs: list[dict] = []
        for table in tables[:1200]:
            link_nodes = table.xpath(".//a[@href]")
            if not link_nodes:
                continue

            link_node = None
            source_url = None
            for candidate in link_nodes:
                maybe_url = _resolve_url((candidate.get("href") or "").strip(), page_url)
                if self._is_probable_job_url(maybe_url, page_url) or _DOC_VACANCY_RE.search(maybe_url or ""):
                    link_node = candidate
                    source_url = maybe_url
                    break
            if link_node is None or not source_url:
                continue

            heading_nodes = table.xpath(".//h1|.//h2|.//h3|.//h4")
            title = ""
            for heading in heading_nodes[:10]:
                candidate_title = self._clean_title(_text(heading))
                if not candidate_title or _NON_ROLE_HEADING_RE.match(candidate_title):
                    continue
                if self._is_valid_title(candidate_title):
                    title = candidate_title
                    break
            if not title:
                link_title = self._clean_title(_text(link_node))
                if self._is_valid_title(link_title):
                    title = link_title
            if not title:
                continue

            # Prefer map/location row text when available.
            location_nodes = table.xpath(
                ".//tr[.//*[contains(@class,'fa-map') or contains(@class,'icon-map')]]/td[last()][1]"
            )
            location = self._clean_text(_text(location_nodes[0])) if location_nodes else None
            if location and location.lower().startswith(title.lower()):
                location = self._clean_text(location[len(title):])
            location = location or None

            description = self._clean_text(_text(table))
            description = description[:1500] if description and len(description) > 40 else None
            jobs.append(self._job(title, source_url, location, None, None, "v10_local_split_table_card", 0.92, description))
        return jobs

    def _extract_detail_anchors(self, root, page_url: str) -> list[dict]:
        anchors = root.xpath("//a[@href]")
        jobs: list[dict] = []
        for a in anchors[:3000]:
            source_url = _resolve_url((a.get("href") or "").strip(), page_url)
            if not self._is_probable_job_url(source_url, page_url):
                continue
            raw_title = ""
            title_nodes = a.xpath(
                ".//*[contains(@class,'body--medium')][1]"
                "|.//*[contains(@class,'job-title')][1]"
                "|.//*[contains(@class,'jobTitle')][1]"
                "|.//h1[1]|.//h2[1]|.//h3[1]|.//h4[1]|.//strong[1]"
            )
            if title_nodes:
                raw_title = _text(title_nodes[0])
            if not raw_title:
                heading = a.xpath(".//h1|.//h2|.//h3|.//h4|.//span")
                raw_title = _text(heading[0]) if heading else _text(a)
            title = self._clean_title(raw_title)
            if not self._is_valid_title(title):
                continue

            location = self._extract_anchor_location(a)
            jobs.append(self._job(title, source_url, location, None, None, "v10_local_anchor", 0.82))
        return jobs

    def _extract_jsonld_jobs(self, root, page_url: str) -> list[dict]:
        scripts = root.xpath("//script[contains(@type,'ld+json')]/text()")
        if not scripts:
            return []
        jobs: list[dict] = []
        for raw in scripts[:40]:
            raw = (raw or "").strip()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                continue
            for obj in self._walk_json(payload):
                if not isinstance(obj, dict):
                    continue
                type_val = str(obj.get("@type") or "").lower()
                if "jobposting" not in type_val:
                    continue
                title = self._clean_title(str(obj.get("title") or ""))
                source_url = _resolve_url(str(obj.get("url") or obj.get("sameAs") or "").strip(), page_url)
                if not self._is_valid_title(title) or not self._is_probable_job_url(source_url, page_url):
                    continue
                jobs.append(
                    self._job(
                        title,
                        source_url,
                        self._extract_jsonld_location(obj),
                        self._clean_text(str(obj.get("baseSalary") or "")) or None,
                        self._clean_text(str(obj.get("employmentType") or "")) or None,
                        "v10_local_jsonld",
                        0.9,
                        self._clean_text(str(obj.get("description") or ""))[:2000] or None,
                    )
                )
        return jobs

    def _extract_jsonld_location(self, obj: dict[str, Any]) -> str | None:
        value = obj.get("jobLocation")
        if isinstance(value, dict):
            address = value.get("address")
            if isinstance(address, dict):
                parts = [
                    str(address.get("addressLocality") or "").strip(),
                    str(address.get("addressRegion") or "").strip(),
                    str(address.get("addressCountry") or "").strip(),
                ]
                text = ", ".join(p for p in parts if p)
                return text or None
            return self._clean_text(str(value.get("name") or "")) or None
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    maybe = self._extract_jsonld_location({"jobLocation": item})
                    if maybe:
                        return maybe
        if isinstance(value, str):
            return self._clean_text(value) or None
        return None

    def _walk_json(self, value: Any):
        if isinstance(value, dict):
            yield value
            for child in value.values():
                yield from self._walk_json(child)
            return
        if isinstance(value, list):
            for item in value:
                yield from self._walk_json(item)

    def _job(
        self,
        title: str,
        source_url: str | None,
        location_raw: str | None,
        salary_raw: str | None,
        employment_type: str | None,
        method: str,
        confidence: float,
        description: str | None = None,
    ) -> dict[str, Any]:
        return {
            "title": title,
            "source_url": source_url or "",
            "location_raw": location_raw or None,
            "salary_raw": salary_raw or None,
            "employment_type": employment_type or None,
            "description": description or None,
            "extraction_method": method,
            "extraction_confidence": confidence,
        }

    def _dedupe_jobs(self, jobs: list[dict]) -> list[dict]:
        deduped: dict[tuple[str, str], dict] = {}
        for job in jobs:
            title = self._clean_title(str(job.get("title") or ""))
            source_url = str(job.get("source_url") or "").strip()
            if not self._is_valid_title(title):
                continue
            if not source_url:
                continue
            canonical_url = self._canonicalize_source_url(source_url)
            key = (canonical_url.lower(), title.lower())
            existing = deduped.get(key)
            if existing is None:
                deduped[key] = {**job, "title": title, "source_url": source_url}
                continue
            if self._job_score(job) > self._job_score(existing):
                deduped[key] = {**job, "title": title, "source_url": source_url}

        # Generic shared apply pages often repeat many marketing/duplicate cards.
        # Keep a small bounded set for such URLs to reduce type-1 inflation.
        filtered: list[dict] = []
        generic_counts: dict[str, int] = {}
        for job in deduped.values():
            source_url = str(job.get("source_url") or "").strip()
            if self._is_generic_apply_url(source_url):
                key = source_url.lower()
                generic_counts[key] = generic_counts.get(key, 0)
                if generic_counts[key] >= 3:
                    continue
                generic_counts[key] += 1
            filtered.append(job)
        return filtered[:MAX_JOBS_PER_PAGE]

    def _job_score(self, job: dict) -> int:
        score = 0
        if job.get("location_raw"):
            score += 2
        if job.get("description"):
            score += 2
        if job.get("employment_type"):
            score += 1
        return score

    def _is_apply_context_url(self, source_url: str | None, link_text: str, page_url: str) -> bool:
        if not source_url:
            return False
        parsed = urlparse(source_url)
        if not parsed.scheme or not parsed.netloc:
            return False
        page_host = (urlparse(page_url).netloc or "").lower()
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        # Keep same-host generic apply pages when card context has a real role heading.
        if page_host and host == page_host and path in {"/career-job", "/career-job/", "/apply", "/apply/"}:
            return True
        # External direct-apply pages from established boards.
        if "indeed." in host and "/job/" in path:
            return True
        if _APPLY_CTA_RE.search(link_text or "") and page_host and host == page_host:
            return True
        return False

    def _is_probable_blog_job_url(self, source_url: str | None, page_url: str) -> bool:
        if not source_url:
            return False
        if self._is_probable_job_url(source_url, page_url):
            return True

        parsed = urlparse(source_url)
        page_host = (urlparse(page_url).netloc or "").lower()
        host = (parsed.netloc or "").lower()
        if not parsed.scheme or not parsed.netloc:
            return False
        if page_host and host != page_host:
            return False

        path = (parsed.path or "").lower()
        if not path or path == "/":
            return False
        if re.search(r"/page/\d+/?$", path):
            return False
        if path.startswith(("/category/", "/tag/", "/author/", "/feed", "/wp-", "/news/")):
            return False
        if _LISTING_PATH_RE.search(path):
            return False
        if len(path.strip("/")) < 8:
            return False
        if "-" not in path:
            return False
        return bool(_ROLE_WORD_RE.search(path))

    def _is_generic_apply_url(self, source_url: str) -> bool:
        parsed = urlparse(source_url)
        path = (parsed.path or "").strip()
        if not path:
            return False
        if parsed.query and _DETAIL_QUERY_ID_RE.search(parsed.query):
            return False
        return bool(_GENERIC_APPLY_PATH_RE.match(path))

    def _is_probable_job_url(self, source_url: str | None, page_url: str) -> bool:
        if not source_url:
            return False
        if source_url.startswith(("mailto:", "tel:")):
            return False
        parsed = urlparse(source_url)
        if not parsed.scheme or not parsed.netloc:
            return False
        path = (parsed.path or "").lower()
        query = (parsed.query or "").lower()
        if _LISTING_PATH_RE.search(path):
            if _DETAIL_QUERY_ID_RE.search(query):
                return True
            return False
        if _JOB_DETAIL_URL_RE.search(source_url):
            return True
        # Keep multilingual slugs (including percent-encoded paths) on common detail routes.
        if re.match(r"^/(?:job|jobs|career|careers)/[^?#]{3,}$", path) and not re.match(
            r"^/(?:job|jobs|career|careers)/(?:list|search|index|category|categories)(?:/|$)",
            path,
        ):
            return True
        # Common card/listing pages use /career?id=<numeric-id> detail links.
        if path.startswith(("/career", "/job", "/jobs", "/vacancy", "/vacancies", "/opening")) and _DETAIL_QUERY_ID_RE.search(query):
            return True
        if source_url.rstrip("/") == (page_url or "").rstrip("/"):
            return False
        return False

    def _extract_inline_location(self, text: str, title: str) -> str | None:
        if not text:
            return None
        match = _INLINE_LOCATION_RE.search(text)
        if match:
            location = re.split(
                r"\b(?:position|job\s*type|employment\s*type|please|send your resume)\b",
                match.group(1),
                maxsplit=1,
                flags=re.IGNORECASE,
            )[0]
            location = self._clean_text(location)
            if location and location.lower() != title.lower():
                return location[:140]
        title_loc = re.search(r"\s+[–-]\s+([A-Za-z][A-Za-z ,.&'-]{1,60})$", title)
        if title_loc:
            return self._clean_text(title_loc.group(1))[:140] or None
        return None

    def _extract_inline_job_type(self, text: str) -> str | None:
        if not text:
            return None
        match = _INLINE_JOB_TYPE_RE.search(text)
        if not match:
            return None
        value = self._clean_text(match.group(1))
        return value[:100] if value else None

    def _same_page_job_url(self, page_url: str, title: str, idx: int) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", self._clean_text(unescape(title)).lower()).strip("-")
        if not slug:
            slug = f"job-{idx}"
        parsed = urlparse(page_url)
        base = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ""))
        return f"{base}#job-{slug[:64]}-{idx}"

    def _is_valid_title(self, title: str) -> bool:
        title = self._clean_title(title)
        if not title or len(title) < 3 or len(title) > 180:
            return False
        if "%" in title and " " not in title:
            return False
        normalized = re.sub(r"[!?.:;,]+$", "", title).strip()
        if _NON_JOB_TITLE_RE.match(normalized):
            return False
        if _GENERIC_CTA_RE.match(normalized):
            return False
        if not any(ch.isalpha() for ch in title):
            return False
        if re.fullmatch(r"[\W_]+", title):
            return False
        return True

    def _clean_title(self, value: str) -> str:
        value = self._clean_text(value)
        # Remove trailing card badges often glued to titles in ATS/board rows.
        value = re.sub(r"\s*(?:new|hot|urgent)\s*$", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+", " ", value).strip(" -|:!?.;\t\r\n")
        return value

    def _clean_text(self, value: str) -> str:
        if not value:
            return ""
        value = re.sub(r"\s+", " ", value).strip()
        return value

    def _extract_location_text(self, value: Any) -> str | None:
        if isinstance(value, str):
            return self._clean_text(value) or None
        if isinstance(value, dict):
            for key in ("name", "displayName", "label"):
                text = self._clean_text(str(value.get(key) or ""))
                if text:
                    return text
            parts = [
                self._clean_text(str(value.get("city") or "")),
                self._clean_text(str(value.get("region") or "")),
                self._clean_text(str(value.get("country") or "")),
            ]
            joined = ", ".join(part for part in parts if part)
            if joined:
                return joined
            nested = value.get("address")
            if nested is not None:
                return self._extract_location_text(nested)
        if isinstance(value, list):
            for item in value:
                text = self._extract_location_text(item)
                if text:
                    return text
        return None

    def _extract_anchor_location(self, anchor) -> str | None:
        location_nodes = anchor.xpath(
            ".//*[contains(@class,'body__secondary')][1]"
            "|.//*[contains(@class,'body--metadata')][1]"
            "|.//*[contains(@class,'job-location')][1]"
            "|.//*[contains(@class,'location')][1]"
            "|.//*[@id='location-text'][1]"
            "|.//*[contains(@id,'location-text')][1]"
        )
        if location_nodes:
            location = self._clean_text(_text(location_nodes[0]))
            if location:
                return location

        candidate_rows = []
        for ancestor in anchor.iterancestors():
            if not isinstance(ancestor.tag, str):
                continue
            tag = ancestor.tag.lower()
            classes = (ancestor.get("class") or "").lower()
            if tag in {"li", "tr", "article"} or "job" in classes or "position" in classes:
                candidate_rows.append(ancestor)
            if len(candidate_rows) >= 8:
                break
        if not candidate_rows:
            return None

        row = None
        location_nodes = []
        for candidate in candidate_rows:
            location_nodes = candidate.xpath(
                ".//p[contains(@class,'location')][1]"
                "|.//*[contains(@class,'job-location')][1]"
                "|.//*[contains(@class,'location')][1]"
                "|.//*[contains(@class,'body__secondary')][1]"
                "|.//*[contains(@class,'body--metadata')][1]"
                "|.//*[@data-automation-id='locations']//dd[1]"
                "|.//*[@id='location-text'][1]"
                "|.//*[contains(@id,'location-text')][1]"
            )
            if location_nodes:
                row = candidate
                break
        if row is None:
            row = candidate_rows[0]
            location_nodes = row.xpath(
                ".//p[contains(@class,'location')][1]"
                "|.//*[contains(@class,'job-location')][1]"
                "|.//*[contains(@class,'location')][1]"
                "|.//*[contains(@class,'body__secondary')][1]"
                "|.//*[contains(@class,'body--metadata')][1]"
                "|.//*[@data-automation-id='locations']//dd[1]"
                "|.//*[@id='location-text'][1]"
                "|.//*[contains(@id,'location-text')][1]"
            )
        if not location_nodes:
            return None
        location = self._clean_text(_text(location_nodes[0]))
        return location or None

    def _canonicalize_source_url(self, source_url: str) -> str:
        parsed = urlparse(source_url)
        if not parsed.scheme or not parsed.netloc:
            return source_url.strip()
        query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        kept_pairs = []
        for key, value in query_pairs:
            key_l = key.lower()
            if key_l.startswith("utm_"):
                continue
            if key_l in {"gh_src", "gh_jid"}:
                continue
            kept_pairs.append((key, value))
        canonical_query = urlencode(kept_pairs, doseq=True)
        return urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                canonical_query,
                "",
            )
        )

    def _is_blocked_page(self, html: str) -> bool:
        sample = (html or "")[:12000]
        return bool(_BLOCKED_PAGE_RE.search(sample))
