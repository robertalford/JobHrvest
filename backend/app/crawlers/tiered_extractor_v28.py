"""
Tiered Extraction Engine v2.8 — direct from v1.6 with simplified high-impact recovery.

High-impact changes:
1. Keep v1.6 as the primary path and only override when fallback quality is clearly better.
2. Add stronger JS-render handling (cookie dismissal, search/accordion/load-more interactions, longer wait).
3. Add Next.js `_next/data` probing for dynamic `[slug]` boards that render empty HTML shells.
4. Expand MartianLogic/MyRecruitmentPlus endpoint probing and recruiter/client variants.
5. Add listing-CTA follow-up fetching (`Current Jobs`, `Search Jobs`, `Browse Jobs`) when landing pages are non-listing hubs.
6. Improve detail-page enrichment quality and reject generic listing-label titles to reduce false positives.
"""

from __future__ import annotations

import asyncio
import html
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
    _parse_html,
    _text,
    _resolve_url,
    _get_el_classes,
    _is_valid_title,
    _AU_LOCATIONS,
    _JOB_TYPE_PATTERN,
    _SALARY_PATTERN,
    MAX_JOBS_PER_PAGE,
    MIN_JOBS_FOR_SUCCESS,
)

logger = logging.getLogger(__name__)


_TITLE_HINT_PATTERN_V28 = re.compile(
    r"\b(?:job|jobs|career|careers|vacanc(?:y|ies)|opening|openings|"
    r"position|positions|role|roles|internship|manager|engineer|developer|"
    r"officer|specialist|assistant|analyst|consultant|coordinator|executive|"
    r"technician|designer|administrator|accountant|recruit(?:er|ment)?|"
    r"director|chef|nurse|teacher|operator|supervisor|"
    r"influencer|akuntan|fotografer|videografer|psikolog(?:i)?|"
    r"konsultan|asisten|staf|staff|karyawan|pegawai|"
    r"gerente|director|vacantes?|empleo|trabajo|lowongan|karir|karier|"
    r"kerjaya|jawatan)\b",
    re.IGNORECASE,
)

_REJECT_TITLE_PATTERN_V28 = re.compile(
    r"^(?:"
    r"my\s+applications?|my\s+forms?|my\s+emails?|my\s+tests?|my\s+interviews?|"
    r"job\s+alerts?|jobs?\s+list|job\s+search|saved\s+jobs?|manage\s+applications?|"
    r"start\s+new\s+application|access\s+existing\s+application|preview\s+application\s+form|"
    r"apply\s+now|apply\s+here|read\s+more|learn\s+more|show\s+more|"
    r"entries\s+feed|comments\s+feed|rss|feed|"
    r"about\s+us|contact\s+us|privacy|terms|help|login|register|"
    r"job\s+name|closing\s+date|posted\s+date|job\s+ref|"
    r"benefits|how\s+to\s+apply|current\s+opportunities|join\s+us(?:\s+and.*)?|"
    r"how\s+.*serves\s+(?:job\s+seekers?|employers?)|"
    r".*recruitment\s+agency|"
    r"vacantes|vacantes\s+inicio|alertas?\s+de\s+vacantes?|bolsa\s+de\s+trabajo|"
    r"asesorado\s+por|"
    r"alamat(?:\s+kantor)?|"
    r"model\s+incubator|"
    r"puesto\s+ciudad\s+beneficios|"
    r"internship\s+details|job\s+type|job\s+card\s+style|job\s+title|"
    r"no\s+jobs?\s+found(?:\s+text)?|jobs?\s+vacancy|"
    r"lowongan\s+kerja(?:\s+\w+)?"
    r")$",
    re.IGNORECASE,
)

_GENERIC_LISTING_LABEL_PATTERN_V28 = re.compile(
    r"^(?:jobs?|careers?|vacancies|vacantes?|job\s+openings?|open\s+positions?|"
    r"bolsa\s+de\s+trabajo|alertas?\s+de\s+vacantes?|join\s+our\s+team|"
    r"open\s+roles?|all\s+jobs?|current\s+vacancies|current\s+jobs?|"
    r"search\s+jobs?|browse\s+(?:jobs?|opportunities)|latest\s+jobs?|"
    r"view\s+(?:all\s+)?jobs?|jobs?\s+vacancy|"
    r"lowongan(?:\s+kerja(?:\s+\w+)?)?)$",
    re.IGNORECASE,
)

_JOB_URL_HINT_PATTERN_V28 = re.compile(
    r"(?:/job|/jobs|/career|/careers|/position|/positions|/vacanc|"
    r"/opening|/openings|/requisition|/requisitions|"
    r"event=jobs\.|jobid=|portal\.na|candidateportal|/apply|/lowongan|/karir|/kerjaya|"
    r"/p/[a-z0-9_-]+|applicationform|job-application-form|embed-jobs)",
    re.IGNORECASE,
)

_JOB_DETAILISH_URL_PATTERN_V28 = re.compile(
    r"(?:/jobs?/[a-z0-9][^/?#]{3,}|/requisition[s]?/[a-z0-9][^/?#]{2,}|"
    r"/p/[a-z0-9_-]{6,}|[?&](?:jobid|job_id|requisitionid|positionid)=)",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V28 = re.compile(
    r"(?:/privacy|/terms|/cookie|/news|/blog|/about|/contact|/investor|"
    r"/help|/login|/logout|/register|/account|/feed(?:/|$)|/rss(?:/|$)|"
    r"event=jobs\.view(?:history|myforms|myemails|mytests|myinterviews)|"
    r"event=help\.|event=reg\.|/comments/feed(?:/|$)|/wp-login(?:\.php)?|"
    r"/wp-json/oembed)",
    re.IGNORECASE,
)

_NON_JOB_SECTION_URL_PATTERN_V28 = re.compile(
    r"/(?:sectors?|insights?|resources?|service(?:s)?|team|culture)(?:/|$)",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN_V28 = re.compile(
    r"job|position|vacanc|opening|requisition|career|posting|listing|accordion",
    re.IGNORECASE,
)

_SCRIPT_ASSIGNMENT_PATTERN_V28 = re.compile(
    r"(?:window\.[A-Za-z0-9_$.]+\s*=\s*|var\s+[A-Za-z0-9_$]+\s*=\s*)(\{.*\}|\[.*\])\s*;?",
    re.DOTALL,
)

_APPLY_CONTEXT_PATTERN_V28 = re.compile(
    r"(?:apply|application|mailto:|job\s+description|closing\s+date|salary|"
    r"full\s*time|part\s*time|contract|permanent|temporary|remote|hybrid|"
    r"info\s+lengkap|more\s+details?)",
    re.IGNORECASE,
)

_CATEGORY_TITLE_PATTERN_V28 = re.compile(
    r"^(?:all\s+jobs?|jobs?\s+by|browse\s+jobs?|view\s+jobs?|department[s]?|locations?)$",
    re.IGNORECASE,
)

_CORPORATE_TITLE_PATTERN_V28 = re.compile(
    r"^(?:home|about|contact|company|our\s+company|our\s+culture|our\s+values|blog|news|events?)$",
    re.IGNORECASE,
)

_PHONE_TITLE_PATTERN_V28 = re.compile(r"^(?:\+?\d[\d\s().-]{6,}|\d{2,4}\s?\d{3,5}\s?\d{3,5})$")

_COMPANY_CAREER_LABEL_PATTERN_V28 = re.compile(
    r"^[a-z0-9&.,'() -]{2,60}\s+careers?$",
    re.IGNORECASE,
)

_MARTIAN_CLIENT_PATTERN_V28 = re.compile(r"/([a-z0-9-]{3,})/?$", re.IGNORECASE)
_MARTIAN_ENDPOINT_URL_PATTERN_V28 = re.compile(
    r"https?://[^\"'\\s]+(?:embed-jobs|job-ads|jobads|api/jobs|api/jobads|api/job-ads|"
    r"pageNumber=\d+[^\"'\\s]*)",
    re.IGNORECASE,
)

_LISTING_CTA_TEXT_PATTERN_V28 = re.compile(
    r"(?:current\s+jobs?|search\s+jobs?|browse\s+(?:jobs?|latest\s+jobs?)|"
    r"view\s+all\s+jobs?|open\s+positions?|vacancies|lowongan|karir|karier|"
    r"see\s+all\s+jobs?|job\s+openings?)",
    re.IGNORECASE,
)

_LISTING_CTA_HREF_PATTERN_V28 = re.compile(
    r"/(?:jobs?|careers?/jobs?|careers?|vacanc|job-openings|openings|"
    r"lowongan|karir|karier|requisition|positions?)",
    re.IGNORECASE,
)

_LOW_SIGNAL_DESC_PATTERN_V28 = re.compile(
    r"^(?:info\s+lengkap|read\s+more|learn\s+more|apply(?:\s+now)?|"
    r"view\s+details?|job\s+details?)$",
    re.IGNORECASE,
)

_ORACLE_SITE_PATTERN_V28 = re.compile(r"/sites/([A-Za-z0-9_]+)/", re.IGNORECASE)
_ORACLE_SITE_NUMBER_PATTERN_V28 = re.compile(r"siteNumber\s*[:=]\s*['\"]([A-Za-z0-9_]+)", re.IGNORECASE)
_ORACLE_API_BASE_PATTERN_V28 = re.compile(r"apiBaseUrl\s*:\s*['\"](https?://[^'\"\\s]+)", re.IGNORECASE)


class TieredExtractorV28(TieredExtractorV16):
    """v2.8 extractor: v1.6-first with guarded structured/link/accordion fallbacks."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(
                super().extract(career_page, company, working_html),
                timeout=24.0,
            )
        except asyncio.TimeoutError:
            logger.warning("v2.8 parent extractor timeout for %s", url)
        except Exception:
            logger.exception("v2.8 parent extractor failed for %s", url)
        parent_jobs = self._dedupe_jobs_v28(parent_jobs or [], url)

        root = _parse_html(working_html)
        candidates: list[tuple[str, list[dict]]] = []
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        structured_jobs = self._extract_structured_jobs_v28(working_html, url)
        if structured_jobs:
            candidates.append(("structured_v28", structured_jobs))

        script_jobs = self._extract_jobs_from_state_scripts_v28(working_html, url)
        if script_jobs:
            candidates.append(("state_script_v28", script_jobs))

        next_data_jobs = await self._extract_next_data_api_jobs_v28(url, working_html)
        if next_data_jobs:
            candidates.append(("next_data_api_v28", next_data_jobs))

        martian_jobs = await self._extract_martianlogic_jobs_v28(url, working_html)
        if martian_jobs:
            candidates.append(("martianlogic_v28", martian_jobs))

        oracle_api_jobs = await self._extract_oracle_api_jobs_v28(url, working_html)
        if oracle_api_jobs:
            candidates.append(("oracle_api_v28", oracle_api_jobs))

        if root is not None:
            elementor_jobs = self._extract_from_elementor_cards_v28(root, url)
            if elementor_jobs:
                candidates.append(("elementor_cards_v28", elementor_jobs))

            link_jobs = self._extract_from_job_links_v28(root, url)
            if link_jobs:
                candidates.append(("job_links_v28", link_jobs))

            accordion_jobs = self._extract_from_accordion_sections_v28(root, url)
            if accordion_jobs:
                candidates.append(("accordion_v28", accordion_jobs))

            heading_jobs = self._extract_from_heading_rows_v28(root, url)
            if heading_jobs:
                candidates.append(("heading_rows_v28", heading_jobs))

            row_jobs = self._extract_from_repeating_rows_v28(root, url)
            if row_jobs:
                candidates.append(("repeating_rows_v28", row_jobs))

        best_label, best_jobs = self._pick_best_jobset_v28(candidates, url)
        if root is not None and (
            not best_jobs or self._looks_like_listing_label_set_v28(best_jobs)
        ):
            cta_jobs = await self._follow_listing_cta_pages_v28(url, root)
            if cta_jobs and (
                not best_jobs
                or self._jobset_score_v28(cta_jobs, url) > self._jobset_score_v28(best_jobs, url) + 2.0
            ):
                best_label = "listing_cta_follow_v28"
                best_jobs = cta_jobs

        if not best_jobs:
            return []

        if (
            best_label != "parent_v16"
            and len(best_jobs) >= MIN_JOBS_FOR_SUCCESS
            and any(self._is_job_like_url_v28(j.get("source_url") or "") for j in best_jobs)
        ):
            try:
                best_jobs = await asyncio.wait_for(
                    self._enrich_from_detail_pages(best_jobs),
                    timeout=18.0,
                )
            except asyncio.TimeoutError:
                logger.warning("v2.8 enrichment timeout for %s", url)
            except Exception:
                logger.exception("v2.8 enrichment failed for %s", url)
            best_jobs = self._dedupe_jobs_v28(best_jobs, url)
        elif any(self._needs_detail_refresh_v28(job) for job in best_jobs[:12]):
            # Parent output can still be shallow on CTA/grid pages; enrich selectively.
            try:
                best_jobs = await asyncio.wait_for(
                    self._enrich_from_detail_pages(best_jobs),
                    timeout=16.0,
                )
            except Exception:
                logger.debug("v2.8 selective enrichment skipped for %s", url)
            best_jobs = self._dedupe_jobs_v28(best_jobs, url)

        return best_jobs[:MAX_JOBS_PER_PAGE]

    # ------------------------------------------------------------------
    # Structured / state-script fallbacks
    # ------------------------------------------------------------------

    def _extract_structured_jobs_v28(self, html: str, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        for match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html,
            re.IGNORECASE | re.DOTALL,
        ):
            raw = (match.group(1) or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            jobs.extend(self._extract_jobs_from_json_obj_v28(data, page_url, "tier0_jsonld_v28"))

        return self._dedupe_jobs_v28(jobs, page_url)

    def _extract_jobs_from_state_scripts_v28(self, html: str, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        script_payloads: list[str] = []

        next_data_match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html,
            re.IGNORECASE | re.DOTALL,
        )
        if next_data_match:
            script_payloads.append(next_data_match.group(1))

        for match in re.finditer(r"<script[^>]*>(.*?)</script>", html, re.IGNORECASE | re.DOTALL):
            body = (match.group(1) or "").strip()
            if len(body) < 40:
                continue
            lowered = body.lower()
            if (
                "__NEXT_DATA__" in body
                or "dehydratedstate" in lowered
                or "job" in lowered
                or "requisition" in lowered
                or "applicationformurl" in lowered
                or "jobpostsdata" in lowered
            ):
                script_payloads.append(body)

        for payload in script_payloads[:40]:
            for parsed in self._parse_json_blobs_v28(payload):
                jobs.extend(self._extract_jobs_from_json_obj_v28(parsed, page_url, "tier0_state_v28"))

        return self._dedupe_jobs_v28(jobs, page_url)

    def _parse_json_blobs_v28(self, script_body: str) -> list[object]:
        results: list[object] = []
        body = (script_body or "").strip()
        if not body:
            return results

        # Direct JSON blob.
        if body.startswith("{") or body.startswith("["):
            try:
                results.append(json.loads(body))
            except Exception:
                pass

        # JS assignment wrappers.
        for m in _SCRIPT_ASSIGNMENT_PATTERN_V28.finditer(body):
            raw = (m.group(1) or "").strip()
            if len(raw) < 2:
                continue
            try:
                results.append(json.loads(raw))
            except Exception:
                continue

        return results

    async def _extract_next_data_api_jobs_v28(self, page_url: str, html_body: str) -> list[dict]:
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html_body or "",
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return []

        try:
            next_data = json.loads(match.group(1))
        except Exception:
            return []

        if not isinstance(next_data, dict):
            return []
        build_id = str(next_data.get("buildId") or "").strip()
        if not build_id:
            return []

        endpoints = self._next_data_candidate_urls_v28(page_url, next_data)
        if not endpoints:
            return []

        jobs: list[dict] = []
        try:
            async with httpx.AsyncClient(
                timeout=4,
                follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,*/*"},
            ) as client:
                for endpoint in endpoints[:6]:
                    try:
                        resp = await client.get(endpoint)
                    except Exception:
                        continue
                    if resp.status_code != 200:
                        continue

                    text = (resp.text or "").strip()
                    if not text.startswith("{") and not text.startswith("["):
                        continue
                    try:
                        payload = resp.json()
                    except Exception:
                        continue

                    jobs.extend(
                        self._extract_jobs_from_json_obj_v28(payload, str(resp.url), "tier0_next_data_api_v28")
                    )
                    if isinstance(payload, dict):
                        props = payload.get("pageProps")
                        if isinstance(props, dict):
                            jobs.extend(
                                self._extract_jobs_from_json_obj_v28(
                                    props, str(resp.url), "tier0_next_data_api_v28"
                                )
                            )
                    jobs = self._dedupe_jobs_v28(jobs, page_url)
                    if len(jobs) >= MAX_JOBS_PER_PAGE:
                        break
        except Exception:
            logger.debug("v2.8 next-data probing failed for %s", page_url)

        return self._dedupe_jobs_v28(jobs, page_url)

    def _next_data_candidate_urls_v28(self, page_url: str, next_data: dict) -> list[str]:
        build_id = str(next_data.get("buildId") or "").strip()
        if not build_id:
            return []

        parsed = urlparse(page_url)
        base = f"{parsed.scheme or 'https'}://{parsed.netloc}"
        path = parsed.path or "/"
        norm_path = "/" + "/".join([p for p in path.split("/") if p])
        if not norm_path:
            norm_path = "/"

        query_obj = next_data.get("query") if isinstance(next_data.get("query"), dict) else {}
        query_pairs = dict(parse_qsl(parsed.query))
        query_pairs.update({str(k): str(v) for k, v in query_obj.items() if isinstance(v, (str, int, float))})
        query = urlencode(query_pairs)

        candidates: list[str] = []
        if norm_path == "/":
            candidates.append(f"{base}/_next/data/{build_id}/index.json")
        else:
            p = norm_path.rstrip("/")
            candidates.append(f"{base}/_next/data/{build_id}{p}.json")
            candidates.append(f"{base}/_next/data/{build_id}{p}/index.json")

        dynamic_key_candidates = [
            str(query_pairs.get("client") or "").strip(),
            str(query_pairs.get("slug") or "").strip(),
            str(query_pairs.get("tenant") or "").strip(),
            str(query_pairs.get("company") or "").strip(),
        ]
        dynamic_key_candidates.extend([seg for seg in norm_path.split("/") if seg and not seg.startswith("[")])

        for key in dynamic_key_candidates:
            if not key:
                continue
            candidates.append(f"{base}/_next/data/{build_id}/{key}.json")
            candidates.append(f"{base}/_next/data/{build_id}/{key}/index.json")

        page_value = str(next_data.get("page") or "").strip()
        if page_value:
            page_path = page_value.replace("[", "").replace("]", "").strip("/")
            if page_path:
                candidates.append(f"{base}/_next/data/{build_id}/{page_path}.json")
                candidates.append(f"{base}/_next/data/{build_id}/{page_path}/index.json")

        if query:
            candidates = [f"{u}?{query}" for u in candidates]

        seen: set[str] = set()
        unique: list[str] = []
        for url in candidates:
            norm = url.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
            unique.append(url)
        return unique

    def _extract_jobs_from_json_obj_v28(
        self,
        data: object,
        page_url: str,
        method: str,
    ) -> list[dict]:
        jobs: list[dict] = []
        queue = [data]
        visited = 0

        while queue and visited < 5000:
            node = queue.pop(0)
            visited += 1
            if isinstance(node, list):
                queue.extend(node[:200])
                continue
            if not isinstance(node, dict):
                continue

            queue.extend(list(node.values())[:200])
            job = self._job_from_json_dict_v28(node, page_url, method)
            if job:
                jobs.append(job)
                if len(jobs) >= MAX_JOBS_PER_PAGE:
                    break

        return jobs

    def _job_from_json_dict_v28(self, node: dict, page_url: str, method: str) -> Optional[dict]:
        title = ""
        title_key = ""
        for key in ("title", "jobTitle", "positionTitle", "requisitionTitle", "name", "jobName"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                title = value.strip()
                title_key = key
                break

        title = self._normalize_title_v28(title)
        if not self._is_valid_title_v28(title):
            return None

        url_raw = None
        for key in (
            "url", "jobUrl", "jobURL", "applyUrl", "jobPostingUrl", "jobDetailUrl",
            "detailsUrl", "externalUrl", "canonicalUrl", "sourceUrl",
            "applicationFormUrl", "applicationUrl", "postingUrl", "jobLink",
        ):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                url_raw = value.strip()
                break

        source_url = _resolve_url(url_raw, page_url) if isinstance(url_raw, str) else None
        if not source_url:
            source_url = page_url

        key_names = " ".join(str(k) for k in node.keys()).lower()
        jobposting_type = str(node.get("@type") or "").strip().lower() == "jobposting"
        strong_id_hint = any(
            k in node
            for k in (
                "jobId",
                "jobID",
                "jobPostingId",
                "requisitionId",
                "positionId",
                "jobAdId",
                "applicationFormUrl",
                "applicationUrl",
            )
        )
        job_key_hint = bool(
            re.search(r"job|position|posting|requisition|vacanc|opening", key_names)
            or any(
                k in node
                for k in (
                    "jobId",
                    "jobID",
                    "jobPostingId",
                    "requisitionId",
                    "positionId",
                    "jobAdId",
                    "applicationFormUrl",
                    "employmentType",
                    "publishDateTime",
                )
            )
        )
        title_hint = self._title_has_job_signal_v28(title)
        url_hint = self._is_job_like_url_v28(source_url)
        key_set = {str(k) for k in node.keys()}
        looks_label_object = key_set.issubset({"id", "name", "label", "value", "path", "children", "parent"})
        taxonomy_hint = bool(re.search(r"department|office|filter|facet|category|taxonomy", key_names))
        if self._is_non_job_url_v28(source_url):
            if not (title_hint and (job_key_hint or strong_id_hint)):
                return None
            source_url = page_url

        if looks_label_object and not job_key_hint:
            return None
        if taxonomy_hint and not (job_key_hint or jobposting_type):
            return None
        if source_url == page_url and not (strong_id_hint or jobposting_type):
            return None
        if _COMPANY_CAREER_LABEL_PATTERN_V28.match(title):
            return None
        if title_key == "name" and not (strong_id_hint or url_hint or jobposting_type):
            return None
        if not (title_hint or strong_id_hint or jobposting_type):
            return None
        if not (url_hint or strong_id_hint or jobposting_type):
            return None

        location = None
        for key in ("location", "jobLocation", "city", "workLocation", "region", "addressLocality"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                location = value.strip()[:200]
                break
            if isinstance(value, dict):
                pieces = [
                    str(value.get("addressLocality") or "").strip(),
                    str(value.get("addressRegion") or "").strip(),
                    str(value.get("addressCountry") or "").strip(),
                ]
                joined = ", ".join(p for p in pieces if p)
                if joined:
                    location = joined[:200]
                    break
            if isinstance(value, list):
                location_parts: list[str] = []
                for entry in value[:5]:
                    if isinstance(entry, str) and entry.strip():
                        location_parts.append(entry.strip())
                    elif isinstance(entry, dict):
                        locality = str(entry.get("addressLocality") or entry.get("city") or "").strip()
                        region = str(entry.get("addressRegion") or entry.get("state") or "").strip()
                        country = str(entry.get("addressCountry") or "").strip()
                        joined = ", ".join(p for p in (locality, region, country) if p)
                        if joined:
                            location_parts.append(joined)
                if location_parts:
                    location = " | ".join(dict.fromkeys(location_parts))[:200]
                    break

        salary = None
        for key in ("salary", "compensation", "baseSalary", "payRate"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                salary = value.strip()[:200]
                break
            if isinstance(value, dict):
                raw = json.dumps(value, ensure_ascii=False)
                sal_match = _SALARY_PATTERN.search(raw)
                if sal_match:
                    salary = sal_match.group(0).strip()
                    break

        emp_type = None
        for key in ("employmentType", "jobType", "workType"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                emp_type = value.strip()[:120]
                break
            if isinstance(value, list):
                joined = ", ".join(str(v).strip() for v in value if str(v).strip())
                if joined:
                    emp_type = joined[:120]
                    break

        desc = None
        for key in ("description", "summary", "introduction", "previewText", "trait", "content"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                text = value.strip()
                if "<" in text and ">" in text:
                    parsed = _parse_html(text)
                    if parsed is not None:
                        text = _text(parsed)
                desc = text[:5000] if text else None
                break

        return {
            "title": title,
            "source_url": source_url,
            "location_raw": location,
            "salary_raw": salary,
            "employment_type": emp_type,
            "description": desc,
            "extraction_method": method,
            "extraction_confidence": 0.86,
        }

    async def _follow_listing_cta_pages_v28(self, page_url: str, root: etree._Element) -> list[dict]:
        candidates: list[str] = []
        seen: set[str] = set()

        for a_el in root.xpath("//a[@href]"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            full_url = _resolve_url(href, page_url)
            if not full_url or full_url in seen:
                continue
            if full_url.rstrip("/") == page_url.rstrip("/"):
                continue

            text_blob = self._normalize_title_v28(_text(a_el) or (a_el.get("title") or ""))
            if not text_blob:
                continue
            if _REJECT_TITLE_PATTERN_V28.match(text_blob.lower()):
                continue

            if not (
                _LISTING_CTA_TEXT_PATTERN_V28.search(text_blob)
                or _LISTING_CTA_HREF_PATTERN_V28.search(full_url)
            ):
                continue

            seen.add(full_url)
            candidates.append(full_url)

        if not candidates:
            return []

        best: list[dict] = []
        best_score = -1.0

        try:
            async with httpx.AsyncClient(
                timeout=6,
                follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "text/html,*/*"},
            ) as client:
                for candidate_url in candidates[:4]:
                    try:
                        resp = await client.get(candidate_url)
                    except Exception:
                        continue
                    if resp.status_code != 200 or len(resp.text or "") < 400:
                        continue

                    jobset = await self._extract_candidate_jobs_from_html_v28(str(resp.url), resp.text)
                    if not jobset:
                        continue
                    score = self._jobset_score_v28(jobset, str(resp.url))
                    if self._passes_jobset_validation_v28(jobset, str(resp.url)) and score > best_score:
                        best = jobset
                        best_score = score
        except Exception:
            logger.debug("v2.8 listing-cta follow failed for %s", page_url)

        return self._dedupe_jobs_v28(best, page_url)

    async def _extract_candidate_jobs_from_html_v28(self, candidate_url: str, html_body: str) -> list[dict]:
        candidate_jobs: list[dict] = []

        candidate_jobs.extend(self._extract_structured_jobs_v28(html_body, candidate_url))
        candidate_jobs.extend(self._extract_jobs_from_state_scripts_v28(html_body, candidate_url))
        candidate_jobs.extend(await self._extract_next_data_api_jobs_v28(candidate_url, html_body))

        root = _parse_html(html_body)
        if root is not None:
            tier2 = self._extract_tier2_v16(candidate_url, html_body) or []
            for job in tier2:
                cloned = dict(job)
                cloned["extraction_method"] = "tier2_heuristic_v28_cta"
                candidate_jobs.append(cloned)
            candidate_jobs.extend(self._extract_from_elementor_cards_v28(root, candidate_url))
            candidate_jobs.extend(self._extract_from_repeating_rows_v28(root, candidate_url))
            candidate_jobs.extend(self._extract_from_heading_rows_v28(root, candidate_url))
            candidate_jobs.extend(self._extract_from_job_links_v28(root, candidate_url))
            candidate_jobs.extend(self._extract_from_accordion_sections_v28(root, candidate_url))

        return self._dedupe_jobs_v28(candidate_jobs, candidate_url)

    def _looks_like_listing_label_set_v28(self, jobs: list[dict]) -> bool:
        if not jobs:
            return False
        titles = [self._normalize_title_v28(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return False
        generic_hits = sum(1 for t in titles if _GENERIC_LISTING_LABEL_PATTERN_V28.match(t))
        if generic_hits >= max(1, int(len(titles) * 0.5)):
            return True
        if len(titles) <= 2 and all(re.search(r"\bjobs?|careers?|vacanc", t, re.IGNORECASE) for t in titles):
            return True
        return False

    # ------------------------------------------------------------------
    # Link/accordion/heading fallbacks
    # ------------------------------------------------------------------

    def _extract_from_job_links_v28(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url) or page_url
            if self._is_non_job_url_v28(source_url):
                continue
            if _NON_JOB_SECTION_URL_PATTERN_V28.search(source_url) and not _JOB_DETAILISH_URL_PATTERN_V28.search(source_url):
                continue

            ancestor_classes = []
            in_structural_nav = False
            for anc in a_el.iterancestors():
                if not isinstance(anc.tag, str):
                    continue
                tag_l = anc.tag.lower()
                if tag_l in {"nav", "header", "footer"}:
                    in_structural_nav = True
                ancestor_classes.append(_get_el_classes(anc))
            ancestor_blob = " ".join(ancestor_classes).lower()
            if in_structural_nav:
                continue
            if re.search(r"\b(menu|navbar|nav-menu|breadcrumb|footer|header|sitemap)\b", ancestor_blob):
                continue

            heading_nodes = a_el.xpath(".//h1 | .//h2 | .//h3 | .//h4")
            title_raw = _text(heading_nodes[0]) if heading_nodes else (_text(a_el) or (a_el.get("title") or ""))
            text = self._normalize_title_v28(title_raw)
            if not self._is_valid_title_v28(text):
                continue
            if len(text) > 100:
                continue

            if _GENERIC_LISTING_LABEL_PATTERN_V28.match(text):
                continue

            parent = a_el.getparent()
            parent_text = _text(parent)[:1200] if parent is not None else ""
            url_hint = self._is_job_like_url_v28(source_url)
            title_hint = self._title_has_job_signal_v28(text)
            context_hint = bool(
                re.search(r"apply|location|department|job ref|posted|closing|employment|info lengkap|lowongan", parent_text, re.IGNORECASE)
            )
            structural_hint = len(parent_text) >= 45 and len(text.split()) >= 2
            if not title_hint and _JOB_DETAILISH_URL_PATTERN_V28.search(source_url):
                structural_hint = structural_hint or len(text.split()) >= 2

            if not (title_hint or (url_hint and (context_hint or structural_hint))):
                continue

            location = None
            loc_match = _AU_LOCATIONS.search(parent_text)
            if loc_match:
                location = loc_match.group(0).strip()

            emp_type = None
            type_match = _JOB_TYPE_PATTERN.search(parent_text)
            if type_match:
                emp_type = type_match.group(0).strip()

            jobs.append(
                {
                    "title": text,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": None,
                    "employment_type": emp_type,
                    "description": parent_text[:5000] if len(parent_text) > 60 else None,
                    "extraction_method": "tier2_links_v28",
                    "extraction_confidence": 0.72 if url_hint else 0.64,
                }
            )

        return self._dedupe_jobs_v28(jobs, page_url)

    def _extract_from_accordion_sections_v28(self, root: etree._Element, page_url: str) -> list[dict]:
        items = root.xpath(
            "//*[contains(@class,'accordion-item') or contains(@class,'elementor-accordion-item') or "
            "contains(@class,'accordion')]"
        )
        if not items:
            return []

        jobs: list[dict] = []
        for item in items[:200]:
            title_el = item.xpath(
                ".//*[contains(@class,'accordion-title') or contains(@class,'tab-title') or "
                "self::h1 or self::h2 or self::h3 or self::h4 or self::button]"
            )
            if not title_el:
                continue

            title = self._normalize_title_v28(_text(title_el[0]))
            if not self._is_valid_title_v28(title):
                continue

            if not self._title_has_job_signal_v28(title):
                continue

            link_el = item.xpath(".//a[@href]")
            link_href = link_el[0].get("href") if link_el else None
            source_url = _resolve_url(link_href, page_url) if link_href else page_url
            if source_url and self._is_non_job_url_v28(source_url):
                source_url = page_url

            item_text = _text(item)[:1800]
            if not _APPLY_CONTEXT_PATTERN_V28.search(item_text):
                if len(item_text) < 120:
                    continue
                if len(title.split()) <= 2:
                    continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url or page_url,
                    "location_raw": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": item_text[:5000] if len(item_text) > 80 else None,
                    "extraction_method": "tier2_accordion_v28",
                    "extraction_confidence": 0.68,
                }
            )

        return self._dedupe_jobs_v28(jobs, page_url)

    def _extract_from_heading_rows_v28(self, root: etree._Element, page_url: str) -> list[dict]:
        containers = root.xpath("//section | //div | //article | //main")
        jobs: list[dict] = []

        for container in containers[:250]:
            headings = container.xpath(".//h2 | .//h3 | .//h4")
            if len(headings) < 2:
                continue

            classes = _get_el_classes(container)
            container_text = _text(container)[:4000]
            apply_hits = len(_APPLY_CONTEXT_PATTERN_V28.findall(container_text))
            has_row_hint = bool(_ROW_CLASS_PATTERN_V28.search(classes))
            if not has_row_hint and apply_hits == 0:
                continue

            local_jobs: list[dict] = []
            for h in headings[:40]:
                title = self._normalize_title_v28(_text(h))
                if not self._is_valid_title_v28(title):
                    continue
                if not self._title_has_job_signal_v28(title):
                    continue

                link = h.xpath(".//a[@href]")
                if not link:
                    link = h.xpath("following::a[@href][1]")
                href = link[0].get("href") if link else None
                source_url = _resolve_url(href, page_url) or page_url
                if self._is_non_job_url_v28(source_url):
                    source_url = page_url

                local_jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": None,
                        "salary_raw": None,
                        "employment_type": None,
                        "description": container_text[:5000] if len(container_text) > 120 else None,
                        "extraction_method": "tier2_heading_rows_v28",
                        "extraction_confidence": 0.66,
                    }
                )

            if len(local_jobs) >= 2:
                jobs.extend(local_jobs)

        return self._dedupe_jobs_v28(jobs, page_url)

    def _extract_from_repeating_rows_v28(self, root: etree._Element, page_url: str) -> list[dict]:
        groups: dict[str, list[etree._Element]] = defaultdict(list)
        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag not in {"li", "div", "article", "tr", "section"}:
                continue
            classes = _get_el_classes(el)
            if not classes or not _ROW_CLASS_PATTERN_V28.search(classes):
                continue
            tokens = classes.lower().split()
            sig = f"{tag}:{' '.join(tokens[:2])}" if tokens else f"{tag}:_"
            groups[sig].append(el)

        jobs: list[dict] = []
        for rows in groups.values():
            if len(rows) < 3:
                continue
            for row in rows[:MAX_JOBS_PER_PAGE]:
                title_nodes = row.xpath(
                    ".//h1|.//h2|.//h3|.//h4|"
                    ".//*[contains(@class,'job-post-title')]|.//a[@href][1]"
                )
                if not title_nodes:
                    continue
                title = self._normalize_title_v28(_text(title_nodes[0]))
                if not self._is_valid_title_v28(title):
                    continue
                if not self._title_has_job_signal_v28(title):
                    continue

                link = row.xpath(".//a[@href and not(starts-with(@href,'#'))][1]")
                href = link[0].get("href") if link else None
                source_url = _resolve_url(href, page_url) or page_url
                if self._is_non_job_url_v28(source_url):
                    source_url = page_url

                row_text = _text(row)
                jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": self._extract_location_v28(row_text),
                        "salary_raw": self._extract_salary_v28(row_text),
                        "employment_type": self._extract_type_v28(row_text),
                        "description": row_text[:5000] if len(row_text) > 70 else None,
                        "extraction_method": "tier2_repeating_rows_v28",
                        "extraction_confidence": 0.72,
                    }
                )

        return self._dedupe_jobs_v28(jobs, page_url)

    def _extract_from_elementor_cards_v28(self, root: etree._Element, page_url: str) -> list[dict]:
        page_text = _text(root)[:8000]
        if not re.search(r"\b(lowongan|karir|careers?|vacanc|open\s+positions?|join\s+our\s+team)\b", page_text, re.IGNORECASE):
            return []

        card_nodes = root.xpath(
            "//*[contains(@class,'elementor-column') and contains(@class,'elementor-inner-column') "
            "and .//h2[contains(@class,'elementor-heading-title')] and .//a[contains(@class,'elementor-button') and @href]]"
        )
        if len(card_nodes) < 2:
            return []

        jobs: list[dict] = []
        for card in card_nodes[: MAX_JOBS_PER_PAGE * 2]:
            heading_nodes = card.xpath(".//h2[contains(@class,'elementor-heading-title')][1]")
            if not heading_nodes:
                continue
            title = self._normalize_title_v28(_text(heading_nodes[0]))
            if not self._is_valid_title_v28(title):
                continue

            link_nodes = card.xpath(".//a[contains(@class,'elementor-button') and @href][1]")
            href = link_nodes[0].get("href") if link_nodes else None
            source_url = _resolve_url(href, page_url) or page_url
            if self._is_non_job_url_v28(source_url):
                continue

            card_text = _text(card)[:1400]
            cta_text = _text(link_nodes[0]) if link_nodes else ""
            context_hits = bool(
                re.search(
                    r"(?:info\s+lengkap|apply|lowongan|karir|job|position|melamar)",
                    f"{card_text} {cta_text}",
                    re.IGNORECASE,
                )
            )
            url_hint = self._is_job_like_url_v28(source_url) or bool(
                re.search(r"/(?:lowongan|karir|careers?|jobs?|vacanc|hiring|apply|influencer|assistant|specialist|designer|akuntan|fotografer)", source_url, re.IGNORECASE)
            )
            if not (context_hits or url_hint or self._title_has_job_signal_v28(title)):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v28(card_text),
                    "salary_raw": self._extract_salary_v28(card_text),
                    "employment_type": self._extract_type_v28(card_text),
                    "description": card_text[:5000] if len(card_text) > 80 else None,
                    "extraction_method": "tier2_elementor_cards_v28",
                    "extraction_confidence": 0.76,
                }
            )

        deduped = self._dedupe_jobs_v28(jobs, page_url)
        return deduped if len(deduped) >= 2 else []

    # ------------------------------------------------------------------
    # MartianLogic / MyRecruitmentPlus API fallback
    # ------------------------------------------------------------------

    async def _extract_martianlogic_jobs_v28(self, page_url: str, html_body: str) -> list[dict]:
        lower = (html_body or "").lower()
        if (
            "myrecruitmentplus" not in lower
            and "martianlogic" not in lower
            and "clientcode" not in lower
            and "__next_data__" not in lower
        ):
            return []

        context = self._extract_martian_context_v28(html_body, page_url)
        client_code = context.get("client_code", "")
        if not client_code:
            return []

        parsed = urlparse(page_url)
        base = f"{parsed.scheme or 'https'}://{parsed.netloc}"
        endpoints = self._martian_probe_urls_v28(
            base,
            page_url,
            client_code,
            context.get("recruiter_id", ""),
            html_body,
        )
        if not endpoints:
            return []

        jobs: list[dict] = []
        try:
            async with httpx.AsyncClient(
                timeout=8,
                follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/html,*/*"},
            ) as client:
                for endpoint in endpoints[:8]:
                    pages = [1, 2, 3, 4] if "pageNumber=" in endpoint else [1]
                    for page_num in pages:
                        probe_url = re.sub(r"pageNumber=\d+", f"pageNumber={page_num}", endpoint)
                        try:
                            resp = await client.get(probe_url)
                        except Exception:
                            break
                        if resp.status_code != 200 or not resp.text:
                            break

                        probe_jobs = self._extract_jobs_from_probe_response_v28(resp.text, str(resp.url), page_url)
                        if not probe_jobs:
                            if page_num > 1:
                                break
                            continue
                        before = len(jobs)
                        jobs.extend(probe_jobs)
                        jobs = self._dedupe_jobs_v28(jobs, page_url)
                        if page_num > 1 and len(jobs) == before:
                            break
                        if len(jobs) >= MAX_JOBS_PER_PAGE:
                            return jobs[:MAX_JOBS_PER_PAGE]
        except Exception:
            logger.exception("v2.8 MartianLogic fallback failed for %s", page_url)

        return self._dedupe_jobs_v28(jobs, page_url)

    def _extract_martian_context_v28(self, html_body: str, page_url: str) -> dict[str, str]:
        result: dict[str, str] = {}
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html_body or "",
            re.IGNORECASE | re.DOTALL,
        )
        if match:
            try:
                data = json.loads(match.group(1))
                page_props = (((data.get("props") or {}).get("pageProps") or {}) if isinstance(data, dict) else {})
                if isinstance(page_props, dict):
                    result["client_code"] = str(page_props.get("clientCode") or "").strip()
                    result["recruiter_id"] = str(page_props.get("recruiterId") or "").strip()
            except Exception:
                pass

        if not result.get("client_code"):
            parsed = urlparse(page_url)
            path_parts = [p for p in parsed.path.split("/") if p]
            if path_parts:
                result["client_code"] = path_parts[0].strip()
            m = _MARTIAN_CLIENT_PATTERN_V28.search(parsed.path or "")
            if m:
                result["client_code"] = m.group(1).strip()

        return result

    def _martian_probe_urls_v28(
        self,
        base_url: str,
        page_url: str,
        client_code: str,
        recruiter_id: str,
        html_body: str,
    ) -> list[str]:
        host_variants = [base_url, "https://web.martianlogic.com", "https://form.myrecruitmentplus.com"]
        url_host = f"{urlparse(page_url).scheme or 'https'}://{urlparse(page_url).netloc}"
        if url_host and url_host not in host_variants:
            host_variants.insert(0, url_host)
        candidates = [
            f"{base_url}/{client_code}/",
            f"{base_url}/{client_code}",
            f"{base_url}/{client_code}/?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"{base_url}/{client_code}/jobs?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"{base_url}/{client_code}/job-ads?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"{base_url}/{client_code}/jobads?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"{base_url}/{client_code}/embed-jobs?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"{base_url}/jobs?client={client_code}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"{base_url}/job-ads?client={client_code}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"{base_url}/jobads?client={client_code}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"{base_url}/embed-jobs?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc&client={client_code}",
            f"{base_url}/?client={client_code}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"{base_url}/?clientCode={client_code}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"{base_url}/api/jobs?clientCode={client_code}&pageNumber=1&pageSize=50",
            f"{base_url}/api/jobads?clientCode={client_code}&pageNumber=1&pageSize=50",
            f"{base_url}/api/job-ads?clientCode={client_code}&pageNumber=1&pageSize=50",
        ]
        for host_variant in host_variants:
            candidates.extend(
                [
                    f"{host_variant}/{client_code}",
                    f"{host_variant}/{client_code}/?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"{host_variant}/{client_code}/jobs?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"{host_variant}/{client_code}/job-ads?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"{host_variant}/jobs?client={client_code}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"{host_variant}/job-ads?client={client_code}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"{host_variant}/jobads?clientCode={client_code}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"{host_variant}/embed-jobs?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc&client={client_code}",
                    f"{host_variant}/api/jobs?clientCode={client_code}&pageNumber=1&pageSize=50",
                    f"{host_variant}/api/jobads?clientCode={client_code}&pageNumber=1&pageSize=50",
                    f"{host_variant}/api/job-ads?clientCode={client_code}&pageNumber=1&pageSize=50",
                ]
            )
        if recruiter_id:
            for host_variant in host_variants:
                candidates.extend(
                    [
                        f"{host_variant}/?client={client_code}&recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                        f"{host_variant}/api/recruiter/{recruiter_id}/jobs?pageNumber=1&pageSize=50",
                        f"{host_variant}/api/recruiter/{recruiter_id}/job-ads?pageNumber=1&pageSize=50",
                    ]
                )

        parsed = urlparse(page_url)
        query = dict(parse_qsl(parsed.query))
        if query.get("client"):
            candidates.append(
                f"{base_url}/?client={query['client']}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc"
            )
        if query.get("clientCode"):
            client_q = query["clientCode"]
            candidates.append(
                f"{base_url}/?client={client_q}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc"
            )
        if query.get("recruiterId"):
            rid_q = query["recruiterId"]
            candidates.append(
                f"{base_url}/?client={client_code}&recruiterId={rid_q}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc"
            )
        if query.get("jobAdId"):
            candidates.append(
                f"{base_url}/?client={client_code}&jobAdId={query['jobAdId']}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc"
            )

        for m in _MARTIAN_ENDPOINT_URL_PATTERN_V28.finditer(html_body or ""):
            candidates.append(m.group(0))

        seen: set[str] = set()
        unique: list[str] = []
        for url in candidates:
            norm = url.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
            unique.append(url)
        return unique

    def _extract_jobs_from_probe_response_v28(self, body: str, response_url: str, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        payload = (body or "").strip()
        if not payload:
            return jobs

        if payload.startswith("{") or payload.startswith("["):
            try:
                parsed = json.loads(payload)
                jobs.extend(self._extract_jobs_from_json_obj_v28(parsed, response_url, "tier0_martian_api_v28"))
            except Exception:
                pass

        root = _parse_html(payload)
        if root is not None:
            tier2_jobs = self._extract_tier2_v16(response_url, payload) or []
            for job in tier2_jobs:
                cloned = dict(job)
                cloned["extraction_method"] = "tier2_heuristic_v28_martian"
                jobs.append(cloned)
            jobs.extend(self._extract_from_repeating_rows_v28(root, response_url))
            jobs.extend(self._extract_from_heading_rows_v28(root, response_url))
            jobs.extend(self._extract_from_accordion_sections_v28(root, response_url))
            jobs.extend(self._extract_structured_jobs_v28(payload, response_url))

        return self._dedupe_jobs_v28(jobs, page_url)

    async def _extract_oracle_api_jobs_v28(self, page_url: str, html_body: str) -> list[dict]:
        page_l = (page_url or "").lower()
        body_l = (html_body or "").lower()
        if "oraclecloud.com" not in page_l and "candidateexperience" not in body_l and "cx_config" not in body_l:
            return []

        parsed = urlparse(page_url)
        host = parsed.hostname or ""
        if not host:
            return []

        api_base = f"https://{host}"
        api_match = _ORACLE_API_BASE_PATTERN_V28.search(html_body or "")
        if api_match:
            api_base = api_match.group(1).rstrip("/")

        site_ids = self._oracle_site_ids_v28(page_url, html_body)
        if not site_ids:
            return []

        best_jobs: list[dict] = []
        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
                for site_id in site_ids[:10]:
                    site_jobs: list[dict] = []
                    seen_ids: set[str] = set()

                    for offset in range(0, 240, 24):
                        finder = (
                            f"findReqs;siteNumber={site_id},"
                            "facetsList=LOCATIONS;WORK_LOCATIONS;TITLES;CATEGORIES;POSTING_DATES,"
                            f"limit=24,offset={offset}"
                        )
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
                            break
                        if resp.status_code != 200:
                            break

                        try:
                            payload = resp.json()
                        except Exception:
                            break

                        items = payload.get("items") if isinstance(payload, dict) else None
                        if not isinstance(items, list) or not items:
                            break
                        listing_holder = items[0] if isinstance(items[0], dict) else {}
                        rows = listing_holder.get("requisitionList")
                        if not isinstance(rows, list) or not rows:
                            break

                        page_count = 0
                        for row in rows:
                            if not isinstance(row, dict):
                                continue
                            title = self._normalize_title_v28(str(row.get("Title") or row.get("title") or "").strip())
                            if not self._is_valid_title_v28(title):
                                continue
                            if not self._title_has_job_signal_v28(title):
                                continue

                            req_id = str(row.get("Id") or row.get("id") or "").strip()
                            dedupe_key = f"{title.lower()}::{req_id.lower()}"
                            if dedupe_key in seen_ids:
                                continue
                            seen_ids.add(dedupe_key)

                            source_url = (
                                f"https://{host}/hcmUI/CandidateExperience/en/sites/{site_id}/job/{req_id}"
                                if req_id else page_url
                            )
                            if self._is_non_job_url_v28(source_url):
                                source_url = page_url

                            location = " ".join(
                                part for part in (
                                    str(row.get("PrimaryLocation") or "").strip(),
                                    str(row.get("PrimaryLocationCountry") or "").strip(),
                                ) if part
                            ) or None

                            listed_date = str(row.get("PostedDate") or "").strip() or None
                            site_jobs.append(
                                {
                                    "title": title,
                                    "source_url": source_url,
                                    "location_raw": location[:200] if location else None,
                                    "salary_raw": None,
                                    "employment_type": None,
                                    "description": listed_date,
                                    "extraction_method": "tier0_oracle_api_v28",
                                    "extraction_confidence": 0.91,
                                }
                            )
                            page_count += 1
                            if len(site_jobs) >= MAX_JOBS_PER_PAGE:
                                break

                        if len(site_jobs) >= MAX_JOBS_PER_PAGE:
                            break
                        if page_count < 24:
                            break

                    if self._jobset_score_v28(site_jobs, page_url) > self._jobset_score_v28(best_jobs, page_url):
                        best_jobs = site_jobs
        except Exception:
            logger.exception("v2.8 Oracle API fallback failed for %s", page_url)

        return self._dedupe_jobs_v28(best_jobs, page_url)

    def _oracle_site_ids_v28(self, page_url: str, html_body: str) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []

        def _add(value: str) -> None:
            site_id = (value or "").strip()
            if not site_id or site_id in seen:
                return
            seen.add(site_id)
            ordered.append(site_id)

        for m in _ORACLE_SITE_PATTERN_V28.finditer(page_url or ""):
            _add(m.group(1))
        for m in _ORACLE_SITE_PATTERN_V28.finditer(html_body or ""):
            _add(m.group(1))
        for m in _ORACLE_SITE_NUMBER_PATTERN_V28.finditer(html_body or ""):
            _add(m.group(1))
        for m in re.finditer(r"siteNumber=([A-Za-z0-9_]+)", html_body or "", re.IGNORECASE):
            _add(m.group(1))

        query_pairs = dict(parse_qsl(urlparse(page_url or "").query))
        if query_pairs.get("siteNumber"):
            _add(query_pairs.get("siteNumber", ""))

        base_ids = list(ordered)
        for base_id in base_ids:
            if re.fullmatch(r"CX(?:_\d+)?", base_id, flags=re.IGNORECASE):
                root_id = base_id.split("_", 1)[0]
                _add(root_id)
                for suffix in ("1001", "1002", "1003", "1004"):
                    _add(f"{root_id}_{suffix}")

        if not ordered:
            for fallback in ("CX", "CX_1001", "CX_1002"):
                _add(fallback)

        return ordered[:12]

    # ------------------------------------------------------------------
    # Selection / validation / helpers
    # ------------------------------------------------------------------

    def _pick_best_jobset_v28(
        self,
        candidates: list[tuple[str, list[dict]]],
        page_url: str,
    ) -> tuple[str, list[dict]]:
        if not candidates:
            return "", []

        best_label = ""
        best_jobs: list[dict] = []
        best_score = -1.0
        parent_score = -1.0
        parent_jobs: list[dict] = []

        for label, jobs in candidates:
            deduped = self._dedupe_jobs_v28(jobs, page_url)
            if not deduped:
                continue

            score = self._jobset_score_v28(deduped, page_url)
            valid = self._passes_jobset_validation_v28(deduped, page_url)

            if label == "parent_v16":
                parent_score = score
                parent_jobs = deduped

            logger.debug(
                "v2.8 candidate %s: jobs=%d score=%.2f valid=%s",
                label,
                len(deduped),
                score,
                valid,
            )

            if valid and score > best_score:
                best_label = label
                best_jobs = deduped
                best_score = score

        if best_jobs:
            # Keep parent output unless fallback is clearly better.
            if parent_jobs and best_label != "parent_v16" and best_score < parent_score + 2.0:
                return "parent_v16", parent_jobs
            return best_label, best_jobs[:MAX_JOBS_PER_PAGE]

        # If nothing passes strict validation, keep parent partial if present.
        if parent_jobs:
            return "parent_v16", parent_jobs[:MAX_JOBS_PER_PAGE]

        # Final fallback: return the largest candidate after dedupe.
        largest = max(
            ((label, self._dedupe_jobs_v28(jobs, page_url)) for label, jobs in candidates),
            key=lambda item: len(item[1]),
            default=("", []),
        )
        return largest[0], largest[1][:MAX_JOBS_PER_PAGE]

    def _passes_jobset_validation_v28(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title_v28(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if self._is_valid_title_v28(t)]
        if not titles:
            return False

        unique_ratio = len(set(t.lower() for t in titles)) / max(1, len(titles))
        if len(titles) > 2 and unique_ratio < 0.6:
            return False

        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V28.match(t.lower()))
        nav_hits = sum(
            1 for t in titles
            if _CATEGORY_TITLE_PATTERN_V28.match(t) or _CORPORATE_TITLE_PATTERN_V28.match(t) or _PHONE_TITLE_PATTERN_V28.match(t)
        )
        if reject_hits >= max(1, int(len(titles) * 0.35)):
            return False
        if nav_hits >= max(1, int(len(titles) * 0.25)):
            return False

        title_hits = sum(1 for t in titles if self._title_has_job_signal_v28(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url_v28(j.get("source_url") or page_url))
        detail_hits = sum(1 for j in jobs if _JOB_DETAILISH_URL_PATTERN_V28.search(j.get("source_url") or ""))
        section_hits = sum(
            1 for j in jobs
            if _NON_JOB_SECTION_URL_PATTERN_V28.search(j.get("source_url") or "")
            and not _JOB_DETAILISH_URL_PATTERN_V28.search(j.get("source_url") or "")
        )
        apply_hits = sum(1 for j in jobs if _APPLY_CONTEXT_PATTERN_V28.search((j.get("description") or "")[:1200]))
        if len(titles) >= 3 and section_hits >= max(1, int(len(titles) * 0.3)):
            return False

        if len(titles) == 1:
            t = titles[0]
            src = jobs[0].get("source_url") or page_url
            return (
                (self._title_has_job_signal_v28(t) and not _GENERIC_LISTING_LABEL_PATTERN_V28.match(t))
                and (self._is_job_like_url_v28(src) or apply_hits >= 1)
            )

        if len(titles) <= 3:
            return title_hits >= 1 and (url_hits >= 1 or detail_hits >= 1 or apply_hits >= 1 or title_hits >= 2)

        needed = max(2, int(len(titles) * 0.3))
        return (
            title_hits >= needed
            and (
                url_hits >= max(1, int(len(titles) * 0.15))
                or detail_hits >= max(1, int(len(titles) * 0.12))
                or apply_hits >= max(1, int(len(titles) * 0.15))
            )
        )

    def _jobset_score_v28(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        titles = [self._normalize_title_v28(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return 0.0

        count = len(titles)
        title_hits = sum(1 for t in titles if self._title_has_job_signal_v28(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url_v28(j.get("source_url") or page_url))
        detail_hits = sum(1 for j in jobs if _JOB_DETAILISH_URL_PATTERN_V28.search(j.get("source_url") or ""))
        section_hits = sum(
            1 for j in jobs
            if _NON_JOB_SECTION_URL_PATTERN_V28.search(j.get("source_url") or "")
            and not _JOB_DETAILISH_URL_PATTERN_V28.search(j.get("source_url") or "")
        )
        apply_hits = sum(1 for j in jobs if _APPLY_CONTEXT_PATTERN_V28.search((j.get("description") or "")[:1200]))
        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V28.match(t.lower()))
        nav_hits = sum(
            1 for t in titles
            if _CATEGORY_TITLE_PATTERN_V28.match(t) or _CORPORATE_TITLE_PATTERN_V28.match(t) or _PHONE_TITLE_PATTERN_V28.match(t)
        )
        unique_titles = len(set(t.lower() for t in titles))

        score = count * 3.2
        score += title_hits * 2.3
        score += url_hits * 1.7
        score += detail_hits * 1.2
        score += apply_hits * 1.5
        score += unique_titles * 0.7
        score -= reject_hits * 3.5
        score -= nav_hits * 4.2
        score -= section_hits * 2.4
        return score

    def _dedupe_jobs_v28(self, jobs: list[dict], page_url: str) -> list[dict]:
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for job in jobs:
            title = self._normalize_title_v28(job.get("title", ""))
            if not self._is_valid_title_v28(title):
                continue

            source_url = (job.get("source_url") or "").strip()
            if not source_url:
                source_url = page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]
            if self._is_non_job_url_v28(source_url):
                continue

            key = (title.lower(), source_url.lower())
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

    def _normalize_title_v28(self, title: str) -> str:
        if not title:
            return ""
        t = html.unescape(" ".join(str(title).replace("\u00a0", " ").split()))
        t = t.strip(" |:-\u2013\u2022")
        t = re.sub(r"[\u200b-\u200d\ufeff]", "", t)
        t = re.sub(r"\.pdf$", "", t, flags=re.IGNORECASE)
        t = re.sub(r"^job\s+description\s*\|\s*", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s{2,}", " ", t)
        t = re.sub(r"\s+Deadline\s*:\s+.*$", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s+Closing\s+Date\s*:\s+.*$", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s+Posted\s+Date\s*:\s+.*$", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s+More\s+Details?$", "", t, flags=re.IGNORECASE)
        t = re.sub(r"%BUTTON_[A-Z_]+%", "", t)
        if " - " in t and len(t) > 40:
            parts = [p.strip() for p in t.split(" - ") if p.strip()]
            if parts and self._title_has_job_signal_v28(parts[0]):
                t = parts[0]
        return t.strip()

    def _is_valid_title_v28(self, title: str) -> bool:
        if not title:
            return False
        if not TieredExtractorV16._is_valid_title_v16(title):
            if not _is_valid_title(title):
                return False

        t = title.strip()
        low = t.lower()
        if _REJECT_TITLE_PATTERN_V28.match(low):
            return False
        if _GENERIC_LISTING_LABEL_PATTERN_V28.match(t):
            return False
        if _CATEGORY_TITLE_PATTERN_V28.match(t):
            return False
        if _CORPORATE_TITLE_PATTERN_V28.match(t):
            return False
        if _PHONE_TITLE_PATTERN_V28.match(t):
            return False
        if _COMPANY_CAREER_LABEL_PATTERN_V28.match(t):
            return False
        if re.search(r"recruitment\s+agency", t, re.IGNORECASE):
            return False

        words = t.split()
        if len(words) > 14:
            return False
        if len(words) <= 1 and not self._title_has_job_signal_v28(t):
            return False
        return True

    def _title_has_job_signal_v28(self, title: str) -> bool:
        if not title:
            return False
        if _title_has_job_noun(title):
            return True
        return bool(_TITLE_HINT_PATTERN_V28.search(title))

    @staticmethod
    def _extract_location_v28(text: str) -> Optional[str]:
        match = _AU_LOCATIONS.search(text or "")
        if match:
            return match.group(0).strip()
        return None

    @staticmethod
    def _extract_salary_v28(text: str) -> Optional[str]:
        match = _SALARY_PATTERN.search(text or "")
        if match:
            return match.group(0).strip()
        return None

    @staticmethod
    def _extract_type_v28(text: str) -> Optional[str]:
        match = _JOB_TYPE_PATTERN.search(text or "")
        if match:
            return match.group(0).strip()
        return None

    def _is_job_like_url_v28(self, src: str) -> bool:
        if not src:
            return False
        if self._is_non_job_url_v28(src):
            return False
        return bool(_JOB_URL_HINT_PATTERN_V28.search(src))

    def _is_non_job_url_v28(self, src: str) -> bool:
        lowered = (src or "").lower()
        if _NON_JOB_URL_PATTERN_V28.search(lowered):
            return True
        if _NON_JOB_SECTION_URL_PATTERN_V28.search(lowered) and not _JOB_DETAILISH_URL_PATTERN_V28.search(lowered):
            return True
        return False

    async def _render_with_playwright_v13(self, url: str) -> Optional[str]:
        """Playwright rendering with longer stabilization + common interactions."""
        try:
            from playwright.async_api import async_playwright
        except Exception:
            return None

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=45000)

                    for selector in (
                        "#accept",
                        "#onetrust-accept-btn-handler",
                        "button.accept",
                        "button:has-text('Accept')",
                        "button:has-text('Accept All')",
                        "button:has-text('I Agree')",
                        "[class*='consent'] button",
                        "[id*='cookie'] button",
                    ):
                        try:
                            loc = page.locator(selector)
                            if await loc.count() > 0:
                                await loc.first.click(timeout=1500)
                                await page.wait_for_timeout(500)
                        except Exception:
                            continue

                    # Trigger empty search/submit forms on listing pages that start blank.
                    try:
                        forms = page.locator("form")
                        for i in range(min(await forms.count(), 5)):
                            form = forms.nth(i)
                            submit = form.locator(
                                "button[type='submit'], input[type='submit'], "
                                "button:has-text('Search'), button:has-text('Find'), "
                                "button:has-text('Submit')"
                            )
                            if await submit.count() > 0:
                                try:
                                    await submit.first.click(timeout=1500)
                                    await page.wait_for_timeout(900)
                                except Exception:
                                    continue
                    except Exception:
                        pass

                    # Expand collapsed content.
                    for selector in (
                        "details:not([open]) > summary",
                        "[aria-expanded='false']",
                        ".accordion-button.collapsed",
                        ".elementor-tab-title",
                    ):
                        try:
                            loc = page.locator(selector)
                            for i in range(min(await loc.count(), 8)):
                                try:
                                    await loc.nth(i).click(timeout=1000)
                                except Exception:
                                    continue
                            await page.wait_for_timeout(500)
                        except Exception:
                            continue

                    # Scroll and click load-more/pagination affordances.
                    for _ in range(3):
                        clicked = False
                        for selector in (
                            "button:has-text('Load More')",
                            "a:has-text('Load More')",
                            "button:has-text('Show More')",
                            "a:has-text('Next')",
                            "button:has-text('Next')",
                        ):
                            try:
                                loc = page.locator(selector)
                                if await loc.count() > 0:
                                    await loc.first.click(timeout=1500)
                                    await page.wait_for_timeout(1200)
                                    clicked = True
                                    break
                            except Exception:
                                continue
                        for _ in range(4):
                            await page.mouse.wheel(0, 1800)
                            await page.wait_for_timeout(700)
                        if not clicked:
                            break

                    await page.wait_for_timeout(6500)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=12000)
                    except Exception:
                        pass
                    return await page.content()
                except Exception:
                    return None
                finally:
                    await browser.close()
        except Exception:
            return None

    def _needs_detail_refresh_v28(self, job: dict) -> bool:
        source_url = str(job.get("source_url") or "")
        if not source_url.startswith("http"):
            return False
        desc = " ".join(str(job.get("description") or "").split())
        title = self._normalize_title_v28(job.get("title", ""))
        method = str(job.get("extraction_method") or "").lower()
        if not desc:
            return True
        if len(desc) < 140:
            return True
        if _LOW_SIGNAL_DESC_PATTERN_V28.match(desc):
            return True
        if title and desc.lower() == title.lower():
            return True
        if re.search(r"(?:info\s+lengkap|read\s+more|learn\s+more|click\s+here)$", desc, re.IGNORECASE):
            return True
        if ("elementor" in method or "links_" in method) and len(desc) < 260:
            return True
        return False

    async def _enrich_from_detail_pages(self, jobs: list[dict]) -> list[dict]:
        jobs_to_enrich = [
            (i, j) for i, j in enumerate(jobs)
            if self._needs_detail_refresh_v28(j)
        ][:20]
        if not jobs_to_enrich:
            return jobs

        async with httpx.AsyncClient(
            timeout=8,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        ) as client:
            async def _enrich_one(idx: int, job: dict) -> tuple[int, dict]:
                try:
                    resp = await client.get(job["source_url"])
                except Exception:
                    return idx, {}
                if resp.status_code != 200 or len(resp.text or "") < 200:
                    return idx, {}

                root = _parse_html(resp.text)
                if root is None:
                    return idx, {}

                enriched: dict[str, Optional[str]] = {}
                body_text = _text(root)

                # Description extraction: prioritize article/main entry content.
                desc = None
                for sel in (
                    "article .entry-content",
                    ".entry-content",
                    "[itemprop='description']",
                    "main article",
                    "main",
                    "article",
                    "[class*='job-description']",
                    "[class*='description']",
                ):
                    try:
                        nodes = root.cssselect(sel)
                    except Exception:
                        nodes = []
                    for node in nodes[:3]:
                        text = " ".join(_text(node).split())
                        if len(text) > 180:
                            desc = text[:5000]
                            break
                    if desc:
                        break
                if not desc and len(body_text) > 220:
                    desc = " ".join(body_text.split())[:5000]
                if desc:
                    enriched["description"] = desc

                # Heading/icon-label style metadata blocks (common in Elementor details pages).
                location = None
                emp_type = None
                for heading in root.xpath("//h2|//h3|//h4|//strong|//dt"):
                    label = " ".join(_text(heading).split()).lower()
                    next_nodes = heading.xpath("following-sibling::*[1]")
                    next_text = " ".join(_text(next_nodes[0]).split()) if next_nodes else ""

                    has_location_icon = bool(
                        heading.xpath(".//*[contains(@class,'fa-map-marker') or contains(@class,'map-marker')]")
                    )
                    has_type_icon = bool(
                        heading.xpath(".//*[contains(@class,'fa-clock') or contains(@class,'clock')]")
                    )

                    if not location and (
                        has_location_icon
                        or re.search(r"(location|lokasi|penempatan|office|city|country)", label, re.IGNORECASE)
                    ):
                        if 2 < len(next_text) < 200:
                            location = next_text[:200]

                    if not emp_type and (
                        has_type_icon
                        or re.search(r"(job\s*type|employment|jenis\s+pekerjaan|tipe)", label, re.IGNORECASE)
                    ):
                        if 2 < len(next_text) < 120:
                            emp_type = next_text[:120]

                if not location:
                    m = re.search(
                        r"(?:location|lokasi|penempatan)\s*[:\-]\s*([^\n|]{2,120})",
                        body_text,
                        re.IGNORECASE,
                    )
                    if m:
                        location = m.group(1).strip()[:200]
                if location:
                    enriched["location_raw"] = location

                if not emp_type:
                    m = re.search(
                        r"(?:employment\s*type|job\s*type|jenis\s+pekerjaan)\s*[:\-]\s*([^\n|]{2,90})",
                        body_text,
                        re.IGNORECASE,
                    )
                    if m:
                        emp_type = m.group(1).strip()[:120]
                if not emp_type:
                    tm = _JOB_TYPE_PATTERN.search(body_text)
                    if tm:
                        emp_type = tm.group(0).strip()[:120]
                if emp_type:
                    enriched["employment_type"] = emp_type

                salary = _SALARY_PATTERN.search(body_text or "")
                if salary:
                    enriched["salary_raw"] = salary.group(0).strip()

                return idx, enriched

            for batch_start in range(0, len(jobs_to_enrich), 5):
                batch = jobs_to_enrich[batch_start:batch_start + 5]
                results = await asyncio.gather(*[_enrich_one(idx, job) for idx, job in batch])
                for idx, enriched in results:
                    if not enriched:
                        continue
                    current = jobs[idx]
                    for key, value in enriched.items():
                        if not value:
                            continue
                        if key == "description":
                            current_desc = str(current.get("description") or "")
                            if (
                                not current_desc
                                or self._needs_detail_refresh_v28(current)
                                or len(value) > len(current_desc) + 120
                            ):
                                current[key] = value
                        elif not current.get(key):
                            current[key] = value

        return jobs
