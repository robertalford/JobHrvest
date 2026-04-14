"""
Tiered Extraction Engine v2.3 — direct from v1.6 with discovery-safe extraction upgrades.

High-impact changes:
1. Timeout-safe parent execution and timeout-safe fallback enrichment.
2. Oracle CX API fallback for SPA-shell requisition pages.
3. Repeated heading+CTA card extraction for Elementor/Bootstrap tile boards.
4. Stronger JSON/state validation to reject department/filter/company labels.
5. Stronger false-positive controls for taxonomy labels and same-page URL-only evidence.
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import re
from collections import defaultdict
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse

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


_TITLE_HINT_PATTERN_V23 = re.compile(
    r"\b(?:job|jobs|career|careers|vacanc(?:y|ies)|opening|openings|"
    r"position|positions|role|roles|internship|manager|engineer|developer|"
    r"officer|specialist|assistant|analyst|consultant|coordinator|executive|owner|"
    r"technician|designer|administrator|accountant|recruit(?:er|ment)?|"
    r"director|chef|nurse|teacher|operator|supervisor|"
    r"influencer|akuntan|psikolog(?:i)?|fotografer|videografer|desainer|"
    r"customer\s+service|layanan\s+pelanggan|administrasi|pemasaran|penjualan|"
    r"gerente|director|vacantes?|empleo|trabajo|lowongan|karir|karier|"
    r"kerjaya|jawatan)\b",
    re.IGNORECASE,
)

_REJECT_TITLE_PATTERN_V23 = re.compile(
    r"^(?:"
    r"my\s+applications?|my\s+forms?|my\s+emails?|my\s+tests?|my\s+interviews?|"
    r"job\s+alerts?|jobs?\s+list|job\s+search|saved\s+jobs?|manage\s+applications?|"
    r"start\s+new\s+application|access\s+existing\s+application|preview\s+application\s+form|"
    r"apply\s+now|apply\s+here|read\s+more|learn\s+more|show\s+more|"
    r"entries\s+feed|comments\s+feed|rss|feed|"
    r"about\s+us|contact\s+us|privacy|terms|help|login|register|"
    r"job\s+name|closing\s+date|posted\s+date|job\s+ref|"
    r"benefits|how\s+to\s+apply|current\s+opportunities|join\s+us(?:\s+and.*)?|"
    r"vacantes|vacantes\s+inicio|alertas?\s+de\s+vacantes?|bolsa\s+de\s+trabajo|"
    r"asesorado\s+por|"
    r"puesto\s+ciudad\s+beneficios"
    r")$",
    re.IGNORECASE,
)

_GENERIC_LISTING_LABEL_PATTERN_V23 = re.compile(
    r"^(?:jobs?|careers?|vacancies|vacantes?|job\s+openings?|open\s+positions?|"
    r"bolsa\s+de\s+trabajo|alertas?\s+de\s+vacantes?|join\s+our\s+team)$",
    re.IGNORECASE,
)

_CATEGORY_TITLE_PATTERN_V23 = re.compile(
    r"^(?:all\s+jobs?|jobs?\s+by|browse\s+jobs?|view\s+jobs?|"
    r".{2,80}\s+jobs?|.{2,80}\s+vacancies?)$",
    re.IGNORECASE,
)

_PHONE_TITLE_PATTERN_V23 = re.compile(
    r"^(?:\+?\d[\d\s().-]{6,}|\d{2,4}\s?\d{3,5}\s?\d{3,5})$"
)

_CONCAT_SUFFIX_PATTERN_V23 = re.compile(
    r"^(.+?[a-z\)])([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,}){0,2}(?:,\s*[A-Z][a-z]{2,})?)$"
)

_CORPORATE_TITLE_PATTERN_V23 = re.compile(
    r"^(?:about|home|contact|consultancy|green\s+tomato|"
    r"company|our\s+company|our\s+values|our\s+culture)$",
    re.IGNORECASE,
)

_COMPANY_CAREER_LABEL_PATTERN_V23 = re.compile(
    r"^[a-z0-9&.,'() -]{2,60}\s+careers?$",
    re.IGNORECASE,
)

_CARD_CONTAINER_HINT_PATTERN_V23 = re.compile(
    r"card|tile|item|column|job|career|position|vacan|opening|elementor",
    re.IGNORECASE,
)

_CTA_TEXT_PATTERN_V23 = re.compile(
    r"(?:apply|details?|detail|read\s+more|view|see|learn\s+more|job\s+description|"
    r"info\s+lengkap|selengkapnya|lihat|lamar|daftar|pelajari)",
    re.IGNORECASE,
)

_JOB_URL_HINT_PATTERN_V23 = re.compile(
    r"(?:/job|/jobs|/career|/careers|/position|/positions|/vacanc|"
    r"/opening|/openings|/requisition|/requisitions|"
    r"event=jobs\.|jobid=|portal\.na|candidateportal|/apply|/lowongan|/karir|/kerjaya)",
    re.IGNORECASE,
)

_JOB_DETAILISH_URL_PATTERN_V23 = re.compile(
    r"(?:/jobs?/[a-z0-9][^/?#]{3,}|/requisition[s]?/[a-z0-9][^/?#]{2,}|"
    r"[?&](?:jobid|job_id|requisitionid|positionid)=)",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V23 = re.compile(
    r"(?:/privacy|/terms|/cookie|/news|/blog|/about|/contact|/investor|"
    r"/help|/login|/logout|/register|/account|/feed(?:/|$)|/rss(?:/|$)|"
    r"/wp-login(?:\.php)?|/mydayforce/login(?:\.aspx)?|/comments/feed(?:/|$)|"
    r"event=jobs\.view(?:history|myforms|myemails|mytests|myinterviews)|"
    r"event=help\.|event=reg\.)",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN_V23 = re.compile(
    r"job|position|vacanc|opening|requisition|career|posting|listing|accordion",
    re.IGNORECASE,
)

_CLASS_HINT_PATTERN_V23 = re.compile(
    r"job|position|vacanc|opening|requisition|career|listing|accordion|card|item",
    re.IGNORECASE,
)

_SCRIPT_ASSIGNMENT_PATTERN_V23 = re.compile(
    r"(?:window\.[A-Za-z0-9_$.]+\s*=\s*|var\s+[A-Za-z0-9_$]+\s*=\s*)(\{.*\}|\[.*\])\s*;?",
    re.DOTALL,
)

_ORACLE_SITE_PATTERN_V23 = re.compile(r"/sites/([A-Za-z0-9_]+)/", re.IGNORECASE)
_ORACLE_SITE_NUMBER_PATTERN_V23 = re.compile(r"siteNumber\s*[:=]\s*['\"]([A-Za-z0-9_]+)", re.IGNORECASE)
_ORACLE_API_BASE_PATTERN_V23 = re.compile(r"apiBaseUrl\s*:\s*['\"](https?://[^'\"\\s]+)", re.IGNORECASE)


class TieredExtractorV23(TieredExtractorV16):
    """v2.3 extractor: v1.6-first with timeout-safe structured/API/card fallbacks."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # Required by agent rules: run parent extractor first.
        parent_jobs: list[dict] = []
        try:
            # Keep parent logic, but avoid full-phase timeout stalls on large sites.
            parent_jobs = await asyncio.wait_for(
                super().extract(career_page, company, html),
                timeout=18.0,
            )
        except asyncio.TimeoutError:
            logger.warning("v2.3 parent extractor timeout for %s; using local fallbacks", url)
        except Exception:
            logger.exception("v2.3 parent extractor failed for %s; using local fallbacks", url)

        parent_jobs = self._dedupe_jobs_v23(parent_jobs or [], url)

        root = _parse_html(html)
        candidates: list[tuple[str, list[dict]]] = []
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        oracle_api_jobs = await self._extract_oracle_api_jobs_v23(url, html)
        if oracle_api_jobs:
            candidates.append(("oracle_api_v23", oracle_api_jobs))

        structured_jobs = self._extract_structured_jobs_v23(html, url)
        if structured_jobs:
            candidates.append(("structured_v23", structured_jobs))

        script_jobs = self._extract_jobs_from_state_scripts_v23(html, url)
        if script_jobs:
            candidates.append(("state_script_v23", script_jobs))

        bound_jobs = self._extract_jobs_from_bound_props_v23(html, url)
        if bound_jobs:
            candidates.append(("bound_props_v23", bound_jobs))

        if root is not None:
            link_jobs = self._extract_from_job_links_v23(root, url)
            if link_jobs:
                candidates.append(("job_links_v23", link_jobs))

            accordion_jobs = self._extract_from_accordion_sections_v23(root, url)
            if accordion_jobs:
                candidates.append(("accordion_v23", accordion_jobs))

            card_button_jobs = self._extract_from_card_buttons_v23(root, url)
            if card_button_jobs:
                candidates.append(("card_buttons_v23", card_button_jobs))

            heading_jobs = self._extract_from_heading_rows_v23(root, url)
            if heading_jobs:
                candidates.append(("heading_rows_v23", heading_jobs))

            repeating_row_jobs = self._extract_from_repeating_rows_v23(root, url)
            if repeating_row_jobs:
                candidates.append(("repeating_rows_v23", repeating_row_jobs))

            heading_card_jobs = self._extract_from_heading_cta_cards_v23(root, url)
            if heading_card_jobs:
                candidates.append(("heading_cta_cards_v23", heading_card_jobs))

        best_label, best_jobs = self._pick_best_jobset_v23(candidates, url)
        if not best_jobs:
            return []

        if (
            best_label != "parent_v16"
            and len(best_jobs) >= MIN_JOBS_FOR_SUCCESS
            and len(best_jobs) <= 40
        ):
            # Enrich fallback output when we have a credible multi-job set.
            try:
                best_jobs = await asyncio.wait_for(
                    self._enrich_from_detail_pages(best_jobs),
                    timeout=14.0,
                )
            except asyncio.TimeoutError:
                logger.warning("v2.3 enrichment timeout for %s; returning non-enriched fallback", url)
            except Exception:
                logger.exception("v2.3 enrichment failed for %s; returning non-enriched fallback", url)
            best_jobs = self._dedupe_jobs_v23(best_jobs, url)

        return best_jobs[:MAX_JOBS_PER_PAGE]

    # ------------------------------------------------------------------
    # Structured / state-script fallbacks
    # ------------------------------------------------------------------

    def _extract_structured_jobs_v23(self, html: str, page_url: str) -> list[dict]:
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
            jobs.extend(self._extract_jobs_from_json_obj_v23(data, page_url, "tier0_jsonld_v23"))

        return self._dedupe_jobs_v23(jobs, page_url)

    def _extract_jobs_from_state_scripts_v23(self, html: str, page_url: str) -> list[dict]:
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
            if "__NEXT_DATA__" in body or "dehydratedState" in body or "job" in body.lower():
                script_payloads.append(body)

        for payload in script_payloads[:40]:
            for parsed in self._parse_json_blobs_v23(payload):
                jobs.extend(self._extract_jobs_from_json_obj_v23(parsed, page_url, "tier0_state_v23"))

        return self._dedupe_jobs_v23(jobs, page_url)

    def _extract_jobs_from_bound_props_v23(self, html_body: str, page_url: str) -> list[dict]:
        """Extract jobs from framework-bound JSON attributes (Vue/Alpine style)."""
        jobs: list[dict] = []
        for m in re.finditer(
            r"(:[A-Za-z0-9_-]*?(?:jobs?|positions?|vacancies?|openings?|requisitions?)[A-Za-z0-9_-]*)="
            r"'([^']{20,})'",
            html_body or "",
            re.IGNORECASE | re.DOTALL,
        ):
            payload = (m.group(2) or "").strip()
            if not payload or (not payload.startswith("{") and not payload.startswith("[")):
                continue
            try:
                parsed = json.loads(payload)
            except Exception:
                continue
            jobs.extend(self._extract_jobs_from_json_obj_v23(parsed, page_url, "tier0_bound_props_v23"))
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break
        return self._dedupe_jobs_v23(jobs, page_url)

    def _parse_json_blobs_v23(self, script_body: str) -> list[object]:
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
        for m in _SCRIPT_ASSIGNMENT_PATTERN_V23.finditer(body):
            raw = (m.group(1) or "").strip()
            if len(raw) < 2:
                continue
            try:
                results.append(json.loads(raw))
            except Exception:
                continue

        return results

    def _extract_jobs_from_json_obj_v23(
        self,
        data: object,
        page_url: str,
        method: str,
    ) -> list[dict]:
        jobs: list[dict] = []
        queue = [data]

        while queue:
            node = queue.pop(0)
            if isinstance(node, list):
                queue.extend(node[:200])
                continue
            if not isinstance(node, dict):
                continue

            queue.extend(list(node.values())[:200])
            job = self._job_from_json_dict_v23(node, page_url, method)
            if job:
                jobs.append(job)
                if len(jobs) >= MAX_JOBS_PER_PAGE:
                    break

        return jobs

    def _job_from_json_dict_v23(self, node: dict, page_url: str, method: str) -> Optional[dict]:
        title = ""
        title_key = ""
        for key in ("title", "jobTitle", "positionTitle", "requisitionTitle", "name", "jobName"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                title = value.strip()
                title_key = key
                break

        title = html.unescape(title)
        title = self._normalize_title_v23(title)
        if not self._is_valid_title_v23(title):
            return None

        url_raw = None
        for key in (
            "url", "jobUrl", "jobURL", "applyUrl", "jobPostingUrl", "jobDetailUrl",
            "detailsUrl", "externalUrl", "canonicalUrl", "sourceUrl",
        ):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                url_raw = value.strip()
                break

        source_url = _resolve_url(url_raw, page_url) if isinstance(url_raw, str) else None
        if not source_url:
            source_url = page_url
        if self._is_non_job_url_v23(source_url):
            return None
        source_url = self._normalize_source_url_v23(source_url, page_url)

        key_names = " ".join(node.keys()).lower()
        job_key_hint = bool(
            re.search(r"job|position|posting|requisition|vacanc|opening", key_names)
            or any(k in node for k in ("jobId", "jobID", "jobPostingId", "requisitionId", "positionId", "jobAdId"))
        )
        strong_key_hint = any(
            k in node for k in (
                "jobId", "jobID", "jobPostingId", "requisitionId", "positionId",
                "jobDetailUrl", "applyUrl", "jobLocation", "employmentType", "externalPostedStartDate",
            )
        )
        jobposting_type = str(node.get("@type") or "").lower() == "jobposting"
        taxonomy_hint = bool(
            "department" in key_names
            or "office" in key_names
            or "filter" in key_names
            or "facet" in key_names
            or "category" in key_names
        )
        path_label_shape = {"id", "name", "label", "value", "path", "children", "parent"}
        low_keys = {str(k).strip().lower() for k in node.keys() if isinstance(k, str)}
        mostly_label_obj = bool(low_keys) and low_keys.issubset(path_label_shape)
        title_hint = self._title_has_job_signal_v23(title)
        detailish_url = bool(_JOB_DETAILISH_URL_PATTERN_V23.search(source_url))
        page_url_hint = self._is_job_like_url_v23(page_url)
        url_hint = self._is_job_like_url_v23(source_url)
        same_as_page = source_url == self._normalize_source_url_v23(page_url, page_url)

        if mostly_label_obj and not strong_key_hint:
            return None
        if taxonomy_hint and not (job_key_hint or strong_key_hint or detailish_url):
            return None
        if _COMPANY_CAREER_LABEL_PATTERN_V23.match(title) and not strong_key_hint:
            return None
        if len(title.split()) == 1 and not (title_hint or strong_key_hint or detailish_url):
            return None
        if title_key == "name" and not (title_hint or strong_key_hint or detailish_url or jobposting_type):
            return None
        if same_as_page and page_url_hint and not (title_hint and (job_key_hint or strong_key_hint or jobposting_type)):
            return None

        if not (job_key_hint or url_hint or strong_key_hint or jobposting_type):
            return None
        if not (title_hint or detailish_url or strong_key_hint or jobposting_type):
            return None
        if _CORPORATE_TITLE_PATTERN_V23.match(title):
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
        for key in ("description", "summary", "introduction", "previewText"):
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

    async def _extract_oracle_api_jobs_v23(self, page_url: str, html_body: str) -> list[dict]:
        """Oracle CX fallback: query requisition API when page HTML is SPA shell."""
        page_l = (page_url or "").lower()
        body_l = (html_body or "").lower()
        if "oraclecloud.com" not in page_l and "candidateexperience" not in body_l and "cx_config" not in body_l:
            return []

        parsed = urlparse(page_url)
        host = parsed.hostname or ""
        if not host:
            return []

        api_base = f"https://{host}"
        api_match = _ORACLE_API_BASE_PATTERN_V23.search(html_body or "")
        if api_match:
            api_base = api_match.group(1).rstrip("/")

        site_ids = self._oracle_site_ids_v23(page_url, html_body)
        if not site_ids:
            return []

        best_jobs: list[dict] = []
        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
                for site_id in site_ids[:8]:
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
                            raw_title = str(row.get("Title") or row.get("title") or "").strip()
                            title = self._normalize_title_v23(raw_title)
                            if not self._is_valid_title_v23(title):
                                continue
                            if not self._title_has_job_signal_v23(title):
                                continue

                            req_id = str(row.get("Id") or row.get("id") or "").strip()
                            dedupe_key = f"{title.lower()}::{req_id.lower()}"
                            if dedupe_key in seen_ids:
                                continue
                            seen_ids.add(dedupe_key)

                            src = (
                                f"https://{host}/hcmUI/CandidateExperience/en/sites/{site_id}/job/{req_id}"
                                if req_id else page_url
                            )
                            if self._is_non_job_url_v23(src):
                                src = page_url

                            location = " ".join(
                                p for p in (
                                    str(row.get("PrimaryLocation") or "").strip(),
                                    str(row.get("PrimaryLocationCountry") or "").strip(),
                                ) if p
                            ) or None

                            listed_date = str(row.get("PostedDate") or "").strip() or None
                            site_jobs.append(
                                {
                                    "title": title,
                                    "source_url": src,
                                    "location_raw": location[:200] if location else None,
                                    "salary_raw": None,
                                    "employment_type": None,
                                    "description": listed_date,
                                    "extraction_method": "tier0_oracle_api_v23",
                                    "extraction_confidence": 0.9,
                                }
                            )
                            page_count += 1
                            if len(site_jobs) >= MAX_JOBS_PER_PAGE:
                                break

                        if len(site_jobs) >= MAX_JOBS_PER_PAGE:
                            break
                        if page_count < 24:
                            break

                    if self._jobset_score_v23(site_jobs, page_url) > self._jobset_score_v23(best_jobs, page_url):
                        best_jobs = site_jobs
        except Exception:
            logger.exception("v2.3 oracle API fallback failed for %s", page_url)

        return self._dedupe_jobs_v23(best_jobs, page_url)

    def _oracle_site_ids_v23(self, page_url: str, html_body: str) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []

        def _add(value: str) -> None:
            v = (value or "").strip()
            if not v or v in seen:
                return
            seen.add(v)
            ordered.append(v)

        for m in _ORACLE_SITE_PATTERN_V23.finditer(page_url or ""):
            _add(m.group(1))
        for m in _ORACLE_SITE_PATTERN_V23.finditer(html_body or ""):
            _add(m.group(1))
        for m in _ORACLE_SITE_NUMBER_PATTERN_V23.finditer(html_body or ""):
            _add(m.group(1))
        for m in re.finditer(r"siteNumber=([A-Za-z0-9_]+)", html_body or "", re.IGNORECASE):
            _add(m.group(1))

        query_pairs = dict(parse_qsl(urlparse(page_url or "").query))
        if query_pairs.get("siteNumber"):
            _add(query_pairs.get("siteNumber", ""))

        # Oracle tenants often expose CX plus tenant-specific variants.
        base_ids = list(ordered)
        for base_id in base_ids:
            if re.fullmatch(r"CX(?:_\d+)?", base_id, flags=re.IGNORECASE):
                root = base_id.split("_", 1)[0]
                _add(root)
                for suffix in ("1001", "1002", "1003", "1004"):
                    _add(f"{root}_{suffix}")

        if not ordered:
            for fallback in ("CX", "CX_1001", "CX_1002"):
                _add(fallback)

        return ordered[:12]

    # ------------------------------------------------------------------
    # Link/accordion/heading fallbacks
    # ------------------------------------------------------------------

    def _extract_from_job_links_v23(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url) or page_url
            if self._is_non_job_url_v23(source_url):
                continue

            text = self._normalize_title_v23(_text(a_el) or (a_el.get("title") or ""))
            if not self._is_valid_title_v23(text):
                continue

            if _GENERIC_LISTING_LABEL_PATTERN_V23.match(text):
                continue
            if _CATEGORY_TITLE_PATTERN_V23.match(text) or _PHONE_TITLE_PATTERN_V23.match(text):
                continue
            if _CORPORATE_TITLE_PATTERN_V23.match(text):
                continue
            if _COMPANY_CAREER_LABEL_PATTERN_V23.match(text):
                continue

            parent = a_el.getparent()
            parent_text = _text(parent)[:1200] if parent is not None else ""
            combined_classes = _get_el_classes(a_el)
            if parent is not None:
                combined_classes += " " + _get_el_classes(parent)

            class_hint = bool(_CLASS_HINT_PATTERN_V23.search(combined_classes))
            url_hint = self._is_job_like_url_v23(source_url)
            detailish_url = bool(_JOB_DETAILISH_URL_PATTERN_V23.search(source_url))
            title_hint = self._title_has_job_signal_v23(text)
            context_hint = bool(
                re.search(r"apply|location|department|job ref|posted|closing|employment", parent_text, re.IGNORECASE)
            )

            if not (title_hint or url_hint or (class_hint and context_hint)):
                continue
            if url_hint and not (title_hint or context_hint or detailish_url):
                # Drop taxonomy/nav links like "Consultancy" living under /jobs* paths.
                continue
            if not title_hint and len(text.split()) == 1 and not detailish_url:
                continue
            if source_url == self._normalize_source_url_v23(page_url, page_url) and not (title_hint and context_hint):
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
                    "extraction_method": "tier2_links_v23",
                    "extraction_confidence": 0.72 if url_hint else 0.64,
                }
            )

        return self._dedupe_jobs_v23(jobs, page_url)

    def _extract_from_accordion_sections_v23(self, root: etree._Element, page_url: str) -> list[dict]:
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

            title = self._normalize_title_v23(_text(title_el[0]))
            if not self._is_valid_title_v23(title):
                continue

            if not self._title_has_job_signal_v23(title):
                continue

            link_el = item.xpath(".//a[@href]")
            link_href = link_el[0].get("href") if link_el else None
            source_url = _resolve_url(link_href, page_url) if link_href else page_url
            if source_url and self._is_non_job_url_v23(source_url):
                source_url = page_url

            item_text = _text(item)[:1800]

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url or page_url,
                    "location_raw": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": item_text[:5000] if len(item_text) > 80 else None,
                    "extraction_method": "tier2_accordion_v23",
                    "extraction_confidence": 0.68,
                }
            )

        return self._dedupe_jobs_v23(jobs, page_url)

    def _extract_from_heading_rows_v23(self, root: etree._Element, page_url: str) -> list[dict]:
        containers = root.xpath("//section | //div | //article | //main")
        jobs: list[dict] = []

        for container in containers[:250]:
            headings = container.xpath(".//h2 | .//h3 | .//h4")
            if len(headings) < 2:
                continue

            classes = _get_el_classes(container)
            container_text = _text(container)[:4000]
            apply_hits = len(re.findall(r"\bapply\b", container_text, re.IGNORECASE))
            has_row_hint = bool(_ROW_CLASS_PATTERN_V23.search(classes))
            if not has_row_hint and apply_hits == 0:
                continue

            local_jobs: list[dict] = []
            for h in headings[:40]:
                title = self._normalize_title_v23(_text(h))
                if not self._is_valid_title_v23(title):
                    continue
                if not self._title_has_job_signal_v23(title):
                    continue

                link = h.xpath(".//a[@href]")
                if not link:
                    link = h.xpath("following::a[@href][1]")
                href = link[0].get("href") if link else None
                source_url = _resolve_url(href, page_url) or page_url
                if self._is_non_job_url_v23(source_url):
                    source_url = page_url

                local_jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": None,
                        "salary_raw": None,
                        "employment_type": None,
                        "description": container_text[:5000] if len(container_text) > 120 else None,
                        "extraction_method": "tier2_heading_rows_v23",
                        "extraction_confidence": 0.66,
                    }
                )

            if len(local_jobs) >= 2:
                jobs.extend(local_jobs)

        return self._dedupe_jobs_v23(jobs, page_url)

    def _extract_from_card_buttons_v23(self, root: etree._Element, page_url: str) -> list[dict]:
        """Extract bootstrap-style collapsed cards where titles live in buttons."""
        cards = root.xpath(
            "//*[contains(@class,'card') and (contains(@class,'accordion') or "
            "contains(@class,'card_dipult') or contains(@class,'card-header'))]"
        )
        if len(cards) < 2:
            return []

        jobs: list[dict] = []
        for card in cards[:260]:
            button_nodes = card.xpath(".//button | .//a[contains(@class,'btn') or contains(@class,'collapsed')]")
            if not button_nodes:
                continue

            title = ""
            for btn in button_nodes[:3]:
                text = self._normalize_title_v23(_text(btn))
                if not self._is_valid_title_v23(text):
                    continue
                if not self._title_has_job_signal_v23(text):
                    continue
                title = text
                break
            if not title:
                continue

            link_node = card.xpath(".//a[@href and not(starts-with(@href,'#'))][1]")
            href = link_node[0].get("href") if link_node else None
            source_url = _resolve_url(href, page_url) or page_url
            if self._is_non_job_url_v23(source_url):
                source_url = page_url

            card_text = _text(card)[:2200]
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": card_text if len(card_text) > 80 else None,
                    "extraction_method": "tier2_card_buttons_v23",
                    "extraction_confidence": 0.74,
                }
            )

        return self._dedupe_jobs_v23(jobs, page_url)

    def _extract_from_repeating_rows_v23(self, root: etree._Element, page_url: str) -> list[dict]:
        """Extract repeated row classes with local heading/button titles."""
        row_groups: dict[str, list[etree._Element]] = defaultdict(list)
        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag not in {"li", "div", "article", "tr", "section"}:
                continue
            cls = _get_el_classes(el)
            if not cls or not _ROW_CLASS_PATTERN_V23.search(cls):
                continue
            sig = f"{tag}:{' '.join(cls.lower().split()[:2])}"
            row_groups[sig].append(el)

        jobs: list[dict] = []
        for _, rows in row_groups.items():
            if len(rows) < 3:
                continue
            for row in rows[:MAX_JOBS_PER_PAGE]:
                title_nodes = row.xpath(
                    ".//h1|.//h2|.//h3|.//h4|.//button[contains(@class,'collapsed')]"
                    "|.//*[contains(@class,'job-post-title')]"
                )
                if not title_nodes:
                    continue
                title = self._normalize_title_v23(_text(title_nodes[0]))
                if not self._is_valid_title_v23(title):
                    continue
                if not self._title_has_job_signal_v23(title):
                    continue

                link = row.xpath(".//a[@href and not(starts-with(@href,'#'))][1]")
                href = link[0].get("href") if link else None
                source_url = _resolve_url(href, page_url) or page_url
                if self._is_non_job_url_v23(source_url):
                    source_url = page_url

                row_text = _text(row)[:1800]
                jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": None,
                        "salary_raw": None,
                        "employment_type": None,
                        "description": row_text if len(row_text) > 70 else None,
                        "extraction_method": "tier2_repeating_rows_v23",
                        "extraction_confidence": 0.71,
                    }
                )

        return self._dedupe_jobs_v23(jobs, page_url)

    def _extract_from_heading_cta_cards_v23(self, root: etree._Element, page_url: str) -> list[dict]:
        """Extract repeated card tiles where heading is the title and CTA link is generic."""
        page_text = _text(root)[:12000]
        page_job_context = self._job_page_context_hint_v23(page_text, page_url)
        groups: dict[str, list[dict]] = defaultdict(list)

        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag not in {"div", "article", "li", "section"}:
                continue

            classes = _get_el_classes(el)
            headings = el.xpath(".//h2 | .//h3 | .//h4")
            if not headings:
                continue

            title = ""
            for heading in headings[:3]:
                candidate = self._normalize_title_v23(_text(heading))
                if not self._is_valid_title_v23(candidate):
                    continue
                if _COMPANY_CAREER_LABEL_PATTERN_V23.match(candidate):
                    continue
                title = candidate
                break
            if not title:
                continue

            links = el.xpath(".//a[@href and not(starts-with(@href,'#')) and not(starts-with(@href,'javascript:'))]")
            if not links:
                continue

            href = (links[0].get("href") or "").strip()
            source_url = self._normalize_source_url_v23(_resolve_url(href, page_url), page_url)
            if self._is_non_job_url_v23(source_url):
                continue

            cta_text = self._normalize_title_v23(_text(links[0]) or (links[0].get("title") or ""))
            cta_hint = bool(_CTA_TEXT_PATTERN_V23.search(cta_text))
            title_hint = self._title_has_job_signal_v23(title)
            url_hint = self._is_job_like_url_v23(source_url) or bool(_JOB_DETAILISH_URL_PATTERN_V23.search(source_url))
            class_hint = bool(_CARD_CONTAINER_HINT_PATTERN_V23.search(classes))

            if not (title_hint or url_hint or cta_hint or class_hint):
                continue

            sig_classes = " ".join(classes.lower().split()[:3]) if classes else ""
            sig = f"{tag}:{sig_classes}" if sig_classes else tag
            groups[sig].append(
                {
                    "title": title,
                    "source_url": source_url,
                    "row_text": _text(el)[:2600],
                    "title_hint": title_hint,
                    "url_hint": url_hint,
                    "cta_hint": cta_hint,
                }
            )

        jobs: list[dict] = []
        for entries in groups.values():
            if len(entries) < 3:
                continue

            title_hits = sum(1 for entry in entries if entry["title_hint"])
            url_hits = sum(1 for entry in entries if entry["url_hint"])
            cta_hits = sum(1 for entry in entries if entry["cta_hint"])
            unique_titles = len({entry["title"].lower() for entry in entries})
            unique_ratio = unique_titles / max(1, len(entries))

            if unique_ratio < 0.6:
                continue

            if not (
                title_hits >= max(2, int(len(entries) * 0.3))
                or (url_hits >= max(2, int(len(entries) * 0.4)) and page_job_context)
                or (cta_hits >= max(2, int(len(entries) * 0.4)) and page_job_context and title_hits >= 1)
            ):
                continue

            for entry in entries[:MAX_JOBS_PER_PAGE]:
                title = entry["title"]
                source_url = entry["source_url"]
                if source_url == self._normalize_source_url_v23(page_url, page_url) and not entry["title_hint"]:
                    continue
                jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": None,
                        "salary_raw": None,
                        "employment_type": None,
                        "description": entry["row_text"] if len(entry["row_text"]) > 80 else None,
                        "extraction_method": "tier2_heading_cta_cards_v23",
                        "extraction_confidence": 0.73 if entry["title_hint"] else 0.67,
                    }
                )

        return self._dedupe_jobs_v23(jobs, page_url)

    # ------------------------------------------------------------------
    # Selection / validation / helpers
    # ------------------------------------------------------------------

    def _pick_best_jobset_v23(
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
            deduped = self._dedupe_jobs_v23(jobs, page_url)
            if not deduped:
                continue

            score = self._jobset_score_v23(deduped, page_url)
            valid = self._passes_jobset_validation_v23(deduped, page_url)

            if label == "parent_v16":
                parent_score = score
                parent_jobs = deduped

            logger.debug(
                "v2.3 candidate %s: jobs=%d score=%.2f valid=%s",
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
            ((label, self._dedupe_jobs_v23(jobs, page_url)) for label, jobs in candidates),
            key=lambda item: len(item[1]),
            default=("", []),
        )
        return largest[0], largest[1][:MAX_JOBS_PER_PAGE]

    def _passes_jobset_validation_v23(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        page_norm = self._normalize_source_url_v23(page_url, page_url)
        titles = [self._normalize_title_v23(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if self._is_valid_title_v23(t)]
        if not titles:
            return False

        unique_ratio = len(set(t.lower() for t in titles)) / max(1, len(titles))
        if len(titles) > 2 and unique_ratio < 0.6:
            return False

        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V23.match(t.lower()))
        if reject_hits >= max(1, int(len(titles) * 0.35)):
            return False
        nav_hits = sum(
            1 for t in titles
            if _CATEGORY_TITLE_PATTERN_V23.match(t) or _PHONE_TITLE_PATTERN_V23.match(t) or _CORPORATE_TITLE_PATTERN_V23.match(t)
        )
        if nav_hits >= max(1, int(len(titles) * 0.25)):
            return False

        title_hits = sum(1 for t in titles if self._title_has_job_signal_v23(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url_v23(j.get("source_url") or page_url))
        external_url_hits = sum(
            1
            for j in jobs
            if self._normalize_source_url_v23(j.get("source_url"), page_url) != page_norm
            and self._is_job_like_url_v23(j.get("source_url") or page_url)
        )
        unique_external_urls = len(
            {
                self._normalize_source_url_v23(j.get("source_url"), page_url)
                for j in jobs
                if self._normalize_source_url_v23(j.get("source_url"), page_url) != page_norm
            }
        )
        detailish_url_hits = sum(
            1 for j in jobs if _JOB_DETAILISH_URL_PATTERN_V23.search(j.get("source_url") or page_url)
        )

        if len(titles) == 1:
            t = titles[0]
            src = jobs[0].get("source_url") or page_url
            return (
                (self._title_has_job_signal_v23(t) and not _GENERIC_LISTING_LABEL_PATTERN_V23.match(t))
                or bool(_JOB_DETAILISH_URL_PATTERN_V23.search(src))
            )

        if len(titles) <= 3:
            return title_hits >= 1 and (
                detailish_url_hits >= 1 or external_url_hits >= 1 or title_hits >= 2
            )

        return (
            title_hits >= max(2, int(len(titles) * 0.3))
            or detailish_url_hits >= max(1, int(len(titles) * 0.15))
            or (
                external_url_hits >= max(2, int(len(titles) * 0.3))
                and unique_external_urls >= max(2, int(len(titles) * 0.2))
                and (title_hits >= 1 or detailish_url_hits >= 1)
            )
            or (
                url_hits >= max(3, int(len(titles) * 0.5))
                and title_hits >= max(2, int(len(titles) * 0.2))
            )
        )

    def _jobset_score_v23(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        page_norm = self._normalize_source_url_v23(page_url, page_url)
        titles = [self._normalize_title_v23(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return 0.0

        count = len(titles)
        title_hits = sum(1 for t in titles if self._title_has_job_signal_v23(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url_v23(j.get("source_url") or page_url))
        external_url_hits = sum(
            1
            for j in jobs
            if self._normalize_source_url_v23(j.get("source_url"), page_url) != page_norm
            and self._is_job_like_url_v23(j.get("source_url") or page_url)
        )
        detailish_url_hits = sum(1 for j in jobs if _JOB_DETAILISH_URL_PATTERN_V23.search(j.get("source_url") or page_url))
        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V23.match(t.lower()))
        nav_hits = sum(
            1 for t in titles
            if _CATEGORY_TITLE_PATTERN_V23.match(t) or _PHONE_TITLE_PATTERN_V23.match(t) or _CORPORATE_TITLE_PATTERN_V23.match(t)
        )
        unique_titles = len(set(t.lower() for t in titles))

        score = count * 3.5
        score += title_hits * 2.2
        score += url_hits * 0.8
        score += external_url_hits * 1.8
        score += detailish_url_hits * 1.6
        score += unique_titles * 0.7
        score -= sum(1 for t in titles if _CONCAT_SUFFIX_PATTERN_V23.match(t)) * 2.2
        score -= reject_hits * 3.5
        score -= nav_hits * 4.0
        return score

    def _dedupe_jobs_v23(self, jobs: list[dict], page_url: str) -> list[dict]:
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for job in jobs:
            title = self._normalize_title_v23(job.get("title", ""))
            if not self._is_valid_title_v23(title):
                continue

            source_url = self._normalize_source_url_v23(job.get("source_url"), page_url)
            if self._is_non_job_url_v23(source_url):
                continue
            if _COMPANY_CAREER_LABEL_PATTERN_V23.match(title):
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

    def _normalize_title_v23(self, title: str) -> str:
        if not title:
            return ""
        t = html.unescape(" ".join(title.replace("\u00a0", " ").split()))
        t = t.strip(" |:-\u2013\u2022")
        t = re.sub(r"\s+\|\s+.*$", "", t)
        t = re.sub(r"[\u200b-\u200d\ufeff]", "", t)
        t = re.sub(r"\s{2,}", " ", t)
        if " - " in t and len(t) > 40:
            parts = [p.strip() for p in t.split(" - ") if p.strip()]
            rhs = parts[1] if len(parts) > 1 else ""
            rhs_loc_like = bool(
                rhs
                and (
                    "," in rhs
                    or re.search(r"\b(remote|hybrid|onsite|location|wfh|office)\b", rhs, re.IGNORECASE)
                    or len(rhs.split()) >= 3
                )
            )
            if parts and self._title_has_job_signal_v23(parts[0]) and rhs_loc_like:
                t = parts[0]
        concat_m = _CONCAT_SUFFIX_PATTERN_V23.match(t)
        if concat_m:
            lead = concat_m.group(1).strip()
            suffix = concat_m.group(2).strip()
            if self._title_has_job_signal_v23(lead) and not self._title_has_job_signal_v23(suffix):
                t = lead
        concat_generic = re.match(r"^(.+?[a-z\)])([A-Z][A-Za-z0-9,&/() -]{4,})$", t)
        if concat_generic:
            lead = concat_generic.group(1).strip()
            suffix = concat_generic.group(2).strip()
            if (
                self._title_has_job_signal_v23(lead)
                and not self._title_has_job_signal_v23(suffix)
                and ("," in suffix or " OR " in suffix or len(suffix.split()) >= 2)
            ):
                t = lead
        return t

    def _is_valid_title_v23(self, title: str) -> bool:
        if not title:
            return False
        if not TieredExtractorV16._is_valid_title_v16(title):
            if not _is_valid_title(title):
                return False

        t = title.strip()
        low = t.lower()
        if _REJECT_TITLE_PATTERN_V23.match(low):
            return False
        if _GENERIC_LISTING_LABEL_PATTERN_V23.match(t):
            return False
        if _CATEGORY_TITLE_PATTERN_V23.match(t):
            return False
        if _PHONE_TITLE_PATTERN_V23.match(t):
            return False
        if _CORPORATE_TITLE_PATTERN_V23.match(t):
            return False
        if _COMPANY_CAREER_LABEL_PATTERN_V23.match(t):
            return False

        words = t.split()
        if len(words) > 14:
            return False
        return True

    def _title_has_job_signal_v23(self, title: str) -> bool:
        if not title:
            return False
        if _title_has_job_noun(title):
            return True
        return bool(_TITLE_HINT_PATTERN_V23.search(title))

    def _normalize_source_url_v23(self, src: Optional[str], page_url: str) -> str:
        source_url = (src or "").strip()
        if not source_url:
            source_url = page_url
        if "#" in source_url:
            source_url = source_url.split("#", 1)[0]
        return source_url

    @staticmethod
    def _job_page_context_hint_v23(page_text: str, page_url: str) -> bool:
        combined = f"{(page_text or '')[:4000]} {page_url or ''}"
        return bool(
            re.search(
                r"\b(job|jobs|career|careers|vacanc(?:y|ies)|opening|position|"
                r"lowongan|karir|kerjaya|hiring|recruit)\b",
                combined,
                re.IGNORECASE,
            )
        )

    def _is_job_like_url_v23(self, src: str) -> bool:
        if not src:
            return False
        if self._is_non_job_url_v23(src):
            return False
        return bool(_JOB_URL_HINT_PATTERN_V23.search(src))

    def _is_non_job_url_v23(self, src: str) -> bool:
        return bool(_NON_JOB_URL_PATTERN_V23.search((src or "").lower()))
