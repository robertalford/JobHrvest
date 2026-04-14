"""
Tiered Extraction Engine v2.2 — direct from v1.6 with discovery-safe extraction upgrades.

High-impact changes:
1. Timeout-safe parent execution: call super().extract first, but recover with local
   fallbacks when parent enrichment/render phases stall.
2. Oracle CX API fallback: pull requisition rows directly from hcmRestApi when DOM is
   SPA-shell and listing rows are absent.
3. Better collapsed-layout extraction: repeated card/button and row-heading parsers for
   Bootstrap/Elementor/ATS accordion-like boards.
4. Stronger JSON/state validation: avoid company/nav labels and category taxonomies being
   treated as jobs.
5. Stronger false-positive controls for phone numbers, "X Jobs" category labels, and
   navigation-heavy link sets.
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


_TITLE_HINT_PATTERN_V22 = re.compile(
    r"\b(?:job|jobs|career|careers|vacanc(?:y|ies)|opening|openings|"
    r"position|positions|role|roles|internship|manager|engineer|developer|"
    r"officer|specialist|assistant|analyst|consultant|coordinator|executive|"
    r"technician|designer|administrator|accountant|recruit(?:er|ment)?|"
    r"director|chef|nurse|teacher|operator|supervisor|"
    r"gerente|director|vacantes?|empleo|trabajo|lowongan|karir|karier|"
    r"kerjaya|jawatan)\b",
    re.IGNORECASE,
)

_REJECT_TITLE_PATTERN_V22 = re.compile(
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

_GENERIC_LISTING_LABEL_PATTERN_V22 = re.compile(
    r"^(?:jobs?|careers?|vacancies|vacantes?|job\s+openings?|open\s+positions?|"
    r"bolsa\s+de\s+trabajo|alertas?\s+de\s+vacantes?|join\s+our\s+team)$",
    re.IGNORECASE,
)

_CATEGORY_TITLE_PATTERN_V22 = re.compile(
    r"^(?:all\s+jobs?|jobs?\s+by|browse\s+jobs?|view\s+jobs?|"
    r".{2,80}\s+jobs?|.{2,80}\s+vacancies?)$",
    re.IGNORECASE,
)

_PHONE_TITLE_PATTERN_V22 = re.compile(
    r"^(?:\+?\d[\d\s().-]{6,}|\d{2,4}\s?\d{3,5}\s?\d{3,5})$"
)

_CORPORATE_TITLE_PATTERN_V22 = re.compile(
    r"^(?:about|home|contact|consultancy|green\s+tomato|"
    r"company|our\s+company|our\s+values|our\s+culture)$",
    re.IGNORECASE,
)

_JOB_URL_HINT_PATTERN_V22 = re.compile(
    r"(?:/job|/jobs|/career|/careers|/position|/positions|/vacanc|"
    r"/opening|/openings|/requisition|/requisitions|"
    r"event=jobs\.|jobid=|portal\.na|candidateportal|/apply|/lowongan|/karir|/kerjaya)",
    re.IGNORECASE,
)

_JOB_DETAILISH_URL_PATTERN_V22 = re.compile(
    r"(?:/jobs?/[a-z0-9][^/?#]{3,}|/requisition[s]?/[a-z0-9][^/?#]{2,}|"
    r"[?&](?:jobid|job_id|requisitionid|positionid)=)",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V22 = re.compile(
    r"(?:/privacy|/terms|/cookie|/news|/blog|/about|/contact|/investor|"
    r"/help|/login|/logout|/register|/account|/feed(?:/|$)|/rss(?:/|$)|"
    r"/wp-login(?:\.php)?|/mydayforce/login(?:\.aspx)?|/comments/feed(?:/|$)|"
    r"event=jobs\.view(?:history|myforms|myemails|mytests|myinterviews)|"
    r"event=help\.|event=reg\.)",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN_V22 = re.compile(
    r"job|position|vacanc|opening|requisition|career|posting|listing|accordion",
    re.IGNORECASE,
)

_CLASS_HINT_PATTERN_V22 = re.compile(
    r"job|position|vacanc|opening|requisition|career|listing|accordion|card|item",
    re.IGNORECASE,
)

_SCRIPT_ASSIGNMENT_PATTERN_V22 = re.compile(
    r"(?:window\.[A-Za-z0-9_$.]+\s*=\s*|var\s+[A-Za-z0-9_$]+\s*=\s*)(\{.*\}|\[.*\])\s*;?",
    re.DOTALL,
)

_ORACLE_SITE_PATTERN_V22 = re.compile(r"/sites/([A-Za-z0-9_]+)/", re.IGNORECASE)
_ORACLE_SITE_NUMBER_PATTERN_V22 = re.compile(r"siteNumber\s*[:=]\s*['\"]([A-Za-z0-9_]+)", re.IGNORECASE)
_ORACLE_API_BASE_PATTERN_V22 = re.compile(r"apiBaseUrl\s*:\s*['\"](https?://[^'\"\\s]+)", re.IGNORECASE)


class TieredExtractorV22(TieredExtractorV16):
    """v2.2 extractor: v1.6-first with timeout-safe structured/API/card fallbacks."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # Required by agent rules: run parent extractor first.
        parent_jobs: list[dict] = []
        try:
            # Keep parent logic, but avoid full-phase timeout stalls on large sites.
            parent_jobs = await asyncio.wait_for(
                super().extract(career_page, company, html),
                timeout=28.0,
            )
        except asyncio.TimeoutError:
            logger.warning("v2.2 parent extractor timeout for %s; using local fallbacks", url)
        except Exception:
            logger.exception("v2.2 parent extractor failed for %s; using local fallbacks", url)

        parent_jobs = self._dedupe_jobs_v22(parent_jobs or [], url)

        root = _parse_html(html)
        candidates: list[tuple[str, list[dict]]] = []
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        oracle_api_jobs = await self._extract_oracle_api_jobs_v22(url, html)
        if oracle_api_jobs:
            candidates.append(("oracle_api_v22", oracle_api_jobs))

        structured_jobs = self._extract_structured_jobs_v22(html, url)
        if structured_jobs:
            candidates.append(("structured_v22", structured_jobs))

        script_jobs = self._extract_jobs_from_state_scripts_v22(html, url)
        if script_jobs:
            candidates.append(("state_script_v22", script_jobs))

        bound_jobs = self._extract_jobs_from_bound_props_v22(html, url)
        if bound_jobs:
            candidates.append(("bound_props_v22", bound_jobs))

        if root is not None:
            link_jobs = self._extract_from_job_links_v22(root, url)
            if link_jobs:
                candidates.append(("job_links_v22", link_jobs))

            accordion_jobs = self._extract_from_accordion_sections_v22(root, url)
            if accordion_jobs:
                candidates.append(("accordion_v22", accordion_jobs))

            card_button_jobs = self._extract_from_card_buttons_v22(root, url)
            if card_button_jobs:
                candidates.append(("card_buttons_v22", card_button_jobs))

            heading_jobs = self._extract_from_heading_rows_v22(root, url)
            if heading_jobs:
                candidates.append(("heading_rows_v22", heading_jobs))

            repeating_row_jobs = self._extract_from_repeating_rows_v22(root, url)
            if repeating_row_jobs:
                candidates.append(("repeating_rows_v22", repeating_row_jobs))

        best_label, best_jobs = self._pick_best_jobset_v22(candidates, url)
        if not best_jobs:
            return []

        if (
            best_label != "parent_v16"
            and len(best_jobs) >= MIN_JOBS_FOR_SUCCESS
            and len(best_jobs) <= 40
        ):
            # Enrich fallback output when we have a credible multi-job set.
            best_jobs = await self._enrich_from_detail_pages(best_jobs)
            best_jobs = self._dedupe_jobs_v22(best_jobs, url)

        return best_jobs[:MAX_JOBS_PER_PAGE]

    # ------------------------------------------------------------------
    # Structured / state-script fallbacks
    # ------------------------------------------------------------------

    def _extract_structured_jobs_v22(self, html: str, page_url: str) -> list[dict]:
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
            jobs.extend(self._extract_jobs_from_json_obj_v22(data, page_url, "tier0_jsonld_v22"))

        return self._dedupe_jobs_v22(jobs, page_url)

    def _extract_jobs_from_state_scripts_v22(self, html: str, page_url: str) -> list[dict]:
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
            for parsed in self._parse_json_blobs_v22(payload):
                jobs.extend(self._extract_jobs_from_json_obj_v22(parsed, page_url, "tier0_state_v22"))

        return self._dedupe_jobs_v22(jobs, page_url)

    def _extract_jobs_from_bound_props_v22(self, html_body: str, page_url: str) -> list[dict]:
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
            jobs.extend(self._extract_jobs_from_json_obj_v22(parsed, page_url, "tier0_bound_props_v22"))
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break
        return self._dedupe_jobs_v22(jobs, page_url)

    def _parse_json_blobs_v22(self, script_body: str) -> list[object]:
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
        for m in _SCRIPT_ASSIGNMENT_PATTERN_V22.finditer(body):
            raw = (m.group(1) or "").strip()
            if len(raw) < 2:
                continue
            try:
                results.append(json.loads(raw))
            except Exception:
                continue

        return results

    def _extract_jobs_from_json_obj_v22(
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
            job = self._job_from_json_dict_v22(node, page_url, method)
            if job:
                jobs.append(job)
                if len(jobs) >= MAX_JOBS_PER_PAGE:
                    break

        return jobs

    def _job_from_json_dict_v22(self, node: dict, page_url: str, method: str) -> Optional[dict]:
        title = ""
        for key in ("title", "jobTitle", "positionTitle", "requisitionTitle", "name", "jobName"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                title = value.strip()
                break

        title = html.unescape(title)
        title = self._normalize_title_v22(title)
        if not self._is_valid_title_v22(title):
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
        if self._is_non_job_url_v22(source_url):
            return None

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
        title_hint = self._title_has_job_signal_v22(title)

        if not (job_key_hint or self._is_job_like_url_v22(source_url) or strong_key_hint):
            return None
        if not (title_hint or self._is_job_like_url_v22(source_url) or strong_key_hint):
            return None
        if _CORPORATE_TITLE_PATTERN_V22.match(title):
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

    async def _extract_oracle_api_jobs_v22(self, page_url: str, html_body: str) -> list[dict]:
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
        api_match = _ORACLE_API_BASE_PATTERN_V22.search(html_body or "")
        if api_match:
            api_base = api_match.group(1).rstrip("/")

        site_ids = self._oracle_site_ids_v22(page_url, html_body)
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
                            title = self._normalize_title_v22(raw_title)
                            if not self._is_valid_title_v22(title):
                                continue
                            if not self._title_has_job_signal_v22(title):
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
                            if self._is_non_job_url_v22(src):
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
                                    "extraction_method": "tier0_oracle_api_v22",
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

                    if self._jobset_score_v22(site_jobs, page_url) > self._jobset_score_v22(best_jobs, page_url):
                        best_jobs = site_jobs
        except Exception:
            logger.exception("v2.2 oracle API fallback failed for %s", page_url)

        return self._dedupe_jobs_v22(best_jobs, page_url)

    def _oracle_site_ids_v22(self, page_url: str, html_body: str) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []

        def _add(value: str) -> None:
            v = (value or "").strip()
            if not v or v in seen:
                return
            seen.add(v)
            ordered.append(v)

        for m in _ORACLE_SITE_PATTERN_V22.finditer(page_url or ""):
            _add(m.group(1))
        for m in _ORACLE_SITE_PATTERN_V22.finditer(html_body or ""):
            _add(m.group(1))
        for m in _ORACLE_SITE_NUMBER_PATTERN_V22.finditer(html_body or ""):
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

    def _extract_from_job_links_v22(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url) or page_url
            if self._is_non_job_url_v22(source_url):
                continue

            text = self._normalize_title_v22(_text(a_el) or (a_el.get("title") or ""))
            if not self._is_valid_title_v22(text):
                continue

            if _GENERIC_LISTING_LABEL_PATTERN_V22.match(text):
                continue
            if _CATEGORY_TITLE_PATTERN_V22.match(text) or _PHONE_TITLE_PATTERN_V22.match(text):
                continue
            if _CORPORATE_TITLE_PATTERN_V22.match(text):
                continue

            parent = a_el.getparent()
            parent_text = _text(parent)[:1200] if parent is not None else ""
            combined_classes = _get_el_classes(a_el)
            if parent is not None:
                combined_classes += " " + _get_el_classes(parent)

            class_hint = bool(_CLASS_HINT_PATTERN_V22.search(combined_classes))
            url_hint = self._is_job_like_url_v22(source_url)
            detailish_url = bool(_JOB_DETAILISH_URL_PATTERN_V22.search(source_url))
            title_hint = self._title_has_job_signal_v22(text)
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
                    "extraction_method": "tier2_links_v22",
                    "extraction_confidence": 0.72 if url_hint else 0.64,
                }
            )

        return self._dedupe_jobs_v22(jobs, page_url)

    def _extract_from_accordion_sections_v22(self, root: etree._Element, page_url: str) -> list[dict]:
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

            title = self._normalize_title_v22(_text(title_el[0]))
            if not self._is_valid_title_v22(title):
                continue

            if not self._title_has_job_signal_v22(title):
                continue

            link_el = item.xpath(".//a[@href]")
            link_href = link_el[0].get("href") if link_el else None
            source_url = _resolve_url(link_href, page_url) if link_href else page_url
            if source_url and self._is_non_job_url_v22(source_url):
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
                    "extraction_method": "tier2_accordion_v22",
                    "extraction_confidence": 0.68,
                }
            )

        return self._dedupe_jobs_v22(jobs, page_url)

    def _extract_from_heading_rows_v22(self, root: etree._Element, page_url: str) -> list[dict]:
        containers = root.xpath("//section | //div | //article | //main")
        jobs: list[dict] = []

        for container in containers[:250]:
            headings = container.xpath(".//h2 | .//h3 | .//h4")
            if len(headings) < 2:
                continue

            classes = _get_el_classes(container)
            container_text = _text(container)[:4000]
            apply_hits = len(re.findall(r"\bapply\b", container_text, re.IGNORECASE))
            has_row_hint = bool(_ROW_CLASS_PATTERN_V22.search(classes))
            if not has_row_hint and apply_hits == 0:
                continue

            local_jobs: list[dict] = []
            for h in headings[:40]:
                title = self._normalize_title_v22(_text(h))
                if not self._is_valid_title_v22(title):
                    continue
                if not self._title_has_job_signal_v22(title):
                    continue

                link = h.xpath(".//a[@href]")
                if not link:
                    link = h.xpath("following::a[@href][1]")
                href = link[0].get("href") if link else None
                source_url = _resolve_url(href, page_url) or page_url
                if self._is_non_job_url_v22(source_url):
                    source_url = page_url

                local_jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": None,
                        "salary_raw": None,
                        "employment_type": None,
                        "description": container_text[:5000] if len(container_text) > 120 else None,
                        "extraction_method": "tier2_heading_rows_v22",
                        "extraction_confidence": 0.66,
                    }
                )

            if len(local_jobs) >= 2:
                jobs.extend(local_jobs)

        return self._dedupe_jobs_v22(jobs, page_url)

    def _extract_from_card_buttons_v22(self, root: etree._Element, page_url: str) -> list[dict]:
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
                text = self._normalize_title_v22(_text(btn))
                if not self._is_valid_title_v22(text):
                    continue
                if not self._title_has_job_signal_v22(text):
                    continue
                title = text
                break
            if not title:
                continue

            link_node = card.xpath(".//a[@href and not(starts-with(@href,'#'))][1]")
            href = link_node[0].get("href") if link_node else None
            source_url = _resolve_url(href, page_url) or page_url
            if self._is_non_job_url_v22(source_url):
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
                    "extraction_method": "tier2_card_buttons_v22",
                    "extraction_confidence": 0.74,
                }
            )

        return self._dedupe_jobs_v22(jobs, page_url)

    def _extract_from_repeating_rows_v22(self, root: etree._Element, page_url: str) -> list[dict]:
        """Extract repeated row classes with local heading/button titles."""
        row_groups: dict[str, list[etree._Element]] = defaultdict(list)
        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag not in {"li", "div", "article", "tr", "section"}:
                continue
            cls = _get_el_classes(el)
            if not cls or not _ROW_CLASS_PATTERN_V22.search(cls):
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
                title = self._normalize_title_v22(_text(title_nodes[0]))
                if not self._is_valid_title_v22(title):
                    continue
                if not self._title_has_job_signal_v22(title):
                    continue

                link = row.xpath(".//a[@href and not(starts-with(@href,'#'))][1]")
                href = link[0].get("href") if link else None
                source_url = _resolve_url(href, page_url) or page_url
                if self._is_non_job_url_v22(source_url):
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
                        "extraction_method": "tier2_repeating_rows_v22",
                        "extraction_confidence": 0.71,
                    }
                )

        return self._dedupe_jobs_v22(jobs, page_url)

    # ------------------------------------------------------------------
    # Selection / validation / helpers
    # ------------------------------------------------------------------

    def _pick_best_jobset_v22(
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
            deduped = self._dedupe_jobs_v22(jobs, page_url)
            if not deduped:
                continue

            score = self._jobset_score_v22(deduped, page_url)
            valid = self._passes_jobset_validation_v22(deduped, page_url)

            if label == "parent_v16":
                parent_score = score
                parent_jobs = deduped

            logger.debug(
                "v2.2 candidate %s: jobs=%d score=%.2f valid=%s",
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
            ((label, self._dedupe_jobs_v22(jobs, page_url)) for label, jobs in candidates),
            key=lambda item: len(item[1]),
            default=("", []),
        )
        return largest[0], largest[1][:MAX_JOBS_PER_PAGE]

    def _passes_jobset_validation_v22(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title_v22(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if self._is_valid_title_v22(t)]
        if not titles:
            return False

        unique_ratio = len(set(t.lower() for t in titles)) / max(1, len(titles))
        if len(titles) > 2 and unique_ratio < 0.6:
            return False

        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V22.match(t.lower()))
        if reject_hits >= max(1, int(len(titles) * 0.35)):
            return False
        nav_hits = sum(
            1 for t in titles
            if _CATEGORY_TITLE_PATTERN_V22.match(t) or _PHONE_TITLE_PATTERN_V22.match(t) or _CORPORATE_TITLE_PATTERN_V22.match(t)
        )
        if nav_hits >= max(1, int(len(titles) * 0.25)):
            return False

        title_hits = sum(1 for t in titles if self._title_has_job_signal_v22(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url_v22(j.get("source_url") or page_url))
        detailish_url_hits = sum(
            1 for j in jobs if _JOB_DETAILISH_URL_PATTERN_V22.search(j.get("source_url") or page_url)
        )

        if len(titles) == 1:
            t = titles[0]
            src = jobs[0].get("source_url") or page_url
            return (
                (self._title_has_job_signal_v22(t) and not _GENERIC_LISTING_LABEL_PATTERN_V22.match(t))
                or bool(_JOB_DETAILISH_URL_PATTERN_V22.search(src))
            )

        if len(titles) <= 3:
            return title_hits >= 1 and (detailish_url_hits >= 1 or url_hits >= 1 or title_hits >= 2)

        return (
            title_hits >= max(1, int(len(titles) * 0.2))
            or detailish_url_hits >= max(1, int(len(titles) * 0.15))
            or url_hits >= max(2, int(len(titles) * 0.25))
        )

    def _jobset_score_v22(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        titles = [self._normalize_title_v22(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return 0.0

        count = len(titles)
        title_hits = sum(1 for t in titles if self._title_has_job_signal_v22(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url_v22(j.get("source_url") or page_url))
        detailish_url_hits = sum(1 for j in jobs if _JOB_DETAILISH_URL_PATTERN_V22.search(j.get("source_url") or page_url))
        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V22.match(t.lower()))
        nav_hits = sum(
            1 for t in titles
            if _CATEGORY_TITLE_PATTERN_V22.match(t) or _PHONE_TITLE_PATTERN_V22.match(t) or _CORPORATE_TITLE_PATTERN_V22.match(t)
        )
        unique_titles = len(set(t.lower() for t in titles))

        score = count * 3.5
        score += title_hits * 2.2
        score += url_hits * 1.8
        score += detailish_url_hits * 1.6
        score += unique_titles * 0.7
        score -= reject_hits * 3.5
        score -= nav_hits * 4.0
        return score

    def _dedupe_jobs_v22(self, jobs: list[dict], page_url: str) -> list[dict]:
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for job in jobs:
            title = self._normalize_title_v22(job.get("title", ""))
            if not self._is_valid_title_v22(title):
                continue

            source_url = (job.get("source_url") or "").strip()
            if not source_url:
                source_url = page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]
            if self._is_non_job_url_v22(source_url):
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

    def _normalize_title_v22(self, title: str) -> str:
        if not title:
            return ""
        t = html.unescape(" ".join(title.replace("\u00a0", " ").split()))
        t = t.strip(" |:-\u2013\u2022")
        t = re.sub(r"\s+\|\s+.*$", "", t)
        t = re.sub(r"[\u200b-\u200d\ufeff]", "", t)
        t = re.sub(r"\s{2,}", " ", t)
        if " - " in t and len(t) > 40:
            parts = [p.strip() for p in t.split(" - ") if p.strip()]
            if parts and self._title_has_job_signal_v22(parts[0]):
                t = parts[0]
        return t

    def _is_valid_title_v22(self, title: str) -> bool:
        if not title:
            return False
        if not TieredExtractorV16._is_valid_title_v16(title):
            if not _is_valid_title(title):
                return False

        t = title.strip()
        low = t.lower()
        if _REJECT_TITLE_PATTERN_V22.match(low):
            return False
        if _GENERIC_LISTING_LABEL_PATTERN_V22.match(t):
            return False
        if _CATEGORY_TITLE_PATTERN_V22.match(t):
            return False
        if _PHONE_TITLE_PATTERN_V22.match(t):
            return False
        if _CORPORATE_TITLE_PATTERN_V22.match(t):
            return False

        words = t.split()
        if len(words) > 14:
            return False
        return True

    def _title_has_job_signal_v22(self, title: str) -> bool:
        if not title:
            return False
        if _title_has_job_noun(title):
            return True
        return bool(_TITLE_HINT_PATTERN_V22.search(title))

    def _is_job_like_url_v22(self, src: str) -> bool:
        if not src:
            return False
        if self._is_non_job_url_v22(src):
            return False
        return bool(_JOB_URL_HINT_PATTERN_V22.search(src))

    def _is_non_job_url_v22(self, src: str) -> bool:
        return bool(_NON_JOB_URL_PATTERN_V22.search((src or "").lower()))
