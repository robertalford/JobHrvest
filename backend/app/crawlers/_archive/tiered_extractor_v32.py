"""
Tiered Extraction Engine v3.2 — direct from v1.6 with structured-source recovery.

Design goals:
1. Keep v1.6 as a stable baseline candidate.
2. Recover JS-shell boards via API/JSON fallbacks (Martian + Oracle + JSON-LD).
3. Keep false positives low with stricter role-title and URL/context validation.
4. Recover document-based vacancies ("Job description | ...") with strict guards.
"""

from __future__ import annotations

import asyncio
import html as html_lib
import json
import logging
import re
from collections import deque
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


_ROLE_HINT_PATTERN_V32 = re.compile(
    r"\b(?:engineer|developer|manager|director|analyst|specialist|assistant|"
    r"consultant|coordinator|officer|administrator|accountant|technician|"
    r"designer|architect|operator|supervisor|nurse|teacher|chef|driver|"
    r"recruit(?:er|ment)?|executive|intern(?:ship)?|graduate|trainee|"
    r"scientist|chemist|metallurgist|geologist|mechanic|electrician|plumber|"
    r"welder|fabricator|process|maintenance|influencer|fotografer|videografer|"
    r"akuntan|konsultan|asisten|staf|staff|pegawai|karyawan|psycholog|psikolog(?:i)?|"
    r"sarjana|customer\s+service|activator|volunteer|outreach|model)\b",
    re.IGNORECASE,
)

_REJECT_TITLE_PATTERN_V32 = re.compile(
    r"^(?:"
    r"jobs?|careers?|open\s+roles?|open\s+positions?|all\s+jobs?|current\s+jobs?|"
    r"current\s+vacancies|job\s+openings?|search\s+jobs?|browse\s+jobs?|"
    r"view\s+all\s+jobs?|join\s+our\s+team|career\s+opportunities|"
    r"our\s+values|our\s+story|talent\s+stories?|culture|about\s+us|contact|"
    r"privacy|terms|cookie|login|register|sign\s+in|sign\s+up|"
    r"apply(?:\s+now|\s+here)?|read\s+more|learn\s+more|show\s+more|info\s+lengkap|"
    r"job\s+description|internship\s+details|no\s+jobs?\s+found|"
    r"working\s+at\b.*|latest\s+news|main\s+menu|header|footer|home|"
    r"lowongan\s+kerja(?:\s+\w+){0,3}"
    r")$",
    re.IGNORECASE,
)

_GENERIC_LISTING_LABEL_PATTERN_V32 = re.compile(
    r"^(?:jobs?|careers?|vacanc(?:y|ies)|openings?|positions?|roles?|"
    r"join\s+our\s+team|search\s+jobs|current\s+vacancies)$",
    re.IGNORECASE,
)

_DOCUMENT_NON_JOB_PATTERN_V32 = re.compile(
    r"(?:annual\s+report|charity|registration|policy|newsletter|brochure|"
    r"board\s+nominations?|minutes|agenda|financial|media\s+release)",
    re.IGNORECASE,
)

_JOB_URL_HINT_PATTERN_V32 = re.compile(
    r"(?:/jobs?/[^/?#]{3,}|/jobview/|/position|/positions|/vacanc|/opening|"
    r"/requisition|/requisitions|jobid=|job_id=|requisitionid=|positionid=|"
    r"/candidateportal|/portal\.na|applicationform\?jobadid=|"
    r"/join-our-team/[a-z0-9]{10,}|/p/[a-z0-9_-]{6,}|/embed-jobs|/jobs/search|"
    r"/lowongan|/karir|/karier)",
    re.IGNORECASE,
)

_JOB_DETAIL_URL_PATTERN_V32 = re.compile(
    r"(?:/jobview/[a-z0-9-]+/[0-9a-f-]{8,}|/jobs?/[a-z0-9][^/?#]{5,}|"
    r"/join-our-team/[a-z0-9]{10,}|/p/[a-z0-9_-]{6,}|"
    r"[?&](?:jobid|job_id|requisitionid|positionid)=[A-Za-z0-9_-]{2,}|"
    r"applicationform\?jobadid=[A-Za-z0-9_-]{3,}|/job/[A-Za-z0-9_-]{2,})",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V32 = re.compile(
    r"(?:/(?:privacy|terms|cookie|news|blog|about|contact|investor|help|"
    r"login|logout|register|account|team|culture|events?|services?|products?)"
    r"(?:/|$|[?#])|wp-json|/feed(?:/|$)|/rss(?:/|$)|"
    r"/(?:facebook|twitter|linkedin|instagram|youtube)(?:/|$|[?#]))",
    re.IGNORECASE,
)

_DOCUMENT_URL_PATTERN_V32 = re.compile(r"\.(?:pdf|docx?|rtf)(?:$|\?)", re.IGNORECASE)

_APPLY_CONTEXT_PATTERN_V32 = re.compile(
    r"(?:apply|application|apply\s+now|apply\s+here|job\s+description|"
    r"position\s+description|requirements?|qualifications?|closing\s+date|"
    r"full\s*time|part\s*time|contract|permanent|temporary|remote|hybrid|"
    r"cara\s+melamar|how\s+to\s+apply|info\s+lengkap|more\s+details)",
    re.IGNORECASE,
)

_DESCRIPTION_CUT_PATTERN_V32 = re.compile(
    r"\b(?:how\s+to\s+apply|cara\s+melamar|application\s+process|"
    r"instructions?\s+to\s+apply)\b",
    re.IGNORECASE,
)

_TITLE_PHONE_PATTERN_V32 = re.compile(r"^[\d\s\-\+\(\)\.]{7,}$")
_TITLE_MOSTLY_NUMERIC_PATTERN_V32 = re.compile(r"^[\d\s\-\.\,\#\:\/]{4,}$")
_NEXT_DATA_PATTERN_V32 = re.compile(
    r"<script[^>]+id=['\"]__NEXT_DATA__['\"][^>]*>\s*(\{.*?\})\s*</script>",
    re.IGNORECASE | re.DOTALL,
)
_MARTIAN_CLIENT_PATTERN_V32 = re.compile(r'"clientCode"\s*:\s*"([a-z0-9-]{3,})"', re.IGNORECASE)
_MARTIAN_RECRUITER_PATTERN_V32 = re.compile(r'"recruiterId"\s*:\s*([0-9]{2,})', re.IGNORECASE)
_MARTIAN_THEME_PATTERN_V32 = re.compile(r'"jobBoardThemeId"\s*:\s*([0-9]{2,})', re.IGNORECASE)
_MARTIAN_NAME_PATTERN_V32 = re.compile(r'"name"\s*:\s*"([^"]{2,60})"', re.IGNORECASE)
_ORACLE_SITE_PATTERN_V32 = re.compile(r"/sites/([A-Za-z0-9_]+)/", re.IGNORECASE)
_ORACLE_SITE_NUMBER_PATTERN_V32 = re.compile(r"siteNumber\s*[:=]\s*['\"]([A-Za-z0-9_]+)", re.IGNORECASE)
_ORACLE_API_BASE_PATTERN_V32 = re.compile(r"apiBaseUrl\s*:\s*['\"](https?://[^'\"\s]+)", re.IGNORECASE)


class TieredExtractorV32(TieredExtractorV16):
    """v3.2 extractor with structured/API fallbacks and stricter validation."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""
        candidates: list[tuple[str, list[dict]]] = []

        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(super().extract(career_page, company, working_html), timeout=24.0)
        except asyncio.TimeoutError:
            logger.warning("v3.2 parent extractor timeout for %s", url)
        except Exception:
            logger.exception("v3.2 parent extractor failed for %s", url)

        parent_jobs = self._dedupe_jobs_v32(parent_jobs or [], url)
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        jsonld_jobs = self._extract_jsonld_jobs_v32(working_html, url)
        if jsonld_jobs:
            candidates.append(("jsonld_v32", jsonld_jobs))

        root = _parse_html(working_html)
        if root is not None:
            elementor_jobs = self._extract_elementor_cards_v32(root, url)
            if elementor_jobs:
                candidates.append(("elementor_cards_v32", elementor_jobs))

            job_link_jobs = self._extract_job_links_v32(root, url)
            if job_link_jobs:
                candidates.append(("job_links_v32", job_link_jobs))

            doc_jobs = self._extract_document_jobs_v32(root, url)
            if doc_jobs:
                candidates.append(("document_jobs_v32", doc_jobs))

        martian_jobs = await self._extract_martian_jobs_v32(url, working_html)
        if martian_jobs:
            candidates.append(("martian_api_v32", martian_jobs))

        oracle_jobs = await self._extract_oracle_jobs_v32(url, working_html)
        if oracle_jobs:
            candidates.append(("oracle_api_v32", oracle_jobs))

        best_label, best_jobs = self._pick_best_jobset_v32(candidates, url)
        if not best_jobs:
            return []

        if best_label != "parent_v16":
            enrichable = [
                job for job in best_jobs
                if not _DOCUMENT_URL_PATTERN_V32.search(job.get("source_url") or "")
            ]
            if enrichable:
                try:
                    enriched = await asyncio.wait_for(self._enrich_from_detail_pages(enrichable), timeout=18.0)
                    if enriched:
                        merged: list[dict] = []
                        enriched_keys = {
                            (self._normalize_title_v32(j.get("title", "")).lower(), (j.get("source_url") or "").lower())
                            for j in enriched
                        }
                        merged.extend(enriched)
                        for job in best_jobs:
                            key = (
                                self._normalize_title_v32(job.get("title", "")).lower(),
                                (job.get("source_url") or "").lower(),
                            )
                            if key not in enriched_keys:
                                merged.append(job)
                        best_jobs = self._dedupe_jobs_v32(merged, url)
                except asyncio.TimeoutError:
                    logger.warning("v3.2 enrichment timeout for %s", url)
                except Exception:
                    logger.exception("v3.2 enrichment failed for %s", url)

        return best_jobs[:MAX_JOBS_PER_PAGE]

    # ------------------------------------------------------------------
    # Candidate extraction
    # ------------------------------------------------------------------

    def _extract_elementor_cards_v32(self, root: etree._Element, page_url: str) -> list[dict]:
        cards = root.xpath("//*[contains(@class,'elementor-column') and contains(@class,'elementor-inner-column')]")
        if len(cards) < 2:
            return []

        jobs: list[dict] = []
        for card in cards[:360]:
            heading_nodes = card.xpath(
                ".//h1[contains(@class,'elementor-heading-title')] | "
                ".//h2[contains(@class,'elementor-heading-title')] | "
                ".//h3[contains(@class,'elementor-heading-title')]"
            )
            if not heading_nodes:
                continue

            title = self._normalize_title_v32(_text(heading_nodes[0]))
            if not self._is_valid_title_v32(title):
                continue
            if not self._title_has_role_signal_v32(title):
                continue

            source_url = page_url
            for a_el in card.xpath(".//a[@href]"):
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v32(href):
                    continue
                if _DOCUMENT_URL_PATTERN_V32.search(href):
                    continue
                if self._is_detail_url_v32(href):
                    source_url = href
                    break
                if source_url == page_url and self._is_job_like_url_v32(href):
                    source_url = href
                elif source_url == page_url:
                    source_url = href

            card_text = _text(card)[:3200]
            apply_hint = bool(_APPLY_CONTEXT_PATTERN_V32.search(card_text))
            if source_url.rstrip("/") == page_url.rstrip("/") and not apply_hint:
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v32(card, title),
                    "salary_raw": self._extract_salary_v32(card_text),
                    "employment_type": self._extract_employment_type_v32(card_text),
                    "description": self._clean_description_v32(card_text),
                    "extraction_method": "tier2_elementor_cards_v32",
                    "extraction_confidence": 0.74 if self._is_detail_url_v32(source_url) else 0.69,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        jobs = self._dedupe_jobs_v32(jobs, page_url)
        if not self._passes_jobset_validation_v32(jobs, page_url):
            return []
        return jobs

    def _extract_job_links_v32(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        anchors = root.xpath("//a[@href]")

        for a_el in anchors[:7000]:
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url)
            if not source_url or self._is_non_job_url_v32(source_url):
                continue
            if _DOCUMENT_URL_PATTERN_V32.search(source_url):
                continue

            heading_nodes = a_el.xpath(".//h1 | .//h2 | .//h3 | .//h4 | .//h5")
            title_raw = _text(heading_nodes[0]) if heading_nodes else (_text(a_el) or (a_el.get("title") or ""))
            title = self._normalize_title_v32(title_raw)
            if not self._is_valid_title_v32(title):
                continue

            context_el = a_el
            for _ in range(4):
                parent = context_el.getparent()
                if parent is None:
                    break
                parent_text = _text(parent)
                classes = _get_el_classes(parent)
                if len(parent_text) >= 180 or re.search(r"(?:job|position|vacanc|listing|opening|requisition)", classes):
                    context_el = parent
                    break
                context_el = parent

            context_text = _text(context_el)[:3000]
            apply_hint = bool(_APPLY_CONTEXT_PATTERN_V32.search(context_text))
            detail_hint = self._is_detail_url_v32(source_url)
            job_url_hint = self._is_job_like_url_v32(source_url)
            same_page = source_url.rstrip("/") == page_url.rstrip("/")
            has_role = self._title_has_role_signal_v32(title)

            if _GENERIC_LISTING_LABEL_PATTERN_V32.match(title):
                continue
            if same_page and not apply_hint:
                continue
            if not has_role and not detail_hint:
                continue
            if has_role and not (detail_hint or job_url_hint or apply_hint):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v32(context_el, title),
                    "salary_raw": self._extract_salary_v32(context_text),
                    "employment_type": self._extract_employment_type_v32(context_text),
                    "description": self._clean_description_v32(context_text),
                    "extraction_method": "tier2_job_links_v32",
                    "extraction_confidence": 0.76 if detail_hint else 0.7,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        jobs = self._dedupe_jobs_v32(jobs, page_url)
        if not self._passes_jobset_validation_v32(jobs, page_url):
            return []
        return jobs

    def _extract_document_jobs_v32(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        for a_el in root.xpath("//a[@href]")[:3200]:
            href = _resolve_url(a_el.get("href"), page_url)
            if not href or not _DOCUMENT_URL_PATTERN_V32.search(href):
                continue

            link_text = " ".join(_text(a_el).split())
            if not link_text:
                continue
            if _DOCUMENT_NON_JOB_PATTERN_V32.search(link_text):
                continue

            title_candidate = link_text
            if "|" in title_candidate and re.search(r"(?:job|position)\s+description", title_candidate, re.IGNORECASE):
                title_candidate = title_candidate.split("|", 1)[1]
            title_candidate = re.sub(r"\.(?:pdf|docx?|rtf)\s*$", "", title_candidate, flags=re.IGNORECASE).strip()

            if len(title_candidate) < 5:
                parent = a_el.getparent()
                if parent is not None:
                    heading_nodes = parent.xpath(".//h3 | .//h4 | .//h2")
                    if heading_nodes:
                        title_candidate = _text(heading_nodes[0]).strip()

            title = self._normalize_title_v32(title_candidate)
            if not self._is_valid_title_v32(title):
                continue
            if not self._title_has_role_signal_v32(title):
                continue
            if _DOCUMENT_NON_JOB_PATTERN_V32.search(title):
                continue

            row = a_el.getparent() if a_el.getparent() is not None else a_el
            row_text = _text(row)[:2600]
            if not (
                re.search(r"(?:job|position)\s+description", link_text, re.IGNORECASE)
                or _APPLY_CONTEXT_PATTERN_V32.search(row_text)
                or self._title_has_role_signal_v32(title)
            ):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": href,
                    "location_raw": self._extract_location_v32(row, title),
                    "salary_raw": self._extract_salary_v32(row_text),
                    "employment_type": self._extract_employment_type_v32(row_text),
                    "description": self._clean_description_v32(row_text),
                    "extraction_method": "tier2_document_jobs_v32",
                    "extraction_confidence": 0.7,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        jobs = self._dedupe_jobs_v32(jobs, page_url)
        if not self._passes_jobset_validation_v32(jobs, page_url):
            return []
        return jobs

    def _extract_jsonld_jobs_v32(self, html_body: str, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        for match in re.finditer(
            r"<script[^>]+type=['\"]application/ld\+json['\"][^>]*>(.*?)</script>",
            html_body or "",
            re.IGNORECASE | re.DOTALL,
        ):
            payload = html_lib.unescape((match.group(1) or "").strip())
            if not payload:
                continue
            try:
                parsed = json.loads(payload)
            except Exception:
                continue
            jobs.extend(
                self._extract_jobs_from_json_obj_v32(
                    parsed,
                    page_url,
                    method="tier0_jsonld_v32",
                    require_job_type=True,
                )
            )

        jobs = self._dedupe_jobs_v32(jobs, page_url)
        if not self._passes_jobset_validation_v32(jobs, page_url):
            return []
        return jobs

    # ------------------------------------------------------------------
    # API fallbacks
    # ------------------------------------------------------------------

    async def _extract_martian_jobs_v32(self, page_url: str, html_body: str) -> list[dict]:
        lower = (html_body or "").lower()
        if (
            "__next_data__" not in lower
            and "myrecruitmentplus" not in lower
            and "martianlogic" not in lower
            and "clientcode" not in lower
        ):
            return []

        context = self._extract_martian_context_v32(html_body, page_url)
        client_code = context.get("client_code") or ""
        if not client_code:
            return []

        endpoints = self._martian_probe_urls_v32(page_url, context)
        if not endpoints:
            return []

        jobs: list[dict] = []
        request_count = 0
        max_requests = 36

        try:
            async with httpx.AsyncClient(timeout=7, follow_redirects=True) as client:
                for endpoint in endpoints:
                    for probe_url in self._martian_paged_variants_v32(endpoint):
                        if request_count >= max_requests:
                            break
                        request_count += 1
                        try:
                            resp = await client.get(probe_url)
                        except Exception:
                            continue
                        if resp.status_code >= 400 or not resp.text:
                            continue

                        extracted = self._extract_jobs_from_probe_payload_v32(resp.text, str(resp.url), page_url)
                        if extracted:
                            jobs.extend(extracted)
                            if len(jobs) >= 140:
                                break
                    if request_count >= max_requests or len(jobs) >= 140:
                        break
        except Exception:
            logger.debug("v3.2 martian probing failed for %s", page_url)

        jobs = self._dedupe_jobs_v32(jobs, page_url)
        if not self._passes_jobset_validation_v32(jobs, page_url):
            return []
        return jobs

    async def _extract_oracle_jobs_v32(self, page_url: str, html_body: str) -> list[dict]:
        page_l = (page_url or "").lower()
        body_l = (html_body or "").lower()
        if "oraclecloud.com" not in page_l and "candidateexperience" not in body_l and "hcmrestapi" not in body_l:
            return []

        parsed = urlparse(page_url)
        if not parsed.netloc:
            return []

        api_base = f"{parsed.scheme or 'https'}://{parsed.netloc}"
        api_match = _ORACLE_API_BASE_PATTERN_V32.search(html_body or "")
        if api_match:
            api_base = api_match.group(1).rstrip("/")

        site_ids = self._oracle_site_ids_v32(page_url, html_body)
        if not site_ids:
            return []

        jobs: list[dict] = []
        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
                for site_id in site_ids[:10]:
                    for offset in range(0, 264, 24):
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
                        if resp.status_code >= 400:
                            break
                        try:
                            data = resp.json()
                        except Exception:
                            break

                        batch = self._extract_oracle_items_v32(data, page_url, site_id)
                        if not batch:
                            break
                        jobs.extend(batch)
                        if len(batch) < 24:
                            break
                        if len(jobs) >= MAX_JOBS_PER_PAGE:
                            break
                    if len(jobs) >= MAX_JOBS_PER_PAGE:
                        break
        except Exception:
            logger.debug("v3.2 oracle probing failed for %s", page_url)

        jobs = self._dedupe_jobs_v32(jobs, page_url)
        if not self._passes_jobset_validation_v32(jobs, page_url):
            return []
        return jobs

    # ------------------------------------------------------------------
    # Probe / JSON parsing
    # ------------------------------------------------------------------

    def _extract_jobs_from_probe_payload_v32(self, body: str, response_url: str, page_url: str) -> list[dict]:
        payload = (body or "").strip()
        if not payload:
            return []

        jobs: list[dict] = []
        if payload.startswith("{") or payload.startswith("["):
            try:
                parsed = json.loads(payload)
                jobs.extend(self._extract_jobs_from_json_obj_v32(parsed, response_url, method="tier0_api_json_v32"))
            except Exception:
                pass

        root = _parse_html(payload)
        if root is not None:
            jobs.extend(self._extract_job_links_v32(root, response_url))
            jobs.extend(self._extract_elementor_cards_v32(root, response_url))
            jobs.extend(self._extract_document_jobs_v32(root, response_url))
            tier2 = self._extract_tier2_v16(response_url, payload) or []
            for job in tier2:
                cloned = dict(job)
                cloned["extraction_method"] = "tier2_heuristic_v16_probe_v32"
                jobs.append(cloned)

        return self._dedupe_jobs_v32(jobs, page_url)

    def _extract_jobs_from_json_obj_v32(
        self,
        data: object,
        page_url: str,
        method: str,
        require_job_type: bool = False,
    ) -> list[dict]:
        jobs: list[dict] = []
        queue: deque[object] = deque([data])
        visited = 0

        while queue and visited < 8000:
            node = queue.popleft()
            visited += 1

            if isinstance(node, list):
                queue.extend(node[:260])
                continue
            if not isinstance(node, dict):
                continue

            queue.extend(list(node.values())[:260])
            job = self._job_from_json_dict_v32(node, page_url, method, require_job_type=require_job_type)
            if job:
                jobs.append(job)
                if len(jobs) >= MAX_JOBS_PER_PAGE:
                    break

        return jobs

    def _job_from_json_dict_v32(
        self,
        node: dict[str, Any],
        page_url: str,
        method: str,
        require_job_type: bool = False,
    ) -> Optional[dict]:
        node_type_raw = node.get("@type")
        node_type = ""
        if isinstance(node_type_raw, str):
            node_type = node_type_raw.strip().lower()
        elif isinstance(node_type_raw, list):
            node_type = " ".join(str(v).lower() for v in node_type_raw)
        is_jobposting = "jobposting" in node_type
        if require_job_type and not is_jobposting:
            return None

        title = ""
        for key in (
            "title",
            "jobTitle",
            "positionTitle",
            "requisitionTitle",
            "name",
            "jobName",
            "position",
            "job_title",
            "Title",
        ):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                title = value.strip()
                break
        title = self._normalize_title_v32(title)
        if not self._is_valid_title_v32(title):
            return None

        url_raw = None
        for key in (
            "url",
            "jobUrl",
            "jobURL",
            "applyUrl",
            "jobPostingUrl",
            "jobDetailUrl",
            "detailsUrl",
            "externalUrl",
            "canonicalUrl",
            "sourceUrl",
            "applicationFormUrl",
            "applicationUrl",
            "postingUrl",
            "jobLink",
            "ExternalURL",
            "PostingUrl",
        ):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                url_raw = value.strip()
                break

        source_url = _resolve_url(url_raw, page_url) if isinstance(url_raw, str) else page_url
        source_url = source_url or page_url

        strong_id = ""
        for key in (
            "jobId",
            "jobID",
            "jobPostingId",
            "requisitionId",
            "positionId",
            "jobAdId",
            "adId",
            "advertId",
            "referenceNumber",
            "Id",
            "id",
        ):
            value = node.get(key)
            if value in (None, ""):
                continue
            strong_id = str(value).strip()
            if strong_id:
                break
        if strong_id and source_url.rstrip("/") == page_url.rstrip("/"):
            joiner = "&" if "?" in page_url else "?"
            source_url = f"{page_url}{joiner}jobId={strong_id}"

        key_names = " ".join(str(k) for k in node.keys()).lower()
        job_key_hint = bool(re.search(r"job|position|posting|requisition|vacanc|opening|ad", key_names))
        has_role = self._title_has_role_signal_v32(title)
        detail_hint = self._is_detail_url_v32(source_url)
        job_url_hint = self._is_job_like_url_v32(source_url)

        if self._is_non_job_url_v32(source_url):
            return None
        if source_url.rstrip("/") == page_url.rstrip("/") and not (strong_id or is_jobposting):
            return None
        if _GENERIC_LISTING_LABEL_PATTERN_V32.match(title):
            return None
        if not has_role and not (strong_id and (job_url_hint or detail_hint or job_key_hint or is_jobposting)):
            return None
        if not (job_url_hint or detail_hint or strong_id or is_jobposting):
            return None

        description = ""
        for key in ("description", "summary", "shortDescription", "jobDescription", "Description"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                description = value.strip()
                break
        description = self._clean_description_v32(description)

        return {
            "title": title,
            "source_url": source_url,
            "location_raw": self._extract_location_from_json_v32(node),
            "salary_raw": self._extract_salary_from_json_v32(node),
            "employment_type": self._extract_employment_from_json_v32(node),
            "description": description,
            "extraction_method": method,
            "extraction_confidence": 0.88 if (detail_hint or strong_id or is_jobposting) else 0.78,
        }

    # ------------------------------------------------------------------
    # Candidate arbitration / validation
    # ------------------------------------------------------------------

    def _pick_best_jobset_v32(
        self,
        candidates: list[tuple[str, list[dict]]],
        page_url: str,
    ) -> tuple[str, list[dict]]:
        if not candidates:
            return "", []

        scored: list[tuple[str, list[dict], float]] = []
        parent_jobs: list[dict] = []
        parent_score = -1e9
        parent_valid = False

        for label, jobs in candidates:
            deduped = self._dedupe_jobs_v32(jobs, page_url)
            if not deduped:
                continue
            valid = self._passes_jobset_validation_v32(deduped, page_url)
            score = self._jobset_score_v32(deduped, page_url)
            logger.debug("v3.2 candidate %s: jobs=%d valid=%s score=%.2f", label, len(deduped), valid, score)

            if label == "parent_v16":
                parent_jobs = deduped
                parent_score = score
                parent_valid = valid

            if valid:
                scored.append((label, deduped, score))

        if not scored:
            if parent_jobs:
                return "parent_v16", parent_jobs[:MAX_JOBS_PER_PAGE]
            largest = max(
                ((label, self._dedupe_jobs_v32(jobs, page_url)) for label, jobs in candidates),
                key=lambda item: len(item[1]),
                default=("", []),
            )
            return largest[0], largest[1][:MAX_JOBS_PER_PAGE]

        best_label, best_jobs, best_score = max(scored, key=lambda item: item[2])

        for label, jobs, score in scored:
            if label == best_label:
                continue
            overlap = self._title_overlap_ratio_v32(jobs, best_jobs)
            if len(jobs) >= len(best_jobs) + 2 and overlap >= 0.6 and score >= best_score - 1.2:
                best_label, best_jobs, best_score = label, jobs, score

        if parent_jobs and parent_valid and best_label != "parent_v16":
            overlap = self._title_overlap_ratio_v32(best_jobs, parent_jobs)
            if not (len(best_jobs) >= len(parent_jobs) + 2 and overlap >= 0.55):
                if best_score < parent_score + 1.4:
                    return "parent_v16", parent_jobs[:MAX_JOBS_PER_PAGE]

        return best_label, best_jobs[:MAX_JOBS_PER_PAGE]

    def _passes_jobset_validation_v32(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title_v32(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if self._is_valid_title_v32(t)]
        if not titles:
            return False

        role_hits = sum(1 for t in titles if self._title_has_role_signal_v32(t))
        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V32.match(t))
        generic_hits = sum(1 for t in titles if _GENERIC_LISTING_LABEL_PATTERN_V32.match(t))
        detail_hits = sum(1 for j in jobs if self._is_detail_url_v32(j.get("source_url") or page_url))
        job_url_hits = sum(1 for j in jobs if self._is_job_like_url_v32(j.get("source_url") or page_url))
        apply_hits = sum(1 for j in jobs if _APPLY_CONTEXT_PATTERN_V32.search((j.get("description") or "")[:1800]))
        same_page_hits = sum(
            1 for j in jobs if (j.get("source_url") or page_url).rstrip("/") == page_url.rstrip("/")
        )
        doc_hits = sum(1 for j in jobs if _DOCUMENT_URL_PATTERN_V32.search(j.get("source_url") or ""))
        doc_method_hits = sum(
            1 for j in jobs if str(j.get("extraction_method") or "") == "tier2_document_jobs_v32"
        )

        if len(jobs) == 1:
            title = titles[0]
            url = jobs[0].get("source_url") or page_url
            if not self._title_has_role_signal_v32(title):
                return False
            if _DOCUMENT_URL_PATTERN_V32.search(url):
                return doc_method_hits == 1 and not _DOCUMENT_NON_JOB_PATTERN_V32.search(title)
            return bool(self._is_detail_url_v32(url) or self._is_job_like_url_v32(url) or apply_hits >= 1)

        unique_ratio = len(set(t.lower() for t in titles)) / max(1, len(titles))
        if len(titles) >= 4 and unique_ratio < 0.62:
            return False
        if reject_hits >= max(1, int(len(titles) * 0.12)):
            return False
        if generic_hits >= max(1, int(len(titles) * 0.15)):
            return False

        role_ratio = role_hits / max(1, len(titles))
        detail_ratio = (detail_hits + job_url_hits) / max(1, len(titles))
        if role_ratio < 0.65 and not (role_ratio >= 0.55 and detail_ratio >= 0.6):
            return False

        if detail_ratio < 0.28 and apply_hits < max(1, int(len(titles) * 0.4)):
            return False
        if same_page_hits >= max(2, int(len(titles) * 0.82)) and apply_hits < max(1, int(len(titles) * 0.6)):
            return False

        if doc_hits > 0:
            if doc_method_hits < doc_hits:
                return False
            if role_hits < max(1, int(len(titles) * 0.7)):
                return False

        return len(titles) >= MIN_JOBS_FOR_SUCCESS

    def _jobset_score_v32(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        titles = [self._normalize_title_v32(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return 0.0

        role_hits = sum(1 for t in titles if self._title_has_role_signal_v32(t))
        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V32.match(t))
        generic_hits = sum(1 for t in titles if _GENERIC_LISTING_LABEL_PATTERN_V32.match(t))
        detail_hits = sum(1 for j in jobs if self._is_detail_url_v32(j.get("source_url") or page_url))
        job_url_hits = sum(1 for j in jobs if self._is_job_like_url_v32(j.get("source_url") or page_url))
        apply_hits = sum(1 for j in jobs if _APPLY_CONTEXT_PATTERN_V32.search((j.get("description") or "")[:1800]))
        same_page_hits = sum(
            1 for j in jobs if (j.get("source_url") or page_url).rstrip("/") == page_url.rstrip("/")
        )
        rich_desc_hits = sum(1 for j in jobs if len((j.get("description") or "").strip()) >= 160)

        score = len(titles) * 4.4
        score += role_hits * 2.8
        score += detail_hits * 1.8
        score += job_url_hits * 1.2
        score += apply_hits * 1.1
        score += rich_desc_hits * 0.6
        score -= reject_hits * 5.0
        score -= generic_hits * 4.0
        if same_page_hits >= max(2, int(len(titles) * 0.8)) and apply_hits < max(1, int(len(titles) * 0.5)):
            score -= 8.0
        return score

    def _title_overlap_ratio_v32(self, a_jobs: list[dict], b_jobs: list[dict]) -> float:
        a_titles = {self._normalize_title_v32(j.get("title", "")).lower() for j in a_jobs if j.get("title")}
        b_titles = {self._normalize_title_v32(j.get("title", "")).lower() for j in b_jobs if j.get("title")}
        if not a_titles or not b_titles:
            return 0.0
        return len(a_titles & b_titles) / max(1, min(len(a_titles), len(b_titles)))

    def _dedupe_jobs_v32(self, jobs: list[dict], page_url: str) -> list[dict]:
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for raw in jobs:
            title = self._normalize_title_v32(raw.get("title", ""))
            if not self._is_valid_title_v32(title):
                continue

            source_url = (raw.get("source_url") or page_url).strip() or page_url
            source_url = _resolve_url(source_url, page_url) or page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]

            method = str(raw.get("extraction_method") or "")
            if self._is_non_job_url_v32(source_url):
                continue
            if _DOCUMENT_URL_PATTERN_V32.search(source_url) and method != "tier2_document_jobs_v32":
                continue

            key = (title.lower(), source_url.lower())
            if key in seen:
                continue
            seen.add(key)

            cloned = dict(raw)
            cloned["title"] = title
            cloned["source_url"] = source_url
            cloned["description"] = self._clean_description_v32(str(cloned.get("description") or ""))
            deduped.append(cloned)

            if len(deduped) >= MAX_JOBS_PER_PAGE:
                break

        return deduped

    # ------------------------------------------------------------------
    # Title / URL / text helpers
    # ------------------------------------------------------------------

    def _normalize_title_v32(self, title: str) -> str:
        t = html_lib.unescape((title or "").strip())
        t = re.sub(r"^[\-\u2022\s]+", "", t)
        t = re.sub(
            r"^(?:job|position)\s+description\s*[\|\:\-\u2013\u2014]\s*",
            "",
            t,
            flags=re.IGNORECASE,
        )
        t = t.replace("|", " - ")
        t = re.sub(r"\s+", " ", t)
        t = re.sub(r"\s*(?:read\s+more|learn\s+more|apply\s+now|apply\s+here|info\s+lengkap)\s*$", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\.(?:pdf|docx?|rtf)\s*$", "", t, flags=re.IGNORECASE)
        return t.strip(" \t\r\n-–—|:;,.")

    def _is_valid_title_v32(self, title: str) -> bool:
        if not title:
            return False
        t = title.strip()
        if len(t) < 4 or len(t) > 220:
            return False
        if _TITLE_PHONE_PATTERN_V32.match(t) or _TITLE_MOSTLY_NUMERIC_PATTERN_V32.match(t):
            return False
        if "@" in t and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", t):
            return False
        if not re.search(r"[A-Za-z]", t):
            return False
        if _REJECT_TITLE_PATTERN_V32.match(t):
            return False
        if _GENERIC_LISTING_LABEL_PATTERN_V32.match(t):
            return False

        base_valid = TieredExtractorV16._is_valid_title_v16(t)
        if base_valid:
            return True

        words = t.split()
        if len(words) == 1 and len(t) <= 40 and self._title_has_role_signal_v32(t):
            return True
        if len(words) <= 3 and self._title_has_role_signal_v32(t):
            return True
        return False

    def _title_has_role_signal_v32(self, title: str) -> bool:
        if not title:
            return False
        return _title_has_job_noun(title) or bool(_ROLE_HINT_PATTERN_V32.search(title))

    def _is_job_like_url_v32(self, url: str) -> bool:
        value = (url or "").strip()
        if not value:
            return False
        if _DOCUMENT_URL_PATTERN_V32.search(value):
            return False
        if self._is_non_job_url_v32(value):
            return False
        return bool(_JOB_URL_HINT_PATTERN_V32.search(value) or _JOB_DETAIL_URL_PATTERN_V32.search(value))

    def _is_detail_url_v32(self, url: str) -> bool:
        value = (url or "").strip()
        if not value:
            return False
        if _DOCUMENT_URL_PATTERN_V32.search(value):
            return False
        if self._is_non_job_url_v32(value):
            return False
        return bool(_JOB_DETAIL_URL_PATTERN_V32.search(value))

    def _is_non_job_url_v32(self, url: str) -> bool:
        value = (url or "").strip().lower()
        if not value:
            return False
        if value.startswith("mailto:") or value.startswith("tel:"):
            return True
        if _NON_JOB_URL_PATTERN_V32.search(value):
            return True
        return False

    def _clean_description_v32(self, description: str) -> Optional[str]:
        text = " ".join((description or "").split())
        if not text:
            return None
        cut = _DESCRIPTION_CUT_PATTERN_V32.search(text)
        if cut:
            text = text[: cut.start()].strip()
        if len(text) < 45:
            return None
        return text[:5000]

    def _extract_location_v32(self, row: etree._Element, title: str) -> Optional[str]:
        for el in row.iter():
            if not isinstance(el.tag, str):
                continue
            cls = _get_el_classes(el)
            if re.search(r"(?:location|map-marker|city|office|region|country)", cls):
                loc = " ".join(_text(el).split())
                if loc and loc != title and 2 < len(loc) < 160:
                    return loc

        row_text = _text(row)
        match = _AU_LOCATIONS.search(row_text) if row_text else None
        if match:
            return match.group(0)[:120]
        return None

    @staticmethod
    def _extract_salary_v32(text: str) -> Optional[str]:
        if not text:
            return None
        match = _SALARY_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:120]
        return None

    @staticmethod
    def _extract_employment_type_v32(text: str) -> Optional[str]:
        if not text:
            return None
        match = _JOB_TYPE_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:80]
        return None

    def _extract_location_from_json_v32(self, node: dict[str, Any]) -> Optional[str]:
        for key in ("location", "jobLocation", "city", "workLocation", "region", "addressLocality"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()[:180]
            if isinstance(value, dict):
                parts = [
                    str(value.get("addressLocality") or value.get("city") or "").strip(),
                    str(value.get("addressRegion") or value.get("state") or "").strip(),
                    str(value.get("addressCountry") or "").strip(),
                ]
                joined = ", ".join(p for p in parts if p)
                if joined:
                    return joined[:180]
            if isinstance(value, list):
                loc_parts: list[str] = []
                for entry in value[:5]:
                    if isinstance(entry, str) and entry.strip():
                        loc_parts.append(entry.strip())
                    elif isinstance(entry, dict):
                        city = str(entry.get("addressLocality") or entry.get("city") or "").strip()
                        region = str(entry.get("addressRegion") or entry.get("state") or "").strip()
                        country = str(entry.get("addressCountry") or "").strip()
                        joined = ", ".join(p for p in (city, region, country) if p)
                        if joined:
                            loc_parts.append(joined)
                if loc_parts:
                    return " | ".join(dict.fromkeys(loc_parts))[:180]
        return None

    def _extract_salary_from_json_v32(self, node: dict[str, Any]) -> Optional[str]:
        for key in ("salary", "salaryText", "salaryRange", "compensation", "pay"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()[:180]
            if isinstance(value, dict):
                minimum = str(value.get("minValue") or value.get("minimum") or "").strip()
                maximum = str(value.get("maxValue") or value.get("maximum") or "").strip()
                currency = str(value.get("currency") or value.get("currencyCode") or "").strip()
                joined = " - ".join(v for v in (minimum, maximum) if v)
                if joined:
                    return f"{currency} {joined}".strip()[:180]
        return None

    def _extract_employment_from_json_v32(self, node: dict[str, Any]) -> Optional[str]:
        for key in ("employmentType", "jobType", "workType", "timeType"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                found = _JOB_TYPE_PATTERN.search(value)
                if found:
                    return found.group(0).strip()[:80]
                return value.strip()[:80]
        return None

    # ------------------------------------------------------------------
    # Martian helpers
    # ------------------------------------------------------------------

    def _extract_martian_context_v32(self, html_body: str, page_url: str) -> dict[str, str]:
        result = {
            "client_code": "",
            "recruiter_id": "",
            "job_board_theme_id": "",
            "board_name": "",
            "build_id": "",
        }

        next_data_match = _NEXT_DATA_PATTERN_V32.search(html_body or "")
        if next_data_match:
            try:
                parsed = json.loads(next_data_match.group(1))
                if isinstance(parsed, dict):
                    page_props = parsed.get("props", {}).get("pageProps", {})
                    if isinstance(page_props, dict):
                        result["client_code"] = str(page_props.get("clientCode") or "").strip()
                        result["recruiter_id"] = str(page_props.get("recruiterId") or "").strip()
                        result["job_board_theme_id"] = str(page_props.get("jobBoardThemeId") or "").strip()
                        result["board_name"] = str(page_props.get("name") or "").strip()
                    result["build_id"] = str(parsed.get("buildId") or "").strip()
                    query = parsed.get("query")
                    if isinstance(query, dict):
                        if not result["client_code"]:
                            result["client_code"] = str(query.get("client") or query.get("clientCode") or "").strip()
                        if not result["recruiter_id"]:
                            result["recruiter_id"] = str(query.get("recruiterId") or "").strip()
            except Exception:
                pass

        if not result["client_code"]:
            m = _MARTIAN_CLIENT_PATTERN_V32.search(html_body or "")
            if m:
                result["client_code"] = m.group(1).strip()
        if not result["recruiter_id"]:
            m = _MARTIAN_RECRUITER_PATTERN_V32.search(html_body or "")
            if m:
                result["recruiter_id"] = m.group(1).strip()
        if not result["job_board_theme_id"]:
            m = _MARTIAN_THEME_PATTERN_V32.search(html_body or "")
            if m:
                result["job_board_theme_id"] = m.group(1).strip()
        if not result["board_name"]:
            m = _MARTIAN_NAME_PATTERN_V32.search(html_body or "")
            if m:
                result["board_name"] = m.group(1).strip()

        if not result["client_code"]:
            path_parts = [p for p in (urlparse(page_url).path or "").split("/") if p]
            if path_parts:
                candidate = re.sub(r"[^a-z0-9-]", "", path_parts[-1].lower())
                if len(candidate) >= 3:
                    result["client_code"] = candidate
        return result

    def _martian_probe_urls_v32(self, page_url: str, context: dict[str, str]) -> list[str]:
        parsed = urlparse(page_url or "")
        base_host = f"{parsed.scheme or 'https'}://{parsed.netloc}" if parsed.netloc else ""
        if not base_host:
            return []

        client_code = (context.get("client_code") or "").strip()
        recruiter_id = (context.get("recruiter_id") or "").strip()
        theme_id = (context.get("job_board_theme_id") or "").strip()
        board_name = (context.get("board_name") or "").strip()
        build_id = (context.get("build_id") or "").strip()
        if not client_code:
            return []

        hosts = [base_host, "https://web.martianlogic.com", "https://form.myrecruitmentplus.com"]

        query_templates = [
            f"client={client_code}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"clientCode={client_code}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"client={client_code}&page=1&perPage=50&isActive=true",
            f"clientCode={client_code}&page=1&perPage=50&isActive=true",
        ]
        if recruiter_id:
            query_templates.extend(
                [
                    f"client={client_code}&recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"clientCode={client_code}&recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                ]
            )
        if theme_id:
            query_templates.extend(
                [
                    f"client={client_code}&jobBoardThemeId={theme_id}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"clientCode={client_code}&jobBoardThemeId={theme_id}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                ]
            )
        if board_name:
            query_templates.extend(
                [
                    f"client={client_code}&name={board_name}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                    f"clientCode={client_code}&name={board_name}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
                ]
            )

        paths = [
            "/api/jobs/search",
            "/api/jobads/search",
            "/api/job-ads/search",
            "/api/jobs",
            "/api/jobads",
            "/api/job-ads",
            "/jobs/latest",
            "/embed-jobs",
            f"/{client_code}",
            f"/{client_code}/jobs",
            f"/{client_code}/jobads",
            f"/{client_code}/job-ads",
            f"/{client_code}/jobs/search",
            f"/{client_code}/job-ads/search",
            f"/{client_code}/embed-jobs",
            f"/{client_code}/latest",
        ]

        candidates: list[str] = []
        for host in hosts:
            for path in paths:
                base = f"{host}{path}"
                candidates.append(base)
                for query in query_templates:
                    candidates.append(f"{base}?{query}")
            if recruiter_id:
                candidates.extend(
                    [
                        f"{host}/api/recruiter/{recruiter_id}/jobs?pageNumber=1&pageSize=50",
                        f"{host}/api/recruiter/{recruiter_id}/jobads?pageNumber=1&pageSize=50",
                        f"{host}/api/recruiter/{recruiter_id}/job-ads?pageNumber=1&pageSize=50",
                    ]
                )
            if build_id and client_code:
                candidates.extend(
                    [
                        f"{host}/_next/data/{build_id}/{client_code}.json",
                        f"{host}/_next/data/{build_id}/{client_code}/index.json",
                        f"{host}/_next/data/{build_id}/index.json?client={client_code}",
                    ]
                )

        page_query = dict(parse_qsl(parsed.query))
        if page_query.get("jobAdId"):
            candidates.append(
                f"{base_host}/?client={client_code}&jobAdId={page_query['jobAdId']}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc"
            )

        unique: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            norm = candidate.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
            unique.append(candidate)

        def _priority(candidate_url: str) -> int:
            low = candidate_url.lower()
            score = 0
            if "/api/" in low:
                score += 10
            if "search" in low:
                score += 7
            if "embed-jobs" in low or "job-ads" in low or "jobads" in low:
                score += 6
            if "recruiterid=" in low or "/api/recruiter/" in low:
                score += 6
            if "jobboardthemeid=" in low or "name=" in low:
                score += 5
            if "clientcode=" in low or "client=" in low:
                score += 4
            if "pagenumber=1" in low or "page=1" in low:
                score += 3
            if "/_next/data/" in low:
                score += 2
            if low.endswith(f"/{client_code.lower()}") and "?" not in low:
                score -= 4
            return score

        unique.sort(key=_priority, reverse=True)
        return unique

    @staticmethod
    def _martian_paged_variants_v32(endpoint: str) -> list[str]:
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
        unique: list[str] = []
        for value in variants:
            if value in seen:
                continue
            seen.add(value)
            unique.append(value)
        return unique[:3]

    # ------------------------------------------------------------------
    # Oracle helpers
    # ------------------------------------------------------------------

    def _oracle_site_ids_v32(self, page_url: str, html_body: str) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []

        def _add(value: str) -> None:
            site = (value or "").strip()
            if not site or site in seen:
                return
            seen.add(site)
            ordered.append(site)

        for match in _ORACLE_SITE_PATTERN_V32.finditer(page_url or ""):
            _add(match.group(1))
        for match in _ORACLE_SITE_PATTERN_V32.finditer(html_body or ""):
            _add(match.group(1))
        for match in _ORACLE_SITE_NUMBER_PATTERN_V32.finditer(html_body or ""):
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
            for fallback in ("CX", "CX_1001", "CX_1002"):
                _add(fallback)

        return ordered[:14]

    def _extract_oracle_items_v32(self, data: Any, page_url: str, site_id: str) -> list[dict]:
        rows: list[dict[str, Any]] = []

        def _add_row(row: Any) -> None:
            if isinstance(row, dict):
                rows.append(row)

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
                    if any(k in item for k in ("Title", "title", "JobTitle", "jobTitle", "Id", "id")):
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

        jobs: list[dict] = []
        parsed = urlparse(page_url)
        host_base = f"{parsed.scheme or 'https'}://{parsed.netloc}" if parsed.netloc else ""

        for row in rows:
            title = self._normalize_title_v32(
                str(
                    row.get("Title")
                    or row.get("title")
                    or row.get("JobTitle")
                    or row.get("jobTitle")
                    or row.get("requisitionTitle")
                    or ""
                )
            )
            if not self._is_valid_title_v32(title):
                continue
            if not self._title_has_role_signal_v32(title):
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
            if self._is_non_job_url_v32(source_url):
                continue

            location = self._extract_location_from_json_v32(row)
            if not location:
                primary = str(row.get("PrimaryLocation") or "").strip()
                country = str(row.get("PrimaryLocationCountry") or "").strip()
                joined = ", ".join(p for p in (primary, country) if p)
                location = joined or None

            description = self._clean_description_v32(
                str(
                    row.get("Description")
                    or row.get("description")
                    or row.get("ShortDescription")
                    or row.get("ExternalDescriptionStr")
                    or ""
                )
            )
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": self._extract_salary_from_json_v32(row),
                    "employment_type": self._extract_employment_from_json_v32(row),
                    "description": description,
                    "extraction_method": "tier0_oracle_api_v32",
                    "extraction_confidence": 0.9,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return jobs
