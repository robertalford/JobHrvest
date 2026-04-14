"""
Tiered Extraction Engine v3.1 — direct from v1.6 with focused coverage recovery.

High-impact improvements:
1. Always-on job-link sweep: collect job-like detail links even when parent extraction
   already returned jobs (fixes partial capture on split ATS pages).
2. Same-page heading block extraction: accept repeated heading rows with strong row
   evidence (long detail text / apply context), including mailto-driven listings.
3. Repeating row-class aggregation: merge jobs across sibling groups sharing row-like
   classes (position/job/vacancy), avoiding single-container under-capture.
4. Config-shell API fallback for Martian/MyRecruitmentPlus (`__NEXT_DATA__` metadata).
5. Oracle CandidateExperience requisition API fallback for SPA-only listing pages.
6. Simpler coverage-first arbitration with strict title/url validation to avoid type-1 errors.
"""

from __future__ import annotations

import asyncio
import html as html_lib
import json
import logging
import re
from collections import defaultdict, deque
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode, urlparse

import httpx
from lxml import etree

from app.crawlers.tiered_extractor_v16 import TieredExtractorV16, _title_has_job_noun
from app.crawlers.tiered_extractor import (
    _JOB_TYPE_PATTERN,
    _SALARY_PATTERN,
    _AU_LOCATIONS,
    _get_el_classes,
    _parse_html,
    _resolve_url,
    _text,
    MAX_JOBS_PER_PAGE,
    MIN_JOBS_FOR_SUCCESS,
)

logger = logging.getLogger(__name__)


_ROLE_HINT_PATTERN_V31 = re.compile(
    r"\b(?:engineer|developer|manager|director|analyst|specialist|assistant|"
    r"consultant|coordinator|officer|administrator|accountant|technician|"
    r"designer|architect|operator|supervisor|nurse|teacher|chef|driver|"
    r"recruit(?:er|ment)?|executive|intern(?:ship)?|graduate|trainee|"
    r"influencer|fotografer|videografer|akuntan|konsultan|asisten|staf|staff|"
    r"pegawai|karyawan|psycholog|psikolog(?:i)?|customer\s+service|model|sarjana)\b",
    re.IGNORECASE,
)

_REJECT_TITLE_PATTERN_V31 = re.compile(
    r"^(?:"
    r"jobs?|careers?|open\s+roles?|open\s+positions?|all\s+jobs?|current\s+jobs?|"
    r"current\s+vacancies|job\s+openings?|search\s+jobs?|browse\s+jobs?|"
    r"view\s+all\s+jobs?|join\s+our\s+team|career\s+opportunities|"
    r"apply(?:\s+now|\s+here)?|read\s+more|learn\s+more|show\s+more|info\s+lengkap|"
    r"about\s+us|our\s+team|contact|privacy|terms|login|register|"
    r"job\s+description|internship\s+details|no\s+jobs?\s+found"
    r"|lowongan\s+kerja(?:\s+\w+){0,3}"
    r"|business\s+model"
    r")$",
    re.IGNORECASE,
)

_GENERIC_SINGLE_TITLE_PATTERN_V31 = re.compile(
    r"^(?:internship|intern|vacancy|vacancies|positions?|roles?|jobs?)$",
    re.IGNORECASE,
)

_JOB_URL_HINT_PATTERN_V31 = re.compile(
    r"(?:/jobview/|/jobs?/[^/?#]{3,}|/career|/careers|/position|/positions|"
    r"/vacanc|/opening|/openings|/requisition|/requisitions|"
    r"/p/[a-z0-9_-]{6,}|jobid=|job_id=|requisitionid=|positionid=|"
    r"candidateportal|portal\.na|applicationform|embed-jobs|lowongan|karir|karier)",
    re.IGNORECASE,
)

_JOB_DETAILISH_URL_PATTERN_V31 = re.compile(
    r"(?:/jobview/[a-z0-9-]+/[0-9a-f-]{8,}|/jobs?/[a-z0-9][^/?#]{4,}|"
    r"/p/[a-z0-9_-]{6,}|[?&](?:jobid|job_id|requisitionid|positionid)=[A-Za-z0-9_-]{2,})",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V31 = re.compile(
    r"(?:/(?:privacy|terms|cookie|news|blog|about|contact|investor|"
    r"help|login|logout|register|account|team|culture)(?:/|$|[?#])|"
    r"wp-json|/feed(?:/|$)|/rss(?:/|$)|"
    r"\.pdf(?:$|\?)|\.docx?(?:$|\?))",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN_V31 = re.compile(
    r"(?:job|position|vacanc|opening|requisition|career|posting|listing|accordion)",
    re.IGNORECASE,
)

_APPLY_CONTEXT_PATTERN_V31 = re.compile(
    r"(?:apply|application|apply\s+now|apply\s+here|mailto:|"
    r"job\s+description|requirements?|qualifications?|closing\s+date|"
    r"full\s*time|part\s*time|contract|permanent|temporary|remote|hybrid|"
    r"cara\s+melamar|how\s+to\s+apply|info\s+lengkap)",
    re.IGNORECASE,
)

_DESCRIPTION_CUT_PATTERN_V31 = re.compile(
    r"\b(?:how\s+to\s+apply|cara\s+melamar|application\s+process|"
    r"instructions?\s+to\s+apply|cara\s+pendaftaran)\b",
    re.IGNORECASE,
)

_LOCATION_HINT_PATTERN_V31 = re.compile(r"\b(?:location|lokasi|kota|city|office|region)\b", re.IGNORECASE)
_BOILERPLATE_BOUNDARY_PATTERN_V31 = re.compile(
    r"\b(?:apply\s+now|learn\s+more|view\s+all|see\s+all|load\s+more|next\s+page|"
    r"previous|cookie|privacy|terms|sign\s+in|log\s+in|subscribe|follow\s+us|"
    r"read\s+more|show\s+more|about\s+us|contact\s+us|our\s+team|home|menu|search|"
    r"close|back|join\s+us\s+now|view\s+openings|come\s+work\s+with\s+us)\b",
    re.IGNORECASE,
)
_TITLE_PHONE_PATTERN_V31 = re.compile(r"^[\d\s\-\+\(\)\.]{7,}$")
_TITLE_MOSTLY_NUMERIC_PATTERN_V31 = re.compile(r"^[\d\s\-\.\,\#\:\/]{4,}$")
_MARTIAN_CLIENT_PATTERN_V31 = re.compile(r'"clientCode"\s*:\s*"([a-z0-9-]{3,})"', re.IGNORECASE)
_MARTIAN_RECRUITER_PATTERN_V31 = re.compile(r'"recruiterId"\s*:\s*([0-9]{2,})', re.IGNORECASE)
_MARTIAN_THEME_PATTERN_V31 = re.compile(r'"jobBoardThemeId"\s*:\s*([0-9]{2,})', re.IGNORECASE)
_MARTIAN_NAME_PATTERN_V31 = re.compile(r'"name"\s*:\s*"([^"]{2,40})"', re.IGNORECASE)
_NEXT_DATA_PATTERN_V31 = re.compile(
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>\s*(\{.*?\})\s*</script>',
    re.IGNORECASE | re.DOTALL,
)
_ORACLE_SITE_PATTERN_V31 = re.compile(r"/sites/([A-Za-z0-9_]+)/", re.IGNORECASE)
_ORACLE_SITE_NUMBER_PATTERN_V31 = re.compile(r"siteNumber\s*[:=]\s*['\"]([A-Za-z0-9_]+)", re.IGNORECASE)
_ORACLE_API_BASE_PATTERN_V31 = re.compile(r"apiBaseUrl\s*:\s*['\"](https?://[^'\"\\s]+)", re.IGNORECASE)


class TieredExtractorV31(TieredExtractorV16):
    """v3.1 extractor with coverage-first fallbacks and stricter validation."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        candidates: list[tuple[str, list[dict]]] = []

        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(super().extract(career_page, company, working_html), timeout=24.0)
        except asyncio.TimeoutError:
            logger.warning("v3.1 parent extractor timeout for %s", url)
        except Exception:
            logger.exception("v3.1 parent extractor failed for %s", url)

        parent_jobs = self._dedupe_jobs_v31(parent_jobs or [], url)
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        jsonld_jobs = self._extract_jsonld_jobs_v31(working_html, url)
        if jsonld_jobs:
            candidates.append(("jsonld_v31", jsonld_jobs))

        root = _parse_html(working_html)
        if root is not None:
            elementor_jobs = self._extract_elementor_cards_v31(root, url)
            if elementor_jobs:
                candidates.append(("elementor_cards_v31", elementor_jobs))

            heading_jobs = self._extract_heading_rows_v31(root, url)
            if heading_jobs:
                candidates.append(("heading_rows_v31", heading_jobs))

            link_jobs = self._extract_job_links_v31(root, url)
            if link_jobs:
                candidates.append(("job_links_v31", link_jobs))

            grouped_rows = self._extract_repeating_row_groups_v31(root, url)
            if grouped_rows:
                candidates.append(("row_groups_v31", grouped_rows))

        martian_jobs = await self._extract_martian_jobs_v31(url, working_html)
        if martian_jobs:
            candidates.append(("martian_api_v31", martian_jobs))

        oracle_jobs = await self._extract_oracle_jobs_v31(url, working_html)
        if oracle_jobs:
            candidates.append(("oracle_api_v31", oracle_jobs))

        best_label, best_jobs = self._pick_best_jobset_v31(candidates, url)
        if not best_jobs:
            return []

        if best_label != "parent_v16" and any(self._is_job_like_url_v31(j.get("source_url") or "") for j in best_jobs):
            try:
                best_jobs = await asyncio.wait_for(self._enrich_from_detail_pages(best_jobs), timeout=18.0)
            except asyncio.TimeoutError:
                logger.warning("v3.1 enrichment timeout for %s", url)
            except Exception:
                logger.exception("v3.1 enrichment failed for %s", url)
            best_jobs = self._dedupe_jobs_v31(best_jobs, url)

        return best_jobs[:MAX_JOBS_PER_PAGE]

    # ------------------------------------------------------------------
    # Candidate extraction
    # ------------------------------------------------------------------

    def _extract_heading_rows_v31(self, root: etree._Element, page_url: str) -> list[dict]:
        containers = root.xpath("//section | //article | //main | //div")
        jobs: list[dict] = []

        for container in containers[:280]:
            classes = _get_el_classes(container)
            if re.search(r"\b(menu|navbar|header|footer|breadcrumb|sitemap)\b", classes):
                continue

            children = [
                c for c in container
                if isinstance(c.tag, str) and c.tag.lower() not in ("script", "style", "noscript", "svg")
            ]
            if len(children) < 2:
                continue

            row_candidates = [c for c in children if c.xpath(".//h2 | .//h3 | .//h4")]
            if len(row_candidates) < 2:
                continue
            if len(row_candidates) > 100:
                continue

            local_jobs: list[dict] = []
            evidence_hits = 0
            role_hits = 0

            for row in row_candidates[:80]:
                heading_nodes = row.xpath(".//h2 | .//h3 | .//h4")
                if not heading_nodes:
                    continue
                title = self._normalize_title_v31(_text(heading_nodes[0]))
                if not self._is_valid_title_v31(title):
                    continue

                link_nodes = heading_nodes[0].xpath(".//a[@href]")
                if not link_nodes:
                    link_nodes = row.xpath(".//a[@href][1]")
                href = link_nodes[0].get("href") if link_nodes else None
                source_url = _resolve_url(href, page_url) or page_url
                if self._is_non_job_url_v31(source_url):
                    source_url = page_url

                row_text = _text(row)[:9000]
                if len(row_text) < 45:
                    continue

                same_page = source_url.rstrip("/") == page_url.rstrip("/")
                apply_hint = bool(_APPLY_CONTEXT_PATTERN_V31.search(row_text))
                long_detail = len(row_text) >= 220
                job_url_hint = self._is_job_like_url_v31(source_url) or bool(_JOB_DETAILISH_URL_PATTERN_V31.search(source_url))

                if same_page and not (apply_hint or long_detail):
                    continue
                if not (job_url_hint or apply_hint or long_detail):
                    continue

                has_role = self._title_has_role_signal_v31(title)
                if not has_role and not (job_url_hint and len(title.split()) >= 2):
                    continue

                location = self._extract_location_v31(row, title)
                description = self._clean_description_v31(row_text)

                local_jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": location,
                        "salary_raw": self._extract_salary_v31(row_text),
                        "employment_type": self._extract_employment_type_v31(row_text),
                        "description": description,
                        "extraction_method": "tier2_heading_rows_v31",
                        "extraction_confidence": 0.7 if (job_url_hint or apply_hint) else 0.64,
                    }
                )
                if has_role:
                    role_hits += 1
                if job_url_hint or apply_hint or long_detail:
                    evidence_hits += 1

            if len(local_jobs) < MIN_JOBS_FOR_SUCCESS:
                continue
            if role_hits < max(1, int(len(local_jobs) * 0.6)):
                continue
            if evidence_hits < max(1, int(len(local_jobs) * 0.6)):
                continue
            jobs.extend(local_jobs)

        jobs = self._dedupe_jobs_v31(jobs, page_url)
        if not self._passes_jobset_validation_v31(jobs, page_url):
            return []
        return jobs

    def _extract_job_links_v31(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        anchors = root.xpath("//a[@href]")
        for a_el in anchors[:6000]:
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url)
            if not source_url or self._is_non_job_url_v31(source_url):
                continue

            heading_nodes = a_el.xpath(".//h1 | .//h2 | .//h3 | .//h4 | .//h5")
            title_raw = _text(heading_nodes[0]) if heading_nodes else (_text(a_el) or (a_el.get("title") or ""))
            title = self._normalize_title_v31(title_raw)
            if not self._is_valid_title_v31(title):
                continue

            context_el = a_el
            for _ in range(3):
                parent = context_el.getparent()
                if parent is None:
                    break
                parent_text = _text(parent)
                if len(parent_text) >= 160 or _ROW_CLASS_PATTERN_V31.search(_get_el_classes(parent)):
                    context_el = parent
                    break
                context_el = parent

            context_text = _text(context_el)[:2600]
            apply_hint = bool(_APPLY_CONTEXT_PATTERN_V31.search(context_text))
            job_url_hint = self._is_job_like_url_v31(source_url)
            same_page = source_url.rstrip("/") == page_url.rstrip("/")

            if same_page and not apply_hint:
                continue
            if not (job_url_hint or apply_hint):
                continue

            has_role = self._title_has_role_signal_v31(title)
            if not has_role and not (job_url_hint and len(title.split()) >= 2):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v31(context_el, title),
                    "salary_raw": self._extract_salary_v31(context_text),
                    "employment_type": self._extract_employment_type_v31(context_text),
                    "description": self._clean_description_v31(context_text),
                    "extraction_method": "tier2_job_links_v31",
                    "extraction_confidence": 0.74 if job_url_hint else 0.67,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        jobs = self._dedupe_jobs_v31(jobs, page_url)
        if not self._passes_jobset_validation_v31(jobs, page_url):
            return []
        return jobs

    def _extract_repeating_row_groups_v31(self, root: etree._Element, page_url: str) -> list[dict]:
        groups: dict[str, list[etree._Element]] = defaultdict(list)

        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag not in {"li", "div", "article", "tr", "section"}:
                continue
            classes = _get_el_classes(el)
            if not classes or not _ROW_CLASS_PATTERN_V31.search(classes):
                continue
            tokens = classes.split()
            sig = f"{tag}:{' '.join(tokens[:2])}" if tokens else tag
            groups[sig].append(el)

        jobs: list[dict] = []
        for rows in groups.values():
            if len(rows) < 3:
                continue
            for row in rows[:MAX_JOBS_PER_PAGE]:
                job = self._extract_heuristic_job_v16(row, page_url, 12)
                if not job:
                    continue

                title = self._normalize_title_v31(job.get("title") or "")
                source_url = (job.get("source_url") or page_url).strip() or page_url
                row_text = _text(row)[:3500]
                apply_hint = bool(_APPLY_CONTEXT_PATTERN_V31.search(row_text))
                has_role = self._title_has_role_signal_v31(title)
                job_url_hint = self._is_job_like_url_v31(source_url)

                if not self._is_valid_title_v31(title):
                    continue
                if self._is_non_job_url_v31(source_url):
                    continue
                if not (job_url_hint or apply_hint or len(row_text) >= 180):
                    continue
                if not has_role and not apply_hint:
                    continue

                jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": job.get("location_raw") or self._extract_location_v31(row, title),
                        "salary_raw": job.get("salary_raw") or self._extract_salary_v31(row_text),
                        "employment_type": job.get("employment_type") or self._extract_employment_type_v31(row_text),
                        "description": self._clean_description_v31(row_text),
                        "extraction_method": "tier2_row_groups_v31",
                        "extraction_confidence": 0.72 if job_url_hint else 0.65,
                    }
                )
                if len(jobs) >= MAX_JOBS_PER_PAGE:
                    break

        jobs = self._dedupe_jobs_v31(jobs, page_url)
        if not self._passes_jobset_validation_v31(jobs, page_url):
            return []
        return jobs

    def _extract_elementor_cards_v31(self, root: etree._Element, page_url: str) -> list[dict]:
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
            title = self._normalize_title_v31(_text(heading_nodes[0]))
            if not self._is_valid_title_v31(title):
                continue
            if not self._title_has_role_signal_v31(title):
                continue

            source_url = page_url
            for a_el in card.xpath(".//a[@href]"):
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v31(href):
                    continue
                if self._is_job_like_url_v31(href):
                    source_url = href
                    break
                if source_url == page_url:
                    source_url = href

            card_text = _text(card)[:2800]
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v31(card, title),
                    "salary_raw": self._extract_salary_v31(card_text),
                    "employment_type": self._extract_employment_type_v31(card_text),
                    "description": self._clean_description_v31(card_text),
                    "extraction_method": "tier2_elementor_cards_v31",
                    "extraction_confidence": 0.74,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        jobs = self._dedupe_jobs_v31(jobs, page_url)
        if not self._passes_jobset_validation_v31(jobs, page_url):
            return []
        return jobs

    def _extract_jsonld_jobs_v31(self, html_body: str, page_url: str) -> list[dict]:
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
                self._extract_jobs_from_json_obj_v31(
                    parsed,
                    page_url,
                    method="tier0_jsonld_v31",
                    require_job_type=True,
                )
            )

        jobs = self._dedupe_jobs_v31(jobs, page_url)
        if not self._passes_jobset_validation_v31(jobs, page_url):
            return []
        return jobs

    # ------------------------------------------------------------------
    # Config-shell fallbacks
    # ------------------------------------------------------------------

    async def _extract_martian_jobs_v31(self, page_url: str, html_body: str) -> list[dict]:
        lower = (html_body or "").lower()
        if (
            "__next_data__" not in lower
            and "myrecruitmentplus" not in lower
            and "martianlogic" not in lower
            and "clientcode" not in lower
        ):
            return []

        context = self._extract_martian_context_v31(html_body, page_url)
        client_code = context.get("client_code") or ""
        if not client_code:
            return []

        endpoints = self._martian_probe_urls_v31(page_url, context)
        if not endpoints:
            return []

        jobs: list[dict] = []
        request_count = 0
        max_requests = 26

        try:
            async with httpx.AsyncClient(timeout=7, follow_redirects=True) as client:
                for endpoint in endpoints:
                    for probe_url in self._martian_paged_variants_v31(endpoint):
                        if request_count >= max_requests:
                            break
                        request_count += 1
                        try:
                            resp = await client.get(probe_url)
                        except Exception:
                            continue
                        if resp.status_code >= 400 or not resp.text:
                            continue

                        extracted = self._extract_jobs_from_probe_payload_v31(resp.text, str(resp.url), page_url)
                        if extracted:
                            jobs.extend(extracted)
                            if len(jobs) >= 120:
                                break
                    if request_count >= max_requests or len(jobs) >= 120:
                        break
        except Exception:
            logger.debug("v3.1 martian probing failed for %s", page_url)

        jobs = self._dedupe_jobs_v31(jobs, page_url)
        if not self._passes_jobset_validation_v31(jobs, page_url):
            return []
        return jobs

    async def _extract_oracle_jobs_v31(self, page_url: str, html_body: str) -> list[dict]:
        page_l = (page_url or "").lower()
        body_l = (html_body or "").lower()
        if "oraclecloud.com" not in page_l and "candidateexperience" not in body_l and "hcmrestapi" not in body_l:
            return []

        parsed = urlparse(page_url)
        if not parsed.netloc:
            return []

        api_base = f"{parsed.scheme or 'https'}://{parsed.netloc}"
        api_match = _ORACLE_API_BASE_PATTERN_V31.search(html_body or "")
        if api_match:
            api_base = api_match.group(1).rstrip("/")

        site_ids = self._oracle_site_ids_v31(page_url, html_body)
        if not site_ids:
            return []

        jobs: list[dict] = []
        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
                for site_id in site_ids[:8]:
                    for offset in range(0, 216, 24):
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

                        batch = self._extract_oracle_items_v31(data, page_url)
                        if not batch:
                            break
                        jobs.extend(batch)
                        if len(batch) < 24:
                            break
                        if len(jobs) >= MAX_JOBS_PER_PAGE:
                            break
                    if jobs:
                        break
        except Exception:
            logger.debug("v3.1 oracle probing failed for %s", page_url)

        jobs = self._dedupe_jobs_v31(jobs, page_url)
        if not self._passes_jobset_validation_v31(jobs, page_url):
            return []
        return jobs

    # ------------------------------------------------------------------
    # Probe parsing
    # ------------------------------------------------------------------

    def _extract_jobs_from_probe_payload_v31(self, body: str, response_url: str, page_url: str) -> list[dict]:
        payload = (body or "").strip()
        if not payload:
            return []

        jobs: list[dict] = []

        if payload.startswith("{") or payload.startswith("["):
            try:
                parsed = json.loads(payload)
                jobs.extend(self._extract_jobs_from_json_obj_v31(parsed, response_url, method="tier0_api_json_v31"))
            except Exception:
                pass

        root = _parse_html(payload)
        if root is not None:
            jobs.extend(self._extract_job_links_v31(root, response_url))
            jobs.extend(self._extract_heading_rows_v31(root, response_url))
            jobs.extend(self._extract_repeating_row_groups_v31(root, response_url))
            jobs.extend(self._extract_tier2_v16(response_url, payload) or [])
            for job in jobs:
                method = str(job.get("extraction_method") or "")
                if method.startswith("tier2_"):
                    job["extraction_method"] = f"{method}_probe_v31"

        return self._dedupe_jobs_v31(jobs, page_url)

    def _extract_jobs_from_json_obj_v31(
        self,
        data: object,
        page_url: str,
        method: str,
        require_job_type: bool = False,
    ) -> list[dict]:
        jobs: list[dict] = []
        queue: deque[object] = deque([data])
        visited = 0

        while queue and visited < 7000:
            node = queue.popleft()
            visited += 1

            if isinstance(node, list):
                queue.extend(node[:240])
                continue
            if not isinstance(node, dict):
                continue

            queue.extend(list(node.values())[:240])
            job = self._job_from_json_dict_v31(node, page_url, method, require_job_type=require_job_type)
            if job:
                jobs.append(job)
                if len(jobs) >= MAX_JOBS_PER_PAGE:
                    break

        return jobs

    def _job_from_json_dict_v31(
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
        ):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                title = value.strip()
                break
        title = self._normalize_title_v31(title)
        if not self._is_valid_title_v31(title):
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
        ):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                url_raw = value.strip()
                break

        source_url = _resolve_url(url_raw, page_url) if isinstance(url_raw, str) else page_url
        source_url = source_url or page_url

        key_names = " ".join(str(k) for k in node.keys()).lower()
        job_key_hint = bool(re.search(r"job|position|posting|requisition|vacanc|opening", key_names))
        strong_id_hint = any(
            node.get(k) not in (None, "")
            for k in (
                "jobId",
                "jobID",
                "jobPostingId",
                "requisitionId",
                "positionId",
                "jobAdId",
                "adId",
                "advertId",
                "referenceNumber",
            )
        )

        if require_job_type and not is_jobposting:
            return None

        has_role = self._title_has_role_signal_v31(title)
        url_hint = self._is_job_like_url_v31(source_url)
        if not has_role and not (url_hint and (job_key_hint or strong_id_hint or is_jobposting)):
            return None

        if self._is_non_job_url_v31(source_url):
            return None

        if source_url.rstrip("/") == page_url.rstrip("/") and not (strong_id_hint or is_jobposting):
            return None

        description = ""
        for key in ("description", "summary", "shortDescription", "jobDescription"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                description = value.strip()
                break
        description = self._clean_description_v31(description)

        location = self._extract_location_from_json_v31(node)
        salary = self._extract_salary_from_json_v31(node)
        employment_type = self._extract_employment_from_json_v31(node)

        return {
            "title": title,
            "source_url": source_url,
            "location_raw": location,
            "salary_raw": salary,
            "employment_type": employment_type,
            "description": description,
            "extraction_method": method,
            "extraction_confidence": 0.84 if (is_jobposting or url_hint) else 0.75,
        }

    # ------------------------------------------------------------------
    # Candidate arbitration / validation
    # ------------------------------------------------------------------

    def _pick_best_jobset_v31(
        self,
        candidates: list[tuple[str, list[dict]]],
        page_url: str,
    ) -> tuple[str, list[dict]]:
        if not candidates:
            return "", []

        scored: list[tuple[str, list[dict], float]] = []
        parent_jobs: list[dict] = []
        parent_score = -1e9

        for label, jobs in candidates:
            deduped = self._dedupe_jobs_v31(jobs, page_url)
            if not deduped:
                continue

            valid = self._passes_jobset_validation_v31(deduped, page_url)
            score = self._jobset_score_v31(deduped, page_url)
            logger.debug("v3.1 candidate %s: jobs=%d valid=%s score=%.2f", label, len(deduped), valid, score)

            if label == "parent_v16":
                parent_jobs = deduped
                parent_score = score

            if valid:
                scored.append((label, deduped, score))

        if not scored:
            if parent_jobs:
                return "parent_v16", parent_jobs[:MAX_JOBS_PER_PAGE]
            largest = max(
                ((label, self._dedupe_jobs_v31(jobs, page_url)) for label, jobs in candidates),
                key=lambda item: len(item[1]),
                default=("", []),
            )
            return largest[0], largest[1][:MAX_JOBS_PER_PAGE]

        best_label, best_jobs, best_score = max(scored, key=lambda item: item[2])

        for label, jobs, score in scored:
            if label == best_label:
                continue
            overlap = self._title_overlap_ratio_v31(jobs, best_jobs)
            if len(jobs) >= len(best_jobs) + 2 and overlap >= 0.65 and score >= best_score - 1.3:
                best_label, best_jobs, best_score = label, jobs, score

        if parent_jobs and best_label != "parent_v16":
            overlap = self._title_overlap_ratio_v31(best_jobs, parent_jobs)
            if not (len(best_jobs) >= len(parent_jobs) + 2 and overlap >= 0.55):
                if best_score < parent_score + 1.4:
                    return "parent_v16", parent_jobs[:MAX_JOBS_PER_PAGE]

        return best_label, best_jobs[:MAX_JOBS_PER_PAGE]

    def _passes_jobset_validation_v31(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs or len(jobs) < MIN_JOBS_FOR_SUCCESS:
            return False

        titles = [self._normalize_title_v31(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if self._is_valid_title_v31(t)]
        if len(titles) < MIN_JOBS_FOR_SUCCESS:
            return False

        unique_ratio = len(set(t.lower() for t in titles)) / max(1, len(titles))
        if len(titles) > 3 and unique_ratio < 0.62:
            return False

        role_hits = sum(1 for t in titles if self._title_has_role_signal_v31(t))
        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V31.match(t.lower()))
        generic_single_hits = sum(1 for t in titles if _GENERIC_SINGLE_TITLE_PATTERN_V31.match(t.lower()))
        url_hits = sum(1 for j in jobs if self._is_job_like_url_v31(j.get("source_url") or page_url))
        detail_hits = sum(1 for j in jobs if _JOB_DETAILISH_URL_PATTERN_V31.search(j.get("source_url") or ""))
        apply_hits = sum(1 for j in jobs if _APPLY_CONTEXT_PATTERN_V31.search((j.get("description") or "")[:1600]))
        same_page_hits = sum(
            1 for j in jobs if (j.get("source_url") or page_url).rstrip("/") == page_url.rstrip("/")
        )

        if reject_hits >= max(1, int(len(titles) * 0.25)):
            return False
        if generic_single_hits >= max(1, int(len(titles) * 0.2)):
            return False
        if role_hits < max(1, int(len(titles) * 0.55)):
            return False

        if len(titles) >= 4 and (url_hits + detail_hits) < max(1, int(len(titles) * 0.25)) and apply_hits < max(
            1, int(len(titles) * 0.35)
        ):
            return False

        if same_page_hits >= max(2, int(len(titles) * 0.85)) and apply_hits < max(1, int(len(titles) * 0.6)):
            return False

        return True

    def _jobset_score_v31(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        titles = [self._normalize_title_v31(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return 0.0

        role_hits = sum(1 for t in titles if self._title_has_role_signal_v31(t))
        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V31.match(t.lower()))
        generic_single_hits = sum(1 for t in titles if _GENERIC_SINGLE_TITLE_PATTERN_V31.match(t.lower()))
        url_hits = sum(1 for j in jobs if self._is_job_like_url_v31(j.get("source_url") or page_url))
        detail_hits = sum(1 for j in jobs if _JOB_DETAILISH_URL_PATTERN_V31.search(j.get("source_url") or ""))
        apply_hits = sum(1 for j in jobs if _APPLY_CONTEXT_PATTERN_V31.search((j.get("description") or "")[:1600]))
        rich_desc_hits = sum(1 for j in jobs if len((j.get("description") or "").strip()) >= 180)

        score = len(titles) * 4.2
        score += role_hits * 2.6
        score += url_hits * 1.6
        score += detail_hits * 1.4
        score += apply_hits * 1.3
        score += rich_desc_hits * 0.6
        score -= reject_hits * 4.5
        score -= generic_single_hits * 3.8
        return score

    def _title_overlap_ratio_v31(self, a_jobs: list[dict], b_jobs: list[dict]) -> float:
        a_titles = {self._normalize_title_v31(j.get("title", "")).lower() for j in a_jobs if j.get("title")}
        b_titles = {self._normalize_title_v31(j.get("title", "")).lower() for j in b_jobs if j.get("title")}
        if not a_titles or not b_titles:
            return 0.0
        return len(a_titles & b_titles) / max(1, min(len(a_titles), len(b_titles)))

    def _dedupe_jobs_v31(self, jobs: list[dict], page_url: str) -> list[dict]:
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for raw in jobs:
            title = self._normalize_title_v31(raw.get("title", ""))
            if not self._is_valid_title_v31(title):
                continue

            source_url = (raw.get("source_url") or page_url).strip() or page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]
            if self._is_non_job_url_v31(source_url):
                continue

            key = (title.lower(), source_url.lower())
            if key in seen:
                continue
            seen.add(key)

            cloned = dict(raw)
            cloned["title"] = title
            cloned["source_url"] = source_url
            cloned["description"] = self._clean_description_v31(str(cloned.get("description") or ""))
            deduped.append(cloned)

            if len(deduped) >= MAX_JOBS_PER_PAGE:
                break

        return deduped

    # ------------------------------------------------------------------
    # Normalization / validation helpers
    # ------------------------------------------------------------------

    def _normalize_title_v31(self, title: str) -> str:
        t = html_lib.unescape((title or "").strip())
        t = t.replace("|", " - ")
        t = re.sub(r"\s+", " ", t)
        t = t.strip(" \t\r\n-–|:;,.")
        t = re.sub(r"\s*(?:just\s+posted!?|posted\s+today!?|new)\s*$", "", t, flags=re.IGNORECASE)
        t = re.sub(
            r"\s*(?:read\s+more|learn\s+more|apply\s+now|apply\s+here|info\s+lengkap)\s*$",
            "",
            t,
            flags=re.IGNORECASE,
        )
        return t.strip()

    def _is_valid_title_v31(self, title: str) -> bool:
        if not title:
            return False
        t = title.strip()
        if len(t) < 5 or len(t) > 200:
            return False
        if _TITLE_PHONE_PATTERN_V31.match(t) or _TITLE_MOSTLY_NUMERIC_PATTERN_V31.match(t):
            return False
        if "@" in t and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", t):
            return False
        if not re.search(r"[A-Za-z]", t):
            return False
        lower = t.lower()
        if _BOILERPLATE_BOUNDARY_PATTERN_V31.search(lower):
            # Keep role-like titles that contain legitimate words around boundary phrases.
            if not self._title_has_role_signal_v31(t):
                return False
        if _REJECT_TITLE_PATTERN_V31.match(t):
            return False
        if _GENERIC_SINGLE_TITLE_PATTERN_V31.match(t):
            return False
        return True

    def _title_has_role_signal_v31(self, title: str) -> bool:
        if not title:
            return False
        return _title_has_job_noun(title) or bool(_ROLE_HINT_PATTERN_V31.search(title))

    def _is_job_like_url_v31(self, url: str) -> bool:
        value = (url or "").strip()
        if not value:
            return False
        if _NON_JOB_URL_PATTERN_V31.search(value):
            return False
        return bool(_JOB_URL_HINT_PATTERN_V31.search(value) or _JOB_DETAILISH_URL_PATTERN_V31.search(value))

    def _is_non_job_url_v31(self, url: str) -> bool:
        value = (url or "").strip().lower()
        if not value:
            return False
        if value.startswith("mailto:") or value.startswith("tel:"):
            return True
        if _NON_JOB_URL_PATTERN_V31.search(value):
            return True
        return False

    def _clean_description_v31(self, description: str) -> Optional[str]:
        text = " ".join((description or "").split())
        if not text:
            return None
        cut_match = _DESCRIPTION_CUT_PATTERN_V31.search(text)
        if cut_match:
            text = text[: cut_match.start()].strip()
        if len(text) < 45:
            return None
        return text[:5000]

    def _extract_location_v31(self, row: etree._Element, title: str) -> Optional[str]:
        for el in row.iter():
            if not isinstance(el.tag, str):
                continue
            cls = _get_el_classes(el)
            if "location" in cls or "map-marker" in cls or _LOCATION_HINT_PATTERN_V31.search(cls):
                loc = " ".join(_text(el).split())
                if loc and loc != title and 2 < len(loc) < 160:
                    return loc

        row_text = _text(row)
        match = _AU_LOCATIONS.search(row_text) if row_text else None
        if match:
            return match.group(0)[:120]
        return None

    @staticmethod
    def _extract_salary_v31(text: str) -> Optional[str]:
        if not text:
            return None
        match = _SALARY_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:120]
        return None

    @staticmethod
    def _extract_employment_type_v31(text: str) -> Optional[str]:
        if not text:
            return None
        match = _JOB_TYPE_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:80]
        return None

    def _extract_location_from_json_v31(self, node: dict[str, Any]) -> Optional[str]:
        for key in ("location", "jobLocation", "city", "workLocation", "region", "addressLocality"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()[:180]
            if isinstance(value, dict):
                parts = [
                    str(value.get("addressLocality") or "").strip(),
                    str(value.get("addressRegion") or "").strip(),
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

    def _extract_salary_from_json_v31(self, node: dict[str, Any]) -> Optional[str]:
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

    def _extract_employment_from_json_v31(self, node: dict[str, Any]) -> Optional[str]:
        for key in ("employmentType", "jobType", "workType", "timeType"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                found = _JOB_TYPE_PATTERN.search(value)
                if found:
                    return found.group(0).strip()[:80]
                return value.strip()[:80]
        return None

    # ------------------------------------------------------------------
    # Martian context/probing helpers
    # ------------------------------------------------------------------

    def _extract_martian_context_v31(self, html_body: str, page_url: str) -> dict[str, str]:
        result = {
            "client_code": "",
            "recruiter_id": "",
            "job_board_theme_id": "",
            "board_name": "",
            "build_id": "",
        }

        next_data_match = _NEXT_DATA_PATTERN_V31.search(html_body or "")
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
            m = _MARTIAN_CLIENT_PATTERN_V31.search(html_body or "")
            if m:
                result["client_code"] = m.group(1).strip()
        if not result["recruiter_id"]:
            m = _MARTIAN_RECRUITER_PATTERN_V31.search(html_body or "")
            if m:
                result["recruiter_id"] = m.group(1).strip()
        if not result["job_board_theme_id"]:
            m = _MARTIAN_THEME_PATTERN_V31.search(html_body or "")
            if m:
                result["job_board_theme_id"] = m.group(1).strip()
        if not result["board_name"]:
            m = _MARTIAN_NAME_PATTERN_V31.search(html_body or "")
            if m:
                result["board_name"] = m.group(1).strip()

        if not result["client_code"]:
            path_parts = [p for p in (urlparse(page_url).path or "").split("/") if p]
            if path_parts:
                candidate = re.sub(r"[^a-z0-9-]", "", path_parts[-1].lower())
                if len(candidate) >= 3:
                    result["client_code"] = candidate

        return result

    def _martian_probe_urls_v31(self, page_url: str, context: dict[str, str]) -> list[str]:
        parsed = urlparse(page_url or "")
        base_host = f"{parsed.scheme or 'https'}://{parsed.netloc}" if parsed.netloc else ""
        if not base_host:
            return []

        client_code = (context.get("client_code") or "").strip()
        recruiter_id = (context.get("recruiter_id") or "").strip()
        theme_id = (context.get("job_board_theme_id") or "").strip()
        board_name = (context.get("board_name") or "").strip()
        build_id = (context.get("build_id") or "").strip()

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
            f"/{client_code}/embed-jobs",
            f"/{client_code}/latest",
        ]

        candidates: list[str] = []
        for host in hosts:
            for path in paths:
                if "{client_code}" in path or ("//" in path and not path.startswith("http")):
                    continue
                if path.endswith("/") and len(path) > 1:
                    path = path[:-1]
                base = f"{host}{path}"
                candidates.append(base)
                for query in query_templates:
                    candidates.append(f"{base}?{query}")
            if recruiter_id:
                candidates.extend(
                    [
                        f"{host}/api/recruiter/{recruiter_id}/jobs?pageNumber=1&pageSize=50",
                        f"{host}/api/recruiter/{recruiter_id}/jobads?pageNumber=1&pageSize=50",
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

        seen: set[str] = set()
        unique: list[str] = []
        for url in candidates:
            if not url:
                continue
            norm = url.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
            unique.append(url)

        def _priority(candidate_url: str) -> int:
            low = candidate_url.lower()
            score = 0
            if "/api/" in low:
                score += 10
            if "search" in low:
                score += 7
            if "embed-jobs" in low or "jobads" in low or "job-ads" in low:
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
                score -= 3
            return score

        unique.sort(key=_priority, reverse=True)
        return unique

    @staticmethod
    def _martian_paged_variants_v31(endpoint: str) -> list[str]:
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

    def _oracle_site_ids_v31(self, page_url: str, html_body: str) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []

        def _add(value: str) -> None:
            site = (value or "").strip()
            if not site or site in seen:
                return
            seen.add(site)
            ordered.append(site)

        for match in _ORACLE_SITE_PATTERN_V31.finditer(page_url or ""):
            _add(match.group(1))
        for match in _ORACLE_SITE_PATTERN_V31.finditer(html_body or ""):
            _add(match.group(1))
        for match in _ORACLE_SITE_NUMBER_PATTERN_V31.finditer(html_body or ""):
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
            for fallback in ("CX", "CX_1001", "CX_1002"):
                _add(fallback)

        return ordered[:12]

    def _extract_oracle_items_v31(self, data: Any, page_url: str) -> list[dict]:
        items: list[dict[str, Any]] = []
        if isinstance(data, dict):
            raw_items = data.get("items")
            if isinstance(raw_items, list):
                items = [item for item in raw_items if isinstance(item, dict)]
            elif isinstance(data.get("requisitionList"), list):
                items = [item for item in data.get("requisitionList") if isinstance(item, dict)]
            elif isinstance(data.get("requisitionList"), dict):
                nested = data.get("requisitionList", {}).get("items")
                if isinstance(nested, list):
                    items = [item for item in nested if isinstance(item, dict)]

        jobs: list[dict] = []
        for item in items:
            title = self._normalize_title_v31(
                str(
                    item.get("Title")
                    or item.get("title")
                    or item.get("JobTitle")
                    or item.get("jobTitle")
                    or item.get("requisitionTitle")
                    or ""
                )
            )
            if not self._is_valid_title_v31(title):
                continue
            if not self._title_has_role_signal_v31(title):
                continue

            req_id = str(
                item.get("Id")
                or item.get("id")
                or item.get("RequisitionId")
                or item.get("requisitionId")
                or item.get("jobId")
                or ""
            ).strip()

            source_url = ""
            for key in ("ExternalURL", "externalUrl", "PostingUrl", "postingUrl", "jobUrl", "url"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    source_url = value.strip()
                    break
            source_url = _resolve_url(source_url, page_url) or page_url
            if req_id and source_url.rstrip("/") == page_url.rstrip("/"):
                joiner = "&" if "?" in page_url else "?"
                source_url = f"{page_url}{joiner}jobId={req_id}"
            if self._is_non_job_url_v31(source_url):
                continue

            location = self._extract_location_from_json_v31(item)
            description = self._clean_description_v31(
                str(item.get("Description") or item.get("description") or item.get("ShortDescription") or "")
            )
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": self._extract_salary_from_json_v31(item),
                    "employment_type": self._extract_employment_from_json_v31(item),
                    "description": description,
                    "extraction_method": "tier0_oracle_api_v31",
                    "extraction_confidence": 0.9,
                }
            )

        return jobs
