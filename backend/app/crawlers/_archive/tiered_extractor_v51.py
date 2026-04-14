"""
Tiered Extraction Engine v5.1 — direct from v1.6.

v5.1 focuses on high-impact, general pattern recovery with strict validation:
1. RSS/XML feed extraction for discovered feed endpoints.
2. Structured script extraction from __NEXT_DATA__, application/json, and Remix context.
3. Platform DOM extractors for Greenhouse tables and Salesforce fRecruit tables.
4. Same-page heading-block extraction with apply/mailto evidence.
5. Bounded platform path recovery for non-HTML/short payload discoveries.
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
    _parse_html,
    _resolve_url,
    _text,
    MAX_JOBS_PER_PAGE,
    MIN_JOBS_FOR_SUCCESS,
)

logger = logging.getLogger(__name__)

_ROLE_HINT_PATTERN_V51 = re.compile(
    r"\b(?:engineer|developer|manager|director|analyst|specialist|assistant|consultant|"
    r"coordinator|officer|administrator|accountant|technician|designer|architect|operator|"
    r"supervisor|advisor|executive|intern(?:ship)?|recruit(?:er)?|nurse|teacher|driver|chef|"
    r"chemist|mechanic|associate|representative|agent|planner|liaison|scientist|sales|marketing|"
    r"service|customer\s+service|writer|crew|foreman|electrician|labourer|akuntan|influencer|"
    r"videografer|fotografer|psikolog(?:i)?)\b",
    re.IGNORECASE,
)

_TITLE_REJECT_PATTERN_V51 = re.compile(
    r"^(?:join\s+our\s+team|current\s+jobs?|all\s+jobs?|job\s+openings?|search\s+jobs?|"
    r"browse\s+jobs?|view\s+all\s+jobs?|careers?|open\s+roles?|about\s+us|our\s+culture|"
    r"our\s+values?|contact|home|menu|read\s+more|learn\s+more|show\s+more|load\s+more|"
    r"apply(?:\s+now)?|job\s+details?|role\s+details?|job\s+alerts?|my\s+applications?|"
    r"login|register|sign\s+in|beranda)$",
    re.IGNORECASE,
)

_HEADING_REJECT_PATTERN_V51 = re.compile(
    r"^(?:internship\s+details|job\s+description|position\s+description|role\s+description|"
    r"our\s+team|latest\s+news)$",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V51 = re.compile(
    r"(?:/(?:privacy|terms|cookie|news|blog|about|contact|investor|team|culture|services?|"
    r"leadership|people|login|logout|register|account|help|support|wp-json|author|category|"
    r"tag)(?:/|$|[?#]))",
    re.IGNORECASE,
)

_DETAIL_URL_PATTERN_V51 = re.compile(
    r"(?:/jobs?/[^/?#]{3,}|/job/\d{4,}|/careers?/fRecruit__ApplyJob\?|"
    r"/PortalDetail\.na\?.*jobid=|/ViewJob\.na\?|[?&](?:jobid|job_id|requisitionid|"
    r"positionid|vacancyid|jobadid|adid|ajid|vacancyno)=)",
    re.IGNORECASE,
)

_APPLY_CONTEXT_PATTERN_V51 = re.compile(
    r"(?:apply|application|apply\s+now|apply\s+here|requirements?|qualifications?|"
    r"responsibilit|closing\s+date|full\s*time|part\s*time|contract|permanent|temporary|"
    r"how\s+to\s+apply|cara\s+melamar|lamar)",
    re.IGNORECASE,
)

_RSS_ITEM_PATTERN_V51 = re.compile(r"<item\b", re.IGNORECASE)
_NEXT_DATA_PATTERN_V51 = re.compile(
    r"<script[^>]+id=['\"]__NEXT_DATA__['\"][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
_REMIX_CONTEXT_PATTERN_V51 = re.compile(
    r"window\.__remixContext\s*=\s*(\{.*?\})\s*;\s*</script>",
    re.IGNORECASE | re.DOTALL,
)
_MARTIAN_SHELL_PATTERN_V51 = re.compile(
    r"(?:myrecruitmentplus|martianlogic|clientcode|recruiterid|jobboardthemeid|__NEXT_DATA__)",
    re.IGNORECASE,
)


class TieredExtractorV51(TieredExtractorV16):
    """v5.1 extractor with structured/feed/platform recovery and strict gating."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        html_body = html or ""
        started = asyncio.get_running_loop().time()

        candidates: list[tuple[str, list[dict]]] = []

        try:
            parent_jobs = await asyncio.wait_for(
                super().extract(career_page, company, html_body),
                timeout=11.5,
            )
        except asyncio.TimeoutError:
            logger.warning("v5.1 parent extractor timeout for %s", page_url)
            parent_jobs = []
        except Exception:
            logger.exception("v5.1 parent extractor failed for %s", page_url)
            parent_jobs = []

        parent_jobs = self._prepare_jobs_v51(parent_jobs, page_url)
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        local_jobs = self._extract_local_payload_jobs_v51(page_url, html_body)
        if local_jobs:
            candidates.append(("local_v51", local_jobs))

        if self._looks_like_oracle_shell_v51(page_url, html_body) and self._within_budget_v51(started, 18.5):
            oracle_jobs = await self._extract_oracle_api_jobs_v51(page_url, html_body)
            if oracle_jobs:
                candidates.append(("oracle_api_v51", oracle_jobs))

        if self._looks_like_martian_shell_v51(page_url, html_body) and self._within_budget_v51(started, 20.0):
            martian_jobs = await self._extract_martian_shell_jobs_v51(page_url, html_body)
            if martian_jobs:
                candidates.append(("martian_shell_v51", martian_jobs))

        _label, best_jobs, best_score = self._pick_best_candidate_v51(candidates, page_url)

        if self._should_probe_paths_v51(best_jobs, best_score, html_body) and self._within_budget_v51(started, 21.5):
            recovered = await self._recover_from_probe_paths_v51(page_url, html_body)
            if recovered:
                candidates.append(("path_recovery_v51", recovered))

        _label, best_jobs, _best_score = self._pick_best_candidate_v51(candidates, page_url)
        if not best_jobs:
            return []

        final_jobs = self._prepare_jobs_v51(best_jobs, page_url)
        if self._should_enrich_v51(final_jobs, page_url) and self._within_budget_v51(started, 25.0):
            try:
                enriched = await asyncio.wait_for(self._enrich_from_detail_pages(final_jobs), timeout=6.0)
                final_jobs = self._prepare_jobs_v51(enriched, page_url)
            except asyncio.TimeoutError:
                logger.warning("v5.1 detail enrichment timeout for %s", page_url)
            except Exception:
                logger.exception("v5.1 detail enrichment failed for %s", page_url)

        return final_jobs[:MAX_JOBS_PER_PAGE]

    def _extract_local_payload_jobs_v51(self, page_url: str, html_body: str) -> list[dict]:
        if not html_body or len(html_body) < 40:
            return []

        candidates: list[tuple[str, list[dict]]] = []
        is_feed_payload = self._looks_like_feed_xml_v51(html_body)

        feed_jobs = self._extract_rss_xml_jobs_v51(html_body, page_url)
        if feed_jobs:
            candidates.append(("rss_xml_v51", feed_jobs))
            # Feed payloads often include heavy HTML fragments inside <description>;
            # prefer structured feed extraction over generic DOM harvesting.
            if is_feed_payload:
                return feed_jobs

        if not is_feed_payload:
            structured_jobs = self._extract_structured_jobs_v51(html_body, page_url)
            if structured_jobs:
                candidates.append(("structured_v51", structured_jobs))

        root = _parse_html(html_body)
        if root is not None and not is_feed_payload:
            greenhouse = self._extract_greenhouse_rows_v51(root, page_url)
            if greenhouse:
                candidates.append(("greenhouse_rows_v51", greenhouse))

            salesforce = self._extract_salesforce_rows_v51(root, page_url)
            if salesforce:
                candidates.append(("salesforce_rows_v51", salesforce))

            heading_blocks = self._extract_heading_blocks_v51(root, page_url)
            if heading_blocks:
                candidates.append(("heading_blocks_v51", heading_blocks))

            links = self._extract_job_links_v51(root, page_url)
            if links:
                candidates.append(("job_links_v51", links))

        _label, jobs, _score = self._pick_best_candidate_v51(candidates, page_url)
        return jobs

    def _extract_structured_jobs_v51(self, html_body: str, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        match = _NEXT_DATA_PATTERN_V51.search(html_body)
        if match:
            payload = html_lib.unescape((match.group(1) or "").strip())
            try:
                parsed = json.loads(payload)
            except Exception:
                parsed = None
            if parsed is not None:
                jobs.extend(self._extract_jobs_from_json_payload_v51(parsed, page_url, "tier0_next_data_v51"))

        for match in re.finditer(
            r"<script[^>]+type=['\"]application/json['\"][^>]*>(.*?)</script>",
            html_body,
            re.IGNORECASE | re.DOTALL,
        ):
            payload = html_lib.unescape((match.group(1) or "").strip())
            if len(payload) < 30 or len(payload) > 1_500_000:
                continue
            if "<" in payload[:180]:
                continue
            try:
                parsed = json.loads(payload)
            except Exception:
                continue
            jobs.extend(self._extract_jobs_from_json_payload_v51(parsed, page_url, "tier0_script_json_v51"))

        remix_match = _REMIX_CONTEXT_PATTERN_V51.search(html_body)
        if remix_match:
            payload = html_lib.unescape((remix_match.group(1) or "").strip())
            try:
                parsed = json.loads(payload)
            except Exception:
                parsed = None
            if parsed is not None:
                jobs.extend(self._extract_jobs_from_json_payload_v51(parsed, page_url, "tier0_remix_v51"))

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

            for node in self._iter_json_dicts_v51(parsed):
                type_field = node.get("@type")
                is_job = False
                if isinstance(type_field, str):
                    is_job = type_field.lower() == "jobposting"
                elif isinstance(type_field, list):
                    is_job = any(str(v).lower() == "jobposting" for v in type_field)
                if not is_job:
                    continue

                title = self._normalize_title_v51(str(node.get("title") or node.get("name") or ""))
                source_url = _resolve_url(node.get("url") or node.get("sameAs") or node.get("applyUrl"), page_url) or page_url
                if not self._is_title_acceptable_v51(title, source_url):
                    continue

                jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": self._extract_location_from_json_v51(node),
                        "salary_raw": self._extract_salary_v51(json.dumps(node, ensure_ascii=True, default=str)),
                        "employment_type": str(node.get("employmentType") or "").strip() or None,
                        "description": self._clean_description_v51(
                            str(node.get("description") or node.get("responsibilities") or node.get("qualifications") or "")
                        ),
                        "extraction_method": "tier0_jsonld_v51",
                        "extraction_confidence": 0.86,
                    }
                )

        return self._prepare_jobs_v51(jobs, page_url)

    def _extract_jobs_from_json_payload_v51(self, payload: Any, page_url: str, method: str) -> list[dict]:
        jobs: list[dict] = []

        for row in self._iter_json_dicts_v51(payload):
            keyset = {str(k).lower() for k in row.keys()}
            row_has_job_keys = any(
                key in keyset
                for key in {
                    "title",
                    "jobtitle",
                    "jobadtitle",
                    "vacancytitle",
                    "requisitiontitle",
                    "absolute_url",
                    "url",
                    "joburl",
                    "applyurl",
                    "applicationformurl",
                    "description",
                    "externaldescriptionstr",
                    "published_at",
                    "posteddate",
                    "requisitionid",
                    "jobid",
                    "jobadid",
                    "vacancyno",
                }
            )
            title = self._normalize_title_v51(
                str(
                    row.get("title")
                    or row.get("Title")
                    or row.get("jobTitle")
                    or row.get("JobTitle")
                    or row.get("jobAdTitle")
                    or row.get("positionTitle")
                    or row.get("vacancyTitle")
                    or row.get("requisitionTitle")
                    or row.get("name")
                    or ""
                )
            )
            if not title:
                continue
            if not row_has_job_keys and not self._title_has_role_signal_v51(title):
                continue
            if self._looks_like_taxonomy_node_v51(row, title):
                continue

            source_url = ""
            for key in (
                "absolute_url",
                "url",
                "jobUrl",
                "job_url",
                "applyUrl",
                "apply_url",
                "applicationFormUrl",
                "externalUrl",
                "postingUrl",
                "jobPostingUrl",
                "adUrl",
                "detailsUrl",
                "vacancyUrl",
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
                or ""
            ).strip()

            source_url = _resolve_url(source_url, page_url) or page_url
            if req_id and source_url.rstrip("/") == page_url.rstrip("/") and row_has_job_keys and self._title_has_role_signal_v51(title):
                parsed = urlparse(page_url)
                host_base = f"{parsed.scheme or 'https'}://{parsed.netloc}" if parsed.netloc else page_url.rstrip("/")
                source_url = f"{host_base}/jobdetails?jobAdId={req_id}"

            if self._is_non_job_url_v51(source_url):
                continue
            if not self._is_title_acceptable_v51(title, source_url):
                continue
            if not self._title_has_role_signal_v51(title) and not self._job_url_has_detail_evidence_v51(source_url, page_url):
                continue

            description = self._clean_description_v51(
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
            location = self._extract_location_from_json_v51(row)
            employment_type = str(row.get("employmentType") or row.get("jobType") or row.get("JobType") or "").strip() or None
            salary_raw = self._extract_salary_v51(json.dumps(row, ensure_ascii=True, default=str))

            has_evidence = (
                self._job_url_has_detail_evidence_v51(source_url, page_url)
                or bool(location)
                or bool(employment_type)
                or bool(salary_raw)
                or bool(req_id)
                or (description is not None and len(description) >= 120)
            )
            if not has_evidence:
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

        return self._prepare_jobs_v51(jobs, page_url)

    def _extract_greenhouse_rows_v51(self, root: etree._Element, page_url: str) -> list[dict]:
        rows = root.xpath("//tr[contains(@class,'job-post')]")
        if len(rows) < 2:
            return []

        jobs: list[dict] = []
        for row in rows[:500]:
            link_nodes = row.xpath(".//a[@href][1]")
            title_nodes = row.xpath(".//p[contains(@class,'body--medium')][1]")
            if not link_nodes or not title_nodes:
                continue

            title = self._normalize_title_v51(_text(title_nodes[0]))
            source_url = _resolve_url(link_nodes[0].get("href"), page_url)
            if not source_url or not self._is_title_acceptable_v51(title, source_url):
                continue

            location = None
            meta_nodes = row.xpath(".//p[contains(@class,'body--metadata')][1]")
            if meta_nodes:
                location = self._normalize_space_v51(_text(meta_nodes[0]))[:140] or None

            row_text = _text(row)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": self._extract_salary_v51(row_text),
                    "employment_type": self._extract_job_type_v51(row_text),
                    "description": self._clean_description_v51(row_text),
                    "extraction_method": "tier2_greenhouse_rows_v51",
                    "extraction_confidence": 0.9,
                }
            )

        return self._prepare_jobs_v51(jobs, page_url)

    def _extract_salesforce_rows_v51(self, root: etree._Element, page_url: str) -> list[dict]:
        rows = root.xpath("//tr[contains(@class,'dataRow')]")
        if len(rows) < 2:
            return []

        jobs: list[dict] = []
        for row in rows[:500]:
            title_nodes = row.xpath(".//td[2]//a[@href][1]")
            if not title_nodes:
                continue

            title = self._normalize_title_v51(_text(title_nodes[0]))
            source_url = _resolve_url(title_nodes[0].get("href"), page_url)
            if not source_url or not self._is_title_acceptable_v51(title, source_url):
                continue

            loc_nodes = row.xpath(".//td[4][1]")
            location = self._normalize_space_v51(_text(loc_nodes[0]))[:140] if loc_nodes else None
            row_text = _text(row)

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location or None,
                    "salary_raw": self._extract_salary_v51(row_text),
                    "employment_type": self._extract_job_type_v51(row_text),
                    "description": self._clean_description_v51(row_text),
                    "extraction_method": "tier2_salesforce_table_v51",
                    "extraction_confidence": 0.88,
                }
            )

        return self._prepare_jobs_v51(jobs, page_url)

    def _extract_heading_blocks_v51(self, root: etree._Element, page_url: str) -> list[dict]:
        headings = root.xpath("//h2 | //h3 | //h4")
        if len(headings) < 2:
            return []

        jobs: list[dict] = []
        seen_titles: set[str] = set()

        for heading in headings[:700]:
            raw_title = self._normalize_space_v51(_text(heading))
            if not raw_title:
                continue

            title = self._normalize_title_v51(raw_title)
            if not self._title_has_role_signal_v51(title):
                continue
            if title.lower() in seen_titles:
                continue

            parent = heading.getparent()
            if parent is None:
                continue

            block_text = self._normalize_space_v51(_text(parent))
            if len(block_text) < 90:
                continue

            apply_evidence = bool(_APPLY_CONTEXT_PATTERN_V51.search(block_text))
            has_mailto = bool(parent.xpath(".//a[starts-with(@href,'mailto:')]") or heading.xpath("following::a[starts-with(@href,'mailto:')][1]"))
            if not (apply_evidence or has_mailto):
                continue

            source_url = page_url
            link_nodes = parent.xpath(".//a[@href]")
            for a_el in link_nodes:
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v51(href):
                    continue
                link_text = self._normalize_space_v51(_text(a_el)).lower()
                if link_text in {"details", "view details", "job details", "read more", "apply", "apply now"}:
                    source_url = href
                    break
                if self._job_url_has_detail_evidence_v51(href, page_url):
                    source_url = href
                    break

            if not self._is_title_acceptable_v51(title, source_url):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v51(block_text),
                    "salary_raw": self._extract_salary_v51(block_text),
                    "employment_type": self._extract_job_type_v51(block_text),
                    "description": self._clean_description_v51(block_text),
                    "extraction_method": "tier2_heading_block_v51",
                    "extraction_confidence": 0.78,
                }
            )
            seen_titles.add(title.lower())

        return self._prepare_jobs_v51(jobs, page_url)

    def _extract_job_links_v51(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        for a_el in root.xpath("//a[@href]")[:4200]:
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url)
            if not source_url or self._is_non_job_url_v51(source_url):
                continue
            if not self._job_url_has_detail_evidence_v51(source_url, page_url):
                continue

            heading_nodes = a_el.xpath(".//h1 | .//h2 | .//h3 | .//h4")
            raw_title = _text(heading_nodes[0]) if heading_nodes else _text(a_el)
            title = self._normalize_title_v51(raw_title)
            if not self._is_title_acceptable_v51(title, source_url):
                continue

            context = a_el
            cursor = a_el
            for _ in range(4):
                parent = cursor.getparent()
                if parent is None:
                    break
                cursor = parent
                link_count = len(cursor.xpath(".//a[@href]"))
                if 1 <= link_count <= 16:
                    context = cursor
                    break

            context_text = self._normalize_space_v51(_text(context))
            if not (
                self._job_url_has_detail_evidence_v51(source_url, page_url)
                or _APPLY_CONTEXT_PATTERN_V51.search(context_text)
                or len(context_text) >= 120
            ):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v51(context_text),
                    "salary_raw": self._extract_salary_v51(context_text),
                    "employment_type": self._extract_job_type_v51(context_text),
                    "description": self._clean_description_v51(context_text),
                    "extraction_method": "tier2_job_links_v51",
                    "extraction_confidence": 0.74,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return self._prepare_jobs_v51(jobs, page_url)

    def _extract_rss_xml_jobs_v51(self, html_body: str, page_url: str) -> list[dict]:
        sample = (html_body or "")[:5000].lstrip().lower()
        if not (
            sample.startswith("<?xml")
            or "<rss" in sample
            or "<feed" in sample
            or (_RSS_ITEM_PATTERN_V51.search(sample) and "<channel" in sample)
        ):
            return []

        raw = (html_body or "").strip()
        if not raw:
            return []

        try:
            xml_root = etree.fromstring(raw.encode("utf-8", errors="replace"), parser=etree.XMLParser(recover=True))
        except Exception:
            return []

        items = xml_root.xpath("//*[local-name()='item']")
        if not items:
            items = xml_root.xpath("//*[local-name()='entry']")
        if not items:
            return []

        jobs: list[dict] = []
        for item in items[:MAX_JOBS_PER_PAGE * 3]:
            title_text = self._normalize_title_v51(self._xml_child_text_v51(item, "title"))
            if not title_text:
                continue

            source_url = _resolve_url(self._xml_child_text_v51(item, "link"), page_url)
            if not source_url:
                link_nodes = item.xpath("./*[local-name()='link']/@href")
                if link_nodes:
                    source_url = _resolve_url(str(link_nodes[0]), page_url)
            source_url = source_url or page_url

            if not self._is_title_acceptable_v51(title_text, source_url):
                continue

            desc_raw = self._xml_child_text_v51(item, "description")
            if not desc_raw:
                desc_raw = self._xml_child_text_v51(item, "content")
            desc_text = self._clean_description_v51(self._strip_html_v51(desc_raw))

            location = None
            loc_match = re.search(r"\bLocation\s*:\s*([^<\n]{2,140})", desc_raw or "", re.IGNORECASE)
            if loc_match:
                location = self._normalize_space_v51(loc_match.group(1))[:140]
            if not location:
                location = self._extract_location_v51(desc_text or "")

            jobs.append(
                {
                    "title": title_text,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": self._extract_salary_v51(desc_text or ""),
                    "employment_type": self._extract_job_type_v51(desc_text or ""),
                    "description": desc_text,
                    "extraction_method": "tier0_rss_xml_v51",
                    "extraction_confidence": 0.84,
                }
            )

        return self._prepare_jobs_v51(jobs, page_url)

    async def _extract_oracle_api_jobs_v51(self, page_url: str, html_body: str) -> list[dict]:
        parsed = urlparse(page_url)
        if not parsed.netloc:
            return []

        base = f"{parsed.scheme or 'https'}://{parsed.netloc}"

        site_candidates: list[str] = []
        for value in re.findall(r"siteNumber\s*[:=]\s*['\"]([A-Z0-9_]+)", html_body or "", flags=re.IGNORECASE):
            if value and value not in site_candidates:
                site_candidates.append(value)

        for value in re.findall(r"/sites/([A-Z0-9_]+)/", page_url or "", flags=re.IGNORECASE):
            if value and value not in site_candidates:
                site_candidates.append(value)

        for fallback in ("CX_1001", "CX"):
            if fallback not in site_candidates:
                site_candidates.append(fallback)

        jobs: list[dict] = []
        async with httpx.AsyncClient(
            timeout=4.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "application/json,text/plain,*/*",
                "Referer": page_url,
            },
        ) as client:
            for site_number in site_candidates[:4]:
                api_url = (
                    f"{base}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
                    f"?onlyData=true&expand=requisitionList.secondaryLocations"
                    f"&finder=findReqs;siteNumber={site_number},"
                    f"facetsList=LOCATIONS%3BWORK_LOCATIONS%3BTITLES%3BCATEGORIES%3BPOSTING_DATES,"
                    f"limit=50,offset=0"
                )
                try:
                    resp = await client.get(api_url)
                except Exception:
                    continue
                if resp.status_code >= 400 or not resp.text:
                    continue

                try:
                    payload = resp.json()
                except Exception:
                    continue

                rows = []
                items = payload.get("items") if isinstance(payload, dict) else None
                if isinstance(items, list):
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        req_list = item.get("requisitionList")
                        if isinstance(req_list, list):
                            rows.extend(r for r in req_list if isinstance(r, dict))
                        elif item.get("Title"):
                            rows.append(item)

                if not rows:
                    continue

                for row in rows:
                    title = self._normalize_title_v51(str(row.get("Title") or row.get("title") or ""))
                    req_id = str(row.get("Id") or row.get("id") or row.get("RequisitionNumber") or "").strip()
                    if not req_id:
                        continue
                    source_url = f"{base}/hcmUI/CandidateExperience/en/sites/{site_number}/job/{req_id}"
                    if not self._is_title_acceptable_v51(title, source_url):
                        continue

                    jobs.append(
                        {
                            "title": title,
                            "source_url": source_url,
                            "location_raw": self._normalize_space_v51(
                                " ".join(
                                    p
                                    for p in (
                                        str(row.get("PrimaryLocation") or "").strip(),
                                        str(row.get("PrimaryLocationCountry") or "").strip(),
                                    )
                                    if p
                                )
                            )
                            or None,
                            "salary_raw": None,
                            "employment_type": None,
                            "description": None,
                            "extraction_method": "tier0_oracle_api_v51",
                            "extraction_confidence": 0.88,
                        }
                    )

                prepared = self._prepare_jobs_v51(jobs, page_url)
                if len(prepared) >= MIN_JOBS_FOR_SUCCESS:
                    return prepared

        return self._prepare_jobs_v51(jobs, page_url)

    async def _extract_martian_shell_jobs_v51(self, page_url: str, html_body: str) -> list[dict]:
        context = self._extract_martian_context_v51(page_url, html_body)
        if not (context.get("client_code") or context.get("recruiter_id")):
            return []

        probe_urls = self._martian_probe_urls_v51(page_url, context)
        if not probe_urls:
            return []

        jobs: list[dict] = []
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
            for endpoint in probe_urls[:12]:
                norm = endpoint.rstrip("/")
                if not endpoint or norm in seen:
                    continue
                seen.add(norm)

                try:
                    resp = await client.get(endpoint)
                except Exception:
                    continue

                if resp.status_code >= 400 or not resp.text:
                    continue

                payload_jobs = self._extract_jobs_from_martian_payload_v51(resp.text, str(resp.url), page_url)
                if payload_jobs:
                    jobs.extend(payload_jobs)
                    prepared = self._prepare_jobs_v51(jobs, page_url)
                    if len(prepared) >= MIN_JOBS_FOR_SUCCESS:
                        return prepared

        return self._prepare_jobs_v51(jobs, page_url)

    def _extract_jobs_from_martian_payload_v51(self, body: str, response_url: str, page_url: str) -> list[dict]:
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
                jobs.extend(self._extract_jobs_from_json_payload_v51(parsed, response_url, "tier0_martian_probe_v51"))

        root = _parse_html(payload)
        if root is not None and any(marker in payload[:3000].lower() for marker in ("job-card", "p-4", "mx-4", "apply", "job-post")):
            jobs.extend(self._extract_greenhouse_rows_v51(root, response_url))
            jobs.extend(self._extract_salesforce_rows_v51(root, response_url))
            jobs.extend(self._extract_job_links_v51(root, response_url))

        return self._prepare_jobs_v51(jobs, page_url)

    def _extract_martian_context_v51(self, page_url: str, html_body: str) -> dict[str, str]:
        context = {
            "client_code": "",
            "recruiter_id": "",
            "build_id": "",
            "next_query": "",
        }

        match = _NEXT_DATA_PATTERN_V51.search(html_body or "")
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
                context["build_id"] = str(parsed.get("buildId") or "").strip()

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

    def _martian_probe_urls_v51(self, page_url: str, context: dict[str, str]) -> list[str]:
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

        urls: list[str] = []
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
                urls.append(base)
                for q in query_parts:
                    urls.append(f"{base}?{q}")

            if client_code:
                base = f"{host}/{client_code}"
                urls.append(base)
                for q in query_parts:
                    urls.append(f"{base}?{q}")

            if build_id:
                urls.extend(self._next_data_probe_urls_v51(host, page_url, context))

        deduped = sorted({u.rstrip("/"): u for u in urls if u}.values())
        deduped.sort(key=lambda u: self._martian_endpoint_priority_v51(u, parsed.netloc.lower()), reverse=True)
        return deduped[:18]

    def _next_data_probe_urls_v51(self, host: str, page_url: str, context: dict[str, str]) -> list[str]:
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
    def _martian_endpoint_priority_v51(url: str, page_host: str) -> int:
        low = (url or "").lower()
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()

        score = 0
        if host == page_host:
            score += 14
        if "/api/" in low:
            score += 8
        if "search" in low:
            score += 6
        if "/_next/data/" in low:
            score += 5
        if "client=" in low or "clientcode=" in low:
            score += 4
        if "recruiterid=" in low:
            score += 3
        return score

    async def _recover_from_probe_paths_v51(self, page_url: str, html_body: str) -> list[dict]:
        parsed = urlparse(page_url)
        if not parsed.netloc:
            return []

        base = f"{parsed.scheme or 'https'}://{parsed.netloc}"
        probe_urls = self._build_recovery_probe_urls_v51(base, page_url, html_body)
        if not probe_urls:
            return []

        candidates: list[tuple[str, list[dict]]] = []
        async with httpx.AsyncClient(
            timeout=3.8,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": page_url,
            },
        ) as client:
            for probe_url in probe_urls[:8]:
                try:
                    resp = await client.get(probe_url)
                except Exception:
                    continue

                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 120:
                    continue

                resolved = str(resp.url)
                if self._looks_non_html_payload_v51(body) and not self._looks_like_feed_xml_v51(body):
                    continue

                extracted = self._extract_local_payload_jobs_v51(resolved, body)
                if extracted:
                    candidates.append((resolved, extracted))

        if not candidates:
            return []

        _label, jobs, _score = self._pick_best_candidate_v51(candidates, page_url)
        return jobs

    def _build_recovery_probe_urls_v51(self, base_url: str, page_url: str, html_body: str) -> list[str]:
        parsed = urlparse(page_url)
        host = (parsed.netloc or "").lower()

        paths = [
            "/careers",
            "/career",
            "/jobs",
            "/job-openings",
            "/openings",
            "/vacancies",
            "/join-our-team",
            "/recruit/Portal.na",
        ]

        if "salesforce-sites.com" in host:
            paths.extend([
                "/careers/",
                "/careers/fRecruit__ApplyJobList?portal=English",
            ])

        if "oraclecloud.com" in host:
            paths.extend([
                "/hcmUI/CandidateExperience/en/sites/CX/requisitions",
                "/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions",
            ])
            for site in re.findall(r"siteNumber\s*[:=]\s*['\"]([A-Z0-9_]+)", html_body or "", flags=re.IGNORECASE):
                site = site.strip()
                if site:
                    paths.append(f"/hcmUI/CandidateExperience/en/sites/{site}/requisitions")

        if "zohorecruit" in host:
            paths.append("/recruit/Portal.na")

        urls: list[str] = []
        for path in paths:
            if path.startswith("http"):
                urls.append(path)
            else:
                urls.append(f"{base_url}{path}")

        if "job-boards.greenhouse.io" in host:
            query = dict(parse_qsl(parsed.query))
            slug = (query.get("for") or "").strip()
            if not slug:
                parts = [seg for seg in parsed.path.split("/") if seg]
                if parts:
                    maybe_slug = re.sub(r"[^a-z0-9-]", "", parts[-1].lower())
                    if maybe_slug and maybe_slug not in {"embed", "job_board", "jobs"}:
                        slug = maybe_slug
            if slug:
                urls.append(f"https://job-boards.greenhouse.io/embed/job_board?for={slug}")
                urls.append(f"https://job-boards.greenhouse.io/{slug}")

        deduped: list[str] = []
        seen: set[str] = set()
        for url in urls:
            norm = url.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
            deduped.append(url)
        return deduped

    def _pick_best_candidate_v51(
        self,
        candidates: list[tuple[str, list[dict]]],
        page_url: str,
    ) -> tuple[str, list[dict], float]:
        if not candidates:
            return "", [], 0.0

        scored: list[tuple[str, list[dict], float]] = []
        for label, jobs in candidates:
            prepared = self._prepare_jobs_v51(jobs, page_url)
            if not prepared:
                continue
            scored.append((label, prepared, self._candidate_score_v51(prepared, page_url)))

        if not scored:
            return "", [], 0.0

        best_label, best_jobs, best_score = max(scored, key=lambda item: item[2])

        for label, jobs, score in scored:
            if label == best_label:
                continue
            overlap = self._title_overlap_ratio_v51(jobs, best_jobs)
            if len(jobs) >= len(best_jobs) + 1 and overlap >= 0.6 and score >= best_score - 1.2:
                best_label, best_jobs, best_score = label, jobs, score

        return best_label, best_jobs[:MAX_JOBS_PER_PAGE], best_score

    def _prepare_jobs_v51(self, jobs: list[dict], page_url: str) -> list[dict]:
        cleaned: list[dict] = []

        for idx, raw in enumerate(jobs or []):
            title = self._normalize_title_v51(str(raw.get("title") or ""))
            source_url = _resolve_url(raw.get("source_url"), page_url) or page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]

            description = self._clean_description_v51(str(raw.get("description") or ""))
            if self._is_non_job_url_v51(source_url):
                continue
            if not self._is_title_acceptable_v51(title, source_url):
                continue

            cleaned.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": raw.get("location_raw") or None,
                    "salary_raw": raw.get("salary_raw") or None,
                    "employment_type": raw.get("employment_type") or self._extract_job_type_v51(description or ""),
                    "description": description,
                    "extraction_method": raw.get("extraction_method") or "tier2_v51",
                    "extraction_confidence": raw.get("extraction_confidence", 0.66),
                    "_order": idx,
                }
            )

        deduped = self._dedupe_jobs_v51(cleaned, page_url)
        if not self._is_valid_jobset_v51(deduped, page_url):
            return []
        return deduped

    def _dedupe_jobs_v51(self, jobs: list[dict], page_url: str) -> list[dict]:
        by_key: dict[tuple[str, str], dict] = {}
        by_url: dict[str, dict] = {}
        page_norm = (page_url or "").rstrip("/").lower()

        for job in jobs:
            title = self._normalize_title_v51(job.get("title", ""))
            url = _resolve_url(job.get("source_url"), page_url) or page_url
            key = (title.lower(), url.lower())
            existing = by_key.get(key)
            if existing is None or self._title_quality_score_v51(title) > self._title_quality_score_v51(existing.get("title", "")):
                by_key[key] = job

        for job in by_key.values():
            norm_url = (_resolve_url(job.get("source_url"), page_url) or page_url).rstrip("/").lower()
            if norm_url == page_norm:
                same_page_key = f"{norm_url}#title:{self._normalize_title_v51(job.get('title', '')).lower()}"
                by_url[same_page_key] = job
                continue

            current = by_url.get(norm_url)
            if current is None:
                by_url[norm_url] = job
                continue
            if self._title_quality_score_v51(job.get("title", "")) > self._title_quality_score_v51(current.get("title", "")):
                by_url[norm_url] = job

        deduped = sorted(by_url.values(), key=lambda item: int(item.get("_order", 0)))
        for item in deduped:
            item.pop("_order", None)
        return deduped[:MAX_JOBS_PER_PAGE]

    def _is_valid_jobset_v51(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title_v51(j.get("title", "")) for j in jobs if j.get("title")]
        if not titles:
            return False

        role_hits = sum(1 for t in titles if self._title_has_role_signal_v51(t))
        reject_hits = sum(1 for t in titles if _TITLE_REJECT_PATTERN_V51.match(t.lower()))
        evidence_hits = sum(1 for j in jobs if self._job_has_evidence_v51(j, page_url))

        if reject_hits >= max(1, int(len(titles) * 0.2)):
            return False

        if len(titles) <= 2:
            return role_hits == len(titles) and evidence_hits >= 1

        if role_hits < max(2, int(len(titles) * 0.5)):
            return False

        return evidence_hits >= max(2, int(len(titles) * 0.4))

    def _candidate_score_v51(self, jobs: list[dict], page_url: str) -> float:
        titles = [self._normalize_title_v51(j.get("title", "")) for j in jobs]
        role_hits = sum(1 for t in titles if self._title_has_role_signal_v51(t))
        detail_hits = sum(1 for j in jobs if self._job_url_has_detail_evidence_v51(str(j.get("source_url") or ""), page_url))
        evidence_hits = sum(1 for j in jobs if self._job_has_evidence_v51(j, page_url))

        score = len(jobs) * 4.4
        score += role_hits * 2.5
        score += detail_hits * 2.2
        score += evidence_hits * 1.3
        return score

    def _job_has_evidence_v51(self, job: dict, page_url: str) -> bool:
        source_url = (job.get("source_url") or page_url).strip() or page_url
        desc = str(job.get("description") or "")

        if self._job_url_has_detail_evidence_v51(source_url, page_url):
            return True
        if job.get("salary_raw") or job.get("employment_type") or job.get("location_raw"):
            return True
        if _APPLY_CONTEXT_PATTERN_V51.search(desc):
            return True
        if "mailto:" in source_url.lower():
            return True
        return len(desc.strip()) >= 170

    def _normalize_title_v51(self, title: str) -> str:
        value = html_lib.unescape((title or "").strip())
        value = re.sub(r"\s+", " ", value)

        # Split glued boundaries like "EngineerVijayawada" -> "Engineer Vijayawada".
        value = re.sub(r"(?<=[a-z])(?=[A-Z][a-z])", " ", value)

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

        # Remove obvious trailing location suffixes after deglue.
        value = re.sub(r"\s+(?:Remote|Indonesia|India|Malaysia|Singapore|Australia|Philippines)$", "", value, flags=re.IGNORECASE)
        return value

    def _is_title_acceptable_v51(self, title: str, source_url: str) -> bool:
        if not self._is_valid_title_text_v51(title):
            return False

        if self._title_has_role_signal_v51(title):
            return True

        if self._is_job_like_url_v51(source_url) and len(title.split()) <= 8:
            return True

        return False

    def _is_valid_title_text_v51(self, title: str) -> bool:
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
        if _TITLE_REJECT_PATTERN_V51.match(lower):
            return False
        if _HEADING_REJECT_PATTERN_V51.match(lower):
            return False
        if any(phrase == lower for phrase in ("sign in", "log in", "register", "apply now", "search jobs")):
            return False

        if super()._is_valid_title_v16(t):
            return True

        if len(t.split()) == 1 and len(t) <= 32 and self._title_has_role_signal_v51(t):
            return True

        return False

    def _title_has_role_signal_v51(self, title: str) -> bool:
        if not title:
            return False
        return bool(_ROLE_HINT_PATTERN_V51.search(title) or _title_has_job_noun(title) or self._is_acronym_title_v51(title))

    @staticmethod
    def _is_acronym_title_v51(title: str) -> bool:
        t = (title or "").strip()
        if not re.fullmatch(r"[A-Z][A-Z0-9&/\-\+]{1,10}", t):
            return False
        return t.lower() not in {"home", "menu", "faq", "apply"}

    def _job_url_has_detail_evidence_v51(self, url: str, page_url: str) -> bool:
        value = (url or "").strip()
        if not value or self._is_non_job_url_v51(value):
            return False
        if self._is_job_like_url_v51(value):
            return True

        if page_url and value.rstrip("/") == page_url.rstrip("/"):
            return False

        parsed = urlparse(value)
        parts = [p for p in (parsed.path or "").split("/") if p]
        if not parts:
            return False

        leaf = parts[-1].lower()
        if leaf in {"career", "careers", "jobs", "job", "vacancies", "vacancy", "openings", "positions", "position", "join-our-team"}:
            return False
        if leaf in {"index", "home", "about", "contact", "news", "blog", "privacy", "terms"}:
            return False

        if len(leaf) >= 6 and ("-" in leaf or re.search(r"\d", leaf) or len(parts) >= 2):
            return True
        return len(leaf) >= 10

    def _is_job_like_url_v51(self, url: str) -> bool:
        value = (url or "").strip()
        if not value or self._is_non_job_url_v51(value):
            return False
        if _DETAIL_URL_PATTERN_V51.search(value):
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
        if re.search(r"(?:^|&)(?:jobid|job_id|requisitionid|vacancyid|jobadid|adid|ajid|vacancyno)=", query):
            return True

        return False

    @staticmethod
    def _is_non_job_url_v51(url: str) -> bool:
        value = (url or "").strip().lower()
        if not value:
            return False
        if value.startswith("mailto:") or value.startswith("tel:"):
            return True
        return bool(_NON_JOB_URL_PATTERN_V51.search(value))

    @staticmethod
    def _looks_non_html_payload_v51(body: Optional[str]) -> bool:
        if not body:
            return True
        sample = body[:1200].lstrip()
        if sample.startswith("%PDF-"):
            return True
        low = sample.lower()
        if (low.startswith("{") or low.startswith("[")) and "<html" not in low[:300]:
            return True
        return False

    @staticmethod
    def _looks_like_feed_xml_v51(body: str) -> bool:
        sample = (body or "")[:4000].lstrip().lower()
        return sample.startswith("<?xml") or "<rss" in sample or "<feed" in sample

    @staticmethod
    def _looks_like_oracle_shell_v51(page_url: str, html_body: str) -> bool:
        low = (html_body or "").lower()
        if "oraclecloud.com" in (page_url or "").lower():
            return True
        return "cx_config" in low and "candidateexperience" in low

    @staticmethod
    def _looks_like_martian_shell_v51(page_url: str, html_body: str) -> bool:
        lower = (html_body or "").lower()
        if "<div id=\"__next\"></div>" in lower and "__next_data__" in lower:
            return True
        if len(lower) < 350:
            return False
        if _MARTIAN_SHELL_PATTERN_V51.search(lower):
            return True
        if "myrecruitmentplus" in (page_url or "").lower() or "martianlogic" in (page_url or "").lower():
            return True
        return False

    def _should_probe_paths_v51(self, best_jobs: list[dict], best_score: float, html_body: str) -> bool:
        if not best_jobs:
            return True
        if len(best_jobs) < MIN_JOBS_FOR_SUCCESS:
            return True
        if self._looks_non_html_payload_v51(html_body):
            return True
        if len(html_body or "") < 1400:
            return True
        return best_score < 12.0

    def _should_enrich_v51(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False
        off_page = [
            j
            for j in jobs
            if (j.get("source_url") or "").startswith("http")
            and (j.get("source_url") or page_url).rstrip("/") != page_url.rstrip("/")
            and not self._is_non_job_url_v51(j.get("source_url") or "")
        ]
        if not off_page:
            return False

        low_quality = sum(1 for j in off_page if not j.get("description") or len(str(j.get("description") or "")) < 170)
        if low_quality >= 2:
            return True

        return len(off_page) >= 5

    @staticmethod
    def _within_budget_v51(started: float, limit_seconds: float) -> bool:
        return (asyncio.get_running_loop().time() - started) < limit_seconds

    def _extract_location_v51(self, text: str) -> Optional[str]:
        if not text:
            return None
        match = _AU_LOCATIONS.search(text)
        if match:
            return match.group(0)[:120]

        quick = re.search(r"\b(?:Remote(?:\s*[-–]\s*[A-Za-z ]+)?|[A-Za-z ]+,\s*[A-Za-z]{2,})\b", text)
        if quick:
            return self._normalize_space_v51(quick.group(0))[:120]
        return None

    @staticmethod
    def _extract_salary_v51(text: str) -> Optional[str]:
        if not text:
            return None
        match = _SALARY_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:120]
        return None

    @staticmethod
    def _extract_job_type_v51(text: str) -> Optional[str]:
        if not text:
            return None
        match = _JOB_TYPE_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:80]
        return None

    def _extract_location_from_json_v51(self, node: dict[str, Any]) -> Optional[str]:
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
                    nested = self._extract_location_from_json_v51(item)
                    if nested:
                        return nested
        return None

    @staticmethod
    def _iter_json_dicts_v51(payload: Any) -> list[dict[str, Any]]:
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
            if len(out) >= 8000:
                break
        return out

    @staticmethod
    def _looks_like_taxonomy_node_v51(node: dict[str, Any], title: str) -> bool:
        lowered = title.lower().strip()
        if _TITLE_REJECT_PATTERN_V51.match(lowered):
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
            "absolute_url",
        }

        if len(keyset & taxonomy_keys) >= 2 and not (keyset & evidence_keys):
            return True
        if "count" in keyset and any(k in keyset for k in ("department", "category", "team")):
            return True
        return False

    def _title_quality_score_v51(self, title: str) -> float:
        t = self._normalize_title_v51(title)
        if not t:
            return -10.0
        score = 0.0
        score += 2.0 if self._title_has_role_signal_v51(t) else 0.0
        score += 1.0 if self._is_valid_title_text_v51(t) else 0.0
        score -= max(0.0, (len(t) - 90) / 80.0)
        return score

    def _title_overlap_ratio_v51(self, a_jobs: list[dict], b_jobs: list[dict]) -> float:
        a_titles = {self._normalize_title_v51(j.get("title", "")).lower() for j in a_jobs if j.get("title")}
        b_titles = {self._normalize_title_v51(j.get("title", "")).lower() for j in b_jobs if j.get("title")}
        if not a_titles or not b_titles:
            return 0.0
        return len(a_titles & b_titles) / max(1, min(len(a_titles), len(b_titles)))

    def _clean_description_v51(self, description: str) -> Optional[str]:
        text = self._normalize_space_v51(description)
        if not text:
            return None
        cut = re.search(r"\b(?:how\s+to\s+apply|cara\s+melamar|application\s+process)\b", text, re.IGNORECASE)
        if cut:
            text = text[: cut.start()].strip()
        if len(text) < 45:
            return None
        return text[:5000]

    @staticmethod
    def _normalize_space_v51(text: str) -> str:
        return " ".join((text or "").split())

    @staticmethod
    def _strip_html_v51(value: str) -> str:
        if not value:
            return ""
        root = _parse_html(value)
        if root is None:
            return value
        return _text(root)

    @staticmethod
    def _xml_child_text_v51(node: etree._Element, child_name: str) -> str:
        out = node.xpath(f"./*[local-name()='{child_name}'][1]")
        if not out:
            return ""
        raw = out[0]
        if isinstance(raw, etree._Element):
            txt = "".join(raw.itertext())
            return txt.strip()
        return str(raw).strip()
