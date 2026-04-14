"""
Tiered Extraction Engine v2.5 — direct from v1.6 with recovery-first upgrades.

High-impact changes:
1. Detail-to-listing recovery: when discovery lands on a single job detail page,
   probe nearby listing URLs and re-extract.
2. Feed-first Tier 0 parsing: robust XML/RSS + JSON-LD extraction, including
   CDATA-heavy feeds.
3. ATS recovery fallbacks: Oracle CX requisition API pull + Greenhouse board probing.
4. Prose/accordion extraction for heading-led listings with apply/context checks.
5. Stricter jobset validation to suppress navigation/corporate false positives.
"""

from __future__ import annotations

import asyncio
import html
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


_TITLE_HINT_PATTERN_V25 = re.compile(
    r"\b(?:"
    r"job|jobs|career|careers|vacanc(?:y|ies)|opening|openings|position|positions|"
    r"hiring|recruit(?:er|ment)?|"
    r"engineer|developer|manager|director|officer|specialist|assistant|analyst|"
    r"consultant|coordinator|administrator|executive|technician|designer|activator|"
    r"architect|accountant|nurse|teacher|operator|supervisor|owner|"
    r"intern(?:ship)?|trainee|"
    r"lowongan|loker|karir|karier|kerjaya|pekerjaan|jawatan|"
    r"penganalisis|kredit|latihan|industri|"
    r"vacantes?|empleo|trabajo|vagas?|stellen(?:angebote)?|jobsuche"
    r")\b",
    re.IGNORECASE,
)

_REJECT_TITLE_PATTERN_V25 = re.compile(
    r"^(?:"
    r"apply\s+now|apply\s+here|read\s+more|learn\s+more|show\s+more|"
    r"open\s+roles?|careers?|jobs?|job\s+openings?|current\s+vacancies|"
    r"job\s+alerts?|saved\s+jobs?|manage\s+applications?|"
    r"about\s+us|contact\s+us|privacy|terms|help|login|register|my\s+account|"
    r"info\s+korporat|hubungi\s+kami|organisasi|perkhidmatan|sumber|media|"
    r"department|departments|locations?|all\s+jobs?|"
    r"job\s+description|internship\s+details|board\s+nominations?|board\s+role\s+description"
    r")$",
    re.IGNORECASE,
)

_GENERIC_LISTING_LABEL_PATTERN_V25 = re.compile(
    r"^(?:"
    r"jobs?|careers?|vacancies|open\s+positions?|job\s+openings?|"
    r"lowongan|loker|kerjaya|current\s+vacancies|open\s+roles?|"
    r"pelajar\s+dan\s+latihan\s+industri"
    r")$",
    re.IGNORECASE,
)

_CATEGORY_TITLE_PATTERN_V25 = re.compile(
    r"^(?:all\s+jobs?|jobs?\s+by|browse\s+jobs?|view\s+jobs?|"
    r".{2,80}\s+jobs?|.{2,80}\s+vacancies?)$",
    re.IGNORECASE,
)

_CORPORATE_TITLE_PATTERN_V25 = re.compile(
    r"^(?:"
    r"home|about|contact|company|our\s+company|our\s+culture|our\s+values|"
    r"blog|news|events|investor|investors|"
    r"info\s+korporat|hubungi\s+kami|organisasi|perkhidmatan|sumber|media"
    r")$",
    re.IGNORECASE,
)

_PHONE_TITLE_PATTERN_V25 = re.compile(r"^(?:\+?\d[\d\s().-]{6,}|\d{2,4}\s?\d{3,5}\s?\d{3,5})$")

_JOB_URL_HINT_PATTERN_V25 = re.compile(
    r"(?:/job|/jobs|/job-openings|/career|/careers|/position|/positions|/vacanc|"
    r"/opening|/openings|/requisition|/requisitions|/apply|"
    r"/portal\.na|/Portal\.na|/PortalDetail\.na|/ViewJob\.na|"
    r"candidateportal|hcmui/candidateexperience|embed/job_board|"
    r"jobid=|job_id=|requisitionid=)",
    re.IGNORECASE,
)

_LISTING_URL_PATTERN_V25 = re.compile(
    r"(?:/jobs?$|/jobs/|/job-openings|/careers?$|/vacanc|/openings?|"
    r"/requisitions?$|/Portal\.na|/embed/job_board|/jobs/search|/lowongan|/kerjaya)",
    re.IGNORECASE,
)

_DETAIL_URL_PATTERN_V25 = re.compile(
    r"(?:/jobs?/\d+[A-Za-z0-9_-]*|/jobs?/[a-z0-9][^/?#]{5,}|"
    r"/requisition[s]?/[a-z0-9][^/?#]{2,}|"
    r"[?&](?:jobid|job_id|requisitionid|positionid)=|/ViewJob\.na)",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V25 = re.compile(
    r"(?:/privacy|/terms|/cookie|/news|/blog|/about|/contact|/investor|"
    r"/help|/login|/logout|/register|/account|/comments/feed(?:/|$)|"
    r"/wp-login(?:\.php)?|event=help\.|event=reg\.)",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN_V25 = re.compile(
    r"job|position|vacanc|opening|requisition|career|posting|listing|accordion|awsm",
    re.IGNORECASE,
)

_APPLY_CONTEXT_PATTERN_V25 = re.compile(
    r"(?:apply|application|job\s+description|mailto:|closing\s+date|salary|"
    r"full\s*time|part\s*time|contract|permanent|temporary|remote|hybrid)",
    re.IGNORECASE,
)

_SCRIPT_ASSIGNMENT_PATTERN_V25 = re.compile(
    r"(?:window\.[A-Za-z0-9_$.]+\s*=\s*|var\s+[A-Za-z0-9_$]+\s*=\s*)(\{.*\}|\[.*\])\s*;?",
    re.DOTALL,
)

_ORACLE_SITE_PATTERN_V25 = re.compile(r"/sites/([A-Za-z0-9_]+)/", re.IGNORECASE)
_ORACLE_SITE_NUMBER_PATTERN_V25 = re.compile(r"siteNumber\s*[:=]\s*['\"]([A-Za-z0-9_]+)", re.IGNORECASE)
_ORACLE_API_BASE_PATTERN_V25 = re.compile(r"apiBaseUrl\s*:\s*['\"](https?://[^'\"\s]+)", re.IGNORECASE)


class TieredExtractorV25(TieredExtractorV16):
    """v2.5 extractor: recovery-first strategy on top of v1.6 core."""

    async def extract(self, career_page, company, html_body: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        body = html_body or ""

        page_l = (page_url or "").lower()
        body_l = body.lstrip()[:1200].lower()
        if "downloadrssfeed" in page_l or body_l.startswith("<?xml") or body_l.startswith("<rss") or body_l.startswith("<feed"):
            feed_jobs = self._extract_structured_jobs_v25(body, page_url)
            if feed_jobs:
                return feed_jobs[:MAX_JOBS_PER_PAGE]

        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(
                super().extract(career_page, company, body),
                timeout=24.0,
            )
        except asyncio.TimeoutError:
            logger.warning("v2.5 parent extractor timeout for %s", page_url)
        except Exception:
            logger.exception("v2.5 parent extractor failed for %s", page_url)

        parent_jobs = self._dedupe_jobs_v25(parent_jobs or [], page_url)

        candidates: list[tuple[str, list[dict]]] = []
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        structured_jobs = self._extract_structured_jobs_v25(body, page_url)
        if structured_jobs:
            candidates.append(("structured_v25", structured_jobs))

        oracle_jobs = await self._extract_oracle_api_jobs_v25(page_url, body)
        if oracle_jobs:
            candidates.append(("oracle_api_v25", oracle_jobs))

        root = _parse_html(body)
        if root is not None:
            greenhouse_dom_jobs = self._extract_greenhouse_dom_jobs_v25(root, page_url)
            if greenhouse_dom_jobs:
                candidates.append(("greenhouse_dom_v25", greenhouse_dom_jobs))

            table_jobs = self._extract_from_table_rows_v25(root, page_url)
            if table_jobs:
                candidates.append(("table_rows_v25", table_jobs))

            accordion_jobs = self._extract_from_accordion_sections_v25(root, page_url)
            if accordion_jobs:
                candidates.append(("accordion_v25", accordion_jobs))

            prose_jobs = self._extract_from_prose_headings_v25(root, page_url)
            if prose_jobs:
                candidates.append(("prose_v25", prose_jobs))

            collapsible_jobs = self._extract_from_collapsible_headings_v25(root, page_url)
            if collapsible_jobs:
                candidates.append(("collapsible_v25", collapsible_jobs))

            row_jobs = self._extract_from_repeating_rows_v25(root, page_url)
            if row_jobs:
                candidates.append(("repeating_rows_v25", row_jobs))

            link_cluster_jobs = self._extract_from_link_clusters_v25(root, page_url)
            if link_cluster_jobs:
                candidates.append(("link_clusters_v25", link_cluster_jobs))

        if self._should_try_detail_recovery_v25(page_url, body, parent_jobs):
            recovered_jobs = await self._recover_listing_jobs_v25(page_url, body)
            if recovered_jobs:
                candidates.append(("detail_recovery_v25", recovered_jobs))

        if "greenhouse" in (page_url or "").lower() and len(parent_jobs) <= 1:
            greenhouse_probe_jobs = await self._probe_greenhouse_board_jobs_v25(page_url)
            if greenhouse_probe_jobs:
                candidates.append(("greenhouse_probe_v25", greenhouse_probe_jobs))

        best_label, best_jobs = self._pick_best_jobset_v25(candidates, page_url)
        if not best_jobs:
            return []

        if (
            best_label != "parent_v16"
            and len(best_jobs) >= MIN_JOBS_FOR_SUCCESS
            and len(best_jobs) <= 40
        ):
            try:
                best_jobs = await asyncio.wait_for(self._enrich_from_detail_pages(best_jobs), timeout=12.0)
            except asyncio.TimeoutError:
                logger.warning("v2.5 enrichment timeout for %s", page_url)
            except Exception:
                logger.exception("v2.5 enrichment failed for %s", page_url)

        return self._dedupe_jobs_v25(best_jobs, page_url)[:MAX_JOBS_PER_PAGE]

    # ------------------------------------------------------------------
    # Tier 0 structured extraction
    # ------------------------------------------------------------------

    def _extract_structured_jobs_v25(self, html_body: str, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        stripped = (html_body or "").lstrip()
        if (
            stripped.startswith("<?xml")
            or stripped.startswith("<rss")
            or stripped.startswith("<feed")
            or "<rss" in stripped[:2000].lower()
            or "<item>" in stripped[:4000].lower()
        ):
            jobs.extend(self._parse_xml_feed_v25(html_body, page_url))

        for match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html_body,
            re.IGNORECASE | re.DOTALL,
        ):
            raw = (match.group(1) or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            jobs.extend(self._extract_jobs_from_jsonld_v25(data, page_url))

        return self._dedupe_jobs_v25(jobs, page_url)

    def _parse_xml_feed_v25(self, body: str, source_url: str) -> list[dict]:
        jobs: list[dict] = []
        try:
            root = etree.fromstring((body or "").encode("utf-8", errors="replace"))
        except Exception:
            return []

        for el in root.iter():
            if isinstance(el.tag, str) and "}" in el.tag:
                el.tag = el.tag.split("}", 1)[1]

        items = root.xpath("./channel/item | ./item | ./channel/entry | ./entry")
        if not items:
            items = root.findall(".//item") or root.findall(".//entry")

        max_items = 30 if "downloadrssfeed" in (source_url or "").lower() else 120

        for item in items[:max_items]:
            title = self._normalize_title_v25((item.findtext("title") or "").strip())
            if not self._is_valid_title_v25(title):
                continue
            if not self._title_has_job_signal_v25(title):
                continue

            link = ""
            link_el = item.find("link")
            if link_el is not None:
                link = (link_el.text or link_el.get("href") or "").strip()
            if link and not link.startswith("http"):
                link = _resolve_url(link, source_url) or source_url

            description = (
                item.findtext("description")
                or item.findtext("content")
                or item.findtext("summary")
                or ""
            ).strip()
            description = description[:5000] if description else None

            loc_match = _AU_LOCATIONS.search(description or "")
            location_raw = loc_match.group(0).strip() if loc_match else None

            type_match = _JOB_TYPE_PATTERN.search(description or "")
            employment_type = type_match.group(0).strip() if type_match else None

            salary_match = _SALARY_PATTERN.search(description or "")
            salary_raw = salary_match.group(0).strip() if salary_match else None

            jobs.append(
                {
                    "title": title,
                    "source_url": link or source_url,
                    "location_raw": location_raw,
                    "salary_raw": salary_raw,
                    "employment_type": employment_type,
                    "description": description,
                    "extraction_method": "tier0_xml_feed_v25",
                    "extraction_confidence": 0.92,
                }
            )

        return jobs

    def _extract_jobs_from_jsonld_v25(self, data: Any, page_url: str) -> list[dict]:
        queue = [data]
        jobs: list[dict] = []

        while queue:
            node = queue.pop(0)
            if isinstance(node, list):
                queue.extend(node[:200])
                continue
            if not isinstance(node, dict):
                continue

            graph = node.get("@graph")
            if isinstance(graph, list):
                queue.extend(graph[:200])
            queue.extend(list(node.values())[:120])

            node_type = node.get("@type")
            if isinstance(node_type, list):
                node_type = node_type[0] if node_type else ""
            if str(node_type).lower() != "jobposting":
                continue

            title = self._normalize_title_v25(str(node.get("title") or node.get("name") or "").strip())
            if not self._is_valid_title_v25(title):
                continue

            src = str(node.get("url") or node.get("sameAs") or page_url).strip()
            source_url = _resolve_url(src, page_url) or page_url

            description = node.get("description")
            if isinstance(description, str):
                if "<" in description:
                    desc_root = _parse_html(description)
                    description = _text(desc_root) if desc_root is not None else description
                description = description.strip()[:5000] or None
            else:
                description = None

            location_raw = self._extract_location_from_jsonld_v25(node)
            salary_raw = self._extract_salary_from_jsonld_v25(node)

            employment_type = node.get("employmentType")
            if isinstance(employment_type, list):
                employment_type = ", ".join(str(v).strip() for v in employment_type if str(v).strip()) or None
            elif employment_type is not None:
                employment_type = str(employment_type).strip() or None

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location_raw,
                    "salary_raw": salary_raw,
                    "employment_type": employment_type,
                    "description": description,
                    "extraction_method": "tier0_jsonld_v25",
                    "extraction_confidence": 0.95,
                }
            )

            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return jobs

    @staticmethod
    def _extract_location_from_jsonld_v25(item: dict) -> Optional[str]:
        loc = item.get("jobLocation")
        if not loc:
            return None

        entries = loc if isinstance(loc, list) else [loc]
        parts: list[str] = []
        for entry in entries:
            if isinstance(entry, str):
                if entry.strip():
                    parts.append(entry.strip())
                continue
            if not isinstance(entry, dict):
                continue
            address = entry.get("address")
            if isinstance(address, str):
                if address.strip():
                    parts.append(address.strip())
                continue
            if isinstance(address, dict):
                city = str(address.get("addressLocality") or "").strip()
                region = str(address.get("addressRegion") or "").strip()
                country = str(address.get("addressCountry") or "").strip()
                row = ", ".join(v for v in (city, region, country) if v)
                if row:
                    parts.append(row)

        if not parts:
            return None
        return " | ".join(dict.fromkeys(parts))[:200]

    @staticmethod
    def _extract_salary_from_jsonld_v25(item: dict) -> Optional[str]:
        base = item.get("baseSalary")
        if isinstance(base, dict):
            currency = str(base.get("currency") or "").strip()
            value = base.get("value")
            if isinstance(value, dict):
                min_v = value.get("minValue")
                max_v = value.get("maxValue")
                unit = str(value.get("unitText") or "").strip()
                if min_v and max_v:
                    return f"{currency} {min_v}-{max_v} {unit}".strip()
                if min_v:
                    return f"{currency} {min_v} {unit}".strip()
        return None

    # ------------------------------------------------------------------
    # DOM-based fallbacks
    # ------------------------------------------------------------------

    def _extract_greenhouse_dom_jobs_v25(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        rows = root.xpath("//*[contains(@class,'job-post')]")
        for row in rows[:MAX_JOBS_PER_PAGE]:
            title = ""
            title_nodes = row.xpath(".//*[contains(@class,'body--medium')] | .//h1 | .//h2 | .//h3 | .//a")
            for node in title_nodes[:6]:
                candidate = self._normalize_title_v25(_text(node))
                if not self._is_valid_title_v25(candidate):
                    continue
                if not self._title_has_job_signal_v25(candidate):
                    continue
                title = candidate
                break
            if not title:
                continue

            link = row.xpath(".//a[@href][1]")
            href = link[0].get("href") if link else None
            source_url = _resolve_url(href, page_url) or page_url

            location_text = None
            loc_node = row.xpath(".//*[contains(@class,'location') or contains(@class,'metadata')]")
            if loc_node:
                loc_candidate = " ".join(_text(loc_node[0]).split())
                if 1 <= len(loc_candidate) <= 160:
                    location_text = loc_candidate

            desc = _text(row)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location_text,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": desc[:5000] if len(desc) > 80 else None,
                    "extraction_method": "tier2_greenhouse_dom_v25",
                    "extraction_confidence": 0.88,
                }
            )

        return self._dedupe_jobs_v25(jobs, page_url)

    def _extract_from_table_rows_v25(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        rows = root.xpath(
            "//tr[contains(@class,'jobDetailRow') or contains(@class,'job-row') or contains(@class,'job-row-item')]"
        )
        for row in rows[:MAX_JOBS_PER_PAGE]:
            link = row.xpath(".//a[@href][1]")
            if not link:
                continue
            title = self._normalize_title_v25(_text(link[0]))
            if not self._is_valid_title_v25(title):
                continue
            if not self._title_has_job_signal_v25(title):
                continue

            source_url = _resolve_url(link[0].get("href"), page_url) or page_url
            if self._is_non_job_url_v25(source_url):
                source_url = page_url

            row_text = _text(row)
            location = None
            loc_match = _AU_LOCATIONS.search(row_text)
            if loc_match:
                location = loc_match.group(0).strip()

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": row_text[:5000] if len(row_text) > 40 else None,
                    "extraction_method": "tier2_table_rows_v25",
                    "extraction_confidence": 0.82,
                }
            )

        return self._dedupe_jobs_v25(jobs, page_url)

    def _extract_from_accordion_sections_v25(self, root: etree._Element, page_url: str) -> list[dict]:
        items = root.xpath(
            "//*[contains(@class,'accordion-item') or contains(@class,'elementor-accordion-item') or "
            "contains(@class,'accordion')]"
        )
        if not items:
            return []

        jobs: list[dict] = []
        for item in items[:260]:
            title_nodes = item.xpath(
                ".//*[contains(@class,'accordion-button') or contains(@class,'accordion-title') or "
                "self::h1 or self::h2 or self::h3 or self::h4 or self::button]"
            )
            if not title_nodes:
                continue

            title = ""
            for node in title_nodes[:4]:
                candidate = self._normalize_title_v25(_text(node))
                if not self._is_valid_title_v25(candidate):
                    continue
                if not self._title_has_job_signal_v25(candidate):
                    continue
                title = candidate
                break
            if not title:
                continue

            item_text = _text(item)[:2200]
            if not _APPLY_CONTEXT_PATTERN_V25.search(item_text):
                if len(item_text) < 120:
                    continue
                if len(title.split()) <= 1:
                    continue

            link_nodes = item.xpath(".//a[@href and not(starts-with(@href,'#'))]")
            href = None
            for ln in link_nodes[:4]:
                h = (ln.get("href") or "").strip()
                if not h:
                    continue
                full = _resolve_url(h, page_url) or page_url
                if self._is_non_job_url_v25(full):
                    continue
                href = h
                break

            source_url = _resolve_url(href, page_url) if href else page_url
            source_url = source_url or page_url

            loc_match = _AU_LOCATIONS.search(item_text)
            location = loc_match.group(0).strip() if loc_match else None

            type_match = _JOB_TYPE_PATTERN.search(item_text)
            emp_type = type_match.group(0).strip() if type_match else None

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": None,
                    "employment_type": emp_type,
                    "description": item_text[:5000] if len(item_text) > 80 else None,
                    "extraction_method": "tier2_accordion_v25",
                    "extraction_confidence": 0.78,
                }
            )

        return self._dedupe_jobs_v25(jobs, page_url)

    def _extract_from_prose_headings_v25(self, root: etree._Element, page_url: str) -> list[dict]:
        containers = root.xpath(
            "//*[contains(@class,'prose') or contains(@class,'field--name-body') or "
            "contains(@class,'entry-content') or self::article]"
        )
        if not containers:
            containers = root.xpath("//main | //article")

        jobs: list[dict] = []
        for container in containers[:80]:
            headings = container.xpath(".//h2 | .//h3 | .//h4")
            if not headings:
                continue

            for heading in headings[:80]:
                title = self._normalize_title_v25(_text(heading))
                if not self._is_valid_title_v25(title):
                    continue
                if not self._title_has_job_signal_v25(title):
                    continue

                context_parts: list[str] = []
                link_href: Optional[str] = None

                for sib in heading.itersiblings():
                    if isinstance(sib.tag, str) and sib.tag.lower() in {"h1", "h2", "h3", "h4"}:
                        break
                    sib_text = _text(sib)
                    if sib_text:
                        context_parts.append(sib_text)
                    if link_href is None:
                        links = sib.xpath(".//a[@href]")
                        for ln in links[:3]:
                            h = (ln.get("href") or "").strip()
                            if not h:
                                continue
                            resolved = _resolve_url(h, page_url) or page_url
                            if self._is_non_job_url_v25(resolved):
                                continue
                            link_href = h
                            break
                    if len(" ".join(context_parts)) >= 1800:
                        break

                context_text = " ".join(context_parts)
                if len(context_text) < 80:
                    continue
                if not _APPLY_CONTEXT_PATTERN_V25.search(context_text):
                    # Guard against region lists and generic section headings.
                    if len(title.split()) <= 3:
                        continue

                source_url = _resolve_url(link_href, page_url) if link_href else page_url
                if source_url and self._is_non_job_url_v25(source_url):
                    source_url = page_url

                loc_match = _AU_LOCATIONS.search(context_text)
                location = loc_match.group(0).strip() if loc_match else None

                type_match = _JOB_TYPE_PATTERN.search(context_text)
                emp_type = type_match.group(0).strip() if type_match else None

                salary_match = _SALARY_PATTERN.search(context_text)
                salary = salary_match.group(0).strip() if salary_match else None

                jobs.append(
                    {
                        "title": title,
                        "source_url": source_url or page_url,
                        "location_raw": location,
                        "salary_raw": salary,
                        "employment_type": emp_type,
                        "description": context_text[:5000],
                        "extraction_method": "tier2_prose_headings_v25",
                        "extraction_confidence": 0.74,
                    }
                )

        return self._dedupe_jobs_v25(jobs, page_url)

    def _extract_from_collapsible_headings_v25(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        headings = root.xpath("//h2 | //h3 | //h4")
        for heading in headings[:240]:
            title = self._normalize_title_v25(_text(heading))
            if not self._is_valid_title_v25(title):
                continue
            if not self._title_has_job_signal_v25(title):
                continue

            context_parts: list[str] = []
            link_href: Optional[str] = None

            for sib in heading.itersiblings():
                if isinstance(sib.tag, str) and sib.tag.lower() in {"h1", "h2", "h3", "h4"}:
                    break
                sib_text = _text(sib)
                if sib_text:
                    context_parts.append(sib_text)
                if link_href is None:
                    links = sib.xpath(".//a[@href]")
                    for ln in links[:4]:
                        h = (ln.get("href") or "").strip()
                        if not h:
                            continue
                        resolved = _resolve_url(h, page_url) or page_url
                        if self._is_non_job_url_v25(resolved):
                            continue
                        link_href = h
                        break
                if len(" ".join(context_parts)) >= 1800:
                    break

            context_text = " ".join(context_parts)
            if len(context_text) < 80:
                continue
            if not _APPLY_CONTEXT_PATTERN_V25.search(context_text):
                continue

            source_url = _resolve_url(link_href, page_url) if link_href else page_url
            source_url = source_url or page_url
            if self._is_non_job_url_v25(source_url):
                source_url = page_url

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": context_text[:5000],
                    "extraction_method": "tier2_collapsible_headings_v25",
                    "extraction_confidence": 0.79,
                }
            )

        return self._dedupe_jobs_v25(jobs, page_url)

    def _extract_from_repeating_rows_v25(self, root: etree._Element, page_url: str) -> list[dict]:
        row_groups: dict[str, list[etree._Element]] = defaultdict(list)
        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag not in {"li", "div", "article", "tr", "section"}:
                continue
            classes = _get_el_classes(el)
            if not classes:
                continue
            if not _ROW_CLASS_PATTERN_V25.search(classes):
                continue

            tokens = classes.lower().split()
            sig = f"{tag}:{' '.join(tokens[:2])}" if tokens else f"{tag}:_"
            row_groups[sig].append(el)

        jobs: list[dict] = []
        for rows in row_groups.values():
            if len(rows) < 3:
                continue
            for row in rows[:MAX_JOBS_PER_PAGE]:
                title_nodes = row.xpath(
                    ".//h1|.//h2|.//h3|.//h4|.//button|"
                    ".//*[contains(@class,'job-post-title')]|.//a[@href][1]"
                )
                if not title_nodes:
                    continue
                title = self._normalize_title_v25(_text(title_nodes[0]))
                if not self._is_valid_title_v25(title):
                    continue
                if not self._title_has_job_signal_v25(title):
                    continue

                link = row.xpath(".//a[@href and not(starts-with(@href,'#'))][1]")
                href = link[0].get("href") if link else None
                source_url = _resolve_url(href, page_url) or page_url
                if self._is_non_job_url_v25(source_url):
                    source_url = page_url

                row_text = _text(row)
                location = None
                loc_match = _AU_LOCATIONS.search(row_text)
                if loc_match:
                    location = loc_match.group(0).strip()

                jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": location,
                        "salary_raw": None,
                        "employment_type": None,
                        "description": row_text[:5000] if len(row_text) > 70 else None,
                        "extraction_method": "tier2_repeating_rows_v25",
                        "extraction_confidence": 0.72,
                    }
                )

        return self._dedupe_jobs_v25(jobs, page_url)

    def _extract_from_link_clusters_v25(self, root: etree._Element, page_url: str) -> list[dict]:
        grouped: dict[str, list[dict]] = defaultdict(list)

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url) or page_url
            if self._is_non_job_url_v25(source_url):
                continue

            title = self._normalize_title_v25(_text(a_el) or (a_el.get("title") or ""))
            if not self._is_valid_title_v25(title):
                continue

            parent = a_el.getparent()
            parent_text = _text(parent)[:1600] if parent is not None else ""
            title_hint = self._title_has_job_signal_v25(title)
            url_hint = self._is_job_like_url_v25(source_url)
            context_hint = bool(_APPLY_CONTEXT_PATTERN_V25.search(parent_text))

            if not (title_hint or (url_hint and context_hint)):
                continue

            parent_tag = parent.tag.lower() if (parent is not None and isinstance(parent.tag, str)) else "_"
            parent_cls = _get_el_classes(parent).split()[:1] if parent is not None else []
            signature = f"{parent_tag}:{parent_cls[0] if parent_cls else '_'}"

            grouped[signature].append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": parent_text[:5000] if len(parent_text) > 70 else None,
                    "extraction_method": "tier2_link_clusters_v25",
                    "extraction_confidence": 0.69,
                }
            )

        jobs: list[dict] = []
        for cluster_jobs in grouped.values():
            deduped_cluster = self._dedupe_jobs_v25(cluster_jobs, page_url)
            if len(deduped_cluster) < 2:
                continue
            if not self._passes_jobset_validation_v25(deduped_cluster, page_url):
                continue
            jobs.extend(deduped_cluster)

        return self._dedupe_jobs_v25(jobs, page_url)

    # ------------------------------------------------------------------
    # Remote recovery probes
    # ------------------------------------------------------------------

    async def _probe_greenhouse_board_jobs_v25(self, page_url: str) -> list[dict]:
        parsed = urlparse(page_url or "")
        host = (parsed.hostname or "").lower()
        if "greenhouse" not in host:
            return []

        org = ""
        query = dict(parse_qsl(parsed.query))
        if query.get("for"):
            org = query["for"].strip()

        if not org:
            path_parts = [p for p in parsed.path.split("/") if p]
            if path_parts:
                if path_parts[0] == "embed":
                    org = query.get("for", "").strip()
                else:
                    org = path_parts[0]

        if not org:
            return []

        candidates = [
            f"https://{host}/{org}",
            f"https://{host}/embed/job_board?for={org}",
            f"https://boards.greenhouse.io/{org}",
        ]

        best_jobs: list[dict] = []
        best_score = -1.0

        async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
            for candidate_url in dict.fromkeys(candidates):
                try:
                    resp = await client.get(candidate_url)
                except Exception:
                    continue
                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload_v25(body):
                    continue

                root = _parse_html(body)
                if root is None:
                    continue

                jobs = self._extract_greenhouse_dom_jobs_v25(root, str(resp.url))
                if not jobs:
                    jobs = self._extract_from_link_clusters_v25(root, str(resp.url))

                score = self._jobset_score_v25(jobs, str(resp.url))
                if jobs and score > best_score:
                    best_score = score
                    best_jobs = jobs

        return self._dedupe_jobs_v25(best_jobs, page_url)

    def _should_try_detail_recovery_v25(self, page_url: str, html_body: str, parent_jobs: list[dict]) -> bool:
        if _DETAIL_URL_PATTERN_V25.search(page_url or ""):
            return True
        if len(parent_jobs) <= 1:
            return True

        low = (html_body or "").lower()
        if "awsm-job-single-wrap" in low or "job-post-container" in low:
            return True
        if "apply now" in low and "requisitions" in low:
            return True
        return False

    async def _recover_listing_jobs_v25(self, page_url: str, html_body: str) -> list[dict]:
        parsed = urlparse(page_url)
        if not parsed.scheme or not parsed.netloc:
            return []

        base = f"{parsed.scheme}://{parsed.netloc}"
        candidates: list[str] = []

        def _add(url: str) -> None:
            u = (url or "").strip()
            if not u:
                return
            if not re.match(r"^https?://", u, re.IGNORECASE):
                u = _resolve_url(u, page_url) or ""
            if not u:
                return
            if urlparse(u).hostname and urlparse(u).hostname != parsed.hostname:
                host = (urlparse(u).hostname or "").lower()
                parsed_host = (parsed.hostname or "").lower()
                parsed_parts = parsed_host.split(".")
                parsed_base = ".".join(parsed_parts[-2:]) if len(parsed_parts) >= 2 else parsed_host
                if not host.endswith(parsed_base):
                    if "greenhouse" not in host:
                        return
            if _DETAIL_URL_PATTERN_V25.search(u):
                # Keep detail links only for Greenhouse board hosts where list URL is derived.
                if "greenhouse" not in (urlparse(u).hostname or ""):
                    return
            candidates.append(u)

        for path in (
            "/jobs",
            "/careers",
            "/career",
            "/job-openings",
            "/vacancies",
            "/requisitions",
            "/jobs/search",
            "/recruit/Portal.na",
            "/ms/kerjaya",
            "/kerjaya",
        ):
            _add(base + path)

        if "zohorecruit" in (parsed.hostname or ""):
            _add(base + "/recruit/Portal.na")

        if "greenhouse" in (parsed.hostname or ""):
            path_parts = [p for p in parsed.path.split("/") if p]
            if path_parts:
                org = path_parts[0]
                _add(f"https://{parsed.hostname}/{org}")
                _add(f"https://{parsed.hostname}/embed/job_board?for={org}")
                _add(f"https://boards.greenhouse.io/{org}")

        if "oraclecloud.com" in (parsed.hostname or ""):
            for site_id in self._oracle_site_ids_v25(page_url, html_body)[:8]:
                _add(f"{base}/hcmUI/CandidateExperience/en/sites/{site_id}/requisitions")

        root = _parse_html(html_body)
        if root is not None:
            for a_el in root.iter("a"):
                href = (a_el.get("href") or "").strip()
                if not href or href.startswith("#") or href.startswith("javascript:"):
                    continue
                full = _resolve_url(href, page_url) or ""
                if not full:
                    continue
                if _LISTING_URL_PATTERN_V25.search(full):
                    _add(full)

            for m in re.finditer(
                r"https?://[^\"'\s]+/(?:hcmUI/CandidateExperience/[^\"'\s]+/requisitions|"
                r"recruit/Portal\.na[^\"'\s]*|jobs/search[^\"'\s]*|job-openings/?|"
                r"embed/job_board\?[^\"'\s]+)",
                html_body or "",
                re.IGNORECASE,
            ):
                _add(m.group(0))

        deduped = list(dict.fromkeys(candidates))[:18]
        if not deduped:
            return []

        best_jobs: list[dict] = []
        best_score = -1.0

        async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
            for candidate_url in deduped:
                try:
                    resp = await client.get(candidate_url)
                except Exception:
                    continue
                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload_v25(body):
                    continue

                jobs = self._extract_jobs_from_html_snapshot_v25(body, str(resp.url))
                score = self._jobset_score_v25(jobs, str(resp.url))
                if jobs and score > best_score:
                    best_score = score
                    best_jobs = jobs

        return self._dedupe_jobs_v25(best_jobs, page_url)

    def _extract_jobs_from_html_snapshot_v25(self, html_body: str, page_url: str) -> list[dict]:
        page_l = (page_url or "").lower()
        body_l = (html_body or "").lstrip()[:1200].lower()
        if "downloadrssfeed" in page_l or body_l.startswith("<?xml") or body_l.startswith("<rss") or body_l.startswith("<feed"):
            return self._extract_structured_jobs_v25(html_body, page_url)

        candidates: list[tuple[str, list[dict]]] = []

        structured = self._extract_structured_jobs_v25(html_body, page_url)
        if structured:
            candidates.append(("structured", structured))

        root = _parse_html(html_body)
        if root is not None:
            greenhouse_dom_jobs = self._extract_greenhouse_dom_jobs_v25(root, page_url)
            if greenhouse_dom_jobs:
                candidates.append(("greenhouse_dom", greenhouse_dom_jobs))

            table_jobs = self._extract_from_table_rows_v25(root, page_url)
            if table_jobs:
                candidates.append(("table", table_jobs))

            accordion_jobs = self._extract_from_accordion_sections_v25(root, page_url)
            if accordion_jobs:
                candidates.append(("accordion", accordion_jobs))

            prose_jobs = self._extract_from_prose_headings_v25(root, page_url)
            if prose_jobs:
                candidates.append(("prose", prose_jobs))

            collapsible_jobs = self._extract_from_collapsible_headings_v25(root, page_url)
            if collapsible_jobs:
                candidates.append(("collapsible", collapsible_jobs))

            row_jobs = self._extract_from_repeating_rows_v25(root, page_url)
            if row_jobs:
                candidates.append(("rows", row_jobs))

            link_jobs = self._extract_from_link_clusters_v25(root, page_url)
            if link_jobs:
                candidates.append(("links", link_jobs))

        _, best_jobs = self._pick_best_jobset_v25(candidates, page_url)
        return best_jobs

    async def _extract_oracle_api_jobs_v25(self, page_url: str, html_body: str) -> list[dict]:
        page_l = (page_url or "").lower()
        body_l = (html_body or "").lower()
        if "oraclecloud.com" not in page_l and "candidateexperience" not in body_l and "cx_config" not in body_l:
            return []

        parsed = urlparse(page_url)
        host = parsed.hostname or ""
        if not host:
            return []

        api_base = f"https://{host}"
        api_match = _ORACLE_API_BASE_PATTERN_V25.search(html_body or "")
        if api_match:
            api_base = api_match.group(1).rstrip("/")

        site_ids = self._oracle_site_ids_v25(page_url, html_body)
        if not site_ids:
            return []

        best_jobs: list[dict] = []

        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
                for site_id in site_ids[:8]:
                    site_jobs: list[dict] = []
                    seen_keys: set[str] = set()

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
                        holder = items[0] if isinstance(items[0], dict) else {}
                        rows = holder.get("requisitionList")
                        if not isinstance(rows, list) or not rows:
                            break

                        page_count = 0
                        for row in rows:
                            if not isinstance(row, dict):
                                continue
                            raw_title = str(row.get("Title") or row.get("title") or "").strip()
                            title = self._normalize_title_v25(raw_title)
                            if not self._is_valid_title_v25(title):
                                continue
                            if not self._title_has_job_signal_v25(title):
                                continue

                            req_id = str(row.get("Id") or row.get("id") or "").strip()
                            key = f"{title.lower()}::{req_id.lower()}"
                            if key in seen_keys:
                                continue
                            seen_keys.add(key)

                            src = (
                                f"https://{host}/hcmUI/CandidateExperience/en/sites/{site_id}/job/{req_id}"
                                if req_id else page_url
                            )

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
                                    "extraction_method": "tier0_oracle_api_v25",
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

                    if self._jobset_score_v25(site_jobs, page_url) > self._jobset_score_v25(best_jobs, page_url):
                        best_jobs = site_jobs
        except Exception:
            logger.exception("v2.5 oracle API fallback failed for %s", page_url)

        return self._dedupe_jobs_v25(best_jobs, page_url)

    def _oracle_site_ids_v25(self, page_url: str, html_body: str) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []

        def _add(value: str) -> None:
            v = (value or "").strip()
            if not v or v in seen:
                return
            seen.add(v)
            ordered.append(v)

        for m in _ORACLE_SITE_PATTERN_V25.finditer(page_url or ""):
            _add(m.group(1))
        for m in _ORACLE_SITE_PATTERN_V25.finditer(html_body or ""):
            _add(m.group(1))
        for m in _ORACLE_SITE_NUMBER_PATTERN_V25.finditer(html_body or ""):
            _add(m.group(1))
        for m in re.finditer(r"siteNumber=([A-Za-z0-9_]+)", html_body or "", re.IGNORECASE):
            _add(m.group(1))

        query_pairs = dict(parse_qsl(urlparse(page_url or "").query))
        if query_pairs.get("siteNumber"):
            _add(query_pairs.get("siteNumber", ""))

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
    # Selection / validation / helpers
    # ------------------------------------------------------------------

    def _pick_best_jobset_v25(self, candidates: list[tuple[str, list[dict]]], page_url: str) -> tuple[str, list[dict]]:
        if not candidates:
            return "", []

        best_label = ""
        best_jobs: list[dict] = []
        best_score = -1.0

        parent_label = ""
        parent_jobs: list[dict] = []
        parent_score = -1.0

        for label, jobs in candidates:
            deduped = self._dedupe_jobs_v25(jobs, page_url)
            if not deduped:
                continue

            score = self._jobset_score_v25(deduped, page_url)
            valid = self._passes_jobset_validation_v25(deduped, page_url)

            if label == "parent_v16":
                parent_label = label
                parent_jobs = deduped
                parent_score = score

            logger.debug("v2.5 candidate %s: jobs=%d score=%.2f valid=%s", label, len(deduped), score, valid)

            if valid and score > best_score:
                best_label = label
                best_jobs = deduped
                best_score = score

        if best_jobs:
            # Keep parent unless fallback is materially better.
            if parent_jobs and best_label != parent_label and best_score < parent_score + 2.0:
                return parent_label, parent_jobs[:MAX_JOBS_PER_PAGE]
            return best_label, best_jobs[:MAX_JOBS_PER_PAGE]

        if parent_jobs:
            return parent_label, parent_jobs[:MAX_JOBS_PER_PAGE]

        largest = max(
            ((label, self._dedupe_jobs_v25(jobs, page_url)) for label, jobs in candidates),
            key=lambda item: len(item[1]),
            default=("", []),
        )
        return largest[0], largest[1][:MAX_JOBS_PER_PAGE]

    def _passes_jobset_validation_v25(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title_v25(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if self._is_valid_title_v25(t)]
        if not titles:
            return False

        unique_ratio = len(set(t.lower() for t in titles)) / max(1, len(titles))
        if len(titles) > 2 and unique_ratio < 0.6:
            return False

        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V25.match(t.lower()))
        if reject_hits >= max(1, int(len(titles) * 0.3)):
            return False

        nav_hits = sum(
            1 for t in titles
            if _CATEGORY_TITLE_PATTERN_V25.match(t) or _PHONE_TITLE_PATTERN_V25.match(t) or _CORPORATE_TITLE_PATTERN_V25.match(t)
        )
        if nav_hits >= max(1, int(len(titles) * 0.25)):
            return False

        title_hits = sum(1 for t in titles if self._title_has_job_signal_v25(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url_v25(j.get("source_url") or page_url))
        apply_hits = sum(1 for j in jobs if _APPLY_CONTEXT_PATTERN_V25.search((j.get("description") or "")[:1200]))

        if len(titles) == 1:
            t = titles[0]
            return (
                title_hits == 1
                and not _GENERIC_LISTING_LABEL_PATTERN_V25.match(t)
                and (url_hits >= 1 or apply_hits >= 1)
            )

        if len(titles) <= 3:
            return title_hits >= 2 or (title_hits >= 1 and (url_hits >= 1 or apply_hits >= 1))

        needed = max(2, int(len(titles) * 0.3))
        return (
            title_hits >= needed
            and (url_hits >= max(1, int(len(titles) * 0.15)) or apply_hits >= max(1, int(len(titles) * 0.15)))
        )

    def _jobset_score_v25(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        titles = [self._normalize_title_v25(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return 0.0

        count = len(titles)
        title_hits = sum(1 for t in titles if self._title_has_job_signal_v25(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url_v25(j.get("source_url") or page_url))
        apply_hits = sum(1 for j in jobs if _APPLY_CONTEXT_PATTERN_V25.search((j.get("description") or "")[:1200]))
        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V25.match(t.lower()))
        nav_hits = sum(
            1 for t in titles
            if _CATEGORY_TITLE_PATTERN_V25.match(t) or _PHONE_TITLE_PATTERN_V25.match(t) or _CORPORATE_TITLE_PATTERN_V25.match(t)
        )
        unique_titles = len(set(t.lower() for t in titles))

        score = count * 3.2
        score += title_hits * 2.4
        score += url_hits * 1.8
        score += apply_hits * 1.6
        score += unique_titles * 0.7
        score -= reject_hits * 4.0
        score -= nav_hits * 4.2
        return score

    def _dedupe_jobs_v25(self, jobs: list[dict], page_url: str) -> list[dict]:
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for job in jobs:
            title = self._normalize_title_v25(job.get("title", ""))
            if not self._is_valid_title_v25(title):
                continue

            source_url = (job.get("source_url") or "").strip()
            if not source_url:
                source_url = page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]
            if self._is_non_job_url_v25(source_url):
                # Keep same-page fallback if URL is invalid but title/context is strong.
                source_url = page_url

            key = (title.lower(), source_url.lower())
            if key in seen:
                continue
            seen.add(key)

            clone = dict(job)
            clone["title"] = title
            clone["source_url"] = source_url
            deduped.append(clone)

            if len(deduped) >= MAX_JOBS_PER_PAGE:
                break

        return deduped

    def _normalize_title_v25(self, title: str) -> str:
        if not title:
            return ""

        t = html.unescape(" ".join(str(title).replace("\u00a0", " ").split()))
        t = t.strip(" |:-\u2013\u2014\u2022")
        t = re.sub(r"[\u200b-\u200d\ufeff]", "", t)
        t = re.sub(r"\s{2,}", " ", t)
        t = re.sub(r"^job\s+description\s*\|\s*", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\.pdf$", "", t, flags=re.IGNORECASE)

        # Remove metadata tails commonly appended to listing titles.
        t = re.sub(r"\s+Deadline\s*:\s+.*$", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s+Closing\s+Date\s*:\s+.*$", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s+Posted\s+Date\s*:\s+.*$", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s+Location\s*:\s+.*$", "", t, flags=re.IGNORECASE)

        if " - " in t and len(t) > 40:
            head = t.split(" - ", 1)[0].strip()
            if self._title_has_job_signal_v25(head):
                t = head

        # Trim long trailing metadata chunks like "Country, City Deadline: ... Permanent".
        t = re.sub(
            r"\s+[A-Z][a-z]+(?:,\s*[A-Z][a-z]+){0,2}\s+"
            r"(?:Deadline|Permanent|Temporary|Contract|Trainee)\b.*$",
            "",
            t,
        )

        return t.strip(" |:-\u2013\u2014\u2022")

    def _is_valid_title_v25(self, title: str) -> bool:
        if not title:
            return False

        if not TieredExtractorV16._is_valid_title_v16(title):
            if not _is_valid_title(title):
                return False

        t = title.strip()
        low = t.lower()
        if _REJECT_TITLE_PATTERN_V25.match(low):
            return False
        if _GENERIC_LISTING_LABEL_PATTERN_V25.match(t):
            return False
        if _CATEGORY_TITLE_PATTERN_V25.match(t):
            return False
        if _CORPORATE_TITLE_PATTERN_V25.match(t):
            return False
        if _PHONE_TITLE_PATTERN_V25.match(t):
            return False

        words = t.split()
        if len(words) > 15:
            return False
        if len(words) <= 1 and not self._title_has_job_signal_v25(t):
            return False

        return True

    def _title_has_job_signal_v25(self, title: str) -> bool:
        if not title:
            return False
        if _title_has_job_noun(title):
            return True
        return bool(_TITLE_HINT_PATTERN_V25.search(title))

    def _is_job_like_url_v25(self, src: str) -> bool:
        if not src:
            return False
        if self._is_non_job_url_v25(src):
            return False
        return bool(_JOB_URL_HINT_PATTERN_V25.search(src))

    def _is_non_job_url_v25(self, src: str) -> bool:
        return bool(_NON_JOB_URL_PATTERN_V25.search((src or "").lower()))

    @staticmethod
    def _is_non_html_payload_v25(body: Optional[str]) -> bool:
        if not body:
            return True
        sample = body[:800].lstrip()
        if sample.startswith("%PDF-"):
            return True
        sample_l = sample.lower()
        if (sample_l.startswith("{") or sample_l.startswith("[")) and "<html" not in sample_l[:400]:
            return True
        return False
