"""
Tiered Extraction Engine v2.0 — direct from v1.6, focused on robust generalization.

High-impact changes:
1. Tier 0 structured-data fallback: JSON-LD JobPosting + XML/RSS feed parsing
   (handles discovery landing on feed endpoints like downloadrssfeed).
2. Tier 2 multi-candidate sweep + bucket aggregation across top containers
   (recovers split job lists across sibling sections).
3. Global repeated-row harvesting and Elementor card extraction
   (captures layouts like .jobItem rows and Elementor inner columns).
4. Heading/accordion section fallback for text-heavy pages where titles are in
   headings/buttons with minimal link structure.
5. Stricter title + jobset validation to suppress nav/footer/country-list
   false positives (small-set bypass removed).
"""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from typing import Optional
from urllib.parse import urljoin, urlparse

from lxml import etree

from app.crawlers.tiered_extractor_v16 import TieredExtractorV16, _title_has_job_noun
from app.crawlers.tiered_extractor import (
    _parse_html,
    _text,
    _href,
    _resolve_url,
    _get_el_classes,
    _is_valid_title,
    _JOB_URL_PATTERN,
    _JOB_TYPE_PATTERN,
    _SALARY_PATTERN,
    _AU_LOCATIONS,
    MAX_JOBS_PER_PAGE,
    MIN_JOBS_FOR_SUCCESS,
)

logger = logging.getLogger(__name__)


_TITLE_HINT_PATTERN_V20 = re.compile(
    r"\b(?:job|jobs|career|careers|vacanc(?:y|ies)|opening|openings|"
    r"position|positions|role|roles|internship|intern|apprentice|"
    r"manager|engineer|developer|officer|specialist|assistant|"
    r"analyst|consultant|coordinator|executive|technician|designer|"
    r"administrator|recruit(?:er|ment)?|"
    r"influencer|"
    r"akuntan|asisten|psikolog(?:i)?|fotografer|staf|pegawai|karyawan|"
    r"lowongan|karir|karier|vacature|empleo|trabajo)\b",
    re.IGNORECASE,
)

_CONTEXT_HINT_PATTERN_V20 = re.compile(
    r"\b(?:apply|deadline|closing|location|salary|compensation|"
    r"full[\s-]?time|part[\s-]?time|contract|permanent|temporary|"
    r"casual|remote|hybrid|qualifications?|requirements?|"
    r"info\s+lengkap|read\s+more|view\s+job|details?)\b",
    re.IGNORECASE,
)

_REJECT_TITLE_PATTERN_V20 = re.compile(
    r"^(?:"
    r"powered\s+by|"
    r"read\s+more|"
    r"show\s+all\s+jobs?|"
    r"show\s+advanced|"
    r"open\s+roles?|"
    r"available\s+positions?|"
    r"see\s+what\s+people\s+say.*|"
    r"what\s+it'?s\s+like\s+to\s+work.*|"
    r"why\s+join\s+us\??|"
    r"size\s*&\s*fit|"
    r"shipping\s*&\s*returns|"
    r"alamat\s+kantor|"
    r"model\s+incubator|"
    r"main\s+menu|"
    r"home|about\s+us|contact|"
    r"our\s+people|"
    r"lowongan\s+kerja(?:\s+kudus)?|"
    r"job\s+alerts?|saved\s+jobs?|"
    r"login|register|sign\s*in|sign\s*up|"
    r"apply\s+job|"
    r"job\s+openings?|"
    r"working\s+with\s+us|"
    r"our\s+direction|"
    r"our\s+culture|"
    r"join\s+our\s+team|"
    r"job\s+responsibilities|"
    r"qualifications\s+and\s+skills|"
    r"bisa\s+kamu\s+baca\s+di\s+sini.*|"
    r"our\s+mission|address|keep\s+in\s+touch"
    r")$",
    re.IGNORECASE,
)

_CTA_TITLE_PATTERN_V20 = re.compile(
    r"^(?:become|learn|discover|protect|shop|share|follow|read|view|download|"
    r"contact|about|our\s+(?:team|culture|story|community|mission))\b",
    re.IGNORECASE,
)

_COUNTRY_CURRENCY_PATTERN_V20 = re.compile(
    r"^[A-Za-z][A-Za-z\s\.\'-]{2,50}\([A-Z]{2,4}(?:\s+[A-Za-z\$€£¥]+)?\)$"
)

_TRAILING_META_SPLIT_V20 = re.compile(
    r"(?:\bdeadline\s*:|\bclosing\s+date\b|\blocation\s*:|\bemployment\s+type\s*:|"
    r"\bpermanent\b|\btemporary\b|\bcontract\b|\bcasual\b|"
    r"\bfull[\s-]?time\b|\bpart[\s-]?time\b)",
    re.IGNORECASE,
)

_CAMEL_LOCATION_SPLIT_V20 = re.compile(
    r"^(.{3,100}?)([a-z])([A-Z][A-Za-z\.-]+(?:[,\s]+[A-Z][A-Za-z\.-]+)*)$"
)

_NON_JOB_URL_PATTERN_V20 = re.compile(
    r"/(?:about|contact|news|blog|report|investor|ir|privacy|terms|cookie|"
    r"shop|store|donate|support|resource|event|story|team|culture|values)(?:/|$)",
    re.IGNORECASE,
)

_JOB_URL_HINT_PATTERN_V20 = re.compile(
    r"/(?:job|jobs|career|careers|position|positions|vacanc|opening|openings|"
    r"role|roles|apply|recruit|search|lowongan|karir|karier|vacature|empleo|trabajo|"
    r"portal\.na|/p/)",
    re.IGNORECASE,
)

_ROW_CLASS_STRONG_PATTERN_V20 = re.compile(
    r"job|position|vacanc|opening|posting|recruit|career|lowongan|karir|karier",
    re.IGNORECASE,
)


class TieredExtractorV20(TieredExtractorV16):
    """v2.0 extractor: structured-data fallback + stronger generalized Tier 2."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        """Run v1.6 pipeline first, then apply v2.0 structured fallback if better."""
        url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # Required by agent rules: call parent extract() first.
        parent_jobs = await super().extract(career_page, company, html)
        parent_jobs = self._dedupe_jobs(parent_jobs or [], url)

        structured_jobs = self._extract_structured_data_v20(html, url)
        structured_jobs = self._dedupe_jobs(structured_jobs, url)

        if structured_jobs:
            parent_score = self._jobset_score(parent_jobs, url) if parent_jobs else -1.0
            structured_score = self._jobset_score(structured_jobs, url)

            # Prefer structured extraction when parent output is weak or clearly smaller.
            if (
                not parent_jobs
                or not self._passes_jobset_validation(parent_jobs, url)
                or structured_score > parent_score + 2.0
            ):
                logger.info(
                    "v2.0 selecting structured-data extraction for %s (%d jobs)",
                    url,
                    len(structured_jobs),
                )
                if len(structured_jobs) >= MIN_JOBS_FOR_SUCCESS:
                    enriched = await self._enrich_from_detail_pages(structured_jobs)
                    return enriched[:MAX_JOBS_PER_PAGE]
                return structured_jobs[:MAX_JOBS_PER_PAGE]

        return parent_jobs[:MAX_JOBS_PER_PAGE]

    # ------------------------------------------------------------------
    # Tier 0 structured data
    # ------------------------------------------------------------------

    def _extract_structured_data_v20(self, html: str, url: str) -> list[dict]:
        jobs: list[dict] = []

        # Handle direct XML/RSS payloads (e.g. Zoho downloadrssfeed endpoints).
        stripped = html.lstrip()
        if stripped.startswith("<?xml") or stripped.startswith("<rss") or stripped.startswith("<feed"):
            jobs.extend(self._parse_xml_feed_v20(html, url))

        # JSON-LD JobPosting blocks.
        for match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html,
            re.DOTALL | re.IGNORECASE,
        ):
            raw = (match.group(1) or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            jobs.extend(self._parse_jsonld_item_v20(data, url))

        return jobs

    def _parse_jsonld_item_v20(self, data, page_url: str) -> list[dict]:
        items = data if isinstance(data, list) else [data]
        jobs: list[dict] = []

        while items:
            item = items.pop(0)
            if not isinstance(item, dict):
                continue

            graph = item.get("@graph")
            if isinstance(graph, list):
                items.extend(graph)

            item_type = item.get("@type", "")
            if isinstance(item_type, list):
                item_type = item_type[0] if item_type else ""
            if item_type != "JobPosting":
                continue

            title = (item.get("title") or item.get("name") or "").strip()
            title = self._normalize_title_v20(title)
            if not self._is_valid_title_v20(title):
                continue

            source_url = (item.get("url") or item.get("sameAs") or page_url).strip()
            if source_url and not source_url.startswith("http"):
                source_url = urljoin(page_url, source_url)

            location_raw = self._extract_location_from_jsonld_v20(item)
            salary_raw = self._extract_salary_from_jsonld_v20(item)

            employment_type = item.get("employmentType")
            if isinstance(employment_type, list):
                employment_type = ", ".join(str(x) for x in employment_type if x)
            elif employment_type is not None:
                employment_type = str(employment_type).strip()

            description = item.get("description") or ""
            if isinstance(description, str) and "<" in description:
                parsed = _parse_html(description)
                if parsed is not None:
                    description = _text(parsed)
            if isinstance(description, str):
                description = description.strip()[:5000] or None
            else:
                description = None

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url or page_url,
                    "location_raw": location_raw,
                    "salary_raw": salary_raw,
                    "employment_type": employment_type or None,
                    "description": description,
                    "extraction_method": "tier0_jsonld_v20",
                    "extraction_confidence": 0.95,
                }
            )

        return jobs

    def _parse_xml_feed_v20(self, body: str, source_url: str) -> list[dict]:
        jobs: list[dict] = []
        try:
            root = etree.fromstring(body.encode("utf-8", errors="replace"))
        except Exception:
            return []

        # Strip namespaces for easier element lookup.
        for el in root.iter():
            if isinstance(el.tag, str) and "}" in el.tag:
                el.tag = el.tag.split("}", 1)[1]

        items = root.xpath("./channel/item | ./item | ./channel/entry | ./entry")
        if not items:
            items = root.findall(".//item")
            if not items:
                items = root.findall(".//entry")

        max_items = 30 if "downloadrssfeed" in source_url.lower() else 120

        for item in items[:max_items]:
            title = (item.findtext("title") or "").strip()
            title = self._normalize_title_v20(title)
            if not self._is_valid_title_v20(title):
                continue

            link = ""
            link_el = item.find("link")
            if link_el is not None:
                link = (link_el.text or link_el.get("href") or "").strip()
            if link and not link.startswith("http"):
                link = urljoin(source_url, link)

            description = (
                item.findtext("description")
                or item.findtext("content")
                or item.findtext("summary")
                or ""
            ).strip()
            if description and len(description) > 5000:
                description = description[:5000]

            location_raw = None
            # Common feed text pattern: "Location: ..."
            loc_match = re.search(r"location\s*:\s*([^<\n\r]+)", description, re.IGNORECASE)
            if loc_match:
                location_raw = loc_match.group(1).strip()[:200]

            employment_type = None
            type_match = _JOB_TYPE_PATTERN.search(description)
            if type_match:
                employment_type = type_match.group(0).strip()

            salary_raw = None
            sal_match = _SALARY_PATTERN.search(description)
            if sal_match:
                salary_raw = sal_match.group(0).strip()

            jobs.append(
                {
                    "title": title,
                    "source_url": link or source_url,
                    "location_raw": location_raw,
                    "salary_raw": salary_raw,
                    "employment_type": employment_type,
                    "description": description or None,
                    "extraction_method": "tier0_xml_feed_v20",
                    "extraction_confidence": 0.9,
                }
            )

        return jobs

    @staticmethod
    def _extract_location_from_jsonld_v20(item: dict) -> Optional[str]:
        loc = item.get("jobLocation")
        if not loc:
            return None
        locations = loc if isinstance(loc, list) else [loc]
        parts: list[str] = []
        for entry in locations:
            if isinstance(entry, str):
                parts.append(entry)
                continue
            if not isinstance(entry, dict):
                continue
            address = entry.get("address")
            if isinstance(address, str):
                parts.append(address)
                continue
            if isinstance(address, dict):
                city = str(address.get("addressLocality") or "").strip()
                region = str(address.get("addressRegion") or "").strip()
                country = address.get("addressCountry")
                if isinstance(country, dict):
                    country = country.get("name")
                country = str(country or "").strip()
                text = ", ".join(p for p in [city, region, country] if p)
                if text:
                    parts.append(text)
        return "; ".join(parts) if parts else None

    @staticmethod
    def _extract_salary_from_jsonld_v20(item: dict) -> Optional[str]:
        salary = item.get("baseSalary") or item.get("estimatedSalary")
        if not salary:
            return None
        if isinstance(salary, str):
            return salary.strip() or None
        if not isinstance(salary, dict):
            return None

        currency = str(salary.get("currency") or "").strip()
        value = salary.get("value")
        if isinstance(value, dict):
            min_val = value.get("minValue")
            max_val = value.get("maxValue")
            unit = str(value.get("unitText") or "").strip()
            if min_val and max_val:
                return f"{currency} {min_val}-{max_val} {unit}".strip()
            if min_val:
                return f"{currency} {min_val} {unit}".strip()
            if max_val:
                return f"{currency} {max_val} {unit}".strip()
        if isinstance(value, (int, float, str)):
            return f"{currency} {value}".strip()
        return None

    # ------------------------------------------------------------------
    # Tier 2 replacement (called by v1.6 super.extract)
    # ------------------------------------------------------------------

    def _extract_tier2_v16(self, url: str, html: str) -> Optional[list[dict]]:
        """Override v1.6 Tier 2 with broader candidate aggregation + validation."""
        root = _parse_html(html)
        if root is None:
            return None

        is_elementor = "elementor" in html[:6000].lower()
        page_apply_count = self._count_page_apply_buttons(root)

        candidates: list[tuple[str, list[dict]]] = []

        base_jobs = super()._extract_tier2_v16(url, html)
        if base_jobs:
            candidates.append(("v16_base", self._dedupe_jobs(base_jobs, url)))

        sweep_jobs = self._extract_from_candidate_sweep_v20(root, url, is_elementor, page_apply_count)
        if sweep_jobs:
            candidates.append(("candidate_sweep", sweep_jobs))

        row_jobs = self._extract_from_repeated_rows_v20(root, url)
        if row_jobs:
            candidates.append(("repeated_rows", row_jobs))

        heading_jobs = self._extract_from_heading_sections_v20(root, url)
        if heading_jobs:
            candidates.append(("heading_sections", heading_jobs))

        elementor_jobs = self._extract_from_elementor_cards_v20(root, url)
        if elementor_jobs:
            candidates.append(("elementor_cards", elementor_jobs))

        return self._pick_best_jobset_v20(candidates, url)

    def _extract_from_candidate_sweep_v20(
        self,
        root: etree._Element,
        url: str,
        is_elementor: bool,
        page_apply_count: int,
    ) -> Optional[list[dict]]:
        scored = self._score_containers_v20(root, url, is_elementor, page_apply_count)
        if not scored:
            return None

        scored.sort(key=lambda c: (c[1], c[2]), reverse=True)
        bucketed: dict[str, list[dict]] = defaultdict(list)

        for el, score, _children in scored[:25]:
            tag = (el.tag or "").lower() if isinstance(el.tag, str) else ""
            if tag in {"a", "span", "p", "h1", "h2", "h3", "h4", "h5", "h6"}:
                continue

            jobs = self._extract_jobs_v20(el, url, score)
            jobs = self._dedupe_jobs(jobs, url)
            if not jobs:
                continue

            bucket = self._container_bucket_key_v20(el)
            bucketed[bucket].extend(jobs)

        best: Optional[list[dict]] = None
        best_score = -1.0

        for jobs in bucketed.values():
            deduped = self._dedupe_jobs(jobs, url)
            score = self._jobset_score(deduped, url)
            if self._passes_jobset_validation(deduped, url) and score > best_score:
                best = deduped
                best_score = score

        return best

    def _extract_jobs_v20(
        self,
        container: etree._Element,
        base_url: str,
        container_score: int,
    ) -> list[dict]:
        jobs = super()._extract_jobs_v16(container, base_url, container_score)

        # Supplemental extraction for accordion/button style sections.
        jobs.extend(self._extract_from_buttons_in_container_v20(container, base_url, container_score))

        return self._dedupe_jobs(jobs, base_url)

    def _extract_from_buttons_in_container_v20(
        self,
        container: etree._Element,
        base_url: str,
        container_score: int,
    ) -> list[dict]:
        jobs: list[dict] = []

        for btn in container.iter("button"):
            title = self._normalize_title_v20(_text(btn))
            if not self._is_valid_title_v20(title):
                continue
            if not self._title_has_job_signal(title):
                continue

            # Nearby content may include location/type hints.
            parent = btn.getparent()
            parent_text = _text(parent)[:1000] if parent is not None else ""

            employment_type = None
            type_match = _JOB_TYPE_PATTERN.search(parent_text)
            if type_match:
                employment_type = type_match.group(0).strip()

            location = None
            loc_match = _AU_LOCATIONS.search(parent_text)
            if loc_match:
                location = loc_match.group(0).strip()

            jobs.append(
                {
                    "title": title,
                    "source_url": base_url,
                    "location_raw": location,
                    "salary_raw": None,
                    "employment_type": employment_type,
                    "description": parent_text[:5000] if len(parent_text) > 60 else None,
                    "extraction_method": "tier2_accordion_button_v20",
                    "extraction_confidence": min(0.52 + container_score * 0.02, 0.84),
                }
            )

        return jobs

    def _extract_from_repeated_rows_v20(self, root: etree._Element, url: str) -> Optional[list[dict]]:
        buckets: dict[str, list[etree._Element]] = defaultdict(list)

        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag not in {"li", "div", "tr", "article", "section"}:
                continue

            classes = _get_el_classes(el)
            if not classes or not _ROW_CLASS_STRONG_PATTERN_V20.search(classes):
                continue

            tokens = classes.split()
            token = next((t for t in tokens if _ROW_CLASS_STRONG_PATTERN_V20.search(t)), tokens[0])
            key = f"{tag}:{token}"
            buckets[key].append(el)

        best: Optional[list[dict]] = None
        best_score = -1.0

        for rows in buckets.values():
            if len(rows) < 3 or len(rows) > MAX_JOBS_PER_PAGE:
                continue

            jobs: list[dict] = []
            for row in rows:
                job = self._extract_heuristic_job_v20(row, url, container_score=14)
                if job:
                    jobs.append(job)

            deduped = self._dedupe_jobs(jobs, url)
            score = self._jobset_score(deduped, url)
            if self._passes_jobset_validation(deduped, url) and score > best_score:
                best = deduped
                best_score = score

        return best

    def _extract_from_heading_sections_v20(self, root: etree._Element, url: str) -> Optional[list[dict]]:
        headings = root.xpath("//main//h2 | //main//h3 | //article//h2 | //article//h3")
        if not headings:
            headings = root.xpath("//h2 | //h3")

        jobs: list[dict] = []

        for h in headings:
            title = self._normalize_title_v20(_text(h))
            if not self._is_valid_title_v20(title):
                continue

            ancestor_classes = self._collect_ancestor_classes_v20(h, depth=5)
            if not re.search(r"content|prose|article|entry|post|career|vacanc|job|elementor", ancestor_classes):
                continue

            parent_text = _text(h.getparent())[:1200] if h.getparent() is not None else ""
            if not (self._title_has_job_signal(title) or _CONTEXT_HINT_PATTERN_V20.search(parent_text)):
                continue

            link_href = _href(h)
            if not link_href:
                sibling_link = h.xpath("following::a[@href][1]")
                if sibling_link:
                    link_href = sibling_link[0].get("href")

            jobs.append(
                {
                    "title": title,
                    "source_url": _resolve_url(link_href, url) or url,
                    "location_raw": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": parent_text[:5000] if len(parent_text) > 80 else None,
                    "extraction_method": "tier2_heading_sections_v20",
                    "extraction_confidence": 0.62,
                }
            )

        deduped = self._dedupe_jobs(jobs, url)
        return deduped if self._passes_jobset_validation(deduped, url) else None

    def _extract_from_elementor_cards_v20(self, root: etree._Element, url: str) -> Optional[list[dict]]:
        """Extract from Elementor inner-column cards with heading + action link."""
        cards = root.xpath("//*[contains(@class,'elementor-inner-column')]")
        if not cards:
            return None

        jobs: list[dict] = []
        for card in cards:
            heading = card.xpath(".//*[self::h1 or self::h2 or self::h3][contains(@class,'elementor-heading-title')]")
            if not heading:
                continue

            title = self._normalize_title_v20(_text(heading[0]))
            if not self._is_valid_title_v20(title):
                continue

            # Prefer explicit CTA button/link inside the same card.
            link_href = None
            anchors = card.xpath(".//a[@href]")
            for a in anchors:
                href = (a.get("href") or "").strip()
                if not href or href.startswith("#"):
                    continue
                a_text = _text(a)
                if (
                    "button" in _get_el_classes(a)
                    or _CONTEXT_HINT_PATTERN_V20.search(a_text)
                    or _JOB_URL_HINT_PATTERN_V20.search(href)
                ):
                    link_href = href
                    break
            if not link_href and anchors:
                link_href = anchors[0].get("href")

            card_text = _text(card)[:1600]
            if not (self._title_has_job_signal(title) or _CONTEXT_HINT_PATTERN_V20.search(card_text)):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": _resolve_url(link_href, url) or url,
                    "location_raw": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": card_text[:5000] if len(card_text) > 60 else None,
                    "extraction_method": "tier2_elementor_cards_v20",
                    "extraction_confidence": 0.7,
                }
            )

        deduped = self._dedupe_jobs(jobs, url)
        return deduped if self._passes_jobset_validation(deduped, url) else None

    def _extract_heuristic_job_v20(
        self,
        row: etree._Element,
        base_url: str,
        container_score: int,
    ) -> Optional[dict]:
        job = self._extract_heuristic_job(row, base_url, container_score)
        if not job:
            return None

        title = self._normalize_title_v20(job.get("title", ""))
        if not self._is_valid_title_v20(title):
            return None

        job["title"] = title
        job["extraction_method"] = "tier2_heuristic_v20"
        return job

    # ------------------------------------------------------------------
    # Scoring/validation
    # ------------------------------------------------------------------

    def _score_containers_v20(
        self,
        root: etree._Element,
        page_url: str,
        is_elementor: bool,
        page_apply_count: int,
    ) -> list[tuple[etree._Element, int, int]]:
        scored = self._score_containers_v16(root, page_url, is_elementor, page_apply_count)
        rescored: list[tuple[etree._Element, int, int]] = []

        for el, score, child_count in scored:
            classes = _get_el_classes(el)
            el_id = (el.get("id") or "").lower()
            combined = f"{classes} {el_id}"

            # Penalize nav/footer/locale/country containers.
            if re.search(r"\b(?:menu|nav|footer|header|breadcrumb|locale|language|country|currency|social|account)\b", combined):
                score -= 10

            # Penalize containers dominated by country/currency list items.
            children = [
                c for c in el
                if isinstance(c.tag, str) and c.tag.lower() not in ("script", "style", "br", "hr")
            ]
            if children:
                sample_titles = []
                for child in children[: min(80, len(children))]:
                    t = self._normalize_title_v20(_text(child))
                    if 2 < len(t) < 120:
                        sample_titles.append(t)
                if sample_titles:
                    cc_hits = sum(1 for t in sample_titles if _COUNTRY_CURRENCY_PATTERN_V20.match(t))
                    if cc_hits >= max(3, int(len(sample_titles) * 0.35)):
                        score -= 18

            if score >= 5:
                rescored.append((el, score, child_count))

        return rescored

    def _pick_best_jobset_v20(self, candidates: list[tuple[str, list[dict]]], page_url: str) -> Optional[list[dict]]:
        best: Optional[list[dict]] = None
        best_score = -1.0

        fallback_partial: Optional[list[dict]] = None
        fallback_partial_score = -1.0

        for label, jobs in candidates:
            deduped = self._dedupe_jobs(jobs, page_url)
            if not deduped:
                continue

            score = self._jobset_score(deduped, page_url)
            valid = self._passes_jobset_validation(deduped, page_url)

            logger.debug(
                "v2.0 candidate %s: jobs=%d score=%.2f valid=%s",
                label,
                len(deduped),
                score,
                valid,
            )

            if valid and score > best_score:
                best = deduped
                best_score = score

            # Keep a conservative 1-job fallback only if strongly job-like.
            if len(deduped) == 1 and score > fallback_partial_score:
                title = deduped[0].get("title", "")
                if self._title_has_job_signal(title) or self._is_job_like_url(deduped[0], page_url):
                    fallback_partial = deduped
                    fallback_partial_score = score

        if best:
            return best[:MAX_JOBS_PER_PAGE]
        return fallback_partial[:MAX_JOBS_PER_PAGE] if fallback_partial else None

    def _passes_jobset_validation(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title_v20(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if self._is_valid_title_v20(t)]
        if not titles:
            return False

        unique_ratio = len(set(t.lower() for t in titles)) / max(1, len(titles))
        if unique_ratio < 0.6:
            return False

        country_like = sum(1 for t in titles if _COUNTRY_CURRENCY_PATTERN_V20.match(t))
        if country_like >= max(2, int(len(titles) * 0.35)):
            return False

        reject_like = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V20.match(t.lower()))
        if reject_like >= max(1, int(len(titles) * 0.35)):
            return False

        job_signal_hits = sum(1 for t in titles if self._title_has_job_signal(t))
        job_url_hits = sum(1 for j in jobs if self._is_job_like_url(j, page_url))

        if len(titles) == 1:
            return job_signal_hits >= 1 or job_url_hits >= 1

        if len(titles) <= 3:
            return job_url_hits >= 2 and job_signal_hits >= 1 and reject_like == 0

        return (
            job_signal_hits >= max(1, int(len(titles) * 0.2))
            or job_url_hits >= max(2, int(len(titles) * 0.3))
        )

    def _jobset_score(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        titles = [self._normalize_title_v20(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]

        count = len(titles)
        job_signals = sum(1 for t in titles if self._title_has_job_signal(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url(j, page_url))
        unique_urls = len({(j.get("source_url") or "").strip().lower() for j in jobs if j.get("source_url")})

        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V20.match(t.lower()))
        country_hits = sum(1 for t in titles if _COUNTRY_CURRENCY_PATTERN_V20.match(t))

        score = count * 4.0
        score += job_signals * 2.8
        score += url_hits * 1.7
        score += min(unique_urls, count)
        score -= reject_hits * 3.0
        score -= country_hits * 4.0

        return score

    def _dedupe_jobs(self, jobs: list[dict], page_url: str) -> list[dict]:
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for job in jobs:
            title = self._normalize_title_v20(job.get("title", ""))
            if not self._is_valid_title_v20(title):
                continue

            source_url = (job.get("source_url") or "").strip()
            if not source_url:
                source_url = page_url

            key = (title.lower(), source_url.lower())
            if key in seen:
                continue
            seen.add(key)

            cloned = dict(job)
            cloned["title"] = title
            cloned["source_url"] = source_url
            deduped.append(cloned)

        return deduped[:MAX_JOBS_PER_PAGE]

    def _normalize_title_v20(self, title: str) -> str:
        if not title:
            return ""

        t = " ".join(title.replace("\u00a0", " ").split())
        t = t.replace("%HEADER_", "").replace("%LABEL_", "")

        meta_match = _TRAILING_META_SPLIT_V20.search(t)
        if meta_match and meta_match.start() > 6:
            t = t[:meta_match.start()].strip(" -|:\u2013")

        camel = _CAMEL_LOCATION_SPLIT_V20.match(t)
        if camel:
            left, pivot, right = camel.groups()
            if 3 <= len(right) <= 60 and not self._title_has_job_signal(right):
                t = f"{left}{pivot}".strip()

        parts = re.split(r"\s{2,}|\n+|\t+|\s[\-|\u2013|\u2022]\s", t)
        for part in parts:
            part = part.strip()
            if len(part) < 3:
                continue
            if self._title_has_job_signal(part) or _is_valid_title(part):
                t = part
                break

        t = " ".join(t.strip(" |:-\u2013\u2022").split())
        return t

    def _is_valid_title_v20(self, title: str) -> bool:
        if not title:
            return False

        t = title.strip()
        low = t.lower()

        if _REJECT_TITLE_PATTERN_V20.match(low):
            return False

        if _COUNTRY_CURRENCY_PATTERN_V20.match(t):
            return False

        base_valid = TieredExtractorV16._is_valid_title_v16(t)
        if not base_valid:
            # Relax v1.6-only English/format constraints when we still have a
            # strong job-title signal (e.g. multilingual or acronym-heavy titles).
            if not self._title_has_job_signal(t):
                return False
            if len(t) < 3 or len(t) > 120:
                return False
            alnum = sum(1 for c in t if c.isalnum() or c.isspace())
            if alnum < len(t) * 0.45:
                return False

        if _CTA_TITLE_PATTERN_V20.search(low) and not self._title_has_job_signal(t):
            return False

        words = t.split()
        if len(words) > 12:
            return False
        if len(words) > 8 and not self._title_has_job_signal(t):
            return False

        return True

    def _title_has_job_signal(self, title: str) -> bool:
        if not title:
            return False
        if _title_has_job_noun(title):
            return True
        return bool(_TITLE_HINT_PATTERN_V20.search(title))

    def _is_job_like_url(self, job: dict, page_url: str) -> bool:
        src = (job.get("source_url") or "").strip()
        if not src or src == page_url:
            return False

        try:
            parsed = urlparse(src)
        except Exception:
            return False

        path = (parsed.path or "").lower()
        query = (parsed.query or "").lower()

        if _NON_JOB_URL_PATTERN_V20.search(path):
            return False
        if _JOB_URL_HINT_PATTERN_V20.search(path):
            return True
        if "search=" in query or ("job" in query and "id=" in query):
            return True
        if re.search(r"/p/[^/]{4,}", path):
            return True

        return False

    @staticmethod
    def _container_bucket_key_v20(el: etree._Element) -> str:
        tag = (el.tag or "").lower() if isinstance(el.tag, str) else "el"
        classes = _get_el_classes(el).split()
        if not classes:
            return tag

        important = [
            c
            for c in classes
            if any(k in c for k in ("job", "career", "vacan", "position", "opening", "listing", "search", "elementor"))
        ]
        token = important[0] if important else classes[0]
        return f"{tag}:{token}"

    @staticmethod
    def _collect_ancestor_classes_v20(el: etree._Element, depth: int = 4) -> str:
        classes: list[str] = []
        node = el
        for _ in range(depth):
            node = node.getparent()
            if node is None or not isinstance(node.tag, str):
                break
            cls = (node.get("class") or "").strip()
            if cls:
                classes.append(cls.lower())
        return " ".join(classes)
