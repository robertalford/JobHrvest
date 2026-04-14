"""
Tiered Extraction Engine v3.8 — direct from v1.6.

High-impact improvements:
1. Short-page recovery: refetch weak/empty pages and probe high-value listing paths.
2. Hub traversal: follow strong listing links (Job Openings / Join Our Team / Lowongan / Kerjaya / Portal.na).
3. Strict small-set validation to block nav/editorial false positives (e.g. Our Leaders / Our Ecosystem).
4. ATS/card fallbacks for Zoho table rows, Elementor cards, accordion listings, and strong job links.
5. Candidate arbitration that favors role-rich, evidence-backed sets over noisy heading-only sets.
"""

from __future__ import annotations

import asyncio
import html as html_lib
import logging
import re
from collections import defaultdict
from typing import Optional
from urllib.parse import urljoin, urlparse

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


_ROLE_HINT_PATTERN_V38 = re.compile(
    r"\b(?:engineer|developer|manager|director|analyst|specialist|assistant|"
    r"consultant|coordinator|officer|administrator|accountant|technician|owner|"
    r"designer|architect|operator|supervisor|advisor|executive|intern(?:ship)?|"
    r"recruit(?:er|ment)?|nurse|teacher|driver|chef|sales|marketing|finance|"
    r"hr|akuntan|konsultan|asisten|staf|staff|pegawai|karyawan|influencer|"
    r"videografer|fotografer|psikolog(?:i)?|customer\s+service|model|sarjana|fashion)\b",
    re.IGNORECASE,
)

_TITLE_REJECT_PATTERN_V38 = re.compile(
    r"^(?:our\s+leaders?|our\s+ecosystem|our\s+values?|talent\s+stories?|"
    r"franchise\s+institute|skim\s+pembiayaan\s+francaisor|sewaan\s+premis|"
    r"join\s+our\s+team|current\s+jobs?|all\s+jobs?|job\s+openings?|"
    r"search\s+jobs?|browse\s+jobs?|view\s+all\s+jobs?|"
    r"careers?|about\s+us|our\s+culture|our\s+direction|contact|home|menu|"
    r"read\s+more|learn\s+more|show\s+more|load\s+more|info\s+lengkap|"
    r"get\s+started|sign\s+up\s+for\s+alerts?)$",
    re.IGNORECASE,
)

_LISTING_LINK_TEXT_PATTERN_V38 = re.compile(
    r"\b(?:job\s+openings?|current\s+vacancies|join\s+our\s+team|"
    r"view\s+all\s+jobs?|search\s+jobs|browse\s+jobs|lowongan|kerjaya|"
    r"karir|loker|careers?)\b",
    re.IGNORECASE,
)

_LISTING_URL_PATTERN_V38 = re.compile(
    r"/(?:career|careers|jobs?|job-openings?|vacanc|opening|openings|position|"
    r"requisition|portal\.na|candidateportal|join-our-team|current-vacancies|"
    r"lowongan|loker|kerjaya|karir)",
    re.IGNORECASE,
)

_DETAILISH_URL_PATTERN_V38 = re.compile(
    r"(?:/jobs?/[A-Za-z0-9][^/?#]{3,}|/career/openings?/|/jobdetails(?:/|$|\?)|"
    r"PortalDetail\.na\?.*jobid=|/join-our-team/[A-Za-z0-9]{6,}|"
    r"[?&](?:jobid|job_id|requisitionid|positionid|vacancyid|jobadid|adid|ajid)=)",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V38 = re.compile(
    r"(?:/(?:privacy|terms|cookie|news|blog|about|contact|investor|"
    r"team|culture|our-culture|our-values|our-ecosystem|our-direction|"
    r"talent-story|services?|franchise|login|logout|register|account|help)(?:/|$|[?#])|"
    r"/fRecruit__Apply(?:Register|ExpressInterest)|wp-json|/feed(?:/|$)|/rss(?:/|$)|"
    r"\.pdf(?:$|\?)|\.docx?(?:$|\?))",
    re.IGNORECASE,
)

_APPLY_EVIDENCE_PATTERN_V38 = re.compile(
    r"(?:apply|application|apply\s+now|apply\s+here|mailto:|"
    r"requirements?|qualifications?|responsibilit|closing\s+date|"
    r"full\s*time|part\s*time|contract|permanent|temporary|"
    r"how\s+to\s+apply|cara\s+melamar)",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN_V38 = re.compile(
    r"(?:job|position|vacanc|opening|requisition|career|posting|listing|accordion)",
    re.IGNORECASE,
)

_ACRONYM_TITLE_PATTERN_V38 = re.compile(r"^[A-Z][A-Z0-9&/\-\+]{1,10}$")


class TieredExtractorV38(TieredExtractorV16):
    """v3.8 extractor with discovery-aware recovery and stricter quality gating."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        recovered_html = await self._recover_short_html_v38(page_url, working_html)
        if recovered_html:
            working_html = recovered_html

        candidates: list[tuple[str, list[dict]]] = []

        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(super().extract(career_page, company, working_html), timeout=24.0)
        except asyncio.TimeoutError:
            logger.warning("v3.8 parent extractor timeout for %s", page_url)
        except Exception:
            logger.exception("v3.8 parent extractor failed for %s", page_url)

        parent_jobs = self._prepare_candidate_jobs_v38(parent_jobs or [], page_url)
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        local_jobs = self._extract_from_single_page_v38(page_url, working_html)
        if local_jobs:
            candidates.append(("local_v38", local_jobs))

        best_label, best_jobs, best_score = self._pick_best_candidate_v38(candidates, page_url)

        need_subpage_follow = (
            not best_jobs
            or len(best_jobs) < MIN_JOBS_FOR_SUCCESS
            or best_score < 11.0
        )
        if need_subpage_follow:
            subpage_urls = self._collect_listing_subpages_v38(page_url, working_html)
            fetched = 0
            for subpage_url in subpage_urls[:8]:
                if fetched >= 6:
                    break
                sub_html = await self._fetch_html_v38(subpage_url)
                if not sub_html or len(sub_html) < 200:
                    continue
                fetched += 1
                sub_jobs = self._extract_from_single_page_v38(subpage_url, sub_html)
                if sub_jobs:
                    candidates.append((f"subpage_v38:{fetched}", sub_jobs))

            best_label, best_jobs, best_score = self._pick_best_candidate_v38(candidates, page_url)

        if not best_jobs:
            return []

        if best_label != "parent_v16" and any(
            self._is_job_like_url_v38(j.get("source_url") or "")
            and (j.get("source_url") or page_url).rstrip("/") != page_url.rstrip("/")
            for j in best_jobs
        ):
            try:
                best_jobs = await asyncio.wait_for(self._enrich_from_detail_pages(best_jobs), timeout=16.0)
            except asyncio.TimeoutError:
                logger.warning("v3.8 enrichment timeout for %s", page_url)
            except Exception:
                logger.exception("v3.8 enrichment failed for %s", page_url)

        final_jobs = self._prepare_candidate_jobs_v38(best_jobs, page_url)
        return final_jobs[:MAX_JOBS_PER_PAGE]

    async def _recover_short_html_v38(self, page_url: str, html_body: str) -> Optional[str]:
        body = html_body or ""
        short_or_failed = len(body) < 300 or "FETCH FAILED" in body[:120]

        probe_urls: list[str] = []
        parsed = urlparse(page_url)
        if parsed.scheme and parsed.netloc:
            base = f"{parsed.scheme}://{parsed.netloc}"
            probe_urls.extend([
                page_url,
                page_url.rstrip("/") + "/",
            ])
            if short_or_failed or parsed.path.rstrip("/") in {"", "/career", "/careers", "/jobs"}:
                probe_urls.extend(
                    [
                        base + "/career/job-openings",
                        base + "/careers/join-our-team",
                        base + "/recruit/Portal.na",
                        base + "/ms/kerjaya",
                        base + "/lowongan",
                        base + "/jobs/Careers",
                    ]
                )

        if not probe_urls:
            return body

        best_html = body
        best_score = self._page_listing_score_v38(page_url, body)
        seen: set[str] = set()

        async with httpx.AsyncClient(
            timeout=7,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        ) as client:
            for candidate in probe_urls:
                norm = candidate.rstrip("/")
                if not candidate or norm in seen:
                    continue
                seen.add(norm)

                try:
                    resp = await client.get(candidate)
                except Exception:
                    continue

                text = resp.text or ""
                if resp.status_code != 200 or len(text) < 200:
                    continue
                if self._looks_non_html_payload_v38(text):
                    continue

                score = self._page_listing_score_v38(str(resp.url), text)
                if score > best_score + 0.15:
                    best_html = text
                    best_score = score

        return best_html

    async def _fetch_html_v38(self, url: str) -> Optional[str]:
        try:
            async with httpx.AsyncClient(
                timeout=7,
                follow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            ) as client:
                resp = await client.get(url)
        except Exception:
            return None

        text = resp.text or ""
        if resp.status_code != 200 or len(text) < 200 or self._looks_non_html_payload_v38(text):
            return None
        return text

    def _extract_from_single_page_v38(self, page_url: str, html_body: str) -> list[dict]:
        if not html_body or len(html_body) < 200:
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
            label, jobs, _score = self._pick_best_candidate_v38(candidates, page_url)
            return jobs

        zoho_rows = self._extract_zoho_rows_v38(root, page_url)
        if zoho_rows:
            candidates.append(("zoho_rows_v38", zoho_rows))

        repeated_rows = self._extract_repeating_rows_v38(root, page_url)
        if repeated_rows:
            candidates.append(("repeating_rows_v38", repeated_rows))

        elementor_cards = self._extract_elementor_cards_v38(root, page_url)
        if elementor_cards:
            candidates.append(("elementor_cards_v38", elementor_cards))

        accordion_jobs = self._extract_accordion_jobs_v38(root, page_url)
        if accordion_jobs:
            candidates.append(("accordion_jobs_v38", accordion_jobs))

        heading_rows = self._extract_heading_rows_v38(root, page_url)
        if heading_rows:
            candidates.append(("heading_rows_v38", heading_rows))

        greenhouse_posts = self._extract_greenhouse_posts_v38(root, page_url)
        if greenhouse_posts:
            candidates.append(("greenhouse_posts_v38", greenhouse_posts))

        job_links = self._extract_job_links_v38(root, page_url)
        if job_links:
            candidates.append(("job_links_v38", job_links))

        _label, jobs, _score = self._pick_best_candidate_v38(candidates, page_url)
        return jobs

    def _collect_listing_subpages_v38(self, page_url: str, html_body: str) -> list[str]:
        if not html_body or len(html_body) < 200:
            return []

        root = _parse_html(html_body)
        if root is None:
            return []

        candidates: list[tuple[str, float]] = []
        seen: set[str] = set()

        for a_el in root.xpath("//a[@href]"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            text = self._safe_text_v38(a_el)
            if self._is_rejected_listing_link_v38(href, text):
                continue

            full_url = _resolve_url(href, page_url)
            if not full_url:
                continue
            norm = full_url.rstrip("/")
            if norm in seen or norm == page_url.rstrip("/"):
                continue
            seen.add(norm)

            if not self._is_related_host_v38(page_url, full_url):
                host = (urlparse(full_url).hostname or "").lower()
                if not any(x in host for x in ("greenhouse.io", "zohorecruit", "oraclecloud.com")):
                    continue

            score = 0.0
            if _LISTING_URL_PATTERN_V38.search(full_url):
                score += 5.0
            if _LISTING_LINK_TEXT_PATTERN_V38.search(text):
                score += 4.0
            if "job-openings" in full_url.lower() or "portal.na" in full_url.lower():
                score += 2.0
            if re.search(r"/(?:our-culture|our-values|talent-story|our-ecosystem)(?:/|$)", full_url, re.IGNORECASE):
                score -= 6.0
            if score > 0:
                candidates.append((full_url, score))

        candidates.sort(key=lambda x: x[1], reverse=True)
        return [url for url, _score in candidates[:12]]

    def _extract_zoho_rows_v38(self, root: etree._Element, page_url: str) -> list[dict]:
        rows = root.xpath("//tr[contains(@class,'jobDetailRow')]")
        if len(rows) < MIN_JOBS_FOR_SUCCESS:
            return []

        jobs: list[dict] = []
        for row in rows[:MAX_JOBS_PER_PAGE]:
            a_el = row.xpath(".//a[contains(@class,'jobdetail')][1]")
            if not a_el:
                continue

            title = self._normalize_title_v38(_text(a_el[0]))
            href = a_el[0].get("href") or ""
            source_url = _resolve_url(href, page_url) or page_url
            row_text = _text(row)
            tds = row.xpath("./td")
            location = _text(tds[1]) if len(tds) > 1 else None
            salary = _text(tds[4]) if len(tds) > 4 else None

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": salary,
                    "employment_type": self._extract_employment_type_v38(row_text),
                    "description": self._clean_description_v38(row_text),
                    "extraction_method": "tier2_zoho_rows_v38",
                    "extraction_confidence": 0.86,
                }
            )

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_repeating_rows_v38(self, root: etree._Element, page_url: str) -> list[dict]:
        groups: dict[str, list[etree._Element]] = defaultdict(list)

        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag not in {"li", "div", "article", "tr", "section"}:
                continue

            classes = _get_el_classes(el)
            if not classes or not _ROW_CLASS_PATTERN_V38.search(classes):
                continue

            tokens = classes.split()
            if not tokens:
                continue
            key = f"{tag}:{tokens[0]}"
            groups[key].append(el)

        jobs: list[dict] = []

        for rows in groups.values():
            if len(rows) < 3:
                continue
            for row in rows[:MAX_JOBS_PER_PAGE]:
                job = self._extract_row_job_v38(row, page_url, "tier2_repeating_rows_v38", 0.72)
                if job:
                    jobs.append(job)

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_elementor_cards_v38(self, root: etree._Element, page_url: str) -> list[dict]:
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
            title = self._normalize_title_v38(_text(heading_nodes[0]))
            if not self._title_has_role_signal_v38(title):
                continue

            source_url = page_url
            for a_el in card.xpath(".//a[@href]"):
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v38(href):
                    continue
                source_url = href
                if self._is_job_like_url_v38(href):
                    break

            card_text = _text(card)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v38(card, title),
                    "salary_raw": self._extract_salary_v38(card_text),
                    "employment_type": self._extract_employment_type_v38(card_text),
                    "description": self._clean_description_v38(card_text),
                    "extraction_method": "tier2_elementor_cards_v38",
                    "extraction_confidence": 0.76,
                }
            )

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_accordion_jobs_v38(self, root: etree._Element, page_url: str) -> list[dict]:
        rows = root.xpath(
            "//*[contains(@class,'accordion-item') or contains(@class,'accordion__item') or contains(@class,'collapse-item')]"
        )
        if len(rows) < 1:
            return []

        jobs: list[dict] = []
        for row in rows[:MAX_JOBS_PER_PAGE]:
            heading = row.xpath(".//h2[1] | .//h3[1] | .//button[1]")
            if not heading:
                continue

            title = self._normalize_title_v38(_text(heading[0]))
            row_text = _text(row)
            if not self._title_has_role_signal_v38(title):
                continue
            if not _APPLY_EVIDENCE_PATTERN_V38.search(row_text) and len(row_text) < 150:
                continue

            source_url = page_url
            for a_el in row.xpath(".//a[@href]"):
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v38(href):
                    continue
                source_url = href
                if self._is_job_like_url_v38(href):
                    break

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v38(row, title),
                    "salary_raw": self._extract_salary_v38(row_text),
                    "employment_type": self._extract_employment_type_v38(row_text),
                    "description": self._clean_description_v38(row_text),
                    "extraction_method": "tier2_accordion_v38",
                    "extraction_confidence": 0.72,
                }
            )

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_job_links_v38(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        for a_el in root.xpath("//a[@href]")[:5000]:
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url)
            if not source_url or self._is_non_job_url_v38(source_url):
                continue

            heading_nodes = a_el.xpath(".//h1 | .//h2 | .//h3 | .//h4 | .//h5")
            raw_title = _text(heading_nodes[0]) if heading_nodes else _text(a_el)
            title = self._normalize_title_v38(raw_title)
            if not self._is_valid_title_v38(title):
                continue

            context = a_el.getparent()
            if context is None:
                context = a_el
            context_text = _text(context)

            url_hint = self._is_job_like_url_v38(source_url)
            apply_hint = bool(_APPLY_EVIDENCE_PATTERN_V38.search(context_text))

            if not (url_hint or apply_hint):
                continue
            if not self._title_has_role_signal_v38(title) and not (self._is_acronym_title_v38(title) and url_hint):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v38(context, title),
                    "salary_raw": self._extract_salary_v38(context_text),
                    "employment_type": self._extract_employment_type_v38(context_text),
                    "description": self._clean_description_v38(context_text),
                    "extraction_method": "tier2_job_links_v38",
                    "extraction_confidence": 0.74 if url_hint else 0.67,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_heading_rows_v38(self, root: etree._Element, page_url: str) -> list[dict]:
        headings = root.xpath("//h2 | //h3 | //h4")
        if len(headings) < 2:
            return []

        jobs: list[dict] = []
        for heading in headings[:400]:
            title = self._normalize_title_v38(_text(heading))
            if not self._is_valid_title_v38(title):
                continue
            if not self._title_has_role_signal_v38(title):
                continue

            container = heading.getparent()
            if container is None:
                container = heading
            row_text = _text(container)
            if len(row_text) < 20:
                continue

            location = None
            for loc_el in container.xpath(".//p | .//span | .//div"):
                loc_text = " ".join(_text(loc_el).split())
                if not loc_text or loc_text == title:
                    continue
                if 2 < len(loc_text) < 120 and re.search(r"[A-Za-z]", loc_text):
                    location = loc_text
                    break

            jobs.append(
                {
                    "title": title,
                    "source_url": page_url,
                    "location_raw": location,
                    "salary_raw": self._extract_salary_v38(row_text),
                    "employment_type": self._extract_employment_type_v38(row_text),
                    "description": self._clean_description_v38(row_text),
                    "extraction_method": "tier2_heading_rows_v38",
                    "extraction_confidence": 0.66,
                }
            )

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_greenhouse_posts_v38(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        for a_el in root.xpath("//a[@href]")[:4000]:
            href = (a_el.get("href") or "").strip()
            source_url = _resolve_url(href, page_url)
            if not source_url:
                continue
            if not re.search(r"/jobs/[0-9]{4,}", source_url, re.IGNORECASE):
                continue
            if self._is_non_job_url_v38(source_url):
                continue

            row = a_el
            cursor = a_el
            for _ in range(6):
                parent = cursor.getparent()
                if parent is None:
                    break
                if "job-post" in _get_el_classes(parent).lower():
                    row = parent
                    break
                cursor = parent

            title = ""
            title_nodes = row.xpath(".//*[contains(@class,'body--medium')][1]")
            if title_nodes:
                title = self._normalize_title_v38(_text(title_nodes[0]))
            if not title:
                title = self._normalize_title_v38(_text(a_el))
            if not self._is_valid_title_v38(title):
                continue
            if not self._title_has_role_signal_v38(title):
                continue

            location = None
            location_nodes = row.xpath(".//*[contains(@class,'body--metadata')][1]")
            if location_nodes:
                location = " ".join(_text(location_nodes[0]).split())[:180]

            row_text = _text(row)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": self._extract_salary_v38(row_text),
                    "employment_type": self._extract_employment_type_v38(row_text),
                    "description": self._clean_description_v38(row_text),
                    "extraction_method": "tier2_greenhouse_posts_v38",
                    "extraction_confidence": 0.85,
                }
            )

        return self._prepare_candidate_jobs_v38(jobs, page_url)

    def _extract_row_job_v38(
        self,
        row: etree._Element,
        page_url: str,
        method: str,
        confidence: float,
    ) -> Optional[dict]:
        links = row.xpath(".//a[@href]")
        heading = row.xpath(".//h1[1] | .//h2[1] | .//h3[1] | .//h4[1]")

        title_raw = ""
        if heading:
            title_raw = _text(heading[0])
        elif links:
            title_raw = _text(links[0])
        else:
            first_cell = row.xpath(".//td[1]")
            if first_cell:
                title_raw = _text(first_cell[0])

        title = self._normalize_title_v38(title_raw)
        if not self._is_valid_title_v38(title):
            return None

        source_url = page_url
        for link in links:
            href = _resolve_url(link.get("href"), page_url)
            if not href or self._is_non_job_url_v38(href):
                continue
            source_url = href
            if self._is_job_like_url_v38(href):
                break

        row_text = _text(row)
        if not self._title_has_role_signal_v38(title):
            if not (self._is_acronym_title_v38(title) and self._is_job_like_url_v38(source_url)):
                return None

        return {
            "title": title,
            "source_url": source_url,
            "location_raw": self._extract_location_v38(row, title),
            "salary_raw": self._extract_salary_v38(row_text),
            "employment_type": self._extract_employment_type_v38(row_text),
            "description": self._clean_description_v38(row_text),
            "extraction_method": method,
            "extraction_confidence": confidence,
        }

    def _pick_best_candidate_v38(
        self,
        candidates: list[tuple[str, list[dict]]],
        page_url: str,
    ) -> tuple[str, list[dict], float]:
        if not candidates:
            return "", [], 0.0

        scored: list[tuple[str, list[dict], float]] = []
        for label, jobs in candidates:
            prepared = self._prepare_candidate_jobs_v38(jobs, page_url)
            if not prepared:
                continue
            score = self._candidate_score_v38(prepared, page_url)
            scored.append((label, prepared, score))

        if not scored:
            return "", [], 0.0

        best_label, best_jobs, best_score = max(scored, key=lambda item: item[2])

        for label, jobs, score in scored:
            if label == best_label:
                continue
            overlap = self._title_overlap_ratio_v38(jobs, best_jobs)
            if len(jobs) >= len(best_jobs) + 1 and overlap >= 0.58 and score >= best_score - 1.4:
                best_label, best_jobs, best_score = label, jobs, score

        return best_label, best_jobs[:MAX_JOBS_PER_PAGE], best_score

    def _prepare_candidate_jobs_v38(self, jobs: list[dict], page_url: str) -> list[dict]:
        cleaned: list[dict] = []

        for idx, raw in enumerate(jobs):
            title = self._normalize_title_v38(raw.get("title", ""))
            source_url = _resolve_url(raw.get("source_url"), page_url) or page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]

            if self._is_non_job_url_v38(source_url):
                continue
            if not self._is_title_acceptable_v38(title, source_url):
                continue

            desc = self._clean_description_v38(str(raw.get("description") or ""))
            cleaned.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": raw.get("location_raw") or None,
                    "salary_raw": raw.get("salary_raw") or None,
                    "employment_type": raw.get("employment_type") or self._extract_employment_type_v38(desc or ""),
                    "description": desc,
                    "extraction_method": raw.get("extraction_method") or "tier2_v38",
                    "extraction_confidence": raw.get("extraction_confidence", 0.65),
                    "_order": idx,
                }
            )

        deduped = self._dedupe_jobs_v38(cleaned, page_url)
        if not self._is_valid_jobset_v38(deduped, page_url):
            return []
        return deduped

    def _dedupe_jobs_v38(self, jobs: list[dict], page_url: str) -> list[dict]:
        by_key: dict[tuple[str, str], dict] = {}

        for job in jobs:
            title = self._normalize_title_v38(job.get("title", ""))
            url = _resolve_url(job.get("source_url"), page_url) or page_url
            key = (title.lower(), url.lower())
            existing = by_key.get(key)
            if existing is None:
                by_key[key] = job
                continue

            if self._title_quality_score_v38(title) > self._title_quality_score_v38(existing.get("title", "")):
                by_key[key] = job

        deduped = sorted(by_key.values(), key=lambda item: int(item.get("_order", 0)))
        for item in deduped:
            item.pop("_order", None)
        return deduped[:MAX_JOBS_PER_PAGE]

    def _is_valid_jobset_v38(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title_v38(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return False

        role_hits = sum(1 for t in titles if self._title_has_role_signal_v38(t))
        reject_hits = sum(1 for t in titles if _TITLE_REJECT_PATTERN_V38.match(t.lower()))
        evidence_hits = sum(1 for j in jobs if self._job_has_evidence_v38(j, page_url))

        if reject_hits >= max(1, int(len(titles) * 0.25)):
            return False

        if len(titles) >= MIN_JOBS_FOR_SUCCESS:
            if role_hits < max(1, int(len(titles) * 0.6)):
                return False
            if evidence_hits < max(1, int(len(titles) * 0.3)):
                return False
            return True

        # Small sets are accepted only with strong precision.
        if role_hits < len(titles):
            return False
        return evidence_hits >= max(1, len(titles) - 1)

    def _candidate_score_v38(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        titles = [self._normalize_title_v38(j.get("title", "")) for j in jobs]
        role_hits = sum(1 for t in titles if self._title_has_role_signal_v38(t))
        reject_hits = sum(1 for t in titles if _TITLE_REJECT_PATTERN_V38.match(t.lower()))
        detail_hits = sum(1 for j in jobs if self._is_job_like_url_v38(j.get("source_url") or ""))
        evidence_hits = sum(1 for j in jobs if self._job_has_evidence_v38(j, page_url))

        score = len(jobs) * 4.8
        score += role_hits * 2.6
        score += detail_hits * 1.5
        score += evidence_hits * 1.8
        score -= reject_hits * 6.0
        return score

    def _job_has_evidence_v38(self, job: dict, page_url: str) -> bool:
        source_url = (job.get("source_url") or page_url).strip() or page_url
        desc = job.get("description") or ""

        if self._is_job_like_url_v38(source_url):
            return True
        if source_url.rstrip("/") != page_url.rstrip("/") and not self._is_non_job_url_v38(source_url):
            parsed = urlparse(source_url)
            parts = [p for p in (parsed.path or "").split("/") if p]
            if parts:
                leaf = parts[-1].lower()
                if leaf not in {
                    "career", "careers", "jobs", "job", "vacancies", "vacancy",
                    "openings", "join-our-team", "current-vacancies",
                } and len(leaf) >= 4:
                    return True
        if job.get("salary_raw") or job.get("employment_type") or job.get("location_raw"):
            return True
        if _APPLY_EVIDENCE_PATTERN_V38.search(desc or ""):
            return True
        return len((desc or "").strip()) >= 180

    def _normalize_title_v38(self, title: str) -> str:
        value = html_lib.unescape((title or "").strip())
        value = re.sub(r"\s+", " ", value)
        value = value.strip(" \t\r\n-–|:;,.>")
        value = re.sub(r"\s*(?:apply\s+now|apply\s+here|read\s+more|learn\s+more|info\s+lengkap)\s*$", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s*(?:deadline\s*:\s*\S+.*|closing\s+date\s*:\s*\S+.*)$", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+OR\s+.*$", "", value, flags=re.IGNORECASE)
        return value.strip(" \t\r\n-–|:;,")

    def _is_valid_title_v38(self, title: str) -> bool:
        if not title:
            return False

        t = title.strip()
        if len(t) < 4 or len(t) > 180:
            return False
        if not re.search(r"[A-Za-z]", t):
            return False
        if _TITLE_REJECT_PATTERN_V38.match(t.lower()):
            return False
        return True

    def _is_title_acceptable_v38(self, title: str, source_url: str) -> bool:
        if self._is_valid_title_v38(title):
            return True
        return self._is_acronym_title_v38(title) and self._is_job_like_url_v38(source_url)

    def _title_has_role_signal_v38(self, title: str) -> bool:
        if not title:
            return False
        return bool(_ROLE_HINT_PATTERN_V38.search(title) or _title_has_job_noun(title) or self._is_acronym_title_v38(title))

    @staticmethod
    def _is_acronym_title_v38(title: str) -> bool:
        t = (title or "").strip()
        if not _ACRONYM_TITLE_PATTERN_V38.match(t):
            return False
        return t.lower() not in {"home", "menu", "faq", "apply"}

    def _is_job_like_url_v38(self, url: str) -> bool:
        value = (url or "").strip()
        if not value:
            return False
        if self._is_non_job_url_v38(value):
            return False
        return bool(_DETAILISH_URL_PATTERN_V38.search(value))

    @staticmethod
    def _is_non_job_url_v38(url: str) -> bool:
        value = (url or "").strip().lower()
        if not value:
            return False
        if value.startswith("mailto:") or value.startswith("tel:"):
            return True
        return bool(_NON_JOB_URL_PATTERN_V38.search(value))

    def _title_quality_score_v38(self, title: str) -> float:
        t = self._normalize_title_v38(title)
        if not t:
            return -10.0
        score = 0.0
        score += 2.0 if self._title_has_role_signal_v38(t) else 0.0
        score += 1.0 if self._is_valid_title_v38(t) else 0.0
        score -= max(0.0, (len(t) - 90) / 80.0)
        return score

    def _title_overlap_ratio_v38(self, a_jobs: list[dict], b_jobs: list[dict]) -> float:
        a_titles = {self._normalize_title_v38(j.get("title", "")).lower() for j in a_jobs if j.get("title")}
        b_titles = {self._normalize_title_v38(j.get("title", "")).lower() for j in b_jobs if j.get("title")}
        if not a_titles or not b_titles:
            return 0.0
        return len(a_titles & b_titles) / max(1, min(len(a_titles), len(b_titles)))

    def _page_listing_score_v38(self, page_url: str, html_body: str) -> float:
        if not html_body or len(html_body) < 200:
            return -20.0
        lower = html_body.lower()
        score = 0.0

        score += min(lower.count("apply now"), 10)
        score += min(lower.count("job"), 10)
        score += min(lower.count("career"), 6)

        if _LISTING_URL_PATTERN_V38.search(page_url or ""):
            score += 3.0
        if "portal.na" in (page_url or "").lower():
            score += 5.0

        root = _parse_html(html_body)
        if root is not None:
            listing_links = 0
            role_links = 0
            for a_el in root.xpath("//a[@href]"):
                href = (a_el.get("href") or "").strip()
                text = self._safe_text_v38(a_el)
                if _LISTING_URL_PATTERN_V38.search(href) or _LISTING_LINK_TEXT_PATTERN_V38.search(text):
                    listing_links += 1
                if _ROLE_HINT_PATTERN_V38.search(text):
                    role_links += 1
            score += min(listing_links * 1.3, 18.0)
            score += min(role_links * 2.0, 18.0)

        return score

    @staticmethod
    def _looks_non_html_payload_v38(body: Optional[str]) -> bool:
        if not body:
            return True
        sample = body[:900].lstrip()
        if sample.startswith("%PDF-"):
            return True
        low = sample.lower()
        if (low.startswith("{") or low.startswith("[")) and "<html" not in low[:300]:
            return True
        return False

    @staticmethod
    def _is_related_host_v38(parent_url: str, child_url: str) -> bool:
        p = urlparse(parent_url).hostname or ""
        c = urlparse(child_url).hostname or ""
        if not p or not c:
            return False
        if p == c:
            return True

        p_parts = p.rsplit(".", 2)
        c_parts = c.rsplit(".", 2)
        p_base = ".".join(p_parts[-2:]) if len(p_parts) >= 2 else p
        c_base = ".".join(c_parts[-2:]) if len(c_parts) >= 2 else c
        return p_base == c_base

    @staticmethod
    def _is_rejected_listing_link_v38(href: str, text: str) -> bool:
        href_l = (href or "").lower()
        text_l = (text or "").lower()
        if _NON_JOB_URL_PATTERN_V38.search(href_l):
            return True
        if re.search(r"\b(?:our\s+culture|our\s+values|talent\s+stories?|our\s+ecosystem)\b", text_l):
            return True
        return False

    @staticmethod
    def _safe_text_v38(el: etree._Element) -> str:
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

    def _clean_description_v38(self, description: str) -> Optional[str]:
        text = " ".join((description or "").split())
        if not text:
            return None
        cut = re.search(r"\b(?:how\s+to\s+apply|cara\s+melamar|application\s+process)\b", text, re.IGNORECASE)
        if cut:
            text = text[: cut.start()].strip()
        if len(text) < 45:
            return None
        return text[:5000]

    def _extract_location_v38(self, row: etree._Element, title: str) -> Optional[str]:
        for el in row.iter():
            if not isinstance(el.tag, str):
                continue
            cls = _get_el_classes(el)
            if "location" in cls or "map-marker" in cls:
                loc = " ".join(_text(el).split())
                if loc and loc != title and 2 < len(loc) < 160:
                    return loc

        row_text = _text(row)
        match = _AU_LOCATIONS.search(row_text) if row_text else None
        if match:
            return match.group(0)[:120]
        return None

    @staticmethod
    def _extract_salary_v38(text: str) -> Optional[str]:
        if not text:
            return None
        match = _SALARY_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:120]
        return None

    @staticmethod
    def _extract_employment_type_v38(text: str) -> Optional[str]:
        if not text:
            return None
        match = _JOB_TYPE_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:80]
        return None
