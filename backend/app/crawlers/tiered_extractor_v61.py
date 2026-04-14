"""
Tiered Extraction Engine v6.1 — Targeted improvements over v6.0.

Changes from v6.0:
  1. Title validation hardening: filter/dropdown labels, marketing CTAs,
     no-vacancy messages, section headings, short generic titles.
  2. Detail page enrichment for ALL paths (not just non-parent).
  3. Elementor/accordion job detection fallback.
  4. Fix Dayforce slug extraction (use subdomain, not URL path).
  5. Fix GrowHire slug extraction (use original URL subdomain).
  6. Timeout budget management with per-phase asyncio.wait_for().
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional
from urllib.parse import urlparse

from lxml import etree

from app.crawlers.tiered_extractor_v60 import (
    TieredExtractorV60,
    _detect_ats_platform,
    _APPLY_CONTEXT,
    _REJECT_TITLE_PATTERN,
    _TITLE_HINT_PATTERN,
)
from app.crawlers.tiered_extractor_v16 import _title_has_job_noun
from app.crawlers.tiered_extractor import (
    _parse_html,
    _text,
    _resolve_url,
    MAX_JOBS_PER_PAGE,
    MIN_JOBS_FOR_SUCCESS,
    _AU_LOCATIONS,
    _JOB_TYPE_PATTERN,
)

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# v6.1 additional rejection patterns
# ---------------------------------------------------------------------------

_FILTER_LABEL_PATTERN = re.compile(
    r"^(?:all\s+locations?|all\s+departments?|all\s+categor(?:y|ies)|"
    r"all\s+jobs?|filter\s+by)$",
    re.IGNORECASE,
)

_MARKETING_CTA_PATTERN = re.compile(
    r"^(?:click\s+here|recruiter\s+success\s+kit|your\s+recruitment\s+journey|"
    r"download)$",
    re.IGNORECASE,
)

_NO_VACANCY_PATTERN = re.compile(
    r"^(?:no\s+vacanc(?:y|ies)|no\s+jobs?\s+found|no\s+positions?\s+available|"
    r"no\s+openings?)$",
    re.IGNORECASE,
)

_SECTION_HEADING_PATTERN = re.compile(
    r"^(?:fresh\s+jobs?|why\s+join\s+us|working\s+at\b.*|life\s+at\b.*|our\s+culture)$",
    re.IGNORECASE,
)

# Title normalization: trailing arrows/read-more
_TRAILING_ARROW = re.compile(
    r"\s*(?:More\s+Details?\s*)?[→»]$|\s*Read\s+More\s*$",
    re.IGNORECASE,
)

# Trailing bullet metadata: "• City • Full Time • 2024-01-01"
_TRAILING_BULLET_META = re.compile(
    r"\s*[•·|]\s+\S+(?:\s*[•·|]\s+\S+){1,}$",
)

# Appended location after country/region patterns
_TRAILING_LOCATION = re.compile(
    r",\s*(?:Philippines|Indonesia|Malaysia|Singapore|Australia|"
    r"New\s+Zealand|India|Thailand|Vietnam|Japan|South\s+Korea|"
    r"Netherlands|Germany|France|Spain|Italy|UK|United\s+Kingdom|"
    r"United\s+States|Canada|Brazil|Mexico|South\s+Africa|"
    r"NL|DE|FR|ES|IT|US|AU|NZ|IN|SG|MY|PH|ID|TH|VN|JP|KR|BR|MX|ZA|CA|GB"
    r")(?:,\s*[\w\s]+)?$",
    re.IGNORECASE,
)


class TieredExtractorV61(TieredExtractorV60):
    """v6.1 extractor: hardened titles, universal enrichment, Elementor fallback,
    Dayforce/GrowHire fixes, timeout budget management."""

    # ==================================================================
    # Change 6: Overridden extract() with per-phase timeout budgets
    # Also Change 2: enrichment for ALL paths
    # Also Change 3: Elementor fallback added to candidates
    # ==================================================================

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        ats_platform = _detect_ats_platform(url, working_html)

        # Phase 1: Parent v1.6 heuristic extraction (20s timeout, down from 24s)
        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(
                super(TieredExtractorV60, self).extract(career_page, company, working_html),
                timeout=20.0,
            )
        except asyncio.TimeoutError:
            logger.warning("v6.1 parent extractor timeout for %s", url)
        except Exception:
            logger.exception("v6.1 parent extractor failed for %s", url)
        parent_jobs = self._dedupe(parent_jobs or [], url)

        # Phase 2: Structured data extraction (always runs — fast, no timeout needed)
        root = _parse_html(working_html)
        candidates: list[tuple[str, list[dict]]] = []
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        structured_jobs = self._extract_structured_jobs(working_html, url)
        if structured_jobs:
            candidates.append(("structured_jsonld", structured_jobs))

        script_jobs = self._extract_jobs_from_state_scripts(working_html, url)
        if script_jobs:
            candidates.append(("state_script", script_jobs))

        # Phase 3: Dedicated ATS extractors (15s timeout)
        if ats_platform:
            try:
                ats_jobs = await asyncio.wait_for(
                    self._extract_ats_specific(ats_platform, url, working_html),
                    timeout=15.0,
                )
            except asyncio.TimeoutError:
                logger.warning("v6.1 ATS %s timeout for %s", ats_platform, url)
                ats_jobs = []
            except Exception:
                logger.exception("v6.1 ATS %s failed for %s", ats_platform, url)
                ats_jobs = []
            if ats_jobs:
                candidates.append((f"ats_{ats_platform}", ats_jobs))

        # Phase 4: DOM fallbacks (only if we don't have good results yet)
        best_so_far = max((len(jobs) for _, jobs in candidates), default=0)
        if best_so_far < 3 and root is not None:
            link_jobs = self._extract_from_job_links(root, url)
            if link_jobs:
                candidates.append(("job_links", link_jobs))

            accordion_jobs = self._extract_from_accordion_sections(root, url)
            if accordion_jobs:
                candidates.append(("accordion", accordion_jobs))

            # Change 3: Elementor/accordion fallback
            elementor_jobs = self._extract_from_elementor_sections(root, url)
            if elementor_jobs:
                candidates.append(("elementor", elementor_jobs))

            heading_jobs = self._extract_from_heading_rows(root, url)
            if heading_jobs:
                candidates.append(("heading_rows", heading_jobs))

            row_jobs = self._extract_from_repeating_rows(root, url)
            if row_jobs:
                candidates.append(("repeating_rows", row_jobs))

        # Pick best candidate set
        best_label, best_jobs = self._pick_best_jobset(candidates, url)
        if not best_jobs:
            return []

        # Change 2: Enrich from detail pages for ALL paths when jobs are
        # missing location_raw or description (18s timeout, up from 12s)
        needs_enrichment = any(
            (not j.get("location_raw") or not j.get("description"))
            for j in best_jobs
        )
        if (
            needs_enrichment
            and len(best_jobs) >= MIN_JOBS_FOR_SUCCESS
            and any(self._is_job_like_url(j.get("source_url") or "") for j in best_jobs)
        ):
            try:
                best_jobs = await asyncio.wait_for(
                    self._enrich_from_detail_pages(best_jobs),
                    timeout=18.0,
                )
            except asyncio.TimeoutError:
                logger.warning("v6.1 enrichment timeout for %s", url)
            except Exception:
                logger.exception("v6.1 enrichment failed for %s", url)
            best_jobs = self._dedupe(best_jobs, url)

        return best_jobs[:MAX_JOBS_PER_PAGE]

    # ==================================================================
    # Change 1: Title validation hardening
    # ==================================================================

    def _is_valid_title_v61(self, title: str) -> bool:
        """Extended title validation with v6.1 rejection patterns."""
        if not self._is_valid_title_v60(title):
            return False

        t = title.strip()

        # Filter/dropdown labels
        if _FILTER_LABEL_PATTERN.match(t):
            return False
        # Marketing CTAs
        if _MARKETING_CTA_PATTERN.match(t):
            return False
        # No-vacancy messages
        if _NO_VACANCY_PATTERN.match(t):
            return False
        # Section headings
        if _SECTION_HEADING_PATTERN.match(t):
            return False

        # Very short generic titles (1-2 words, no job noun, no job-like signal)
        words = t.split()
        if len(words) <= 2 and not _title_has_job_noun(t) and not self._title_has_job_signal(t):
            return False

        return True

    def _is_valid_title_v60(self, title: str) -> bool:
        """Override v60's validation to use v61's stricter version."""
        # Call grandparent's v60 validation first
        if not super()._is_valid_title_v60(title):
            return False
        # Then apply v61 extra checks
        t = title.strip()
        if _FILTER_LABEL_PATTERN.match(t):
            return False
        if _MARKETING_CTA_PATTERN.match(t):
            return False
        if _NO_VACANCY_PATTERN.match(t):
            return False
        if _SECTION_HEADING_PATTERN.match(t):
            return False
        words = t.split()
        if len(words) <= 2 and not _title_has_job_noun(t) and not self._title_has_job_signal(t):
            return False
        return True

    def _normalize_title(self, title: str) -> str:
        """Extended normalization: strip trailing arrows, bullet metadata, appended locations."""
        t = super()._normalize_title(title)
        if not t:
            return t

        # Strip trailing "More Details →" / "→" / "»" / "Read More"
        t = _TRAILING_ARROW.sub("", t).strip()

        # Strip trailing bullet metadata: "• City • Full Time • 2024-01-01"
        t = _TRAILING_BULLET_META.sub("", t).strip()

        # Strip appended location after known country/region patterns
        t = _TRAILING_LOCATION.sub("", t).strip()

        return t

    # ==================================================================
    # Change 3: Elementor/Accordion Job Detection
    # ==================================================================

    def _extract_from_elementor_sections(self, root: etree._Element, page_url: str) -> list[dict]:
        """Extract jobs from Elementor inner-column containers, accordion/toggle titles."""
        jobs: list[dict] = []

        # Strategy 1: Elementor tab/toggle titles
        tab_titles = root.xpath(
            "//*[contains(@class,'elementor-tab-title') or "
            "contains(@class,'elementor-toggle-title')]"
        )
        for el in tab_titles[:100]:
            title = self._normalize_title(_text(el))
            if not self._is_valid_title_v60(title):
                continue
            if not _title_has_job_noun(title):
                continue

            # Look for apply context or responsibilities in the associated content
            parent = el.getparent()
            if parent is not None:
                parent = parent.getparent()
            content_text = _text(parent)[:2000] if parent is not None else ""
            has_apply = bool(_APPLY_CONTEXT.search(content_text))
            has_duties = bool(re.search(
                r"responsibilit|requirement|qualificat|duties|experience|apply",
                content_text, re.IGNORECASE,
            ))
            if not has_apply and not has_duties:
                continue

            link_el = el.xpath(".//a[@href]")
            if not link_el and parent is not None:
                link_el = parent.xpath(".//a[@href]")
            href = link_el[0].get("href") if link_el else None
            source_url = _resolve_url(href, page_url) if href else page_url

            jobs.append({
                "title": title,
                "source_url": source_url or page_url,
                "location_raw": None,
                "salary_raw": None,
                "employment_type": None,
                "description": content_text[:5000] if len(content_text) > 80 else None,
                "extraction_method": "tier2_elementor_tab",
                "extraction_confidence": 0.68,
            })

        # Strategy 2: Elementor inner-column containers with headings
        inner_cols = root.xpath(
            "//*[contains(@class,'elementor-inner-column') or "
            "contains(@class,'elementor-column-wrap')]"
        )
        for col in inner_cols[:100]:
            headings = col.xpath(".//h2 | .//h3 | .//h4")
            if not headings:
                continue

            for h in headings:
                title = self._normalize_title(_text(h))
                if not self._is_valid_title_v60(title):
                    continue
                if not _title_has_job_noun(title):
                    continue

                col_text = _text(col)[:2000]
                has_apply = bool(_APPLY_CONTEXT.search(col_text))
                has_duties = bool(re.search(
                    r"responsibilit|requirement|qualificat|duties|experience",
                    col_text, re.IGNORECASE,
                ))
                # Also check for "Apply" button
                has_apply_btn = bool(col.xpath(
                    ".//a[contains(translate(., 'APPLY', 'apply'), 'apply')]|"
                    ".//button[contains(translate(., 'APPLY', 'apply'), 'apply')]"
                ))

                if not has_apply and not has_duties and not has_apply_btn:
                    continue

                link_el = h.xpath(".//a[@href]")
                if not link_el:
                    link_el = col.xpath(".//a[@href]")
                href = link_el[0].get("href") if link_el else None
                source_url = _resolve_url(href, page_url) if href else page_url

                jobs.append({
                    "title": title,
                    "source_url": source_url or page_url,
                    "location_raw": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": col_text[:5000] if len(col_text) > 80 else None,
                    "extraction_method": "tier2_elementor_col",
                    "extraction_confidence": 0.65,
                })

        return self._dedupe(jobs, page_url)

    # ==================================================================
    # Change 4: Fix Dayforce handler — use subdomain, not URL path
    # ==================================================================

    async def _extract_dayforce(self, url: str, html: str) -> list[dict]:
        """Dayforce HCM: fix slug extraction to use subdomain prefix."""
        parsed = urlparse(url)
        host = parsed.netloc.lower()

        # Bug fix: Dayforce uses the subdomain prefix as slug
        # e.g. globalaus242.dayforcehcm.com → slug is "globalaus242"
        slug = None
        if "dayforcehcm.com" in host:
            slug = host.split(".")[0]
        elif "candidateportal" in url.lower():
            # Fallback: try extracting from URL path
            m = re.search(r"/CandidatePortal/(?:[a-z]{2}-[A-Z]{2}/)?([^/?#]+)", url, re.IGNORECASE)
            if m:
                slug = m.group(1)

        if not slug:
            return []

        base = f"{parsed.scheme}://{parsed.netloc}"
        jobs: list[dict] = []

        # Try Search API (POST)
        search_urls = [
            f"{base}/Api/CandidatePortal/{slug}/Search",
            f"{base}/api/CandidatePortal/{slug}/Search",
        ]
        try:
            async with httpx.AsyncClient(
                timeout=8, follow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
            ) as client:
                for api_url in search_urls:
                    try:
                        resp = await client.post(api_url, json={"searchText": "", "pageSize": 100, "pageNumber": 1})
                        if resp.status_code != 200:
                            resp = await client.get(api_url + "?pageSize=100&pageNumber=1")
                        if resp.status_code != 200:
                            continue
                        data = resp.json()
                    except Exception:
                        continue

                    items = data if isinstance(data, list) else data.get("jobs", data.get("results", data.get("Items", [])))
                    if not isinstance(items, list):
                        continue
                    for item in items[:MAX_JOBS_PER_PAGE]:
                        title = str(item.get("Title") or item.get("title") or item.get("JobTitle") or "").strip()
                        if not self._is_valid_title_v60(title):
                            continue
                        job_id = str(item.get("Id") or item.get("id") or item.get("JobId") or "").strip()
                        detail_url = f"{base}/CandidatePortal/{slug}/JobDetail/{job_id}" if job_id else url
                        location = str(item.get("Location") or item.get("location") or item.get("City") or "").strip() or None
                        emp_type = str(item.get("EmploymentType") or item.get("JobType") or "").strip() or None
                        desc = str(item.get("Description") or item.get("ShortDescription") or "").strip()
                        if desc and "<" in desc:
                            p = _parse_html(desc)
                            if p is not None:
                                desc = _text(p)
                        jobs.append({
                            "title": title,
                            "source_url": detail_url,
                            "location_raw": location,
                            "salary_raw": None,
                            "employment_type": emp_type,
                            "description": desc[:5000] if desc else None,
                            "extraction_method": "ats_dayforce_api",
                            "extraction_confidence": 0.90,
                        })
                    if jobs:
                        break
        except Exception:
            logger.debug("v6.1 Dayforce API failed for %s", url)

        # Fall back to HTML card parsing (same as parent)
        if not jobs and html:
            root = _parse_html(html)
            if root is not None:
                cards = root.xpath(
                    "//*[contains(@class,'job-card') or contains(@class,'job-listing') or "
                    "contains(@class,'job-item') or contains(@class,'search-result')]"
                )
                if not cards:
                    cards = root.xpath("//div[.//a[contains(@href,'JobDetail') or contains(@href,'jobdetail')]]")
                for card in cards[:MAX_JOBS_PER_PAGE]:
                    heading = card.xpath(".//h1|.//h2|.//h3|.//h4|.//*[contains(@class,'title')]")
                    title = self._normalize_title(_text(heading[0])) if heading else ""
                    if not self._is_valid_title_v60(title):
                        continue
                    link = card.xpath(".//a[@href]")
                    href = link[0].get("href") if link else None
                    source_url = _resolve_url(href, url) if href else url
                    card_text = _text(card)
                    jobs.append({
                        "title": title,
                        "source_url": source_url or url,
                        "location_raw": self._extract_location(card_text),
                        "salary_raw": self._extract_salary(card_text),
                        "employment_type": self._extract_type(card_text),
                        "description": card_text[:5000] if len(card_text) > 60 else None,
                        "extraction_method": "ats_dayforce_html",
                        "extraction_confidence": 0.80,
                    })

        return self._dedupe(jobs, url)

    # ==================================================================
    # Change 5: Fix GrowHire handler — use original URL subdomain
    # ==================================================================

    async def _extract_growhire(self, url: str, html: str) -> list[dict]:
        """GrowHire: fix discovery redirect issue by using original URL subdomain."""
        parsed = urlparse(url)
        host = parsed.netloc.lower()

        # Bug fix: extract company slug from original URL subdomain
        # e.g. aviato.jobs.growhire.com → slug is "aviato"
        slug = None
        if "growhire.com" in host:
            parts = host.split(".")
            # Handle both "company.jobs.growhire.com" and "company.growhire.com"
            if len(parts) >= 3 and parts[0] not in ("www", "jobs", "api"):
                slug = parts[0]
            elif len(parts) >= 4 and parts[0] == "jobs":
                # Shouldn't happen, but just in case
                slug = parts[1] if parts[1] not in ("www",) else None

        if not slug:
            return []

        # Construct API/page URLs from the slug
        growhire_base = f"https://{slug}.jobs.growhire.com"
        jobs: list[dict] = []

        # Try API endpoint with correct base
        api_urls = [
            f"{growhire_base}/api/jobs",
            f"https://jobs.growhire.com/{slug}/api/jobs",
            f"https://api.growhire.com/v1/companies/{slug}/jobs",
        ]

        try:
            async with httpx.AsyncClient(
                timeout=8, follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            ) as client:
                for api_url in api_urls:
                    try:
                        resp = await client.get(api_url)
                        if resp.status_code != 200:
                            continue
                        data = resp.json()
                    except Exception:
                        continue

                    items = data if isinstance(data, list) else data.get("jobs", data.get("data", data.get("results", [])))
                    if not isinstance(items, list):
                        continue
                    for item in items[:MAX_JOBS_PER_PAGE]:
                        title = str(item.get("title") or item.get("name") or item.get("jobTitle") or "").strip()
                        if not self._is_valid_title_v60(title):
                            continue
                        job_id = str(item.get("id") or item.get("slug") or "").strip()
                        detail_url = str(item.get("url") or item.get("applyUrl") or "").strip()
                        if not detail_url and job_id:
                            detail_url = f"{growhire_base}/job/{job_id}"
                        detail_url = _resolve_url(detail_url, growhire_base) if detail_url else growhire_base
                        location = str(item.get("location") or item.get("city") or "").strip() or None
                        emp_type = str(item.get("employmentType") or item.get("type") or item.get("jobType") or "").strip() or None
                        desc = str(item.get("description") or item.get("summary") or "").strip()
                        if desc and "<" in desc:
                            p = _parse_html(desc)
                            if p is not None:
                                desc = _text(p)
                        jobs.append({
                            "title": title,
                            "source_url": detail_url,
                            "location_raw": location,
                            "salary_raw": None,
                            "employment_type": emp_type,
                            "description": desc[:5000] if desc else None,
                            "extraction_method": "ats_growhire_api",
                            "extraction_confidence": 0.91,
                        })
                    if jobs:
                        break
        except Exception:
            logger.debug("v6.1 GrowHire API failed for %s (slug=%s)", url, slug)

        # Fall back to HTML parsing using provided html
        if not jobs and html:
            root = _parse_html(html)
            if root is not None:
                cards = root.xpath(
                    "//*[contains(@class,'job-card') or contains(@class,'job-listing') or "
                    "contains(@class,'job-item') or contains(@class,'position-card')]"
                )
                if not cards:
                    cards = root.xpath("//div[.//a[contains(@href,'/job/')]]|//li[.//a[contains(@href,'/job/')]]")
                for card in cards[:MAX_JOBS_PER_PAGE]:
                    heading = card.xpath(".//h1|.//h2|.//h3|.//h4|.//*[contains(@class,'title')]")
                    title = self._normalize_title(_text(heading[0])) if heading else ""
                    if not self._is_valid_title_v60(title):
                        continue
                    link = card.xpath(".//a[@href]")
                    href = link[0].get("href") if link else None
                    source_url = _resolve_url(href, url) if href else url
                    card_text = _text(card)
                    loc_el = card.xpath(".//*[contains(@class,'location')]")
                    location = _text(loc_el[0]).strip() if loc_el else self._extract_location(card_text)
                    jobs.append({
                        "title": title,
                        "source_url": source_url or url,
                        "location_raw": location,
                        "salary_raw": self._extract_salary(card_text),
                        "employment_type": self._extract_type(card_text),
                        "description": card_text[:5000] if len(card_text) > 60 else None,
                        "extraction_method": "ats_growhire_html",
                        "extraction_confidence": 0.82,
                    })

        return self._dedupe(jobs, url)
