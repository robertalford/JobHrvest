"""
Tiered Extraction Engine v5.3 — Consolidated "clean slate" extractor.

Built from v2.6 (82% accuracy, the all-time best) with targeted cherry-picks:
  - Boundary-aware boilerplate title validation (from v2.9)
  - Coverage-first candidate arbitration (from v3.0)
  - Dedicated ATS platform extractors replacing heuristic fallback chains:
    * Oracle CandidateExperience (requisitions API)
    * Greenhouse (embed board API)
    * Salesforce Recruit (fRecruit table parsing)
    * MartianLogic/MyRecruitmentPlus (streamlined API probing)

Design principles:
  - v1.6 heuristic is the primary extraction path (proven reliable)
  - Structured data (JSON-LD, embedded state) is a strong secondary signal
  - ATS-specific extractors are isolated modules, not tangled fallback chains
  - Candidate arbitration prefers quality over volume
  - 60s phase timeout budget: 24s parent + 12s ATS probing + 12s enrichment + 12s buffer
"""

from __future__ import annotations

import asyncio
import html as html_mod
import json
import logging
import re
from collections import defaultdict
from typing import Any, Optional
from urllib.parse import parse_qsl, urljoin, urlparse

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

# ---------------------------------------------------------------------------
# Patterns (from v2.6, with boundary-aware fix from v2.9)
# ---------------------------------------------------------------------------

_TITLE_HINT_PATTERN = re.compile(
    r"\b(?:job|jobs|career|careers|vacanc(?:y|ies)|opening|openings|"
    r"position|positions|role|roles|internship|manager|engineer|developer|"
    r"officer|specialist|assistant|analyst|consultant|coordinator|executive|"
    r"technician|designer|administrator|accountant|recruit(?:er|ment)?|"
    r"director|chef|nurse|teacher|operator|supervisor|influencer|akuntan|"
    r"fotografer|videografer|psikologi|model(?:er|ler)?|penganalisis|"
    r"asisten|customer\s+service|"
    r"latihan\s+industri|"
    r"gerente|director|vacantes?|empleo|trabajo|lowongan|karir|karier|"
    r"kerjaya|jawatan)\b",
    re.IGNORECASE,
)

_REJECT_TITLE_PATTERN = re.compile(
    r"^(?:"
    r"my\s+applications?|my\s+forms?|my\s+emails?|my\s+tests?|my\s+interviews?|"
    r"job\s+alerts?|jobs?\s+list|job\s+search|saved\s+jobs?|manage\s+applications?|"
    r"start\s+new\s+application|access\s+existing\s+application|preview\s+application\s+form|"
    r"apply\s+now|apply\s+here|read\s+more|learn\s+more|show\s+more|"
    r"entries\s+feed|comments\s+feed|rss|feed|"
    r"about\s+us|contact\s+us|privacy|terms|help|login|register|"
    r"job\s+name|closing\s+date|posted\s+date|job\s+ref|"
    r"benefits|how\s+to\s+apply|current\s+opportunities|join\s+us(?:\s+and.*)?|"
    r"internship\s+details|job\s+type|job\s+card\s+style|job\s+title|"
    r"no\s+jobs?\s+found(?:\s+text)?|jobs?\s+vacancy|"
    r"lowongan\s+kerja(?:\s+\w+)?|model\s+incubator"
    r")$",
    re.IGNORECASE,
)

_GENERIC_LISTING_LABEL = re.compile(
    r"^(?:jobs?|careers?|vacancies|vacantes?|job\s+openings?|open\s+positions?|"
    r"join\s+our\s+team|open\s+roles?|all\s+jobs?|current\s+vacancies|"
    r"jobs?\s+vacancy|lowongan(?:\s+kerja(?:\s+\w+)?)?)$",
    re.IGNORECASE,
)

_JOB_URL_HINT = re.compile(
    r"(?:/job|/jobs|/career|/careers|/position|/positions|/vacanc|"
    r"/opening|/openings|/requisition|/requisitions|"
    r"event=jobs\.|jobid=|portal\.na|candidateportal|/apply|/lowongan|/karir|/kerjaya|"
    r"/p/[a-z0-9_-]+|applicationform|job-application-form|embed-jobs|"
    # v3.5: query-style ATS detail URLs
    r"jobdetails|ajid=|jobAdId=|adId=|vacancyId=)",
    re.IGNORECASE,
)

_NON_JOB_URL = re.compile(
    r"(?:/privacy|/terms|/cookie|/news|/blog|/about|/contact|/investor|"
    r"/help|/login|/logout|/register|/account|/feed(?:/|$)|/rss(?:/|$)|"
    r"event=jobs\.view(?:history|myforms|myemails|mytests|myinterviews)|"
    r"event=help\.|event=reg\.|/comments/feed(?:/|$)|/wp-login(?:\.php)?|"
    r"/wp-json/oembed|linkedin\.com|facebook\.com|instagram\.com|twitter\.com|x\.com|youtube\.com)",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN = re.compile(
    r"job|position|vacanc|opening|requisition|career|posting|listing|accordion",
    re.IGNORECASE,
)

_SCRIPT_ASSIGNMENT = re.compile(
    r"(?:window\.[A-Za-z0-9_$.]+\s*=\s*|var\s+[A-Za-z0-9_$]+\s*=\s*)(\{.*\}|\[.*\])\s*;?",
    re.DOTALL,
)

_APPLY_CONTEXT = re.compile(
    r"(?:apply|application|mailto:|job\s+description|closing\s+date|salary|"
    r"full\s*time|part\s*time|contract|permanent|temporary|remote|hybrid|"
    r"info\s+lengkap|more\s+details?)",
    re.IGNORECASE,
)

_CATEGORY_TITLE = re.compile(
    r"^(?:all\s+jobs?|jobs?\s+by|browse\s+jobs?|view\s+jobs?|department[s]?|locations?)$",
    re.IGNORECASE,
)

_CORPORATE_TITLE = re.compile(
    r"^(?:home|about|contact|company|our\s+company|our\s+culture|our\s+values|blog|news|events?)$",
    re.IGNORECASE,
)

_PHONE_TITLE = re.compile(r"^(?:\+?\d[\d\s().-]{6,}|\d{2,4}\s?\d{3,5}\s?\d{3,5})$")

_COMPANY_CAREER_LABEL = re.compile(
    r"^[a-z0-9&.,'() -]{2,60}\s+careers?$",
    re.IGNORECASE,
)
_CTA_TITLE_PATTERN = re.compile(
    r"^(?:submit\s+a\s+vacancy|register\s+a\s+profile|log\s+in\s+to\s+your\s+profile|"
    r"search\s+jobs|view\s+our\s+listed\s+job\s+opportunities|how\s+.*\s+serves?\s+job\s+seekers?)$",
    re.IGNORECASE,
)
_MARKETING_COPY_PATTERN = re.compile(
    r"\b(?:recruitment\s+experts?|ensure\s+your|advance\s+your\s+career|"
    r"tell\s+us\s+about|job\s+seeker\s+profile|thrives?)\b",
    re.IGNORECASE,
)

# v2.9 cherry-pick: boundary-aware boilerplate substring check
# Instead of raw substring matching (which causes "design intern" → "sign in" collision),
# use word-boundary-aware patterns for the risky boilerplate terms.
_INLINE_BOILERPLATE = re.compile(
    r"(?<![a-z])(?:sign\s*in|log\s*in|sign\s*up|log\s*out|my\s+account|"
    r"cookie\s+policy|privacy\s+policy|terms\s+of\s+(?:service|use))(?![a-z])",
    re.IGNORECASE,
)

_MARTIAN_CLIENT = re.compile(r"/([a-z0-9-]{3,})/?$", re.IGNORECASE)
_APPLYFLOW_CONFIG = re.compile(
    r"var\s+afConfig\s*=\s*(\{.*?\})\s*;",
    re.IGNORECASE | re.DOTALL,
)
_APPLYFLOW_ROW_MARKER = re.compile(
    r"(?:af-search-result-item|af-job-title|applyflow|jobview/)",
    re.IGNORECASE,
)
_HUB_LINK_TEXT = re.compile(
    r"\b(?:job\s+openings?|search\s+jobs?|current\s+jobs?|all\s+jobs?|view\s+jobs?|"
    r"join\s+our\s+team|open\s+roles?|lowongan|kerjaya|karir|loker)\b",
    re.IGNORECASE,
)
_HUB_LINK_HREF = re.compile(
    r"/(?:jobs?|careers?|career|job-openings?|openings?|vacanc(?:y|ies)|join-our-team|"
    r"lowongan|kerjaya|karir|loker)(?:/|$|[?#])",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# ATS platform detection
# ---------------------------------------------------------------------------

def _detect_ats_platform(url: str, html: str) -> Optional[str]:
    """Detect which ATS platform a page belongs to."""
    lower_url = url.lower()
    lower_html = (html or "")[:5000].lower()

    if "greenhouse.io" in lower_url or "boards.greenhouse.io" in lower_url:
        return "greenhouse"
    if "oraclecloud.com" in lower_url or "candidateexperience" in lower_url:
        return "oracle_cx"
    if "salesforce-sites.com" in lower_url or "frecruit__applyjob" in lower_html:
        return "salesforce"
    if ("myrecruitmentplus" in lower_html or "martianlogic" in lower_html
            or "clientcode" in lower_html):
        if "__next_data__" in lower_html:
            return "martian"
    if "applyflow" in lower_html or "seeker.applyflow.com" in lower_html or "afconfig" in lower_html:
        return "applyflow"
    if "workday" in lower_url or "myworkdayjobs" in lower_url:
        return "workday"
    if "lever.co" in lower_url:
        return "lever"

    return None


class TieredExtractorV53(TieredExtractorV16):
    """v5.3 consolidated extractor: v1.6 primary + structured fallbacks + dedicated ATS extractors."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        # Detect ATS platform early — if we have a dedicated handler, use it first
        ats_platform = _detect_ats_platform(url, working_html)

        # Phase 1: Parent v1.6 heuristic extraction (24s timeout)
        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(
                super().extract(career_page, company, working_html),
                timeout=24.0,
            )
        except asyncio.TimeoutError:
            logger.warning("v5.3 parent extractor timeout for %s", url)
        except Exception:
            logger.exception("v5.3 parent extractor failed for %s", url)
        parent_jobs = self._dedupe(parent_jobs or [], url)

        # Phase 2: Structured and static extraction (always runs — fast)
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

        if root is not None:
            static_candidates = [
                ("elementor_cards", self._extract_elementor_cards(root, url)),
                ("heading_rows", self._extract_from_heading_rows(root, url)),
                ("accordion", self._extract_from_accordion_sections(root, url)),
                ("hidden_inputs", self._extract_from_hidden_input_titles(root, url)),
                ("repeating_rows", self._extract_from_repeating_rows(root, url)),
                ("job_links", self._extract_from_job_links(root, url)),
                ("applyflow_rows", self._extract_applyflow_rows(root, url)),
            ]
            for label, jobs in static_candidates:
                if jobs:
                    candidates.append((label, jobs))

        # Phase 3: Dedicated ATS extractors (only if platform detected)
        if ats_platform:
            ats_jobs = await self._extract_ats_specific(ats_platform, url, working_html)
            if ats_jobs:
                candidates.append((f"ats_{ats_platform}", ats_jobs))

        # Pick best candidate set, then traverse listing hubs if coverage is low.
        best_label, best_jobs = self._pick_best_jobset(candidates, url)
        if len(best_jobs) <= 3:
            hub_jobs = await self._follow_listing_hub_links(url, working_html)
            if hub_jobs:
                candidates.append(("hub_followup", hub_jobs))
                best_label, best_jobs = self._pick_best_jobset(candidates, url)

        if not best_jobs:
            return []

        # Enrich from detail pages (12s timeout)
        if (
            best_label != "parent_v16"
            and len(best_jobs) >= MIN_JOBS_FOR_SUCCESS
            and any(self._is_job_like_url(j.get("source_url") or "") for j in best_jobs)
        ):
            try:
                best_jobs = await asyncio.wait_for(
                    self._enrich_from_detail_pages(best_jobs),
                    timeout=12.0,
                )
            except asyncio.TimeoutError:
                logger.warning("v5.3 enrichment timeout for %s", url)
            except Exception:
                logger.exception("v5.3 enrichment failed for %s", url)
            best_jobs = self._dedupe(best_jobs, url)

        return best_jobs[:MAX_JOBS_PER_PAGE]

    # ==================================================================
    # ATS-SPECIFIC EXTRACTORS (isolated, dedicated handlers)
    # ==================================================================

    async def _extract_ats_specific(self, platform: str, url: str, html: str) -> list[dict]:
        """Dispatch to dedicated ATS extractor."""
        try:
            if platform == "oracle_cx":
                return await self._extract_oracle_cx(url, html)
            elif platform == "greenhouse":
                return await self._extract_greenhouse(url, html)
            elif platform == "salesforce":
                return self._extract_salesforce(url, html)
            elif platform == "martian":
                return await self._extract_martian(url, html)
            elif platform == "applyflow":
                return await self._extract_applyflow(url, html)
            # workday and lever fall through to heuristic extraction
        except asyncio.TimeoutError:
            logger.warning("v5.3 ATS %s timeout for %s", platform, url)
        except Exception:
            logger.exception("v5.3 ATS %s failed for %s", platform, url)
        return []

    # --- Oracle CandidateExperience ---

    async def _extract_oracle_cx(self, url: str, html: str) -> list[dict]:
        """Oracle CandidateExperience: probe requisitions API with tenant siteNumber variants."""
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"

        # Extract siteNumber variants from URL and HTML
        site_ids: list[str] = []
        # From URL path
        m = re.search(r"/sites/([A-Za-z0-9_-]+)", url)
        if m:
            site_ids.append(m.group(1))
        # From HTML
        for m in re.finditer(r'siteNumber["\s:=]+["\']?([A-Za-z0-9_-]+)', html[:10000]):
            sid = m.group(1)
            if sid not in site_ids:
                site_ids.append(sid)
        # Common defaults
        for default in ("CX_1001", "CX_1", "CX"):
            if default not in site_ids:
                site_ids.append(default)

        jobs: list[dict] = []
        async with httpx.AsyncClient(
            timeout=8, follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        ) as client:
            for site_id in site_ids[:4]:
                api_url = f"{base}/hcmRestApi/resources/latest/recruitingCEJobRequisitions?onlyData=true&expand=requisitionList.secondaryLocations,flexFieldsFacet.values&finder=findReqs;siteNumber={site_id},facetsList=LOCATIONS;WORK_LOCATIONS;WORKPLACE_TYPES;TITLES;CATEGORIES;ORGANIZATIONS;POSTING_DATES;FLEX_FIELDS&limit=25&offset=0"
                try:
                    resp = await client.get(api_url)
                    if resp.status_code != 200:
                        continue
                    data = resp.json()
                except Exception:
                    continue

                items = data.get("items", [])
                for item in items:
                    # Handle nested requisitionList
                    req_list = item.get("requisitionList", [item])
                    if isinstance(req_list, dict):
                        req_list = [req_list]
                    for req in req_list:
                        title = str(req.get("Title") or req.get("title") or "").strip()
                        if not self._is_valid_title_v53(title):
                            continue
                        req_id = str(req.get("Id") or req.get("id") or "")
                        detail_url = f"{base}/hcmUI/CandidateExperience/en/sites/{site_id}/job/{req_id}" if req_id else url
                        location = str(req.get("PrimaryLocation") or req.get("primaryLocation") or "").strip() or None
                        jobs.append({
                            "title": title,
                            "source_url": detail_url,
                            "location_raw": location,
                            "salary_raw": None,
                            "employment_type": str(req.get("WorkplaceType") or "").strip() or None,
                            "description": str(req.get("ExternalDescriptionStr") or "").strip()[:5000] or None,
                            "extraction_method": "ats_oracle_cx",
                            "extraction_confidence": 0.92,
                        })

                if jobs:
                    break  # Found jobs with this site ID, no need to try others

        return self._dedupe(jobs, url)

    # --- Greenhouse ---

    async def _extract_greenhouse(self, url: str, html: str) -> list[dict]:
        """Greenhouse: try embed board API endpoint."""
        # Extract board slug from URL or HTML
        slug = None
        m = re.search(r"boards\.greenhouse\.io/(?:embed/)?(?:job_board\?for=)?([a-z0-9_-]+)", url, re.IGNORECASE)
        if m:
            slug = m.group(1)
        if not slug:
            m = re.search(r'greenhouse\.io/(?:embed/)?(?:job_board\?for=)?([a-z0-9_-]+)', html[:5000], re.IGNORECASE)
            if m:
                slug = m.group(1)
        if not slug:
            # Try extracting from meta tags or script content
            m = re.search(r'"boardToken"\s*:\s*"([a-z0-9_-]+)"', html[:10000], re.IGNORECASE)
            if m:
                slug = m.group(1)

        if not slug:
            return []

        api_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
        jobs: list[dict] = []
        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
                resp = await client.get(api_url)
                if resp.status_code != 200:
                    return []
                data = resp.json()
                for job_data in (data.get("jobs") or []):
                    title = str(job_data.get("title") or "").strip()
                    if not self._is_valid_title_v53(title):
                        continue
                    abs_url = str(job_data.get("absolute_url") or "").strip()
                    location_name = ""
                    loc = job_data.get("location")
                    if isinstance(loc, dict):
                        location_name = str(loc.get("name") or "").strip()
                    elif isinstance(loc, str):
                        location_name = loc.strip()

                    desc = str(job_data.get("content") or "").strip()
                    if desc and "<" in desc:
                        parsed = _parse_html(desc)
                        if parsed is not None:
                            desc = _text(parsed)

                    jobs.append({
                        "title": title,
                        "source_url": abs_url or url,
                        "location_raw": location_name or None,
                        "salary_raw": None,
                        "employment_type": None,
                        "description": desc[:5000] if desc else None,
                        "extraction_method": "ats_greenhouse",
                        "extraction_confidence": 0.94,
                    })
        except Exception:
            logger.exception("v5.3 Greenhouse API failed for slug %s", slug)

        return self._dedupe(jobs, url)

    # --- Salesforce Recruit ---

    def _extract_salesforce(self, url: str, html: str) -> list[dict]:
        """Salesforce fRecruit: parse dataRow tables from fRecruit__ApplyJobList pages."""
        root = _parse_html(html)
        if root is None:
            return []

        jobs: list[dict] = []
        # Look for Salesforce table rows
        rows = root.xpath("//tr[contains(@class,'dataRow')]")
        if not rows:
            # Try broader table structure
            rows = root.xpath("//table//tr[position()>1]")

        for row in rows[:MAX_JOBS_PER_PAGE]:
            cells = row.xpath(".//td")
            if not cells:
                continue

            # Title from first cell with a link
            title = ""
            source_url = url
            for cell in cells:
                links = cell.xpath(".//a[@href]")
                if links:
                    title = self._normalize_title(_text(links[0]))
                    href = links[0].get("href", "")
                    if href:
                        source_url = _resolve_url(href, url) or url
                    break

            if not title:
                title = self._normalize_title(_text(cells[0]))

            if not self._is_valid_title_v53(title):
                continue

            # Location/type from remaining cells
            location = None
            emp_type = None
            for cell in cells[1:]:
                cell_text = _text(cell).strip()
                if not cell_text or cell_text == title:
                    continue
                if not location and _AU_LOCATIONS.search(cell_text):
                    location = cell_text[:200]
                elif not emp_type and _JOB_TYPE_PATTERN.search(cell_text):
                    emp_type = cell_text[:120]

            jobs.append({
                "title": title,
                "source_url": source_url,
                "location_raw": location,
                "salary_raw": None,
                "employment_type": emp_type,
                "description": None,
                "extraction_method": "ats_salesforce",
                "extraction_confidence": 0.88,
            })

        return self._dedupe(jobs, url)

    # --- ApplyFlow ---

    async def _extract_applyflow(self, url: str, html: str) -> list[dict]:
        """ApplyFlow: query known JSON/HTML endpoints using afConfig and script hints."""
        cfg = self._extract_applyflow_config(html)
        parsed = urlparse(url)
        if not parsed.netloc:
            return []

        page_host = f"{parsed.scheme or 'https'}://{parsed.netloc}".rstrip("/")
        probe_urls = self._build_applyflow_probe_urls(url, cfg)
        if not probe_urls:
            return []

        jobs: list[dict] = []
        seen: set[str] = set()
        async with httpx.AsyncClient(
            timeout=4.5,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "application/json,text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                "Referer": url,
            },
        ) as client:
            for probe_url in probe_urls[:20]:
                norm = probe_url.rstrip("/")
                if not probe_url or norm in seen:
                    continue
                seen.add(norm)

                try:
                    resp = await client.get(probe_url)
                except Exception:
                    continue
                if resp.status_code >= 400 or not resp.text:
                    continue

                response_url = str(resp.url)
                body = resp.text
                payload_jobs = self._parse_applyflow_response(body, response_url, page_host)
                if payload_jobs:
                    jobs.extend(payload_jobs)
                    jobs = self._dedupe(jobs, url)
                    if len(jobs) >= MIN_JOBS_FOR_SUCCESS:
                        break

        return self._dedupe(jobs, url)

    def _extract_applyflow_config(self, html: str) -> dict[str, str]:
        result: dict[str, str] = {}
        match = _APPLYFLOW_CONFIG.search(html or "")
        if match:
            try:
                parsed = json.loads((match.group(1) or "").strip())
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                for key in ("site_code", "job_buckets", "site_job_buckets", "seeker_buckets"):
                    value = parsed.get(key)
                    if isinstance(value, str) and value.strip():
                        result[key] = value.strip()
        return result

    def _build_applyflow_probe_urls(self, page_url: str, config: dict[str, str]) -> list[str]:
        parsed = urlparse(page_url)
        page_host = f"{parsed.scheme or 'https'}://{parsed.netloc}".rstrip("/")
        job_bucket = (config.get("job_buckets") or config.get("site_job_buckets") or "").strip()
        site_code = (config.get("site_code") or config.get("seeker_buckets") or "").strip()

        urls: list[str] = []
        base_paths = [
            "/af-api/jobs",
            "/af-api/job",
            "/af-api/job-listings",
            "/af-api/wp/v2/jobs",
            "/af-api/wp/v2/job",
            "/wp-json/wp/v2/jobs",
            "/wp-json/wp/v2/job",
            "/wp-json/applyflow/v1/jobs",
            "/wp-json/af/v1/jobs",
            "/jobs?search=",
        ]
        for path in base_paths:
            urls.append(f"{page_host}{path}")

        # Seeker API variants used by ApplyFlow boards.
        seeker_base = "https://seeker.applyflow.com"
        seeker_paths = [
            "/api/jobs/search",
            "/api/job-search",
            "/api/jobs",
            "/api/v1/jobs/search",
            "/api/job-ads/search",
        ]
        queries = [
            "page=1&pageSize=100",
            "pageNumber=1&pageSize=100&isActive=true",
        ]
        if site_code:
            queries.extend([f"site_code={site_code}&page=1&pageSize=100", f"siteCode={site_code}&search="])
        if job_bucket:
            queries.extend([f"bucket={job_bucket}&page=1&pageSize=100", f"job_buckets={job_bucket}&search="])
        for path in seeker_paths:
            base = f"{seeker_base}{path}"
            urls.append(base)
            for q in queries:
                urls.append(f"{base}?{q}")

        # Mine endpoint hints from external JS chunks.
        for hint in self._extract_applyflow_endpoint_hints(page_url, (config.get("site_code") or "")):
            urls.append(hint)

        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in urls:
            if not candidate:
                continue
            norm = candidate.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
            deduped.append(candidate)
        return deduped

    def _extract_applyflow_endpoint_hints(self, page_url: str, site_code: str) -> list[str]:
        # Keep this lightweight and deterministic: infer common endpoints from known hosts.
        parsed = urlparse(page_url)
        if not parsed.netloc:
            return []
        host = f"{parsed.scheme or 'https'}://{parsed.netloc}".rstrip("/")
        hints = [
            f"{host}/af-api/search/jobs",
            f"{host}/af-api/jobs/search",
            f"{host}/af-api/wp/v2/pages",
        ]
        if site_code:
            hints.extend(
                [
                    f"https://seeker.applyflow.com/api/jobs/search?site_code={site_code}&page=1&pageSize=100",
                    f"https://seeker.applyflow.com/api/job-search?siteCode={site_code}&search=",
                ]
            )
        return hints

    def _parse_applyflow_response(self, body: str, response_url: str, page_url: str) -> list[dict]:
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
                jobs.extend(self._extract_jobs_from_json_obj(parsed, response_url, "ats_applyflow_api"))

        if not jobs and _APPLYFLOW_ROW_MARKER.search(payload[:15000]):
            root = _parse_html(payload)
            if root is not None:
                jobs.extend(self._extract_applyflow_rows(root, response_url))

        return self._dedupe(jobs, page_url)

    def _extract_applyflow_rows(self, root: etree._Element, page_url: str) -> list[dict]:
        rows = root.xpath("//*[contains(@class,'af-search-result-item')]")
        if len(rows) < 2:
            return []

        jobs: list[dict] = []
        for row in rows[:MAX_JOBS_PER_PAGE]:
            title_node = row.xpath(
                ".//*[contains(@class,'af-job-title')]//a[@href][1] | "
                ".//*[contains(@class,'af-job-title')][1] | .//a[contains(@href,'jobview')][1]"
            )
            if not title_node:
                continue

            title = self._normalize_title(_text(title_node[0]))
            href = title_node[0].get("href") if hasattr(title_node[0], "get") else None
            source_url = _resolve_url(href, page_url) or page_url
            if not self._is_valid_title_v53(title):
                continue
            if not self._title_has_job_signal(title):
                continue

            row_text = _text(row)[:2200]
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location(row_text),
                    "salary_raw": self._extract_salary(row_text),
                    "employment_type": self._extract_type(row_text),
                    "description": row_text if len(row_text) > 80 else None,
                    "extraction_method": "tier2_applyflow_rows",
                    "extraction_confidence": 0.83,
                }
            )

        return self._dedupe(jobs, page_url)

    # --- MartianLogic / MyRecruitmentPlus ---

    async def _extract_martian(self, url: str, html: str) -> list[dict]:
        """MartianLogic/MyRecruitmentPlus: host-diversified API probing."""
        context = self._extract_martian_context(html, url)
        client_code = context.get("client_code", "")
        if not client_code:
            return []

        parsed = urlparse(url)
        page_host = f"{parsed.scheme or 'https'}://{parsed.netloc}".rstrip("/")
        hosts = [
            page_host,
            "https://web.martianlogic.com",
            "https://jobs.myrecruitmentplus.com",
            "https://form.myrecruitmentplus.com",
        ]
        if client_code:
            hosts.extend([f"https://{client_code}.myrecruitmentplus.com", f"https://{client_code}.martianlogic.com"])

        probes: list[str] = []
        query_variants = [
            "pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            "page=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            "search=&pageNumber=1&pageSize=50",
        ]
        for host in hosts:
            host = host.rstrip("/")
            probes.extend(
                [
                    f"{host}/{client_code}/",
                    f"{host}/{client_code}/embed-jobs",
                    f"{host}/embed-jobs?client={client_code}",
                    f"{host}/?client={client_code}",
                    f"{host}/api/jobs/search?clientCode={client_code}&pageNumber=1&pageSize=50",
                    f"{host}/api/job-search?clientCode={client_code}&search=",
                    f"{host}/api/job-ads/search?clientCode={client_code}&pageNumber=1&pageSize=50",
                ]
            )
            for q in query_variants:
                probes.append(f"{host}/{client_code}/?{q}")
                probes.append(f"{host}/embed-jobs?client={client_code}&{q}")
                probes.append(f"{host}/?client={client_code}&{q}")

        recruiter_id = context.get("recruiter_id", "")
        if recruiter_id:
            for host in hosts:
                host = host.rstrip("/")
                probes.append(f"{host}/api/job-ads?recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true")
                probes.append(f"{host}/api/search?recruiterId={recruiter_id}&pageNumber=1&pageSize=50")
                probes.append(f"{host}/api/jobs/search?recruiterId={recruiter_id}&pageNumber=1&pageSize=50")

        seen_probe: set[str] = set()
        ordered_probes: list[str] = []
        for probe in probes:
            norm = probe.rstrip("/")
            if norm in seen_probe:
                continue
            seen_probe.add(norm)
            ordered_probes.append(probe)

        jobs: list[dict] = []
        try:
            async with httpx.AsyncClient(
                timeout=5.0, follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/html,*/*"},
            ) as client:
                miss_streak = 0
                for probe_url in ordered_probes[:18]:
                    if miss_streak >= 3:
                        break
                    try:
                        resp = await client.get(probe_url)
                    except Exception:
                        miss_streak += 1
                        continue
                    if resp.status_code != 200 or not resp.text:
                        miss_streak += 1
                        continue

                    miss_streak = 0
                    probe_jobs = self._parse_martian_response(resp.text, str(resp.url), url)
                    if probe_jobs:
                        jobs.extend(probe_jobs)
                        jobs = self._dedupe(jobs, url)
                        if len(jobs) >= MIN_JOBS_FOR_SUCCESS:
                            # Try pagination
                            for page_num in range(2, 5):
                                paged = re.sub(r"pageNumber=\d+", f"pageNumber={page_num}", probe_url)
                                if paged == probe_url:
                                    paged = probe_url + ("&" if "?" in probe_url else "?") + f"pageNumber={page_num}"
                                try:
                                    resp2 = await client.get(paged)
                                except Exception:
                                    break
                                if resp2.status_code != 200:
                                    break
                                more = self._parse_martian_response(resp2.text, str(resp2.url), url)
                                if not more:
                                    break
                                before = len(jobs)
                                jobs.extend(more)
                                jobs = self._dedupe(jobs, url)
                                if len(jobs) == before:
                                    break
                            break  # Found jobs, stop probing
        except Exception:
            logger.exception("v5.3 MartianLogic probing failed for %s", url)

        return self._dedupe(jobs, url)

    def _extract_martian_context(self, html: str, url: str) -> dict[str, str]:
        result: dict[str, str] = {}
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html or "", re.IGNORECASE | re.DOTALL,
        )
        if match:
            try:
                data = json.loads(match.group(1))
                pp = ((data.get("props") or {}).get("pageProps") or {}) if isinstance(data, dict) else {}
                if isinstance(pp, dict):
                    result["client_code"] = str(pp.get("clientCode") or "").strip()
                    result["recruiter_id"] = str(pp.get("recruiterId") or "").strip()
            except Exception:
                pass

        if not result.get("client_code"):
            parsed = urlparse(url)
            path_parts = [p for p in parsed.path.split("/") if p]
            if path_parts:
                result["client_code"] = path_parts[0].strip()
            m = _MARTIAN_CLIENT.search(parsed.path or "")
            if m:
                result["client_code"] = m.group(1).strip()

        return result

    def _parse_martian_response(self, body: str, response_url: str, page_url: str) -> list[dict]:
        """Parse Martian API response — JSON or HTML."""
        jobs: list[dict] = []
        payload = (body or "").strip()
        if not payload:
            return jobs

        # Try JSON first
        if payload.startswith("{") or payload.startswith("["):
            try:
                parsed = json.loads(payload)
                jobs.extend(self._extract_jobs_from_json_obj(parsed, response_url, "ats_martian_api"))
            except Exception:
                pass

        # Also try HTML extraction
        if not jobs:
            root = _parse_html(payload)
            if root is not None:
                tier2 = self._extract_tier2_v16(response_url, payload) or []
                for j in tier2:
                    j["extraction_method"] = "ats_martian_html"
                jobs.extend(tier2)
                jobs.extend(self._extract_from_heading_rows(root, response_url))
                jobs.extend(self._extract_from_accordion_sections(root, response_url))
                jobs.extend(self._extract_from_hidden_input_titles(root, response_url))
                jobs.extend(self._extract_from_repeating_rows(root, response_url))
                jobs.extend(self._extract_from_job_links(root, response_url))

        return self._dedupe(jobs, page_url)

    # ==================================================================
    # STRUCTURED DATA EXTRACTION (from v2.6)
    # ==================================================================

    def _extract_structured_jobs(self, html: str, page_url: str) -> list[dict]:
        """Extract from JSON-LD JobPosting schema."""
        jobs: list[dict] = []
        for match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html, re.IGNORECASE | re.DOTALL,
        ):
            raw = (match.group(1) or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            jobs.extend(self._extract_jobs_from_json_obj(data, page_url, "tier0_jsonld"))

        return self._dedupe(jobs, page_url)

    def _extract_jobs_from_state_scripts(self, html: str, page_url: str) -> list[dict]:
        """Extract from __NEXT_DATA__, dehydrated state, and embedded JSON."""
        jobs: list[dict] = []
        payloads: list[str] = []

        # __NEXT_DATA__
        nd = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.IGNORECASE | re.DOTALL)
        if nd:
            payloads.append(nd.group(1))

        # __remixContext (Greenhouse Remix)
        rc = re.search(r'window\.__remixContext\s*=\s*(\{.*?\});?\s*</script>', html, re.DOTALL)
        if rc:
            payloads.append(rc.group(1))

        # Other script blocks with job-like content
        for match in re.finditer(r"<script[^>]*>(.*?)</script>", html, re.IGNORECASE | re.DOTALL):
            body = (match.group(1) or "").strip()
            if len(body) < 40:
                continue
            lowered = body.lower()
            if any(kw in lowered for kw in ("dehydratedstate", "jobpostsdata", "requisition", "applicationformurl")):
                payloads.append(body)

        for payload in payloads[:30]:
            for parsed in self._parse_json_blobs(payload):
                jobs.extend(self._extract_jobs_from_json_obj(parsed, page_url, "tier0_state"))

        return self._dedupe(jobs, page_url)

    async def _follow_listing_hub_links(self, page_url: str, html: str) -> list[dict]:
        """Follow high-signal listing links (lowongan/jobs/openings) when initial coverage is small."""
        root = _parse_html(html or "")
        if root is None:
            return []

        parsed_page = urlparse(page_url)
        page_host = (parsed_page.netloc or "").lower()
        page_base = ".".join(page_host.split(".")[-2:]) if page_host else ""

        scored_links: list[tuple[int, str]] = []
        seen: set[str] = set()
        for a_el in root.xpath("//a[@href]")[:2500]:
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            full_url = _resolve_url(href, page_url)
            if not full_url:
                continue
            parsed = urlparse(full_url)
            host = (parsed.netloc or "").lower()
            if host and page_host:
                host_base = ".".join(host.split(".")[-2:]) if host else ""
                if host != page_host and host_base != page_base:
                    continue

            norm = full_url.rstrip("/")
            if not norm or norm == page_url.rstrip("/") or norm in seen:
                continue
            seen.add(norm)

            text = " ".join(_text(a_el).split())
            score = 0
            if _HUB_LINK_TEXT.search(text):
                score += 10
            if _HUB_LINK_HREF.search(parsed.path or ""):
                score += 8
            if "search=" in (parsed.query or "").lower():
                score += 3
            if score > 0:
                scored_links.append((score, full_url))

        if not scored_links:
            return []
        scored_links.sort(key=lambda item: item[0], reverse=True)

        candidates: list[tuple[str, list[dict]]] = []
        async with httpx.AsyncClient(
            timeout=4.5,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8"},
        ) as client:
            for _score, link in scored_links[:5]:
                try:
                    resp = await client.get(link)
                except Exception:
                    continue
                if resp.status_code != 200 or len(resp.text or "") < 200:
                    continue
                target_url = str(resp.url)
                target_html = resp.text or ""
                local_jobs = self._extract_lightweight_candidates(target_url, target_html)
                if local_jobs:
                    candidates.append((target_url, local_jobs))

        if not candidates:
            return []
        _label, jobs = self._pick_best_jobset(candidates, page_url)
        return jobs

    def _extract_lightweight_candidates(self, page_url: str, html: str) -> list[dict]:
        jobs: list[dict] = []
        jobs.extend(self._extract_structured_jobs(html, page_url))
        jobs.extend(self._extract_jobs_from_state_scripts(html, page_url))
        root = _parse_html(html)
        if root is None:
            return self._dedupe(jobs, page_url)
        jobs.extend(self._extract_applyflow_rows(root, page_url))
        jobs.extend(self._extract_elementor_cards(root, page_url))
        jobs.extend(self._extract_from_heading_rows(root, page_url))
        jobs.extend(self._extract_from_accordion_sections(root, page_url))
        jobs.extend(self._extract_from_hidden_input_titles(root, page_url))
        jobs.extend(self._extract_from_repeating_rows(root, page_url))
        jobs.extend(self._extract_from_job_links(root, page_url))
        return self._dedupe(jobs, page_url)

    def _parse_json_blobs(self, script_body: str) -> list[object]:
        results: list[object] = []
        body = (script_body or "").strip()
        if not body:
            return results

        if body.startswith("{") or body.startswith("["):
            try:
                results.append(json.loads(body))
            except Exception:
                pass

        for m in _SCRIPT_ASSIGNMENT.finditer(body):
            raw = (m.group(1) or "").strip()
            if len(raw) < 2:
                continue
            try:
                results.append(json.loads(raw))
            except Exception:
                continue

        return results

    def _extract_jobs_from_json_obj(self, data: object, page_url: str, method: str) -> list[dict]:
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
            job = self._job_from_json_dict(node, page_url, method)
            if job:
                jobs.append(job)
                if len(jobs) >= MAX_JOBS_PER_PAGE:
                    break

        return jobs

    def _job_from_json_dict(self, node: dict, page_url: str, method: str) -> Optional[dict]:
        title = ""
        title_key = ""
        for key in ("title", "jobTitle", "positionTitle", "requisitionTitle", "name", "jobName"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                title = value.strip()
                title_key = key
                break

        title = self._normalize_title(title)
        if not self._is_valid_title_v53(title):
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

        # Validate this is actually a job object, not a category/taxonomy label
        key_names = " ".join(str(k) for k in node.keys()).lower()
        jobposting_type = str(node.get("@type") or "").strip().lower() == "jobposting"
        strong_id_hint = any(
            k in node for k in (
                "jobId", "jobID", "jobPostingId", "requisitionId", "positionId",
                "jobAdId", "applicationFormUrl", "applicationUrl",
            )
        )
        job_key_hint = bool(
            re.search(r"job|position|posting|requisition|vacanc|opening", key_names)
            or strong_id_hint
        )
        title_hint = self._title_has_job_signal(title)
        url_hint = self._is_job_like_url(source_url)
        key_set = {str(k) for k in node.keys()}
        looks_label = key_set.issubset({"id", "name", "label", "value", "path", "children", "parent"})
        taxonomy_hint = bool(re.search(r"department|office|filter|facet|category|taxonomy", key_names))

        if self._is_non_job_url(source_url):
            if not (title_hint and (job_key_hint or strong_id_hint)):
                return None
            source_url = page_url

        if looks_label and not job_key_hint:
            return None
        if taxonomy_hint and not (job_key_hint or jobposting_type):
            return None
        if source_url == page_url and not (strong_id_hint or jobposting_type):
            return None
        if _COMPANY_CAREER_LABEL.match(title):
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
                parts: list[str] = []
                for entry in value[:5]:
                    if isinstance(entry, str) and entry.strip():
                        parts.append(entry.strip())
                    elif isinstance(entry, dict):
                        locality = str(entry.get("addressLocality") or entry.get("city") or "").strip()
                        region = str(entry.get("addressRegion") or entry.get("state") or "").strip()
                        country = str(entry.get("addressCountry") or "").strip()
                        joined = ", ".join(p for p in (locality, region, country) if p)
                        if joined:
                            parts.append(joined)
                if parts:
                    location = " | ".join(dict.fromkeys(parts))[:200]
                    break

        salary = None
        for key in ("salary", "compensation", "baseSalary", "payRate"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                salary = value.strip()[:200]
                break
            if isinstance(value, dict):
                raw = json.dumps(value, ensure_ascii=False)
                m = _SALARY_PATTERN.search(raw)
                if m:
                    salary = m.group(0).strip()
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
        for key in ("description", "summary", "introduction", "previewText", "content"):
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

    # ==================================================================
    # DOM FALLBACKS (from v2.6, cleaned up)
    # ==================================================================

    def _extract_elementor_cards(self, root: etree._Element, page_url: str) -> list[dict]:
        rows = root.xpath(
            "//div[contains(@class,'elementor-column') and contains(@class,'elementor-inner-column')]"
        )
        if len(rows) < 2:
            return []

        jobs: list[dict] = []
        for row in rows[:600]:
            heading = row.xpath(".//h2[1] | .//h3[1] | .//h4[1]")
            if not heading:
                continue
            title = self._normalize_title(_text(heading[0]))
            if not self._is_valid_title_v53(title):
                continue
            if not self._title_has_job_signal(title):
                continue
            if _GENERIC_LISTING_LABEL.match(title):
                continue

            link_nodes = row.xpath(".//a[@href]")
            if not link_nodes:
                continue

            source_url = page_url
            for a_el in link_nodes:
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url(href):
                    continue
                source_url = href
                if self._is_job_like_url(href):
                    break

            row_text = _text(row)[:2200]
            has_local_evidence = bool(_APPLY_CONTEXT.search(row_text)) or len(row_text) >= 110 or source_url != page_url
            if not has_local_evidence:
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location(row_text),
                    "salary_raw": self._extract_salary(row_text),
                    "employment_type": self._extract_type(row_text),
                    "description": row_text if len(row_text) > 80 else None,
                    "extraction_method": "tier2_elementor_cards",
                    "extraction_confidence": 0.82,
                }
            )

        return self._dedupe(jobs, page_url)

    def _extract_from_job_links(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url) or page_url
            if self._is_non_job_url(source_url):
                continue

            heading_nodes = a_el.xpath(".//h1 | .//h2 | .//h3 | .//h4")
            title_raw = _text(heading_nodes[0]) if heading_nodes else (_text(a_el) or (a_el.get("title") or ""))
            text = self._normalize_title(title_raw)
            if not self._is_valid_title_v53(text):
                continue
            if len(text) > 100:
                continue
            if _GENERIC_LISTING_LABEL.match(text):
                continue

            parent = a_el.getparent()
            parent_text = _text(parent)[:1200] if parent is not None else ""
            url_hint = self._is_job_like_url(source_url)
            title_hint = self._title_has_job_signal(text)
            context_hint = bool(
                re.search(r"apply|location|department|job ref|posted|closing|employment", parent_text, re.IGNORECASE)
            )
            structural_hint = len(parent_text) >= 45 and len(text.split()) >= 2

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

            jobs.append({
                "title": text,
                "source_url": source_url,
                "location_raw": location,
                "salary_raw": None,
                "employment_type": emp_type,
                "description": parent_text[:5000] if len(parent_text) > 60 else None,
                "extraction_method": "tier2_links",
                "extraction_confidence": 0.72 if url_hint else 0.64,
            })

        return self._dedupe(jobs, page_url)

    def _extract_from_accordion_sections(self, root: etree._Element, page_url: str) -> list[dict]:
        items = root.xpath(
            "//*[contains(@class,'accordion-item') or contains(@class,'elementor-accordion-item') or "
            "contains(@class,'accordion') or contains(@class,'card_dipult') or "
            "contains(@class,'collapse-item') or .//button[contains(@class,'btn-link') and contains(@data-target,'collapse')]]"
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

            title = self._normalize_title(_text(title_el[0]))
            if not self._is_valid_title_v53(title):
                continue
            if not self._title_has_job_signal(title):
                continue

            link_el = item.xpath(".//a[@href]")
            link_href = link_el[0].get("href") if link_el else None
            source_url = _resolve_url(link_href, page_url) if link_href else page_url
            if source_url and self._is_non_job_url(source_url):
                source_url = page_url

            item_text = _text(item)[:1800]
            if not _APPLY_CONTEXT.search(item_text):
                if len(item_text) < 120:
                    continue
                if len(title.split()) <= 2:
                    continue

            jobs.append({
                "title": title,
                "source_url": source_url or page_url,
                "location_raw": None,
                "salary_raw": None,
                "employment_type": None,
                "description": item_text[:5000] if len(item_text) > 80 else None,
                "extraction_method": "tier2_accordion",
                "extraction_confidence": 0.68,
            })

        return self._dedupe(jobs, page_url)

    def _extract_from_heading_rows(self, root: etree._Element, page_url: str) -> list[dict]:
        containers = root.xpath("//section | //div | //article | //main")
        jobs: list[dict] = []

        for container in containers[:250]:
            headings = container.xpath(".//h2 | .//h3 | .//h4 | .//button[contains(@class,'btn-link')]")
            if len(headings) < 2:
                continue

            classes = _get_el_classes(container)
            container_text = _text(container)[:4000]
            apply_hits = len(_APPLY_CONTEXT.findall(container_text))
            has_row_hint = bool(_ROW_CLASS_PATTERN.search(classes))
            if not has_row_hint and apply_hits == 0:
                continue

            local_jobs: list[dict] = []
            for h in headings[:40]:
                title = self._normalize_title(_text(h))
                if not self._is_valid_title_v53(title):
                    continue
                if not self._title_has_job_signal(title):
                    continue
                if _GENERIC_LISTING_LABEL.match(title):
                    continue

                # Use row-local context (not the whole container) to avoid broad page noise.
                row = h.getparent()
                if row is None:
                    row = container
                row_text = _text(row)[:2600]
                if len(row_text) < 100:
                    sib = row.getnext()
                    taken = 0
                    while sib is not None and taken < 2:
                        sib_text = _text(sib)
                        if sib_text:
                            row_text += " " + sib_text[:1200]
                        sib = sib.getnext()
                        taken += 1

                apply_evidence = bool(_APPLY_CONTEXT.search(row_text))
                if not apply_evidence and len(row_text) < 160:
                    continue

                link = row.xpath(".//a[@href]")
                if not link:
                    link = h.xpath(".//a[@href]")
                if not link:
                    link = h.xpath("following::a[@href][1]")
                href = link[0].get("href") if link else None
                source_url = _resolve_url(href, page_url) or page_url
                if self._is_non_job_url(source_url):
                    source_url = page_url

                local_jobs.append({
                    "title": title,
                    "source_url": source_url,
                    "location_raw": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": row_text[:5000] if len(row_text) > 80 else None,
                    "extraction_method": "tier2_heading_rows",
                    "extraction_confidence": 0.66,
                })

            if len(local_jobs) >= 2:
                jobs.extend(local_jobs)

        return self._dedupe(jobs, page_url)

    def _extract_from_hidden_input_titles(self, root: etree._Element, page_url: str) -> list[dict]:
        """Recover accordion/card listings where title is stored in hidden input fields."""
        jobs: list[dict] = []
        inputs = root.xpath("//input[starts-with(@name,'job_title_') and @value]")
        if len(inputs) < 2:
            return []

        for input_el in inputs[:MAX_JOBS_PER_PAGE]:
            title = self._normalize_title(str(input_el.get("value") or ""))
            if not self._is_valid_title_v53(title):
                continue
            if not self._title_has_job_signal(title):
                continue

            row = input_el.getparent()
            climb = 0
            while row is not None and climb < 5:
                classes = _get_el_classes(row)
                if "card" in classes or "accordion" in classes or "collapse" in classes:
                    break
                row = row.getparent()
                climb += 1
            if row is None:
                row = input_el

            row_text = _text(row)[:3200]
            if not _APPLY_CONTEXT.search(row_text) and len(row_text) < 140:
                continue

            source_url = page_url
            for a_el in row.xpath(".//a[@href]"):
                href = _resolve_url(a_el.get("href"), page_url)
                if not href:
                    continue
                if self._is_non_job_url(href):
                    continue
                if self._is_job_like_url(href):
                    source_url = href
                    break

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location(row_text),
                    "salary_raw": self._extract_salary(row_text),
                    "employment_type": self._extract_type(row_text),
                    "description": row_text if len(row_text) > 80 else None,
                    "extraction_method": "tier2_hidden_input_rows",
                    "extraction_confidence": 0.84,
                }
            )

        return self._dedupe(jobs, page_url)

    def _extract_from_repeating_rows(self, root: etree._Element, page_url: str) -> list[dict]:
        groups: dict[str, list[etree._Element]] = defaultdict(list)
        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag not in {"li", "div", "article", "tr", "section"}:
                continue
            classes = _get_el_classes(el)
            if not classes or not _ROW_CLASS_PATTERN.search(classes):
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
                title = self._normalize_title(_text(title_nodes[0]))
                if not self._is_valid_title_v53(title):
                    continue
                if not self._title_has_job_signal(title):
                    continue

                link = row.xpath(".//a[@href and not(starts-with(@href,'#'))][1]")
                href = link[0].get("href") if link else None
                source_url = _resolve_url(href, page_url) or page_url
                if self._is_non_job_url(source_url):
                    source_url = page_url

                row_text = _text(row)
                jobs.append({
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location(row_text),
                    "salary_raw": self._extract_salary(row_text),
                    "employment_type": self._extract_type(row_text),
                    "description": row_text[:5000] if len(row_text) > 70 else None,
                    "extraction_method": "tier2_repeating_rows",
                    "extraction_confidence": 0.72,
                })

        return self._dedupe(jobs, page_url)

    # ==================================================================
    # CANDIDATE SELECTION (v2.6 + coverage-first from v3.0)
    # ==================================================================

    def _pick_best_jobset(
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
            deduped = self._dedupe(jobs, page_url)
            if not deduped:
                continue

            score = self._jobset_score(deduped, page_url)
            valid = self._passes_jobset_validation(deduped, page_url)

            if label == "parent_v16":
                parent_score = score
                parent_jobs = deduped

            logger.debug("v5.3 candidate %s: jobs=%d score=%.2f valid=%s", label, len(deduped), score, valid)

            if valid and score > best_score:
                best_label = label
                best_jobs = deduped
                best_score = score

        if best_jobs:
            # Coverage-first tie-break: if another valid candidate is larger and mostly overlaps,
            # prefer the larger set when quality is close.
            for label, jobs in candidates:
                deduped = self._dedupe(jobs, page_url)
                if not deduped or label == best_label:
                    continue
                score = self._jobset_score(deduped, page_url)
                if not self._passes_jobset_validation(deduped, page_url):
                    continue
                overlap = self._title_overlap_ratio(deduped, best_jobs)
                if len(deduped) >= len(best_jobs) + 2 and overlap >= 0.5 and score >= best_score - 2.0:
                    best_label, best_jobs, best_score = label, deduped, score

            # v3.0 cherry-pick: coverage-first — prefer larger validated sets
            # Only keep parent if it's genuinely better, not just because it ran first
            if parent_jobs and best_label != "parent_v16":
                # Parent wins only if it's clearly better (higher score)
                # NOT just "close enough" — we want the best quality set
                if parent_score > best_score:
                    return "parent_v16", parent_jobs
            return best_label, best_jobs[:MAX_JOBS_PER_PAGE]

        # Fallback to parent partial
        if parent_jobs:
            return "parent_v16", parent_jobs[:MAX_JOBS_PER_PAGE]

        # Final fallback: largest candidate
        largest = max(
            ((label, self._dedupe(jobs, page_url)) for label, jobs in candidates),
            key=lambda item: len(item[1]),
            default=("", []),
        )
        return largest[0], largest[1][:MAX_JOBS_PER_PAGE]

    @staticmethod
    def _title_overlap_ratio(a_jobs: list[dict], b_jobs: list[dict]) -> float:
        a = {str(j.get("title") or "").strip().lower() for j in a_jobs if j.get("title")}
        b = {str(j.get("title") or "").strip().lower() for j in b_jobs if j.get("title")}
        if not a or not b:
            return 0.0
        return len(a & b) / max(1, min(len(a), len(b)))

    # ==================================================================
    # VALIDATION & SCORING (from v2.6)
    # ==================================================================

    def _passes_jobset_validation(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if self._is_valid_title_v53(t)]
        if not titles:
            return False

        unique_ratio = len(set(t.lower() for t in titles)) / max(1, len(titles))
        if len(titles) > 2 and unique_ratio < 0.6:
            return False

        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN.match(t.lower()))
        nav_hits = sum(
            1 for t in titles
            if _CATEGORY_TITLE.match(t) or _CORPORATE_TITLE.match(t) or _PHONE_TITLE.match(t)
        )
        if reject_hits >= max(1, int(len(titles) * 0.35)):
            return False
        if nav_hits >= max(1, int(len(titles) * 0.25)):
            return False

        title_hits = sum(1 for t in titles if self._title_has_job_signal(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url(j.get("source_url") or page_url))
        apply_hits = sum(1 for j in jobs if _APPLY_CONTEXT.search((j.get("description") or "")[:1200]))

        if len(titles) == 1:
            t = titles[0]
            src = jobs[0].get("source_url") or page_url
            return (
                (self._title_has_job_signal(t) and not _GENERIC_LISTING_LABEL.match(t))
                and (self._is_job_like_url(src) or apply_hits >= 1)
            )

        if len(titles) <= 3:
            return title_hits >= max(1, int(len(titles) * 0.67)) and (url_hits >= 1 or apply_hits >= 1)

        needed = max(2, int(len(titles) * 0.3))
        return (
            title_hits >= needed
            and (url_hits >= max(1, int(len(titles) * 0.15)) or apply_hits >= max(1, int(len(titles) * 0.15)))
        )

    def _jobset_score(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        titles = [self._normalize_title(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return 0.0

        count = len(titles)
        title_hits = sum(1 for t in titles if self._title_has_job_signal(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url(j.get("source_url") or page_url))
        apply_hits = sum(1 for j in jobs if _APPLY_CONTEXT.search((j.get("description") or "")[:1200]))
        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN.match(t.lower()))
        nav_hits = sum(
            1 for t in titles
            if _CATEGORY_TITLE.match(t) or _CORPORATE_TITLE.match(t) or _PHONE_TITLE.match(t)
        )
        unique_titles = len(set(t.lower() for t in titles))

        score = count * 3.2
        score += title_hits * 2.3
        score += url_hits * 1.7
        score += apply_hits * 1.5
        score += unique_titles * 0.7
        score -= reject_hits * 3.5
        score -= nav_hits * 4.2
        return score

    # ==================================================================
    # HELPERS
    # ==================================================================

    def _dedupe(self, jobs: list[dict], page_url: str) -> list[dict]:
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for job in jobs:
            title = self._normalize_title(job.get("title", ""))
            if not self._is_valid_title_v53(title):
                continue

            source_url = (job.get("source_url") or "").strip()
            if not source_url:
                source_url = page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]
            source_url = source_url.rstrip("/") or source_url
            if self._is_non_job_url(source_url):
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

    def _normalize_title(self, title: str) -> str:
        if not title:
            return ""
        t = html_mod.unescape(" ".join(str(title).replace("\u00a0", " ").split()))
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
        # v2.9: preserve dotted titles like ".NET Software Engineer"
        if " - " in t and len(t) > 40:
            parts = [p.strip() for p in t.split(" - ") if p.strip()]
            if parts and self._title_has_job_signal(parts[0]):
                t = parts[0]
        return t.strip()

    def _is_valid_title_v53(self, title: str) -> bool:
        """Title validation with v2.9 boundary-aware boilerplate checks."""
        if not title:
            return False
        if not TieredExtractorV16._is_valid_title_v16(title):
            if not _is_valid_title(title):
                return False

        t = title.strip()
        low = t.lower()

        if _REJECT_TITLE_PATTERN.match(low):
            return False
        if _GENERIC_LISTING_LABEL.match(t):
            return False
        if _CATEGORY_TITLE.match(t):
            return False
        if _CORPORATE_TITLE.match(t):
            return False
        if _PHONE_TITLE.match(t):
            return False
        if _COMPANY_CAREER_LABEL.match(t):
            return False
        if _CTA_TITLE_PATTERN.match(t):
            return False
        if _MARKETING_COPY_PATTERN.search(t):
            return False
        if "@" in t:
            return False

        words = t.split()
        if len(words) > 14:
            return False

        # v2.9 cherry-pick: use boundary-aware pattern instead of raw substring
        if _INLINE_BOILERPLATE.search(low):
            if len(words) <= 5 and not self._title_has_job_signal(t):
                return False

        if len(words) <= 1 and not self._title_has_job_signal(t):
            return False
        if len(words) <= 2 and not self._title_has_job_signal(t):
            return False
        if not self._title_has_job_signal(t):
            return False
        return True

    def _title_has_job_signal(self, title: str) -> bool:
        if not title:
            return False
        if _title_has_job_noun(title):
            return True
        return bool(_TITLE_HINT_PATTERN.search(title))

    @staticmethod
    def _extract_location(text: str) -> Optional[str]:
        match = _AU_LOCATIONS.search(text or "")
        return match.group(0).strip() if match else None

    @staticmethod
    def _extract_salary(text: str) -> Optional[str]:
        match = _SALARY_PATTERN.search(text or "")
        return match.group(0).strip() if match else None

    @staticmethod
    def _extract_type(text: str) -> Optional[str]:
        match = _JOB_TYPE_PATTERN.search(text or "")
        return match.group(0).strip() if match else None

    def _is_job_like_url(self, src: str) -> bool:
        if not src:
            return False
        if self._is_non_job_url(src):
            return False
        return bool(_JOB_URL_HINT.search(src))

    @staticmethod
    def _is_non_job_url(src: str) -> bool:
        return bool(_NON_JOB_URL.search(src or ""))
