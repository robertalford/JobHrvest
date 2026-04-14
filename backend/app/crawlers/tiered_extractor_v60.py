"""
Tiered Extraction Engine v6.0 — Consolidated "clean slate" extractor.

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
    r"director|chef|nurse|teacher|operator|supervisor|"
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
    r"lowongan\s+kerja(?:\s+\w+)?|"
    # FP mitigation: nav/listing page section headings
    r"open\s+jobs?|current\s+jobs?|featured\s+jobs?|latest\s+jobs?|"
    r"browse\s+(?:all\s+)?jobs?|search\s+jobs?|view\s+(?:all\s+)?jobs?|"
    r"career\s+opportunities|"
    # FP mitigation: blog/CMS artifacts
    r"leave\s+a\s+comment.*|related\s+posts?.*|read\s+the\s+full\s+article|"
    r"share\s+this\s+post|post\s+a\s+comment|comments?\s+(?:are\s+)?closed|"
    r"career\s+spotlight:.*|"
    # FP mitigation: contact/admin info parsed as titles
    r"applications?\s+can\s+be\s+submitted\s+to.*|"
    r"contact\s+[A-Z][a-z]+.*|"  # "contact Board Administrator, Claire..."
    r"students?\s+and\s+industrial\s+training|"
    # FP mitigation: department/category names (not jobs)
    r"supply\s+chain\s*&?\s*operations?|people\s*&?\s*culture|"
    r"it\s*&?\s*technology|finance\s*&?\s*accounting|"
    r"human\s+resources?|marketing\s*&?\s*(?:sales|communications?)|"
    r"career\s+menu|"
    # FP mitigation: non-job page content (addresses, products)
    r"alamat\s+kantor|model\s+incubator"
    r")$",
    re.IGNORECASE,
)

_GENERIC_LISTING_LABEL = re.compile(
    r"^(?:jobs?|careers?|vacancies|vacantes?|job\s+openings?|open\s+positions?|"
    r"join\s+our\s+team|open\s+roles?|all\s+jobs?|current\s+vacancies|"
    r"jobs?\s+vacancy|lowongan(?:\s+kerja(?:\s+\w+)?)?|"
    # Additional section/listing labels
    r"open\s+jobs?|current\s+jobs?|featured\s+jobs?|latest\s+jobs?|"
    r"our\s+vacancies|available\s+positions?|career\s+opportunities|"
    r"explore\s+(?:our\s+)?(?:jobs?|careers?|opportunities))$",
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
    r"/wp-json/oembed)",
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
    r"^(?:home|about|contact|company|our\s+company|our\s+culture|our\s+values|blog|news|events?|"
    # Department/function names (not job titles)
    r"supply\s+chain|operations|people\s+&\s+culture|it\s+&\s+technology|"
    r"finance|human\s+resources|marketing|sales|legal|engineering|"
    r"customer\s+(?:service|support)|research\s+&\s+development|"
    r"project\s+base\s+solution|career\s+menu)$",
    re.IGNORECASE,
)

_PHONE_TITLE = re.compile(r"^(?:\+?\d[\d\s().-]{6,}|\d{2,4}\s?\d{3,5}\s?\d{3,5})$")

_COMPANY_CAREER_LABEL = re.compile(
    r"^[a-z0-9&.,'() -]{2,60}\s+careers?$",
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
    if "workday" in lower_url or "myworkdayjobs" in lower_url:
        return "workday"
    if "lever.co" in lower_url:
        return "lever"
    if "breezy.hr" in lower_url:
        return "breezy"
    if "dayforcehcm.com" in lower_url or "candidateportal" in lower_url:
        return "dayforce"
    if "recruiting.ultipro.com" in lower_url or "ultipro.com" in lower_url:
        return "ultipro"
    if "jobs.growhire.com" in lower_url or "growhire.com" in lower_url:
        return "growhire"
    if "acquiretm.com" in lower_url:
        return "acquiretm"

    return None


class TieredExtractorV60(TieredExtractorV16):
    """v6.0 consolidated extractor: v1.6 primary + structured fallbacks + dedicated ATS extractors."""

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
            logger.warning("v6.0 parent extractor timeout for %s", url)
        except Exception:
            logger.exception("v6.0 parent extractor failed for %s", url)
        parent_jobs = self._dedupe(parent_jobs or [], url)

        # Phase 2: Structured data extraction (always runs — fast)
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

        # Phase 3: Dedicated ATS extractors (only if platform detected)
        if ats_platform:
            ats_jobs = await self._extract_ats_specific(ats_platform, url, working_html)
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
                logger.warning("v6.0 enrichment timeout for %s", url)
            except Exception:
                logger.exception("v6.0 enrichment failed for %s", url)
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
            elif platform == "breezy":
                return await self._extract_breezy(url, html)
            elif platform == "dayforce":
                return await self._extract_dayforce(url, html)
            elif platform == "ultipro":
                return await self._extract_ultipro(url, html)
            elif platform == "growhire":
                return await self._extract_growhire(url, html)
            elif platform == "acquiretm":
                return self._extract_acquiretm(url, html)
            # workday and lever fall through to heuristic extraction
        except asyncio.TimeoutError:
            logger.warning("v6.0 ATS %s timeout for %s", platform, url)
        except Exception:
            logger.exception("v6.0 ATS %s failed for %s", platform, url)
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
                        if not self._is_valid_title_v60(title):
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
                    if not self._is_valid_title_v60(title):
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
            logger.exception("v6.0 Greenhouse API failed for slug %s", slug)

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

            if not self._is_valid_title_v60(title):
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

    # --- MartianLogic / MyRecruitmentPlus ---

    async def _extract_martian(self, url: str, html: str) -> list[dict]:
        """MartianLogic/MyRecruitmentPlus: streamlined API probing."""
        context = self._extract_martian_context(html, url)
        client_code = context.get("client_code", "")
        if not client_code:
            return []

        parsed = urlparse(url)
        base = f"{parsed.scheme or 'https'}://{parsed.netloc}"

        # Build focused probe list (not 50+ permutations like later versions)
        probes = [
            f"{base}/{client_code}/?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"{base}/{client_code}/embed-jobs?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"{base}/embed-jobs?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc&client={client_code}",
            f"{base}/?client={client_code}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
        ]

        # Add recruiter-scoped variants if available
        recruiter_id = context.get("recruiter_id", "")
        if recruiter_id:
            probes.append(
                f"{base}/api/job-ads?recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true"
            )
            probes.append(
                f"{base}/api/search?recruiterId={recruiter_id}&pageNumber=1&pageSize=50"
            )

        jobs: list[dict] = []
        try:
            async with httpx.AsyncClient(
                timeout=6, follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/html,*/*"},
            ) as client:
                miss_streak = 0
                for probe_url in probes[:8]:
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
                        if len(jobs) >= 3:
                            # Try pagination
                            for page_num in range(2, 5):
                                paged = re.sub(r"pageNumber=\d+", f"pageNumber={page_num}", probe_url)
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
            logger.exception("v6.0 MartianLogic probing failed for %s", url)

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

        return self._dedupe(jobs, page_url)

    # --- Breezy HR ---

    async def _extract_breezy(self, url: str, html: str) -> list[dict]:
        """Breezy HR: try JSON API first, fall back to HTML parsing."""
        parsed = urlparse(url)
        # Extract company slug from subdomain (COMPANY.breezy.hr)
        host = parsed.netloc.lower()
        slug = host.split(".")[0] if "breezy.hr" in host else None
        if not slug:
            return []

        jobs: list[dict] = []
        # Try JSON API
        api_url = f"https://{slug}.breezy.hr/json"
        try:
            async with httpx.AsyncClient(
                timeout=8, follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            ) as client:
                resp = await client.get(api_url)
                if resp.status_code == 200:
                    data = resp.json()
                    items = data if isinstance(data, list) else data.get("jobs", data.get("positions", []))
                    if isinstance(items, list):
                        for item in items[:MAX_JOBS_PER_PAGE]:
                            title = str(item.get("name") or item.get("title") or "").strip()
                            if not self._is_valid_title_v60(title):
                                continue
                            job_id = str(item.get("id") or item.get("friendly_id") or "").strip()
                            detail_url = f"https://{slug}.breezy.hr/p/{job_id}" if job_id else url
                            location_parts = []
                            for loc_key in ("city", "state", "country"):
                                loc_val = item.get("location", {})
                                if isinstance(loc_val, dict):
                                    v = str(loc_val.get(loc_key) or "").strip()
                                elif isinstance(loc_val, str):
                                    v = loc_val.strip() if loc_key == "city" else ""
                                else:
                                    v = ""
                                if v:
                                    location_parts.append(v)
                            location = ", ".join(location_parts) if location_parts else None
                            emp_type = str(item.get("type", {}).get("name", "") if isinstance(item.get("type"), dict) else item.get("type") or "").strip() or None
                            desc = str(item.get("description") or "").strip()
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
                                "extraction_method": "ats_breezy_api",
                                "extraction_confidence": 0.93,
                            })
        except Exception:
            logger.debug("v6.0 Breezy API failed for %s", slug)

        # Fall back to HTML parsing if API returned nothing
        if not jobs and html:
            root = _parse_html(html)
            if root is not None:
                for li in root.xpath("//li[contains(@class,'position')]"):
                    h2 = li.xpath(".//h2")
                    title = self._normalize_title(_text(h2[0])) if h2 else ""
                    if not self._is_valid_title_v60(title):
                        continue
                    link = li.xpath(".//a[@href]")
                    href = link[0].get("href") if link else None
                    source_url = _resolve_url(href, url) if href else url
                    loc_el = li.xpath(".//*[contains(@class,'location')]")
                    location = _text(loc_el[0]).strip() if loc_el else None
                    dept_el = li.xpath(".//*[contains(@class,'department')]")
                    dept = _text(dept_el[0]).strip() if dept_el else None
                    type_el = li.xpath(".//*[contains(@class,'type')]")
                    emp_type = _text(type_el[0]).strip() if type_el else None
                    jobs.append({
                        "title": title,
                        "source_url": source_url or url,
                        "location_raw": location,
                        "salary_raw": None,
                        "employment_type": emp_type,
                        "description": dept,
                        "extraction_method": "ats_breezy_html",
                        "extraction_confidence": 0.85,
                    })

        return self._dedupe(jobs, url)

    # --- Dayforce HCM ---

    async def _extract_dayforce(self, url: str, html: str) -> list[dict]:
        """Dayforce HCM: try Search API first, fall back to HTML parsing."""
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"

        # Extract portal slug from URL path: /CandidatePortal/en-AU/SLUG/ or /CandidatePortal/SLUG/
        slug = None
        m = re.search(r"/CandidatePortal/(?:[a-z]{2}-[A-Z]{2}/)?([^/?#]+)", url, re.IGNORECASE)
        if m:
            slug = m.group(1)

        # Also try extracting from any path component after dayforcehcm.com
        if not slug:
            path_parts = [p for p in parsed.path.split("/") if p and p.lower() != "candidateportal"]
            # Skip locale parts like en-AU
            for part in path_parts:
                if not re.match(r"^[a-z]{2}-[A-Z]{2}$", part):
                    slug = part
                    break

        if not slug:
            return []

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
                            # Also try GET
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
            logger.debug("v6.0 Dayforce API failed for %s", url)

        # Fall back to HTML card parsing
        if not jobs and html:
            root = _parse_html(html)
            if root is not None:
                # Dayforce uses repeating card elements for job listings
                cards = root.xpath(
                    "//*[contains(@class,'job-card') or contains(@class,'job-listing') or "
                    "contains(@class,'job-item') or contains(@class,'search-result')]"
                )
                if not cards:
                    # Try broader approach: divs/articles with job-like links
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

    # --- UltiPro ---

    async def _extract_ultipro(self, url: str, html: str) -> list[dict]:
        """UltiPro (recruiting.ultipro.com): try API first, fall back to HTML."""
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"

        # Extract board slug from /jobboard/SLUG/ pattern
        slug = None
        m = re.search(r"/jobboard/([^/?#]+)", url, re.IGNORECASE)
        if m:
            slug = m.group(1)

        jobs: list[dict] = []
        if slug:
            # Try API endpoints
            api_urls = [
                f"{base}/api/jobs?locationId=&industryId=&jobFunctionId=&pageSize=100&boardCode={slug}",
                f"{base}/api/{slug}/jobs?pageSize=100",
                f"{base}/jobboard/api/{slug}/jobs?pageSize=100",
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

                        items = data if isinstance(data, list) else data.get("jobs", data.get("opportunities", data.get("requisitions", [])))
                        if not isinstance(items, list):
                            continue
                        for item in items[:MAX_JOBS_PER_PAGE]:
                            title = str(item.get("title") or item.get("Title") or item.get("jobTitle") or "").strip()
                            if not self._is_valid_title_v60(title):
                                continue
                            job_id = str(item.get("id") or item.get("Id") or item.get("jobId") or "").strip()
                            detail_url = f"{base}/jobboard/{slug}/OpportunityDetail?opportunityId={job_id}" if job_id else url
                            location = str(item.get("location") or item.get("Location") or item.get("city") or "").strip() or None
                            emp_type = str(item.get("employmentType") or item.get("jobType") or "").strip() or None
                            desc = str(item.get("description") or item.get("shortDescription") or "").strip()
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
                                "extraction_method": "ats_ultipro_api",
                                "extraction_confidence": 0.91,
                            })
                        if jobs:
                            break
            except Exception:
                logger.debug("v6.0 UltiPro API failed for %s", url)

        # Fall back to HTML parsing
        if not jobs and html:
            root = _parse_html(html)
            if root is not None:
                # UltiPro uses opportunity-* classes or table rows for listings
                cards = root.xpath(
                    "//*[contains(@class,'opportunity') or contains(@class,'job-card') or "
                    "contains(@class,'job-item') or contains(@class,'upc-job')]"
                )
                if not cards:
                    cards = root.xpath("//tr[.//a[contains(@href,'OpportunityDetail') or contains(@href,'opportunityId')]]")
                for card in cards[:MAX_JOBS_PER_PAGE]:
                    heading = card.xpath(".//h1|.//h2|.//h3|.//h4|.//*[contains(@class,'title')]|.//a[@href]")
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
                        "extraction_method": "ats_ultipro_html",
                        "extraction_confidence": 0.78,
                    })

        return self._dedupe(jobs, url)

    # --- GrowHire ---

    async def _extract_growhire(self, url: str, html: str) -> list[dict]:
        """GrowHire (jobs.growhire.com): try JSON API first, fall back to HTML."""
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"

        jobs: list[dict] = []
        # Try API endpoint
        api_url = f"{base}/api/jobs"
        try:
            async with httpx.AsyncClient(
                timeout=8, follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            ) as client:
                resp = await client.get(api_url)
                if resp.status_code == 200:
                    data = resp.json()
                    items = data if isinstance(data, list) else data.get("jobs", data.get("data", data.get("results", [])))
                    if isinstance(items, list):
                        for item in items[:MAX_JOBS_PER_PAGE]:
                            title = str(item.get("title") or item.get("name") or item.get("jobTitle") or "").strip()
                            if not self._is_valid_title_v60(title):
                                continue
                            job_id = str(item.get("id") or item.get("slug") or "").strip()
                            detail_url = str(item.get("url") or item.get("applyUrl") or "").strip()
                            if not detail_url and job_id:
                                detail_url = f"{base}/job/{job_id}"
                            detail_url = _resolve_url(detail_url, url) if detail_url else url
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
        except Exception:
            logger.debug("v6.0 GrowHire API failed for %s", url)

        # Fall back to HTML parsing
        if not jobs and html:
            root = _parse_html(html)
            if root is not None:
                # GrowHire typically renders job cards in list items or divs
                cards = root.xpath(
                    "//*[contains(@class,'job-card') or contains(@class,'job-listing') or "
                    "contains(@class,'job-item') or contains(@class,'position-card')]"
                )
                if not cards:
                    # Broader fallback: any link-bearing container with job URLs
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

    # --- AcquireTM ---

    def _extract_acquiretm(self, url: str, html: str) -> list[dict]:
        """AcquireTM: parse job listings from HTML tables/cards."""
        if not html:
            return []

        root = _parse_html(html)
        if root is None:
            return []

        jobs: list[dict] = []
        # AcquireTM uses table rows or card-based layouts
        # Try table rows first
        rows = root.xpath(
            "//table//tr[.//a[@href]]|"
            "//*[contains(@class,'job-row') or contains(@class,'job-listing') or "
            "contains(@class,'job-item') or contains(@class,'posting')]"
        )
        if not rows:
            # Broader: any repeating element with links containing job-like hrefs
            rows = root.xpath(
                "//tr[.//a[contains(@href,'job') or contains(@href,'position') or contains(@href,'opening')]]|"
                "//div[contains(@class,'row') and .//a[contains(@href,'job') or contains(@href,'position')]]"
            )

        for row in rows[:MAX_JOBS_PER_PAGE]:
            # Title: heading or first link text
            heading = row.xpath(".//h1|.//h2|.//h3|.//h4|.//*[contains(@class,'title')]")
            if heading:
                title = self._normalize_title(_text(heading[0]))
            else:
                links = row.xpath(".//a[@href]")
                title = self._normalize_title(_text(links[0])) if links else ""

            if not self._is_valid_title_v60(title):
                continue

            link = row.xpath(".//a[@href]")
            href = link[0].get("href") if link else None
            source_url = _resolve_url(href, url) if href else url

            row_text = _text(row)
            jobs.append({
                "title": title,
                "source_url": source_url or url,
                "location_raw": self._extract_location(row_text),
                "salary_raw": self._extract_salary(row_text),
                "employment_type": self._extract_type(row_text),
                "description": row_text[:5000] if len(row_text) > 60 else None,
                "extraction_method": "ats_acquiretm",
                "extraction_confidence": 0.82,
            })

        return self._dedupe(jobs, url)

    # ==================================================================
    # PLAYWRIGHT OVERRIDE — improved JS rendering
    # ==================================================================

    async def _render_with_playwright_v13(self, url: str) -> Optional[str]:
        """Override parent: longer wait, cookie dismissal, scroll for lazy loading."""
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                try:
                    await page.goto(url, wait_until="networkidle", timeout=30000)

                    # Dismiss cookie banners before extraction
                    cookie_selectors = [
                        "button:has-text('Accept')",
                        "button:has-text('Accept All')",
                        "button:has-text('Accept Cookies')",
                        "button:has-text('OK')",
                        "button:has-text('I Agree')",
                        "button:has-text('Got it')",
                        "[id*='cookie'] button",
                        "[class*='cookie'] button",
                        "[id*='consent'] button",
                        "[class*='consent'] button",
                    ]
                    for selector in cookie_selectors:
                        try:
                            btn = page.locator(selector).first
                            if await btn.is_visible(timeout=500):
                                await btn.click(timeout=1000)
                                await page.wait_for_timeout(500)
                                break
                        except Exception:
                            continue

                    # Scroll the page to trigger lazy loading
                    try:
                        for _ in range(3):
                            await page.evaluate("window.scrollBy(0, window.innerHeight)")
                            await page.wait_for_timeout(800)
                        # Scroll back to top
                        await page.evaluate("window.scrollTo(0, 0)")
                    except Exception:
                        pass

                    # Wait 5 seconds after networkidle for JS rendering
                    await page.wait_for_timeout(5000)
                    return await page.content()
                except Exception as e:
                    logger.debug("v6.0 Playwright failed for %s: %s", url, e)
                    return None
                finally:
                    await browser.close()
        except ImportError:
            return None
        except Exception:
            return None

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
        if not self._is_valid_title_v60(title):
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
            if not self._is_valid_title_v60(text):
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

            title = self._normalize_title(_text(title_el[0]))
            if not self._is_valid_title_v60(title):
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
            headings = container.xpath(".//h2 | .//h3 | .//h4")
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
                if not self._is_valid_title_v60(title):
                    continue
                if not self._title_has_job_signal(title):
                    continue

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
                    "description": container_text[:5000] if len(container_text) > 120 else None,
                    "extraction_method": "tier2_heading_rows",
                    "extraction_confidence": 0.66,
                })

            if len(local_jobs) >= 2:
                jobs.extend(local_jobs)

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
                if not self._is_valid_title_v60(title):
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

            logger.debug("v6.0 candidate %s: jobs=%d score=%.2f valid=%s", label, len(deduped), score, valid)

            if valid and score > best_score:
                best_label = label
                best_jobs = deduped
                best_score = score

        if best_jobs:
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

    # ==================================================================
    # VALIDATION & SCORING (from v2.6)
    # ==================================================================

    def _passes_jobset_validation(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if self._is_valid_title_v60(t)]
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
            return title_hits >= 1 and (url_hits >= 1 or apply_hits >= 1 or title_hits >= 2)

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
            if not self._is_valid_title_v60(title):
                continue

            source_url = (job.get("source_url") or "").strip()
            if not source_url:
                source_url = page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]
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

    def _is_valid_title_v60(self, title: str) -> bool:
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

        words = t.split()
        if len(words) > 14:
            return False

        # v2.9 cherry-pick: use boundary-aware pattern instead of raw substring
        if _INLINE_BOILERPLATE.search(low):
            if len(words) <= 5 and not self._title_has_job_signal(t):
                return False

        if len(words) <= 1 and not self._title_has_job_signal(t):
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
