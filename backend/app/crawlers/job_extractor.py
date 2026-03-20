"""
Job Extractor — Stage 3 & 4 of the pipeline.

Full multi-method extraction with cross-validation, location/salary parsing, and tagging.

Priority order:
  3a. Schema.org / structured data (extruct) — highest accuracy
  3b. ATS-specific extractors (API or DOM)
  3c. Template-based extraction (learned selectors) — fast
  3d. LLM extraction (instructor + Ollama) — flexible
  3e. Repeating block detection — structural fallback
"""

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
import extruct
from bs4 import BeautifulSoup
from markdownify import markdownify

from app.core.config import settings
from app.crawlers.domain_blocklist import assert_not_blocked
from app.crawlers.http_client import ResilientHTTPClient
from app.crawlers.pagination import PaginationHandler
from app.extractors.cross_validator import CrossValidator
from app.models.career_page import CareerPage
from app.models.company import Company
from app.models.job import Job, JobTag
from app.utils.location_parser import location_normalizer
from app.utils.salary_parser import salary_normalizer
from app.utils.tag_extractor import tag_extractor

logger = logging.getLogger(__name__)


class JobExtractor:
    def __init__(self, db):
        self.db = db
        self.client = ResilientHTTPClient()
        self.pagination = PaginationHandler(self.client)
        self.validator = CrossValidator(db)

    async def extract(self, company: Company, career_page: CareerPage) -> list[Job]:
        """Extract all jobs from a career page with full pagination support."""
        assert_not_blocked(career_page.url)

        try:
            if career_page.requires_js_rendering:
                html = await self.client.get_rendered(career_page.url)
            else:
                resp = await self.client.get(career_page.url)
                if resp.status_code == 304:
                    # ETag confirmed page unchanged — refresh active jobs and exit
                    logger.info(f"304 Not Modified: {career_page.url}")
                    await self._refresh_active_jobs(company, career_page)
                    return []
                html = resp.text
        except Exception as e:
            logger.error(f"Failed to fetch career page {career_page.url}: {e}")
            return []

        # Content hash check — skip full extraction if page unchanged
        content_hash = hashlib.sha256(html.encode()).hexdigest()
        if career_page.last_content_hash == content_hash:
            logger.info(f"No content change for {career_page.url}, skipping extraction")
            # Still update last_seen for active jobs
            await self._refresh_active_jobs(company, career_page)
            return []

        all_jobs_data: list[dict] = []
        seen_urls: set[str] = set()

        # Iterate through all pages (handles pagination)
        async for page_url, page_html in self.pagination.iter_pages(
            career_page.url, html, requires_js=career_page.requires_js_rendering
        ):
            page_jobs = await self._extract_from_page(company, career_page, page_url, page_html)
            for job_data in page_jobs:
                url_key = job_data.get("source_url", "") or job_data.get("external_id", "")
                if url_key and url_key not in seen_urls:
                    seen_urls.add(url_key)
                    all_jobs_data.append(job_data)

        # Persist jobs
        saved = []
        for job_data in all_jobs_data:
            # Enrich with location and salary normalization
            job_data = self._enrich(job_data, company)
            job = await self._upsert_job(company, career_page, job_data)
            if job:
                await self._save_tags(job, job_data)
                saved.append(job)

        # Update page metadata
        career_page.last_content_hash = content_hash
        career_page.last_crawled_at = datetime.now(timezone.utc)
        career_page.last_extraction_at = datetime.now(timezone.utc)
        await self.db.commit()

        logger.info(f"Extracted {len(saved)} jobs from {career_page.url}")
        return saved

    async def _extract_from_page(self, company: Company, career_page: CareerPage, url: str, html: str) -> list[dict]:
        """Run all extraction methods on a single page and cross-validate."""
        methods_results: list[dict] = []

        # 3a: Schema.org structured data (highest priority, fast)
        structured = self._extract_structured_data(html, url)
        if structured:
            methods_results.extend(structured)
            logger.debug(f"schema_org: {len(structured)} jobs from {url}")

        # 3b: ATS-specific extractor (fast, uses public APIs)
        if company.ats_platform and company.ats_platform not in ("unknown", "custom", None):
            ats_jobs = await self._extract_ats(company.ats_platform, url, html)
            if ats_jobs:
                methods_results.extend(ats_jobs)
                logger.debug(f"ats({company.ats_platform}): {len(ats_jobs)} jobs from {url}")

        # 3c: Template-based extraction (fast learned selectors)
        template_jobs = await self._extract_with_template(company, career_page, html)
        if template_jobs:
            methods_results.extend(template_jobs)

        # 3d: Fast heuristic single-job extractor — always try if other methods found nothing.
        # Works on individual job-detail pages; title validation rejects generic pages.
        if not methods_results:
            heuristic = self._extract_heuristic_single_job(html, url)
            if heuristic:
                methods_results.extend(heuristic)
                logger.debug(f"heuristic_single: {len(heuristic)} jobs from {url}")

        # 3e: Structural repeating block detection (listing pages with many jobs)
        if not methods_results:
            structural = self._extract_repeating_blocks(html, url)
            methods_results.extend(structural)
            logger.debug(f"structural: {len(structural)} jobs from {url}")

        # 3f: LLM extraction — for pages with no_structure status, run inline (3B→8B escalation).
        # For normal pages, queue async to ML worker to avoid blocking crawl workers.
        if not methods_results:
            # Inline LLM: run immediately for pages known to have no structural mapping
            page_status = getattr(career_page, "site_status", None)
            if page_status in ("no_structure_new", "no_structure_broken"):
                inline = await self._extract_llm_inline(url, html, career_page)
                if inline:
                    methods_results.extend(inline)
                    logger.debug(f"llm_inline: {len(inline)} jobs from {url}")
            if not methods_results:
                # Async LLM for any remaining zero-result pages
                try:
                    from app.tasks.ml_tasks import llm_extract_page
                    from app.models.career_page import CareerPage as CP
                    from sqlalchemy import select
                    page_record = await self.db.scalar(select(CP).where(CP.url == url))
                    if page_record:
                        llm_extract_page.apply_async(args=[str(page_record.id)], queue="ml", countdown=5)
                        logger.debug(f"Queued async LLM extraction for {url}")
                except Exception as e:
                    logger.debug(f"Could not queue LLM extraction for {url}: {e}")

        # 3g: Description enrichment pass — for any job with a missing or very short
        # description, run the DescriptionExtractor pyramid (layers 0-4, no LLM inline)
        # to fill the gap using the more sophisticated extraction logic.
        if methods_results:
            methods_results = await self._enrich_descriptions_inline(
                methods_results, html, url, company
            )

        # Cross-validate: merge results from multiple methods (fills in missing fields,
        # resolves disagreements by trust rank, logs comparison for analytics).
        if len(methods_results) > 1:
            merged = self._merge_by_url(methods_results)
            return merged
        return methods_results

    async def _enrich_descriptions_inline(
        self,
        results: list[dict],
        html: str,
        url: str,
        company: Company,
    ) -> list[dict]:
        """
        Post-extraction description enrichment (inline, fast — layers 0-4 only).

        For each job in results with a missing or short description (< 300 chars),
        run the DescriptionExtractor pyramid on the same page HTML. Uses up to
        layer 4 (content-density DOM analysis) — no LLM inline, stays fast.
        If a better description is found, it's applied to all jobs on this page.
        """
        needs_desc = any(len(r.get("description") or "") < 300 for r in results)
        if not needs_desc:
            return results

        try:
            from app.extractors.description_extractor import DescriptionExtractor
            extractor = DescriptionExtractor(db=self.db)
            desc_result = await extractor.extract(
                html=html,
                url=url,
                ats_platform=getattr(company, "ats_platform", None),
                max_layer=4,  # Layers 0-4 only — keeps inline crawl fast
            )
            if desc_result and len(desc_result.text) >= 100:
                for r in results:
                    current = r.get("description") or ""
                    if len(desc_result.text) > len(current):
                        r["description"] = desc_result.text
                        logger.debug(
                            f"  [desc_enrichment] applied {desc_result.layer_name} "
                            f"description ({len(desc_result.text)} chars) to {url}"
                        )
        except Exception as e:
            logger.debug(f"  [desc_enrichment] error: {e}")

        return results

    # ── URL and page-type detection ───────────────────────────────────────────

    _SINGLE_JOB_PATTERNS = [
        r'/jobs?/[^/?]+/?$',
        r'/careers?/[^/?]+/?$',
        r'/positions?/[^/?]+/?$',
        r'/vacancies?/[^/?]+/?$',
        r'/openings?/[^/?]+/?$',
        r'/apply/[^/?]+/?$',
        r'/job-posting/[^/?]+/?$',
        r'/job[-_]detail/[^/?]+/?$',
        r'/jobdetails?[/?]',
        r'/vacancy/[^/?]+/?$',
        r'/role/[^/?]+/?$',
        r'/employment/[^/?]+/?$',
        r'/opportunity/[^/?]+/?$',
        r'/joining/[^/?]+/?$',
        r'/join/[^/?]+/?$',
    ]

    @staticmethod
    def _safe_str(val, default: str = "") -> str:
        """Coerce any schema.org value to a plain string (handles @value dicts and lists)."""
        if val is None:
            return default
        if isinstance(val, dict):
            return str(val.get("@value") or val.get("value") or val.get("name") or default)
        if isinstance(val, list):
            return str(val[0]) if val else default
        return str(val)

    def _url_looks_like_single_job(self, url: str) -> bool:
        """Heuristic: does this URL look like a single job posting page?"""
        path = urlparse(url).path.lower()
        return any(re.search(p, path) for p in self._SINGLE_JOB_PATTERNS)

    def _extract_heuristic_single_job(self, html: str, url: str) -> list[dict]:
        """Fast heuristic extractor for single job-posting detail pages.

        Works when schema.org isn't available by reading:
        - og:title / h1 / page title → job title
        - og:description / main content block → description
        - meta / visible "Location:" text → location
        - meta / visible "Salary:" text → salary
        """
        soup = BeautifulSoup(html, "lxml")

        # ── Title ─────────────────────────────────────────────────────────────
        title = None
        og_title = soup.find("meta", property="og:title")
        if og_title:
            title = (og_title.get("content") or "").strip()
        if not title:
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(separator=" ", strip=True)
        if not title:
            title_el = soup.find("title")
            if title_el:
                raw = title_el.get_text(strip=True)
                # Strip " | Company Name" / " - Company Name" suffixes
                for sep in (" | ", " - ", " — ", " · ", " : "):
                    if sep in raw:
                        raw = raw.split(sep)[0].strip()
                title = raw

        if not title or len(title) < 3:
            return []
        # Skip generic page titles that aren't job titles
        _GENERIC = {
            "careers", "jobs", "home", "about", "contact", "vacancies",
            "apply", "opportunities", "positions", "search jobs", "job search",
            "find a job", "employment", "recruitment", "work with us", "join us",
            "join our team", "our team", "team", "staff", "404", "error",
            "page not found",
        }
        title_clean = title.lower().strip()
        if title_clean in _GENERIC:
            return []
        # Reject titles that are too long to be a job title (likely navigation page)
        if len(title) > 150:
            return []
        # Reject if title starts with "Search" or "Browse" (listing pages)
        if re.match(r'^(search|browse|find|all|latest)\b', title_clean):
            return []

        # ── Description ───────────────────────────────────────────────────────
        # Priority order (highest → lowest confidence):
        #  1. Specific job-description containers (common across ATS/career sites)
        #  2. <main> block with noise removed
        #  3. <article> block
        #  4. Body text with nav/header/footer removed
        #  5. og:description as absolute last resort (usually truncated ~160 chars)
        description = ""

        # 1. Try targeted job-description selectors
        _JD_SELECTORS = [
            "[data-job-description]", "[data-testid*='description']",
            "[data-automation*='description']", ".job-description",
            ".job-details__description", ".job-body", ".description__text",
            "#job-description", "#jobDescription", "#job-details",
            "[itemprop='description']", ".careers-description",
            ".posting-description", ".jd-body", ".content-description",
            ".jobsearch-jobDescriptionText",  # Indeed style
        ]
        for sel in _JD_SELECTORS:
            el = soup.select_one(sel)
            if el:
                candidate = el.get_text(separator="\n", strip=True)
                if len(candidate) >= 100:
                    description = candidate[:6000]
                    break

        # 2. <main> block — strip inner nav/aside/header noise before extracting
        if not description:
            main = (soup.find("main") or soup.find(id="main-content")
                    or soup.find(id="main"))
            if main:
                import copy
                main_copy = copy.copy(main)
                for noise in main_copy(["nav", "aside", "header", "footer",
                                        "script", "style", "[role='navigation']",
                                        "[role='banner']", "[role='complementary']"]):
                    noise.decompose()
                candidate = main_copy.get_text(separator="\n", strip=True)
                if len(candidate) >= 100:
                    description = candidate[:6000]

        # 3. <article> block
        if not description:
            article = soup.find("article")
            if article:
                candidate = article.get_text(separator="\n", strip=True)
                if len(candidate) >= 100:
                    description = candidate[:6000]

        # 4. Body text with structural noise removed
        if not description:
            import copy as _copy
            body_soup = _copy.copy(soup)
            for tag in body_soup(["nav", "header", "footer", "script", "style",
                                   "aside", "[role='navigation']", "[role='banner']"]):
                tag.decompose()
            candidate = body_soup.get_text(separator="\n", strip=True)
            if len(candidate) >= 50:
                description = candidate[:6000]

        # 5. og:description — last resort, typically ~160 chars
        if not description:
            og_desc = soup.find("meta", property="og:description")
            if og_desc:
                description = (og_desc.get("content") or "").strip()

        # ── Location ──────────────────────────────────────────────────────────
        location_raw = None

        # 1. Structured meta tags
        for meta_name in ("geo.placename", "location", "job:location", "position:location"):
            m = soup.find("meta", attrs={"name": meta_name})
            if m:
                location_raw = (m.get("content") or "").strip() or None
                break

        # 2. Microdata / itemprop="jobLocation" or "addressLocality"
        if not location_raw:
            for sel in (
                "[itemprop='jobLocation']", "[itemprop='addressLocality']",
                "[itemtype*='JobPosting'] [itemprop='name']",
            ):
                el = soup.select_one(sel)
                if el:
                    txt = el.get_text(strip=True)
                    if txt and len(txt) < 100:
                        location_raw = txt
                        break

        # 3. Common CSS classes used by career sites / ATS platforms
        if not location_raw:
            _LOC_SELECTORS = [
                "[class*='job-location']", "[class*='jobLocation']",
                "[class*='job_location']", "[class*='position-location']",
                "[class*='location-text']", "[class*='job-city']",
                "[data-testid*='location']", "[data-field='location']",
                "[data-automation*='location']", "[aria-label*='location' i]",
                ".sort-by-location",  # Lever HTML
                ".jv-job-list-location",  # Jobvite
                ".job-location", ".location",
            ]
            for sel in _LOC_SELECTORS:
                el = soup.select_one(sel)
                if el:
                    txt = el.get_text(strip=True)
                    if txt and 2 < len(txt) < 120:
                        location_raw = txt
                        break

        # 4. Definition list / table patterns: "Location" label → value
        if not location_raw:
            for dt in soup.find_all(["dt", "th", "strong", "b", "label"]):
                dt_text = dt.get_text(strip=True).lower()
                if dt_text in ("location", "location:", "where", "city", "work location"):
                    sibling = dt.find_next_sibling(["dd", "td"])
                    if sibling:
                        txt = sibling.get_text(strip=True)
                        if txt and len(txt) < 120:
                            location_raw = txt
                            break
                    # Also check adjacent <span> or next text node
                    parent = dt.parent
                    if parent:
                        spans = parent.find_all("span")
                        if len(spans) >= 2:
                            txt = spans[-1].get_text(strip=True)
                            if txt and len(txt) < 120:
                                location_raw = txt
                                break

        # 5. Inline "Location: value" text patterns
        if not location_raw:
            for text_node in soup.find_all(string=True):
                t = text_node.strip()
                if re.match(r'^location\s*:', t, re.IGNORECASE) and len(t) < 120:
                    location_raw = t.split(":", 1)[-1].strip()
                    break

        # 6. Scan page text and description for city/region/country mentions
        if not location_raw:
            _scan_text = description or soup.get_text(separator=" ", strip=True)[:3000]
            _LOC_SCAN = [
                # AU cities
                r'\b(Sydney|Melbourne|Brisbane|Perth|Adelaide|Canberra|Gold Coast|Newcastle|'
                r'Wollongong|Hobart|Darwin|Geelong|Townsville|Cairns|Toowoomba|'
                r'Ballarat|Bendigo|Albury|Launceston|Mackay)\b(?:[,\s]+(?:NSW|VIC|QLD|WA|SA|ACT|NT|TAS|Australia))?',
                # AU state abbreviations (state-only)
                r'\b(?:NSW|VIC|QLD|WA|SA|ACT|NT|TAS)\b',
                # Global cities common in AU-market crawls
                r'\b(Auckland|Wellington|Christchurch|Singapore|Kuala Lumpur|Hong Kong|'
                r'Manila|Jakarta|Bangkok|London|New York|Texas|California|Chicago)\b',
                # Remote/hybrid
                r'\b(?:remote|work from home|wfh|hybrid|australia[-\s]wide)\b',
            ]
            for pat in _LOC_SCAN:
                m = re.search(pat, _scan_text, re.IGNORECASE)
                if m:
                    location_raw = m.group(0).strip()
                    break

        # ── Employment type ────────────────────────────────────────────────────
        employment_type = None
        _EMP_PATTERNS = [
            (r'\bfull[- ]time\b', 'Full-time'),
            (r'\bpart[- ]time\b', 'Part-time'),
            (r'\bcontract(or|ing)?\b', 'Contract'),
            (r'\bcasual\b', 'Casual'),
            (r'\btemporary\b', 'Temporary'),
            (r'\bpermanent\b', 'Permanent'),
            (r'\bfixed[- ]term\b', 'Fixed-term'),
            (r'\bfreelance\b', 'Freelance'),
            (r'\binternship\b', 'Internship'),
        ]
        scan_text = description or ""
        for pat, label in _EMP_PATTERNS:
            if re.search(pat, scan_text, re.IGNORECASE):
                employment_type = label
                break

        # ── Salary ────────────────────────────────────────────────────────────
        salary_raw = None
        for text_node in soup.find_all(string=True):
            t = text_node.strip()
            if re.match(r'^(salary|compensation|remuneration|package)\s*:', t, re.IGNORECASE) and len(t) < 180:
                salary_raw = t.split(":", 1)[-1].strip()
                break

        if not salary_raw and description:
            _SAL_PAT = r'\$[\d,]+(?:k|K)?(?:\s*[-–]\s*\$?[\d,]+(?:k|K)?)?\s*(?:per|/|p\.?a\.?|p\.?h\.?)'
            m = re.search(_SAL_PAT, description)
            if m:
                # Grab a bit of context around the match
                start = max(0, m.start() - 5)
                salary_raw = description[start:m.end() + 10].strip()

        return [{
            "title": title,
            "description": description,
            "location_raw": location_raw,
            "employment_type": employment_type,
            "salary_raw": salary_raw,
            "source_url": url,
            "extraction_method": "heuristic",
            "extraction_confidence": 0.70,
        }]

    def _merge_by_url(self, results: list[dict]) -> list[dict]:
        """Group extraction results by URL/external_id, then merge each group
        with CrossValidator to produce one enriched result per job."""
        groups: dict[str, list[dict]] = {}
        no_key: list[dict] = []
        for r in results:
            key = r.get("external_id") or r.get("source_url") or r.get("title", "")
            if key:
                groups.setdefault(key, []).append(r)
            else:
                no_key.append(r)

        merged_results = []
        for key, group in groups.items():
            if len(group) == 1:
                merged_results.append(group[0])
            else:
                merged = self.validator.merge(group)
                merged_results.append(merged)
        merged_results.extend(no_key)
        return merged_results

    def _deduplicate_by_url(self, results: list[dict]) -> list[dict]:
        """Deduplicate extraction results — when multiple methods find the same job URL,
        keep the highest-confidence result (schema_org > ats_api > ats_html > llm > structural)."""
        METHOD_PRIORITY = {
            "schema_org": 5, "ats_api": 4, "ats_html": 3,
            "llm": 2, "structural": 1, "hybrid": 3,
        }
        seen: dict[str, dict] = {}
        for r in results:
            key = r.get("external_id") or r.get("source_url") or r.get("title", "")
            if not key:
                continue
            if key not in seen:
                seen[key] = r
            else:
                existing = seen[key]
                existing_priority = METHOD_PRIORITY.get(existing.get("extraction_method", ""), 0)
                new_priority = METHOD_PRIORITY.get(r.get("extraction_method", ""), 0)
                if new_priority > existing_priority:
                    seen[key] = r
                elif new_priority == existing_priority:
                    # Same priority — merge fields, keeping non-None values
                    for field, value in r.items():
                        if value and not existing.get(field):
                            existing[field] = value
        return list(seen.values())

    def _extract_structured_data(self, html: str, base_url: str) -> list[dict]:
        """Stage 3a: Extract JobPosting schema.org structured data.
        Handles direct JobPosting, @graph arrays, and ItemList/ListItem wrappers."""
        try:
            data = extruct.extract(html, base_url=base_url, syntaxes=["json-ld", "microdata", "rdfa"])
        except Exception as e:
            logger.debug(f"extruct failed: {e}")
            return []

        jobs = []

        def _is_job_posting(item: dict) -> bool:
            types = item.get("@type", "")
            return types == "JobPosting" or (isinstance(types, list) and "JobPosting" in types)

        def _extract_from_item(item: dict):
            if not isinstance(item, dict):
                return
            if _is_job_posting(item):
                jobs.append(self._map_schema_org(item))
                return
            # Handle @graph: [{...}, {...}]
            for sub in item.get("@graph", []):
                _extract_from_item(sub)
            # Handle ItemList / ListItem
            if item.get("@type") in ("ItemList", "BreadcrumbList"):
                for el in item.get("itemListElement", []):
                    _extract_from_item(el.get("item", el) if isinstance(el, dict) else el)
            # Handle direct list of jobs under a property
            for key in ("jobPosting", "jobPostings", "hasJobPosting"):
                for sub in (item.get(key) or []):
                    _extract_from_item(sub if isinstance(sub, dict) else {})

        for item in data.get("json-ld", []):
            _extract_from_item(item)

        for item in data.get("microdata", []):
            if "JobPosting" in str(item.get("type", "")):
                jobs.append(self._map_schema_org(item.get("properties", {})))

        return jobs

    def _map_schema_org(self, item: dict) -> dict:
        if not isinstance(item, dict):
            return {"title": "", "extraction_method": "schema_org", "extraction_confidence": 0.0}

        loc = item.get("jobLocation", {})
        if isinstance(loc, list):
            loc = loc[0] if loc else {}
        if not isinstance(loc, dict):
            loc = {}
        address = loc.get("address", {})
        address_str = ""  # plain string address if address isn't a dict
        if not isinstance(address, dict):
            address_str = str(address) if address else ""
            address = {}

        base_salary = item.get("baseSalary") or {}
        if not isinstance(base_salary, dict):
            base_salary = {}
        salary_value = base_salary.get("value") or {}
        if not isinstance(salary_value, dict):
            salary_value = {}

        identifier = item.get("identifier")
        external_id = (identifier.get("value") if isinstance(identifier, dict) else str(identifier)) if identifier else None

        # Build location_raw: prefer jobLocation.name, then construct from address fields
        location_raw = self._safe_str(loc.get("name"))
        if not location_raw:
            parts = [
                address.get("addressLocality", ""),
                address.get("addressRegion", ""),
                address.get("addressCountry", ""),
            ]
            location_raw = ", ".join(p for p in parts if p) or address_str

        return {
            "title": self._safe_str(item.get("title") or item.get("name")),
            "description": self._safe_str(item.get("description")),
            "location_raw": location_raw,
            "location_city": address.get("addressLocality", ""),
            "location_state": address.get("addressRegion", ""),
            "location_country": address.get("addressCountry", "AU"),
            "employment_type": self._safe_str(item.get("employmentType")),
            "date_posted": self._safe_str(item.get("datePosted")) or None,
            "date_expires": self._safe_str(item.get("validThrough")) or None,
            "salary_raw": str(base_salary) if base_salary else None,
            "salary_min": salary_value.get("minValue") if isinstance(salary_value, dict) else None,
            "salary_max": salary_value.get("maxValue") if isinstance(salary_value, dict) else None,
            "salary_currency": base_salary.get("currency"),
            "application_url": item.get("url") or item.get("sameAs"),
            "external_id": external_id,
            "extraction_method": "schema_org",
            "extraction_confidence": 0.95,
            "raw_data": item,
        }

    async def _extract_ats(self, platform: str, url: str, html: str) -> list[dict]:
        from app.extractors import ats_extractors
        extractor_cls = ats_extractors.REGISTRY.get(platform)
        if extractor_cls:
            try:
                return await extractor_cls().extract(url, html)
            except Exception as e:
                logger.warning(f"ATS extractor({platform}) failed: {e}")
        return []

    async def _extract_with_template(self, company: Company, career_page: CareerPage, html: str) -> list[dict]:
        """Stage 3c: Use a learned site template if one exists."""
        from sqlalchemy import select
        from app.models.site_template import SiteTemplate
        from app.extractors.template_learner import TemplateLearner

        template = await self.db.scalar(
            select(SiteTemplate).where(
                SiteTemplate.career_page_id == career_page.id,
                SiteTemplate.is_active == True,
                SiteTemplate.template_type == "detail_page",
            )
        )
        if not template or not template.selectors:
            return []

        learner = TemplateLearner()
        result = learner.extract_with_template(html, template.selectors)
        return [result] if result.get("title") else []

    async def _extract_llm(self, url: str, html: str) -> list[dict]:
        """Stage 3d: LLM extraction via Ollama."""
        try:
            from app.extractors.llm_extractor import LLMJobExtractor
            md = markdownify(html, strip=["script", "style"])
            extractor = LLMJobExtractor()
            result = await extractor.extract(url, md)
            if result and result.get("title"):
                result["source_url"] = url
                return [result]
        except Exception as e:
            logger.warning(f"LLM extraction failed for {url}: {e}")
        return []

    async def _extract_llm_inline(self, url: str, html: str, career_page) -> list[dict]:
        """Inline LLM extraction for structureless pages. Escalates 3B → 8B.

        Only runs when site_status is no_structure_new/broken — avoids slowing
        normal crawls where heuristics already found jobs.
        """
        from app.core.config import settings
        import json
        models = ["llama3.2:3b", "llama3.1:8b"]
        md = markdownify(html[:5000], strip=["script", "style"])
        prompt = (
            f"Extract job posting data from this page: {url}\n"
            f"Return ONLY a JSON object with keys: title, location, description (max 400 chars), employment_type.\n"
            f"If this is a job listing page (multiple jobs), return {{\"is_listing\": true}}.\n"
            f"If this is not a job page at all, return {{}}.\n\n"
            f"Page content:\n{md}\n\nJSON:"
        )
        for model in models:
            try:
                async with httpx.AsyncClient(timeout=60) as client:
                    r = await client.post(
                        f"http://{settings.OLLAMA_HOST}:11434/api/generate",
                        json={"model": model, "prompt": prompt, "stream": False,
                              "options": {"temperature": 0.1, "num_predict": 400}},
                    )
                if r.status_code != 200:
                    continue
                raw = r.json().get("response", "").strip()
                m = re.search(r'\{.*?\}', raw, re.DOTALL)
                if not m:
                    continue
                data = json.loads(m.group())
                if data.get("is_listing"):
                    logger.debug(f"llm_inline: listing page (not single job) at {url}")
                    return []
                if not data.get("title"):
                    continue
                logger.debug(f"llm_inline ({model}): extracted '{data['title'][:60]}' from {url}")
                return [{
                    "title": str(data.get("title", "")),
                    "description": str(data.get("description", "")),
                    "location_raw": str(data.get("location", "") or ""),
                    "employment_type": data.get("employment_type"),
                    "source_url": url,
                    "extraction_method": "llm_inline",
                    "extraction_confidence": 0.70,
                }]
            except Exception as e:
                logger.debug(f"llm_inline ({model}) failed for {url}: {e}")
                continue
        return []

    # Generic/navigation titles that are never real job listings
    _STRUCTURAL_REJECT_TITLES = {
        "home", "about", "about us", "contact", "contact us", "news", "blog",
        "careers", "jobs", "work with us", "join us", "join our team", "team",
        "services", "products", "solutions", "portfolio", "gallery", "media",
        "resources", "support", "help", "faq", "privacy", "terms", "legal",
        "sitemap", "search", "login", "register", "sign up", "sign in",
        "our team", "leadership", "management", "board", "investors",
        "data protection notice", "compliance", "cookie policy",
        "apply", "apply now", "apply here", "apply online",
        "vacancies", "opportunities", "openings", "positions",
        "load more", "view all", "see all", "see more", "show more",
        "next", "previous", "page", "back", "forward",
        "facebook", "twitter", "linkedin", "instagram", "youtube",
    }

    def _extract_repeating_blocks(self, html: str, base_url: str) -> list[dict]:
        """Stage 3e: Structural DOM analysis to find job listing blocks.

        Looks for groups of elements with identical structure — a strong signal
        for paginated job listing cards. Filters navigation/generic content.
        """
        soup = BeautifulSoup(html, "lxml")

        # Remove noise sections before analysis
        for tag in soup(["nav", "header", "footer", "script", "style", "noscript"]):
            tag.decompose()

        candidates = []
        for parent in soup.find_all(True):
            children = [c for c in parent.children if c.name]
            if len(children) < 3:
                continue

            groups: dict[str, list] = {}
            for child in children:
                classes = " ".join(sorted(child.get("class", [])))
                key = f"{child.name}.{classes}"
                groups.setdefault(key, []).append(child)

            for key, group in groups.items():
                if len(group) < 3:
                    continue
                links_count = sum(1 for el in group if el.find("a"))
                if links_count / len(group) < 0.6:
                    continue

                # Score by avg text length — job cards have more text than nav items
                avg_text_len = sum(len(el.get_text(strip=True)) for el in group) / len(group)
                if avg_text_len < 15:
                    continue  # too short — likely navigation items

                score = links_count * (avg_text_len / 20)
                candidates.append((score, group))

        if not candidates:
            return []

        candidates.sort(key=lambda x: -x[0])
        best_group = candidates[0][1]

        jobs = []
        for el in best_group:
            link = el.find("a")
            title_el = el.find(["h1", "h2", "h3", "h4", "strong", "b"]) or el
            title = title_el.get_text(strip=True)[:200]

            if not title or len(title) < 5:
                continue
            if title.lower().strip() in self._STRUCTURAL_REJECT_TITLES:
                continue
            # Reject if title looks like a generic nav label (single common word)
            if len(title.split()) == 1 and title.lower() in {
                "careers", "jobs", "apply", "search", "opportunities", "home",
                "about", "news", "blog", "contact", "team", "services",
            }:
                continue
            # Reject titles that are far too long (probably paragraphs, not job titles)
            if len(title) > 150:
                continue

            job_url = urljoin(base_url, link["href"]) if link and link.get("href") else base_url
            # Skip if the URL is an external site or mailto/tel
            if job_url.startswith(("mailto:", "tel:", "#")):
                continue
            # Skip URLs that look like browsing/nav/editorial pages, not job detail pages
            if not self._is_likely_job_url(job_url, base_url):
                continue

            # Try to extract location from within the block element
            loc_raw = None
            for loc_sel in (
                "[class*='location']", "[class*='city']", "[class*='place']",
                "[class*='job-location']", "[itemprop='jobLocation']",
                "[data-testid*='location']", ".sort-by-location", ".jv-job-list-location",
            ):
                loc_el = el.select_one(loc_sel)
                if loc_el:
                    txt = loc_el.get_text(strip=True)
                    if txt and 2 < len(txt) < 120:
                        loc_raw = txt
                        break

            # Also try second line / subtitle of the card (many sites list location there)
            if not loc_raw:
                subtitles = el.find_all(["p", "span", "div"], recursive=False)
                for sub in subtitles[1:3]:  # skip first (usually the title)
                    txt = sub.get_text(strip=True)
                    if txt and txt != title and 2 < len(txt) < 80:
                        # Check if it looks like a location (has state abbr, city, or remote)
                        if re.search(r'\b(?:NSW|VIC|QLD|WA|SA|ACT|NT|TAS|Australia|Remote|Hybrid)\b|'
                                     r'\b(?:Sydney|Melbourne|Brisbane|Perth|Adelaide|Canberra|Darwin|Hobart)\b',
                                     txt, re.IGNORECASE):
                            loc_raw = txt
                            break

            # Fallback: extract city from URL path pattern /job/{city}/ or /{city}/jobs/
            if not loc_raw:
                url_path = urlparse(job_url).path
                m = re.search(r'/jobs?/([a-zA-Z][a-zA-Z\s\-]{2,30})/[^/]+(?:/|$)', url_path)
                if m:
                    candidate = m.group(1).replace("-", " ").strip().title()
                    # Filter out segments that are clearly not cities
                    if candidate.lower() not in ("all", "search", "view", "apply", "new", "list", "category"):
                        loc_raw = candidate

            jobs.append({
                "title": title,
                "source_url": job_url,
                "location_raw": loc_raw,
                "extraction_method": "structural",
                "extraction_confidence": 0.55,
                "raw_data": {"html_snippet": str(el)[:500]},
            })
        return jobs

    # Path segments that reliably indicate a NON-job page
    _NON_JOB_PATH_PATTERNS = re.compile(
        r"/(locations?|countries?|country|cities?|city|regions?|states?|filters?|"
        r"search|browse|categories?|departments?|teams?|insights?|articles?|"
        r"news|blog|press|media|events?|about|contact|faq|help|privacy|"
        r"terms|legal|cookies?|early.careers?|programs?|graduate|internship.program|"
        r"capabilities?|talent.assessment|benefits|culture|diversity|"
        r"early_careers?|apprenticeship.program)[/\?#]",
        re.IGNORECASE,
    )
    # Query param patterns that indicate listing/filter pages
    _NON_JOB_QUERY_PATTERNS = re.compile(
        r"(page=\d+.*categor|categor.*page=\d+|^\?page=\d+$)",
        re.IGNORECASE,
    )

    def _is_likely_job_url(self, url: str, base_url: str) -> bool:
        """Return True if the URL looks like a job detail page, False for listing/nav/editorial.

        Checks:
        - Rejects root paths (depth < 2 meaningful segments)
        - Rejects URLs with known non-job path patterns (locations, insights, etc.)
        - Rejects if URL is identical to the crawled page (would create self-referential job)
        """
        try:
            parsed = urlparse(url)
            base_parsed = urlparse(base_url)
        except Exception:
            return True  # Can't parse — allow through, filtered later

        # Same URL as the page being crawled = nav/section anchor, not a job
        if parsed.path.rstrip("/") == base_parsed.path.rstrip("/") and not parsed.query:
            return False

        # Root or very shallow path (e.g. "/" or "/careers") — listing pages, not jobs
        path_parts = [p for p in parsed.path.split("/") if p]
        if len(path_parts) < 2:
            return False

        # Known non-job path segments in the URL
        url_lower = parsed.path.lower()
        if self._NON_JOB_PATH_PATTERNS.search(url_lower + "/"):
            return False

        # Pagination + category filter query strings (listing pages, not job detail)
        if parsed.query and self._NON_JOB_QUERY_PATTERNS.search(parsed.query):
            return False

        return True

    def _enrich(self, data: dict, company: Company) -> dict:
        """Apply location parsing, salary parsing, and market defaults."""
        # Description normalization — schema.org can return description as a dict
        desc_raw = data.get("description")
        if isinstance(desc_raw, dict):
            data["description"] = str(desc_raw.get("@value") or desc_raw.get("value") or "")
        elif desc_raw is not None:
            data["description"] = str(desc_raw)

        # Location normalization — coerce to string first (schema.org can return dicts)
        loc_raw = data.get("location_raw")
        if isinstance(loc_raw, dict):
            data["location_raw"] = str(loc_raw.get("name") or loc_raw.get("@value") or "")
        elif loc_raw is not None:
            data["location_raw"] = str(loc_raw)
        if data.get("location_raw"):
            parsed_loc = location_normalizer.normalize(data["location_raw"], company.market_code)
            if not data.get("location_city"):
                data["location_city"] = parsed_loc.city
            if not data.get("location_state"):
                data["location_state"] = parsed_loc.state
            if not data.get("location_country"):
                data["location_country"] = parsed_loc.country or "Australia"
            if data.get("is_remote") is None:
                data["is_remote"] = parsed_loc.is_remote
            if not data.get("remote_type"):
                data["remote_type"] = parsed_loc.remote_type

        # Market-based country fallback for all supported markets
        _MARKET_COUNTRIES = {
            "AU": "Australia", "NZ": "New Zealand", "SG": "Singapore",
            "MY": "Malaysia", "HK": "Hong Kong", "PH": "Philippines",
            "ID": "Indonesia", "TH": "Thailand",
        }
        if not data.get("location_country") and company.market_code:
            country = _MARKET_COUNTRIES.get(company.market_code)
            if country:
                data["location_country"] = country

        # Salary normalization
        if data.get("salary_raw") and not data.get("salary_min"):
            parsed_sal = salary_normalizer.normalize(data["salary_raw"], company.market_code)
            if parsed_sal.is_parseable:
                data.update(salary_normalizer.to_dict(parsed_sal))

        return data

    # Generic titles that should never be saved as jobs regardless of method
    _JUNK_TITLES = {
        "careers", "jobs", "home", "about", "contact", "vacancies",
        "apply", "opportunities", "positions", "search jobs", "job search",
        "find a job", "employment", "recruitment", "work with us", "join us",
        "join our team", "our team", "team", "staff", "404", "error",
        "page not found", "data protection notice for applicants",
        "data protection notice", "compliance", "cookie policy",
        "privacy policy", "terms of service", "load more", "view all",
    }

    async def _upsert_job(self, company: Company, page: CareerPage, data: dict) -> Optional[Job]:
        from sqlalchemy import select

        source_url = data.get("source_url") or data.get("application_url") or page.url
        external_id = data.get("external_id")
        title = str(data.get("title") or "").strip()
        description = str(data.get("description") or "").strip()

        if not title:
            return None

        # Reject generic/navigation titles with no description
        if title.lower().rstrip(".!?") in self._JUNK_TITLES and not description:
            return None

        # Structural extractions with no description and very short titles are likely nav links
        if data.get("extraction_method") == "structural" and not description and len(title.split()) <= 2:
            return None

        # Reject jobs from URLs that look like listing/nav/editorial pages (all methods)
        if source_url:
            page_url = page.url if page else ""
            if not self._is_likely_job_url(source_url, page_url or source_url):
                logger.debug(f"Rejected job: non-job URL pattern {source_url} ({data.get('extraction_method')})")
                return None

        # Find existing job
        q = select(Job).where(Job.company_id == company.id)
        if external_id:
            existing = await self.db.scalar(q.where(Job.external_id == external_id))
        else:
            existing = await self.db.scalar(q.where(Job.source_url == source_url, Job.title == title))

        if existing:
            existing.last_seen_at = datetime.now(timezone.utc)
            existing.is_active = True
            needs_rescore = False

            # Opportunistically upgrade a short/missing description
            new_desc = str(data.get("description") or "").strip()
            existing_desc_len = len(existing.description or "")
            if new_desc and len(new_desc) > existing_desc_len and existing_desc_len < 200:
                existing.description = new_desc
                existing.description_enriched_at = datetime.now(timezone.utc)
                needs_rescore = True

            # Fill in missing location fields
            new_loc = str(data.get("location_raw") or "").strip()
            if new_loc and not existing.location_raw:
                existing.location_raw = new_loc
                existing.location_city = existing.location_city or data.get("location_city")
                existing.location_state = existing.location_state or data.get("location_state")
                existing.location_country = existing.location_country or data.get("location_country")
                existing.is_remote = existing.is_remote if existing.is_remote is not None else data.get("is_remote")
                needs_rescore = True

            # Fill in missing employment_type
            new_emp = self._normalize_employment_type(data.get("employment_type"))
            if new_emp and not existing.employment_type:
                existing.employment_type = new_emp
                needs_rescore = True

            if needs_rescore:
                existing.quality_score = None
                existing.quality_scored_at = None

            await self.db.commit()
            return existing

        job = Job(
            company_id=company.id,
            career_page_id=page.id,
            source_url=source_url,
            external_id=external_id,
            title=title,
            description=data.get("description"),
            location_raw=data.get("location_raw"),
            location_city=data.get("location_city"),
            location_state=data.get("location_state"),
            location_country=data.get("location_country", "Australia"),
            is_remote=data.get("is_remote"),
            remote_type=data.get("remote_type"),
            employment_type=self._normalize_employment_type(data.get("employment_type")),
            seniority_level=data.get("seniority_level"),
            department=data.get("department"),
            team=data.get("team"),
            salary_raw=data.get("salary_raw"),
            salary_min=data.get("salary_min"),
            salary_max=data.get("salary_max"),
            salary_currency=data.get("salary_currency"),
            salary_period=data.get("salary_period"),
            requirements=data.get("requirements"),
            benefits=data.get("benefits"),
            application_url=data.get("application_url"),
            extraction_method=data.get("extraction_method"),
            extraction_confidence=data.get("extraction_confidence"),
            raw_data={k: v for k, v in data.items() if k != "raw_data"},
        )
        self.db.add(job)
        await self.db.commit()
        await self.db.refresh(job)
        return job

    async def _save_tags(self, job: Job, data: dict) -> None:
        """Extract and persist job tags."""
        tags = tag_extractor.extract(
            title=data.get("title", ""),
            description=data.get("description", ""),
            requirements=data.get("requirements", ""),
        )
        tag_dicts = tag_extractor.to_tag_dicts(tags, confidence=data.get("extraction_confidence", 0.7))
        for tag_data in tag_dicts:
            self.db.add(JobTag(job_id=job.id, **tag_data))

        # Also save LLM-extracted skills if present
        for skill in data.get("skills_mentioned", []):
            self.db.add(JobTag(job_id=job.id, tag_type="skill", tag_value=skill, confidence=0.8))

        await self.db.commit()

    async def _refresh_active_jobs(self, company: Company, career_page: CareerPage) -> None:
        """Update last_seen_at for jobs that are still active (content unchanged)."""
        from sqlalchemy import update
        await self.db.execute(
            update(Job)
            .where(Job.career_page_id == career_page.id, Job.is_active == True)
            .values(last_seen_at=datetime.now(timezone.utc))
        )
        await self.db.commit()

    def _normalize_employment_type(self, raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        raw_lower = raw.lower()
        if any(k in raw_lower for k in ["full", "permanent", "ft"]):
            return "full_time"
        if any(k in raw_lower for k in ["part", "pt"]):
            return "part_time"
        if any(k in raw_lower for k in ["contract", "contractor", "freelance"]):
            return "contract"
        if "intern" in raw_lower:
            return "internship"
        if any(k in raw_lower for k in ["temp", "casual"]):
            return "temporary"
        return raw.lower().replace(" ", "_")[:30]
