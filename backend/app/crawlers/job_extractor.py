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

        # 3a: Schema.org structured data (highest priority)
        structured = self._extract_structured_data(html, url)
        if structured:
            methods_results.extend(structured)
            logger.debug(f"schema_org: {len(structured)} jobs from {url}")

        # 3b: ATS-specific extractor
        if company.ats_platform and company.ats_platform not in ("unknown", "custom", None):
            ats_jobs = await self._extract_ats(company.ats_platform, url, html)
            if ats_jobs:
                methods_results.extend(ats_jobs)
                logger.debug(f"ats({company.ats_platform}): {len(ats_jobs)} jobs from {url}")

        # 3c: Template-based extraction (fast learned selectors)
        template_jobs = await self._extract_with_template(company, career_page, html)
        if template_jobs:
            methods_results.extend(template_jobs)

        # 3d: LLM extraction (if no good results yet)
        high_confidence = [r for r in methods_results if r.get("extraction_confidence", 0) >= 0.85]
        if not high_confidence:
            llm_jobs = await self._extract_llm(url, html)
            if llm_jobs:
                methods_results.extend(llm_jobs)
                logger.debug(f"llm: {len(llm_jobs)} jobs from {url}")

        # 3e: Structural repeating block detection (last resort)
        if not methods_results:
            structural = self._extract_repeating_blocks(html, url)
            methods_results.extend(structural)
            logger.debug(f"structural: {len(structural)} jobs from {url}")

        # Cross-validate: if multiple methods found results, merge
        if len(methods_results) > 1:
            return [self.validator.merge([r]) for r in methods_results]
        return methods_results

    def _extract_structured_data(self, html: str, base_url: str) -> list[dict]:
        """Stage 3a: Extract JobPosting schema.org structured data."""
        try:
            data = extruct.extract(html, base_url=base_url, syntaxes=["json-ld", "microdata", "rdfa"])
        except Exception as e:
            logger.debug(f"extruct failed: {e}")
            return []

        jobs = []
        for item in data.get("json-ld", []):
            types = item.get("@type", "")
            if types == "JobPosting" or (isinstance(types, list) and "JobPosting" in types):
                jobs.append(self._map_schema_org(item))

        for item in data.get("microdata", []):
            if "JobPosting" in str(item.get("type", "")):
                jobs.append(self._map_schema_org(item.get("properties", {})))

        return jobs

    def _map_schema_org(self, item: dict) -> dict:
        loc = item.get("jobLocation", {})
        if isinstance(loc, list):
            loc = loc[0] if loc else {}
        address = loc.get("address", {}) if isinstance(loc, dict) else {}
        base_salary = item.get("baseSalary", {}) or {}
        salary_value = base_salary.get("value", {}) or {}

        return {
            "title": item.get("title") or item.get("name", ""),
            "description": item.get("description", ""),
            "location_raw": (loc.get("name", "") if isinstance(loc, dict) else ""),
            "location_city": address.get("addressLocality", ""),
            "location_state": address.get("addressRegion", ""),
            "location_country": address.get("addressCountry", "AU"),
            "employment_type": item.get("employmentType", ""),
            "date_posted": item.get("datePosted"),
            "date_expires": item.get("validThrough"),
            "salary_raw": str(base_salary) if base_salary else None,
            "salary_min": salary_value.get("minValue") if isinstance(salary_value, dict) else None,
            "salary_max": salary_value.get("maxValue") if isinstance(salary_value, dict) else None,
            "salary_currency": base_salary.get("currency"),
            "application_url": item.get("url") or item.get("sameAs"),
            "external_id": item.get("identifier", {}).get("value") if isinstance(item.get("identifier"), dict) else None,
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

    def _extract_repeating_blocks(self, html: str, base_url: str) -> list[dict]:
        """Stage 3e: Structural DOM analysis to find job listing blocks."""
        soup = BeautifulSoup(html, "lxml")
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
                if links_count / len(group) > 0.6:
                    score = links_count
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
            if not title or len(title) < 3:
                continue
            job_url = urljoin(base_url, link["href"]) if link and link.get("href") else base_url
            jobs.append({
                "title": title,
                "source_url": job_url,
                "extraction_method": "structural",
                "extraction_confidence": 0.55,
                "raw_data": {"html_snippet": str(el)[:500]},
            })
        return jobs

    def _enrich(self, data: dict, company: Company) -> dict:
        """Apply location parsing, salary parsing, and market defaults."""
        # Location normalization
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

        # Default country to AU for AU-market companies
        if not data.get("location_country") and company.market_code == "AU":
            data["location_country"] = "Australia"

        # Salary normalization
        if data.get("salary_raw") and not data.get("salary_min"):
            parsed_sal = salary_normalizer.normalize(data["salary_raw"], company.market_code)
            if parsed_sal.is_parseable:
                data.update(salary_normalizer.to_dict(parsed_sal))

        return data

    async def _upsert_job(self, company: Company, page: CareerPage, data: dict) -> Optional[Job]:
        from sqlalchemy import select

        source_url = data.get("source_url") or data.get("application_url") or page.url
        external_id = data.get("external_id")
        title = data.get("title", "").strip()

        if not title:
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
