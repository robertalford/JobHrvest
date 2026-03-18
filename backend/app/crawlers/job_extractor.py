"""
Job Extractor — Stage 3 & 4 of the pipeline.

Extracts job listings from career pages using multiple methods:
  3a. Structured data (schema.org / JSON-LD)
  3b. ATS-specific extractors
  3c. Repeating block detection
  3d. LLM listing identification (Phase 3)
"""

import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
import extruct
from bs4 import BeautifulSoup

from app.core.config import settings
from app.crawlers.domain_blocklist import assert_not_blocked
from app.models.career_page import CareerPage
from app.models.company import Company
from app.models.job import Job

logger = logging.getLogger(__name__)


class JobExtractor:
    def __init__(self, db):
        self.db = db
        self.headers = {"User-Agent": settings.CRAWL_USER_AGENT}

    async def extract(self, company: Company, career_page: CareerPage) -> list[Job]:
        """Extract all jobs from a career page. Returns list of Job objects."""
        assert_not_blocked(career_page.url)

        try:
            async with httpx.AsyncClient(
                headers=self.headers,
                timeout=settings.CRAWL_TIMEOUT_SECONDS,
                follow_redirects=True,
            ) as client:
                resp = await client.get(career_page.url)
                html = resp.text
        except Exception as e:
            logger.error(f"Failed to fetch career page {career_page.url}: {e}")
            return []

        # Check content hash — skip if unchanged
        content_hash = hashlib.sha256(html.encode()).hexdigest()
        if career_page.last_content_hash == content_hash:
            logger.info(f"No content change for {career_page.url}, skipping extraction")
            return []

        jobs_data = []

        # 3a: Structured data extraction (highest priority)
        structured = self._extract_structured_data(html, career_page.url)
        if structured:
            jobs_data.extend(structured)
            logger.info(f"Found {len(structured)} jobs via structured data for {career_page.url}")

        # 3b: ATS-specific extraction (if applicable)
        if company.ats_platform and company.ats_platform not in ("unknown", "custom"):
            ats_jobs = await self._extract_ats(company.ats_platform, career_page.url, html)
            if ats_jobs:
                jobs_data.extend(ats_jobs)

        # 3c: Repeating block detection (fallback)
        if not jobs_data:
            block_jobs = self._extract_repeating_blocks(html, career_page.url)
            jobs_data.extend(block_jobs)

        # Persist jobs
        saved = []
        for job_data in jobs_data:
            job = await self._upsert_job(company, career_page, job_data)
            if job:
                saved.append(job)

        # Update page metadata
        career_page.last_content_hash = content_hash
        career_page.last_crawled_at = datetime.now(timezone.utc)
        career_page.last_extraction_at = datetime.now(timezone.utc)
        await self.db.commit()

        return saved

    def _extract_structured_data(self, html: str, base_url: str) -> list[dict]:
        """Extract JobPosting schema.org data using extruct."""
        try:
            data = extruct.extract(
                html,
                base_url=base_url,
                syntaxes=["json-ld", "microdata", "rdfa"],
            )
        except Exception as e:
            logger.warning(f"extruct failed for {base_url}: {e}")
            return []

        jobs = []
        # JSON-LD
        for item in data.get("json-ld", []):
            if item.get("@type") == "JobPosting":
                jobs.append(self._map_schema_org(item))
            elif isinstance(item.get("@type"), list) and "JobPosting" in item["@type"]:
                jobs.append(self._map_schema_org(item))

        # Microdata
        for item in data.get("microdata", []):
            if "JobPosting" in str(item.get("type", "")):
                jobs.append(self._map_schema_org(item.get("properties", {})))

        return jobs

    def _map_schema_org(self, item: dict) -> dict:
        """Map schema.org JobPosting fields to our internal schema."""
        loc = item.get("jobLocation", {})
        if isinstance(loc, list):
            loc = loc[0] if loc else {}
        address = loc.get("address", {}) if isinstance(loc, dict) else {}

        base_salary = item.get("baseSalary", {}) or {}
        salary_value = base_salary.get("value", {}) or {}

        return {
            "title": item.get("title") or item.get("name", ""),
            "description": item.get("description", ""),
            "location_raw": item.get("jobLocation", {}).get("name", "") if isinstance(item.get("jobLocation"), dict) else "",
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
        """Delegate to ATS-specific extractor if available."""
        try:
            from app.extractors import ats_extractors
            extractor_cls = ats_extractors.REGISTRY.get(platform)
            if extractor_cls:
                extractor = extractor_cls()
                return await extractor.extract(url, html)
        except ImportError:
            pass
        return []

    def _extract_repeating_blocks(self, html: str, base_url: str) -> list[dict]:
        """Structural analysis: identify repeating DOM blocks that look like job listings."""
        soup = BeautifulSoup(html, "lxml")
        candidates = []

        # Find groups of sibling elements with similar structure
        for parent in soup.find_all(True):
            children = [c for c in parent.children if c.name]
            if len(children) < 3:
                continue

            # Group by tag + primary class
            groups: dict[str, list] = {}
            for child in children:
                key = f"{child.name}.{' '.join(sorted(child.get('class', [])))}"
                groups.setdefault(key, []).append(child)

            for key, group in groups.items():
                if len(group) < 3:
                    continue
                # Score the group
                score = 0
                links_count = sum(1 for el in group if el.find("a"))
                if links_count / len(group) > 0.7:
                    score += 2  # Most items have links → likely listings
                # Check for title-like text (short, possibly title-cased)
                for el in group[:3]:
                    text = el.get_text(strip=True)
                    if 5 < len(text) < 100:
                        score += 1
                        break

                if score >= 2:
                    candidates.append((score, group))

        if not candidates:
            return []

        # Use the highest-scoring group
        candidates.sort(key=lambda x: -x[0])
        best_group = candidates[0][1]

        jobs = []
        for el in best_group:
            link = el.find("a")
            title_el = el.find(["h1", "h2", "h3", "h4", "strong", "b"]) or el
            title = title_el.get_text(strip=True)[:200]
            if not title:
                continue
            job_url = urljoin(base_url, link["href"]) if link and link.get("href") else base_url
            jobs.append({
                "title": title,
                "source_url": job_url,
                "extraction_method": "structural",
                "extraction_confidence": 0.6,
                "raw_data": {"html_snippet": str(el)[:500]},
            })
        return jobs

    async def _upsert_job(self, company: Company, page: CareerPage, data: dict) -> Optional[Job]:
        from sqlalchemy import select
        source_url = data.get("source_url") or data.get("application_url") or page.url
        external_id = data.get("external_id")

        # Try to find existing job by external_id or source_url
        q = select(Job).where(Job.company_id == company.id)
        if external_id:
            q = q.where(Job.external_id == external_id)
        else:
            q = q.where(Job.source_url == source_url, Job.title == data.get("title", ""))

        existing = await self.db.scalar(q)
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
            title=data.get("title", "Untitled"),
            description=data.get("description"),
            location_raw=data.get("location_raw"),
            location_city=data.get("location_city"),
            location_state=data.get("location_state"),
            location_country=data.get("location_country", "AU"),
            employment_type=data.get("employment_type"),
            salary_raw=data.get("salary_raw"),
            salary_min=data.get("salary_min"),
            salary_max=data.get("salary_max"),
            salary_currency=data.get("salary_currency"),
            application_url=data.get("application_url"),
            extraction_method=data.get("extraction_method", "unknown"),
            extraction_confidence=data.get("extraction_confidence"),
            raw_data=data.get("raw_data"),
        )
        self.db.add(job)
        await self.db.commit()
        await self.db.refresh(job)
        return job
