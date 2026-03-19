"""
Aggregator Link Harvester — Stage 1c.

For permitted aggregator sites ONLY (Indeed, LinkedIn, Glassdoor, etc.):
  - Searches the aggregator for Australian job listings
  - Extracts outbound links to company career pages
  - Adds discovered company domains to the database
  - NEVER extracts job content from the aggregator page itself

Hard rule: every URL is checked against the blocklist before any request.
Jora, SEEK, Jobstreet, and JobsDB must never appear here.
"""

import logging
import re
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from app.core.config import settings
from app.crawlers.domain_blocklist import assert_not_blocked, is_blocked
from app.crawlers.http_client import ResilientHTTPClient

logger = logging.getLogger(__name__)

# Domains that are aggregators themselves — don't add them as companies
AGGREGATOR_DOMAINS = frozenset([
    "indeed.com", "au.indeed.com", "linkedin.com", "glassdoor.com",
    "glassdoor.com.au", "careerone.com.au", "adzuna.com.au",
    "monster.com", "ziprecruiter.com", "careerbuilder.com",
    "reed.co.uk", "totaljobs.com", "angel.co", "wellfound.com",
])


class IndeedAUHarvester:
    """
    Discovers company career pages via Indeed AU (au.indeed.com).

    Strategy:
    1. Search Indeed AU for Australian jobs with configured queries
    2. Parse search results to find the outbound link to the company's own site
    3. Record the company domain and career page URL in our database
    4. NEVER save job content from Indeed — only the outbound company URL
    """

    BASE_URL = "https://au.indeed.com"

    def __init__(self):
        self.client = ResilientHTTPClient()

    async def harvest(self, db, query: str = "jobs", location: str = "Australia", max_pages: int = 5) -> list[dict]:
        """
        Run a discovery harvest for the given query.
        Returns list of discovered {domain, career_url, company_name} dicts.
        """
        assert_not_blocked(self.BASE_URL)
        discovered = []

        for page_num in range(max_pages):
            search_url = f"{self.BASE_URL}/jobs?q={query}&l={location}&start={page_num * 10}"
            try:
                resp = await self.client.get(search_url)
                html = resp.text
            except Exception as e:
                logger.warning(f"Indeed AU search failed (page {page_num}): {e}")
                break

            page_discoveries = self._extract_company_links(html, search_url)
            for disc in page_discoveries:
                if disc["domain"] and not _is_aggregator_domain(disc["domain"]):
                    await self._upsert_company(db, disc)
                    discovered.append(disc)

        # Update aggregator source last_harvest timestamp
        await self._update_harvest_time(db, "Indeed AU")

        return discovered

    def _extract_company_links(self, html: str, base_url: str) -> list[dict]:
        """
        Extract outbound company links from Indeed search results.
        Indeed links: job cards contain a link to the company's own site via "Company website"
        """
        soup = BeautifulSoup(html, "lxml")
        results = []

        # Indeed job cards — each has a data-jk attribute and a company link
        for card in soup.select("[data-jk], .job_seen_beacon, .jobsearch-SerpJobCard"):
            company_name = ""
            company_url = ""

            # Try to find company name
            name_el = card.select_one("[data-testid='company-name'], .companyName, .company")
            if name_el:
                company_name = name_el.get_text(strip=True)

            # Look for "Company website" link or direct href to company domain
            for link in card.find_all("a", href=True):
                href = link["href"]
                abs_url = urljoin(base_url, href)
                parsed = urlparse(abs_url)

                # Skip Indeed-internal links
                if "indeed.com" in parsed.netloc:
                    continue
                if is_blocked(abs_url):
                    continue

                company_url = abs_url
                break

            if company_url:
                domain = urlparse(company_url).netloc.lstrip("www.")
                results.append({
                    "company_name": company_name or domain,
                    "domain": domain,
                    "career_url": company_url,
                    "source": "indeed_au",
                })

        # Also look for "Apply on company site" buttons which link directly to career pages
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            text = link.get_text(strip=True).lower()
            if any(kw in text for kw in ["apply on company site", "apply on employer site", "company website"]):
                abs_url = urljoin(base_url, href)
                if is_blocked(abs_url) or "indeed.com" in abs_url:
                    continue
                domain = urlparse(abs_url).netloc.lstrip("www.")
                if domain and not _is_aggregator_domain(domain):
                    results.append({
                        "company_name": domain,
                        "domain": domain,
                        "career_url": abs_url,
                        "source": "indeed_au_apply_button",
                    })

        return results

    async def _upsert_company(self, db, disc: dict) -> None:
        """Add company to database if not already present."""
        from sqlalchemy import select
        from app.models.company import Company

        domain = disc["domain"]
        existing = await db.scalar(select(Company).where(Company.domain == domain))
        if existing:
            return

        root_url = f"https://{domain}"
        company = Company(
            name=disc["company_name"] or domain,
            domain=domain,
            root_url=root_url,
            market_code="AU",
            discovered_via="aggregator_link",
            crawl_priority=6,  # Slightly lower than seed companies
        )
        db.add(company)
        await db.commit()
        logger.info(f"Discovered new company via Indeed AU: {domain}")

    async def _update_harvest_time(self, db, source_name: str) -> None:
        from sqlalchemy import select, update
        from app.models.aggregator_source import AggregatorSource
        await db.execute(
            update(AggregatorSource)
            .where(AggregatorSource.name == source_name)
            .values(last_link_harvest_at=datetime.now(timezone.utc))
        )
        await db.commit()


class LinkedInHarvester:
    """
    Discovers company career pages via LinkedIn Jobs.
    LinkedIn is link-discovery-only — we follow company profile links to find career pages.
    """

    BASE_URL = "https://www.linkedin.com/jobs"

    def __init__(self):
        self.client = ResilientHTTPClient()

    async def harvest(self, db, query: str = "Australia", max_pages: int = 3) -> list[dict]:
        """Harvest company career page links from LinkedIn job listings."""
        assert_not_blocked(self.BASE_URL)
        discovered = []

        for page_num in range(max_pages):
            search_url = f"{self.BASE_URL}/search?keywords={query}&location=Australia&start={page_num * 25}"
            try:
                resp = await self.client.get(search_url)
                html = resp.text
            except Exception as e:
                logger.warning(f"LinkedIn harvest failed (page {page_num}): {e}")
                break

            soup = BeautifulSoup(html, "lxml")
            for card in soup.select(".job-search-card, .base-card"):
                link = card.find("a", href=re.compile(r"/jobs/view/"))
                if not link:
                    continue
                company_el = card.select_one(".job-search-card__subtitle-link, .base-search-card__subtitle")
                company_name = company_el.get_text(strip=True) if company_el else ""

                # LinkedIn job cards don't directly expose company URLs in search — skip
                # but we could follow job links to get company URL. For Phase 2, record company name.
                if company_name:
                    discovered.append({
                        "company_name": company_name,
                        "domain": "",
                        "career_url": "",
                        "source": "linkedin",
                    })

        await self._update_harvest_time(db, "LinkedIn Jobs")
        return discovered

    async def _update_harvest_time(self, db, source_name: str) -> None:
        from sqlalchemy import update
        from app.models.aggregator_source import AggregatorSource
        await db.execute(
            update(AggregatorSource)
            .where(AggregatorSource.name == source_name)
            .values(last_link_harvest_at=datetime.now(timezone.utc))
        )
        await db.commit()


def _is_aggregator_domain(domain: str) -> bool:
    """Return True if this domain is itself an aggregator (don't add as a company)."""
    domain_lower = domain.lower()
    return any(domain_lower == agg or domain_lower.endswith(f".{agg}") for agg in AGGREGATOR_DOMAINS)
