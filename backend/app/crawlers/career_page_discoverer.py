"""
Career Page Discoverer — Stage 2 of the pipeline.

Discovers careers/jobs pages on a company website using multiple methods:
  2a. Heuristic URL & link analysis
  2b. LLM page classification (Ollama)
  2d. ATS detection shortcut
"""

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from app.core.config import settings
from app.crawlers.domain_blocklist import assert_not_blocked
from app.models.career_page import CareerPage
from app.models.company import Company

logger = logging.getLogger(__name__)

# URL path keywords — weighted signals for career page scoring
URL_KEYWORDS = [
    "careers", "jobs", "opportunities", "vacancies", "work-with-us",
    "join", "openings", "hiring", "employment", "talent", "join-us",
    "join-our-team", "work-here", "careers-at",
]

LINK_TEXT_KEYWORDS = [
    "careers", "jobs", "join us", "we're hiring", "work with us",
    "open positions", "view openings", "job openings", "join the team",
]

# ATS-known career page URL templates
ATS_URL_TEMPLATES = {
    "greenhouse": "https://boards.greenhouse.io/{slug}",
    "lever": "https://jobs.lever.co/{slug}",
    "workday": None,  # URL varies per company
    "bamboohr": "https://{slug}.bamboohr.com/careers",
    "ashby": "https://jobs.ashbyhq.com/{slug}",
    "smartrecruiters": "https://careers.smartrecruiters.com/{slug}",
    "jobvite": "https://jobs.jobvite.com/{slug}",
}


class CareerPageDiscoverer:
    def __init__(self, db):
        self.db = db
        self.headers = {"User-Agent": settings.CRAWL_USER_AGENT}

    async def discover(self, company: Company) -> list[CareerPage]:
        """Discover all career pages for a company. Returns list of saved CareerPage objects."""
        candidates: list[dict] = []

        # 2d: ATS shortcut — if ATS is already known, check for canonical URL
        if company.ats_platform and company.ats_platform != "unknown":
            ats_urls = self._ats_candidate_urls(company)
            candidates.extend(ats_urls)

        # 2a: Heuristic link analysis
        heuristic_results = await self._heuristic_discovery(company.root_url)
        candidates.extend(heuristic_results)

        # Deduplicate by URL
        seen = {}
        for c in candidates:
            url = c["url"]
            if url not in seen or c["confidence"] > seen[url]["confidence"]:
                seen[url] = c

        # Persist discovered career pages
        pages = []
        for url, meta in seen.items():
            page = await self._upsert_career_page(company, url, meta)
            pages.append(page)

        return pages

    def _ats_candidate_urls(self, company: Company) -> list[dict]:
        """Generate ATS-known career page URL candidates from company domain slug."""
        slug = company.domain.split(".")[0]
        results = []
        template = ATS_URL_TEMPLATES.get(company.ats_platform)
        if template:
            url = template.format(slug=slug)
            results.append({
                "url": url,
                "discovery_method": "ats_fingerprint",
                "confidence": company.ats_confidence or 0.9,
                "is_primary": True,
                "page_type": "listing_page",
            })
        return results

    async def _heuristic_discovery(self, root_url: str) -> list[dict]:
        """Crawl site to depth 2 and score each discovered URL for career relevance."""
        assert_not_blocked(root_url)
        visited = set()
        queue = [(root_url, 0)]
        scored: dict[str, float] = {}

        async with httpx.AsyncClient(
            headers=self.headers,
            timeout=settings.CRAWL_TIMEOUT_SECONDS,
            follow_redirects=True,
        ) as client:
            while queue:
                url, depth = queue.pop(0)
                if url in visited or depth > 2:
                    continue
                visited.add(url)

                try:
                    resp = await client.get(url)
                    html = resp.text
                except Exception as e:
                    logger.debug(f"Failed to fetch {url}: {e}")
                    continue

                soup = BeautifulSoup(html, "lxml")
                base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"

                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    abs_url = urljoin(base, href)
                    # Stay on same domain
                    if urlparse(abs_url).netloc != urlparse(root_url).netloc:
                        continue
                    if abs_url in visited:
                        continue
                    try:
                        assert_not_blocked(abs_url)
                    except ValueError:
                        continue

                    score = self._score_url(abs_url, a.get_text(strip=True))
                    if score > 0:
                        scored[abs_url] = max(scored.get(abs_url, 0), score)
                    if depth < 2:
                        queue.append((abs_url, depth + 1))

        results = []
        threshold = 0.3
        for url, score in scored.items():
            if score >= threshold:
                results.append({
                    "url": url,
                    "discovery_method": "heuristic",
                    "confidence": min(score, 0.9),
                    "is_primary": score >= 0.7,
                    "page_type": "listing_page",
                })
        return results

    def _score_url(self, url: str, link_text: str) -> float:
        """Score a URL on how likely it is to be a careers page. Returns 0.0–1.0."""
        score = 0.0
        path = urlparse(url).path.lower()
        text = link_text.lower()

        # URL path keywords (weight 0.3)
        for kw in URL_KEYWORDS:
            if kw in path:
                score += 0.3
                break

        # Link text keywords (weight 0.3)
        for kw in LINK_TEXT_KEYWORDS:
            if kw in text:
                score += 0.3
                break

        # Penalize non-HTML resources
        if any(path.endswith(ext) for ext in [".pdf", ".doc", ".xml", ".zip", ".png", ".jpg"]):
            score = 0.0

        return min(score, 1.0)

    async def _upsert_career_page(self, company: Company, url: str, meta: dict) -> CareerPage:
        from sqlalchemy import select
        existing = await self.db.scalar(
            select(CareerPage).where(CareerPage.company_id == company.id, CareerPage.url == url)
        )
        if existing:
            existing.discovery_confidence = max(existing.discovery_confidence or 0, meta["confidence"])
            existing.updated_at = datetime.now(timezone.utc)
            await self.db.commit()
            return existing

        page = CareerPage(
            company_id=company.id,
            url=url,
            page_type=meta.get("page_type", "listing_page"),
            discovery_method=meta.get("discovery_method", "heuristic"),
            discovery_confidence=meta.get("confidence", 0.5),
            is_primary=meta.get("is_primary", False),
        )
        self.db.add(page)
        await self.db.commit()
        await self.db.refresh(page)
        return page
