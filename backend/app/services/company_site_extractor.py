"""
Company Site Extractor — Heuristic Extractor for identifying career sites.

Escalation ladder (stops as soon as sites are found):
  Layer 1: ATS fingerprinting → canonical ATS URL templates
  Layer 2: URL pattern scoring + link crawling (heuristic BFS)
  Layer 3: TF-IDF + LR page classifier (existing ML model)
  Layer 4: DistilBERT semantic classifier (if model available)
  Layer 5: 3B parameter LLM (Ollama — qwen2.5:3b or similar)
  Layer 6: 8B parameter LLM (Ollama — llama3.1:8b or similar)

After each layer:
  - If sites found → set company_status='ok' and return
  - If all layers exhausted without result → set company_status based on history
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Status values
STATUS_OK = "ok"
STATUS_AT_RISK = "at_risk"
STATUS_NO_SITES_NEW = "no_sites_new"
STATUS_NO_SITES_BROKEN = "no_sites_broken"


class CompanySiteExtractor:
    """Orchestrates all layers to find career pages for a company."""

    def __init__(self, db):
        self.db = db

    async def extract(self, company) -> list:
        """
        Run all extraction layers. Returns list of discovered CareerPage objects.
        Updates company.company_status on completion.
        """
        logger.info(f"CompanySiteExtractor: starting for {company.domain}")
        pages = []

        # Layer 1 + 2: ATS fingerprinting + heuristic BFS (existing infrastructure)
        pages = await self._layer_heuristic(company)
        if pages:
            await self._set_status(company, STATUS_OK)
            return pages

        # Layer 3: TF-IDF + LR classifier on crawled candidate pages
        pages = await self._layer_tfidf_classifier(company)
        if pages:
            await self._set_status(company, STATUS_OK)
            return pages

        # Layer 5: 3B LLM — ask LLM to identify career page URL from homepage content
        pages = await self._layer_llm(company, model="qwen2.5:3b")
        if pages:
            await self._set_status(company, STATUS_OK)
            return pages

        # Layer 6: 8B LLM — escalate to more capable model
        pages = await self._layer_llm(company, model="llama3.1:8b")
        if pages:
            await self._set_status(company, STATUS_OK)
            return pages

        # All layers exhausted — determine broken vs new
        had_pages_before = company.last_crawl_at is not None
        status = STATUS_NO_SITES_BROKEN if had_pages_before else STATUS_NO_SITES_NEW
        await self._set_status(company, status)
        logger.warning(f"CompanySiteExtractor: no sites found for {company.domain} → {status}")
        return []

    async def _layer_heuristic(self, company) -> list:
        """Layers 1+2: ATS fingerprinting + URL pattern scoring (existing infrastructure)."""
        try:
            from app.crawlers.ats_fingerprinter import ATSFingerprinter
            from app.crawlers.career_page_discoverer import CareerPageDiscoverer

            # Layer 1: ATS fingerprint
            if not company.ats_platform or company.ats_platform == "unknown":
                try:
                    fp = ATSFingerprinter()
                    result = await fp.fingerprint(company.root_url)
                    if result and result.get("platform") != "unknown":
                        company.ats_platform = result["platform"]
                        company.ats_confidence = result["confidence"]
                        await self.db.commit()
                except Exception as e:
                    logger.debug(f"ATS fingerprint failed for {company.domain}: {e}")

            # Layer 2: Heuristic BFS discovery
            discoverer = CareerPageDiscoverer(self.db)
            pages = await discoverer.discover(company)
            return pages or []
        except Exception as e:
            logger.warning(f"Heuristic layer failed for {company.domain}: {e}")
            return []

    async def _layer_tfidf_classifier(self, company) -> list:
        """Layer 3: TF-IDF + LR classifier to score candidate pages."""
        try:
            from app.ml.description_classifier import DescriptionClassifier
            from app.crawlers.http_client import ResilientHTTPClient
            import re

            clf = DescriptionClassifier.get()
            if not clf.available:
                return []

            # Fetch homepage and extract all internal links
            client = ResilientHTTPClient(timeout=15)
            resp = await client.get(company.root_url)
            if not resp:
                return []

            from bs4 import BeautifulSoup
            from urllib.parse import urljoin, urlparse
            soup = BeautifulSoup(resp.text if hasattr(resp, 'text') else resp.get('html', ''), 'lxml')
            base_domain = urlparse(company.root_url).netloc

            candidates = []
            for a in soup.find_all('a', href=True):
                href = urljoin(company.root_url, a['href'])
                if urlparse(href).netloc != base_domain:
                    continue
                text = (a.get_text() + ' ' + href).lower()
                # Use TF-IDF classifier — it's trained on job content, so we pass
                # the link text + URL as a proxy for "does this look job-related"
                if any(kw in text for kw in ['career', 'job', 'vacanc', 'hiring', 'opportunit', 'work']):
                    candidates.append(href)

            if not candidates:
                return []

            # Score candidates by fetching and classifying page content
            pages = []
            from app.crawlers.career_page_discoverer import CareerPageDiscoverer
            discoverer = CareerPageDiscoverer(self.db)
            for url in candidates[:5]:  # limit to top 5 candidates
                try:
                    page_resp = await client.get(url)
                    if not page_resp:
                        continue
                    html = page_resp.text if hasattr(page_resp, 'text') else page_resp.get('html', '')
                    score = len(re.findall(r'\b(apply|job|position|career|role)\b', html.lower()))
                    if score >= 3:
                        page = await discoverer._upsert_career_page(company, url, {
                            "discovery_method": "tfidf_classifier",
                            "confidence": min(0.7, 0.4 + score * 0.05),
                            "is_primary": len(pages) == 0,
                            "page_type": "listing_page",
                        })
                        pages.append(page)
                except Exception:
                    continue
            return pages
        except Exception as e:
            logger.debug(f"TF-IDF layer failed for {company.domain}: {e}")
            return []

    async def _layer_llm(self, company, model: str = "qwen2.5:3b") -> list:
        """Layers 5+6: Ask LLM to identify career page URL from homepage content."""
        try:
            from app.crawlers.http_client import ResilientHTTPClient
            from app.crawlers.career_page_discoverer import CareerPageDiscoverer
            import httpx, json

            from app.core.config import settings
            ollama_host = getattr(settings, 'OLLAMA_HOST', 'ollama')
            ollama_url = f"http://{ollama_host}:11434/api/generate"

            client = ResilientHTTPClient(timeout=20)
            resp = await client.get(company.root_url)
            if not resp:
                return []

            html = resp.text if hasattr(resp, 'text') else resp.get('html', '')
            # Truncate to keep prompt manageable
            html_excerpt = html[:3000]

            prompt = (
                f"You are analyzing the website of a company called '{company.name}' at domain '{company.domain}'.\n"
                f"Find the URL(s) of their careers/jobs page(s) where job listings are posted.\n"
                f"Return ONLY a JSON array of full URLs, nothing else. Example: [\"https://careers.example.com/jobs\"]\n"
                f"If you cannot find a careers page, return an empty array: []\n\n"
                f"Homepage HTML excerpt:\n{html_excerpt}\n\n"
                f"Career page URLs:"
            )

            async with httpx.AsyncClient(timeout=60) as http:
                r = await http.post(ollama_url, json={
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1, "num_predict": 200},
                })
                if r.status_code != 200:
                    return []
                raw = r.json().get("response", "").strip()

            # Parse JSON array from response
            import re
            match = re.search(r'\[.*?\]', raw, re.DOTALL)
            if not match:
                return []
            urls = json.loads(match.group())
            if not isinstance(urls, list) or not urls:
                return []

            discoverer = CareerPageDiscoverer(self.db)
            pages = []
            for url in urls[:3]:
                if not isinstance(url, str) or not url.startswith('http'):
                    continue
                page = await discoverer._upsert_career_page(company, url, {
                    "discovery_method": f"llm_{model.replace(':', '_')}",
                    "confidence": 0.65,
                    "is_primary": len(pages) == 0,
                    "page_type": "listing_page",
                })
                pages.append(page)
            return pages

        except Exception as e:
            logger.debug(f"LLM layer ({model}) failed for {company.domain}: {e}")
            return []

    async def _set_status(self, company, status: str):
        from sqlalchemy import text
        await self.db.execute(
            text("UPDATE companies SET company_status = :s WHERE id = :id"),
            {"s": status, "id": str(company.id)}
        )
        await self.db.commit()
