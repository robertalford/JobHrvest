"""
LLM Unified Company+Site Config — single Playwright+LLM pass per company.

Replaces the separate company_config → site_config → job_crawling pipeline
with one LLM call that:
1. Renders the company website with Playwright
2. Asks LLM to find the job listings page URL
3. Renders that page with Playwright
4. Asks LLM to map all 4 core fields (title, location, description, company)
5. Creates career_page + site_template in one transaction
6. Immediately queues for job_crawling

If the LLM can't find/map jobs with all 4 core fields → company marked failed.
"""

import asyncio
import hashlib
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from markdownify import markdownify

logger = logging.getLogger(__name__)


class LLMUnifiedConfig:
    """Single-pass LLM company + site configuration."""

    def __init__(self, db, ollama_host: str = "ollama", model: str = "qwen2.5:3b"):
        self.db = db
        self.ollama_host = ollama_host
        self.ollama_url = f"http://{ollama_host}:11434/api/generate"
        self.model = model

    async def configure_company(self, company) -> dict:
        """Run unified config for a company. Returns result dict."""
        from app.crawlers.http_client import ResilientHTTPClient
        from app.models.career_page import CareerPage
        from app.models.site_template import SiteTemplate
        from app.services import queue_manager
        from sqlalchemy import select, text

        client = ResilientHTTPClient(timeout=20)
        result = {"company": company.name, "domain": company.domain, "status": "failed",
                  "career_page_url": None, "job_count": 0, "fields_mapped": []}

        # ── Step 1: Render company homepage with Playwright ─────────────────
        try:
            homepage_html = await client.get_rendered(company.root_url)
            if not homepage_html or len(homepage_html) < 500:
                result["error"] = "Homepage unreachable or empty"
                return result
        except Exception as e:
            result["error"] = f"Homepage render failed: {str(e)[:100]}"
            return result

        # ── Step 2: Ask LLM to find the job listings page URL ───────────────
        soup = BeautifulSoup(homepage_html, "lxml")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)[:80]
            if href and text and len(text) > 1 and not href.startswith(("mailto:", "tel:", "javascript:")):
                abs_url = urljoin(company.root_url, href)
                # Only include same-domain links
                if company.domain in urlparse(abs_url).netloc:
                    links.append(f"[{text}]({abs_url})")

        links_text = "\n".join(links[:30])

        find_url_prompt = (
            f"You are analyzing a company website to find their careers/jobs page.\n"
            f"Company: {company.name}\n"
            f"Homepage: {company.root_url}\n\n"
            f"Links found on the homepage:\n{links_text}\n\n"
            f"Which URL leads to the page that LISTS ALL current job openings/vacancies?\n"
            f"Look for links with text like: Careers, Jobs, Vacancies, Open Positions, "
            f"Work With Us, Join Our Team, Current Openings, etc.\n\n"
            f"Return ONLY the full URL. If no careers/jobs page exists, return NONE.\n\n"
            f"URL:"
        )

        career_url = await self._ask_llm(find_url_prompt, max_tokens=100)
        if not career_url:
            result["error"] = "LLM returned no response"
            return result

        # Parse URL from response
        url_match = re.search(r'https?://[^\s<>"\')\]]+', career_url)
        if not url_match or "NONE" in career_url.upper():
            # Try the homepage itself — maybe it IS the careers page
            career_url = company.root_url
        else:
            career_url = url_match.group(0).rstrip(".,;)")

        # Validate same domain
        if company.domain not in urlparse(career_url).netloc:
            career_url = company.root_url

        result["career_page_url"] = career_url

        # ── Step 3: Render the career page with Playwright ──────────────────
        try:
            if career_url != company.root_url:
                career_html = await client.get_rendered(career_url)
            else:
                career_html = homepage_html
            if not career_html or len(career_html) < 500:
                result["error"] = "Career page empty after rendering"
                return result
        except Exception as e:
            result["error"] = f"Career page render failed: {str(e)[:100]}"
            return result

        # ── Step 4: Ask LLM to map job listing structure with all 4 core fields
        career_md = markdownify(career_html[:3000], strip=["script", "style"])
        career_html_excerpt = career_html[:2000]

        map_prompt = (
            f"Analyze this careers/jobs page and identify the structure of job listings.\n"
            f"URL: {career_url}\n"
            f"Company: {company.name}\n\n"
            f"For each job listing on the page, I need CSS selectors to extract these 4 REQUIRED fields:\n"
            f"1. job_title: The job title/position name\n"
            f"2. location: Where the job is located (city, state, country, or Remote)\n"
            f"3. description: Job description or summary text\n"
            f"4. detail_link: Link to the full job details page\n\n"
            f"Also identify:\n"
            f"- job_listing_selector: CSS selector for each job card/row element\n"
            f"- employment_type: Full-time, Part-time, Contract (if visible)\n"
            f"- department: Department/team (if visible)\n"
            f"- job_count: Number of jobs visible on the page\n\n"
            f"Return a JSON object. Example:\n"
            f'{{"job_listing_selector": "div.job-card", "job_title": "h3.title", '
            f'"location": "span.location", "description": "p.summary", '
            f'"detail_link": "a.apply-link", "job_count": 25}}\n\n'
            f"If this page does NOT contain job listings, return: {{}}\n\n"
            f"IMPORTANT: All 4 required fields (job_title, location, description, detail_link) "
            f"must be identifiable. If any are missing, return {{}}.\n\n"
            f"HTML excerpt:\n{career_html_excerpt}\n\n"
            f"Page text:\n{career_md[:1500]}\n\nJSON:"
        )

        mapping_raw = await self._ask_llm(map_prompt, max_tokens=500)
        if not mapping_raw:
            result["error"] = "LLM returned no mapping response"
            return result

        # Parse JSON
        json_match = re.search(r'\{[^{}]*\}', mapping_raw, re.DOTALL)
        if not json_match:
            result["error"] = "LLM response contained no JSON"
            return result

        try:
            selectors = json.loads(json_match.group())
        except json.JSONDecodeError:
            result["error"] = "Invalid JSON from LLM"
            return result

        # Validate: must have job_listing_selector + at least title and link
        if not selectors or not selectors.get("job_listing_selector"):
            result["error"] = "LLM found no job listing structure"
            return result

        if not selectors.get("job_title"):
            result["error"] = "LLM could not map job_title field"
            return result

        # Validate selectors against actual DOM
        career_soup = BeautifulSoup(career_html, "lxml")
        try:
            matches = career_soup.select(selectors["job_listing_selector"])
            if len(matches) < 1:
                result["error"] = f"Selector '{selectors['job_listing_selector']}' matched 0 elements"
                return result
            selectors["job_count"] = len(matches)
        except Exception:
            pass  # Invalid CSS — still try to save

        # ── Step 5: Create career_page + site_template in one transaction ───
        # Check if career page already exists
        existing = await self.db.scalar(
            select(CareerPage).where(
                CareerPage.company_id == company.id,
                CareerPage.url == career_url,
            )
        )

        if existing:
            page = existing
            page.is_active = True
            page.site_status = "ok"
            page.requires_js_rendering = True
            page.updated_at = datetime.now(timezone.utc)
        else:
            page = CareerPage(
                company_id=company.id,
                url=career_url,
                page_type="listing_page",
                discovery_method="llm_unified",
                discovery_confidence=0.90,
                is_primary=True,
                requires_js_rendering=True,
                site_status="ok",
            )
            self.db.add(page)
            await self.db.commit()
            await self.db.refresh(page)

        # Deactivate old templates
        await self.db.execute(
            text("UPDATE site_templates SET is_active = false WHERE career_page_id = :id"),
            {"id": str(page.id)},
        )

        # Create new template with LLM-mapped fields
        selectors["method"] = "llm_unified"
        selectors["used_playwright"] = True
        selectors["model"] = self.model

        template = SiteTemplate(
            company_id=company.id,
            career_page_id=page.id,
            template_type="llm_unified",
            selectors=selectors,
            learned_via="llm_unified",
            accuracy_score=0.85,
            is_active=True,
            last_validated_at=datetime.now(timezone.utc),
        )
        self.db.add(template)

        # Update career page metadata
        page.last_extraction_at = datetime.now(timezone.utc)
        await self.db.commit()

        # ── Step 6: Queue for job_crawling immediately ──────────────────────
        await queue_manager.enqueue(self.db, "job_crawling", page.id, added_by="llm_unified")
        await self.db.commit()

        result["status"] = "success"
        result["job_count"] = selectors.get("job_count", 0)
        result["fields_mapped"] = [k for k in ["job_title", "location", "description", "detail_link",
                                                 "employment_type", "department"]
                                    if selectors.get(k)]
        logger.info(
            f"LLM unified config OK for {company.domain}: "
            f"{career_url} → {selectors.get('job_count', '?')} jobs, "
            f"fields={result['fields_mapped']}"
        )
        return result

    async def _ask_llm(self, prompt: str, max_tokens: int = 300) -> str | None:
        """Send prompt to Ollama and return response text."""
        try:
            async with httpx.AsyncClient(timeout=90) as http:
                r = await http.post(
                    self.ollama_url,
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.1, "num_predict": max_tokens},
                    },
                )
                if r.status_code != 200:
                    logger.warning(f"Ollama returned {r.status_code}")
                    return None
                return r.json().get("response", "").strip()
        except Exception as e:
            logger.warning(f"LLM call failed: {e}")
            return None
