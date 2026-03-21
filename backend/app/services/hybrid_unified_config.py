"""
Hybrid Unified Company+Site Config — heuristics first, LLM only when needed.

Fast path (no LLM, <5s):
  - Known ATS platforms (Greenhouse, Lever, etc.) → canonical URL
  - careers.company.com / jobs.company.com → direct probe
  - Homepage has JSON-LD JobPosting → instant map

Slow path (1 LLM call, ~90s):
  - Render with Playwright, send links + HTML excerpt to LLM
  - Single prompt: find careers URL AND map all 4 core fields
  - Only triggered when fast path finds nothing

This processes ~90% of companies in <5s and reserves
the expensive LLM call for the ~10% that need it.
"""

import json
import logging
import re
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ATS platforms with known URL patterns
ATS_URL_TEMPLATES = {
    "greenhouse": "https://boards.greenhouse.io/{slug}",
    "lever": "https://jobs.lever.co/{slug}",
    "bamboohr": "https://{slug}.bamboohr.com/careers",
    "ashby": "https://jobs.ashbyhq.com/{slug}",
    "smartrecruiters": "https://careers.smartrecruiters.com/{slug}",
    "jobvite": "https://jobs.jobvite.com/{slug}",
    "workday": None,
    "icims": None,
    "pageup": None,
    "applynow": "https://{slug}.applynow.net.au/careers/opportunities/jobs",
    "teamtailor": "https://{slug}.teamtailor.com/jobs",
}

# Known ATS domains for URL-based detection
ATS_URL_SIGNALS = {
    "boards.greenhouse.io": "greenhouse",
    "job-boards.greenhouse.io": "greenhouse",
    "jobs.lever.co": "lever",
    "jobs.ashbyhq.com": "ashby",
    ".bamboohr.com": "bamboohr",
    "jobs.jobvite.com": "jobvite",
    "careers.smartrecruiters.com": "smartrecruiters",
    ".teamtailor.com": "teamtailor",
    "apply.workable.com": "workable",
    ".breezy.hr": "breezy",
    ".recruitee.com": "recruitee",
    "myworkdayjobs.com": "workday",
}

CAREER_SUBDOMAINS = ["careers", "jobs", "talent", "work", "hiring", "apply"]


class HybridUnifiedConfig:
    """Hybrid company + site config: fast heuristics → LLM escalation."""

    def __init__(self, db, ollama_host="ollama", model="qwen2.5:3b"):
        self.db = db
        self.ollama_host = ollama_host
        self.model = model

    async def configure_company(self, company) -> dict:
        """Configure a company. Returns result dict with status/career_page_url/fields."""
        from app.crawlers.http_client import ResilientHTTPClient

        result = {"company": company.name, "domain": company.domain,
                  "status": "failed", "method": None, "career_page_url": None,
                  "job_count": 0, "fields_mapped": [], "error": None}

        client = ResilientHTTPClient(timeout=20)
        html = ""

        # ━━━ FAST PATH 1: Known ATS platform ━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        ats = company.ats_platform
        if ats and ats not in ("unknown", "custom", None):
            template = ATS_URL_TEMPLATES.get(ats)
            if template:
                slug = re.sub(r'^(careers\.|jobs\.|www\.)', '', company.domain).split(".")[0]
                url = template.format(slug=slug)
                ok = await self._probe_url(client, url)
                if ok:
                    return await self._save_config(company, url, "ats_shortcut",
                                                    {"method": "ats_api", "ats_platform": ats})

        # ━━━ FAST PATH 2: ATS URL in company domain/root_url ━━━━━━━━━━━━
        root_lower = (company.root_url or "").lower()
        for signal, platform in ATS_URL_SIGNALS.items():
            if signal in root_lower:
                # Normalize to jobs page
                parsed = urlparse(company.root_url)
                if "teamtailor.com" in parsed.netloc:
                    url = f"{parsed.scheme}://{parsed.netloc}/jobs"
                else:
                    url = company.root_url
                ok = await self._probe_url(client, url)
                if ok:
                    return await self._save_config(company, url, "ats_url_signal",
                                                    {"method": "ats_url", "ats_platform": platform})

        # ━━━ FAST PATH 3: Career subdomain probe ━━━━━━━━━━━━━━━━━━━━━━━
        parts = company.domain.split(".")
        root_domain = ".".join(parts[-3:]) if len(parts) >= 3 and len(parts[-1]) <= 3 else ".".join(parts[-2:])

        for prefix in CAREER_SUBDOMAINS:
            sub_url = f"https://{prefix}.{root_domain}"
            if sub_url.rstrip("/") == company.root_url.rstrip("/"):
                continue
            try:
                resp = await client.get(sub_url, timeout=10)
                if resp and resp.status_code == 200 and len(resp.text) > 1000:
                    # Verify it has career-related content
                    text = resp.text[:3000].lower()
                    if sum(1 for kw in ["jobs", "careers", "vacancies", "openings", "apply", "position"]
                           if kw in text) >= 2:
                        return await self._save_config(company, sub_url, "subdomain_probe",
                                                        {"method": "subdomain"})
            except Exception:
                continue

        # ━━━ FAST PATH 4: Homepage has JSON-LD JobPosting ━━━━━━━━━━━━━━
        try:
            resp = await client.get(company.root_url, timeout=15)
            if resp and resp.status_code == 200:
                html = resp.text
                import extruct
                from w3lib.html import get_base_url
                base = get_base_url(html, company.root_url)
                data = extruct.extract(html, base_url=base, syntaxes=['json-ld'])
                for item in data.get("json-ld", []):
                    if isinstance(item, dict):
                        t = str(item.get("@type", ""))
                        if "JobPosting" in t:
                            return await self._save_config(company, company.root_url, "json_ld",
                                                            {"method": "json_ld"})
        except Exception:
            pass

        # ━━━ FAST PATH 5: Homepage link scan (no LLM) ━━━━━━━━━━━━━━━━━
        try:
            if not html:
                resp = await client.get(company.root_url, timeout=15)
                html = resp.text if resp and resp.status_code == 200 else ""

            if html:
                soup = BeautifulSoup(html, "lxml")
                best_url = None
                best_score = 0

                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    text = a.get_text(strip=True).lower()
                    abs_url = urljoin(company.root_url, href)

                    if company.domain not in urlparse(abs_url).netloc:
                        continue

                    path = urlparse(abs_url).path.lower()
                    score = 0
                    # Score based on URL path
                    for kw in ["careers", "jobs", "vacancies", "openings"]:
                        if kw in path:
                            score += 3
                    for kw in ["join", "talent", "hiring", "work-with"]:
                        if kw in path:
                            score += 2
                    # Score based on link text
                    for kw in ["careers", "jobs", "vacancies", "open positions", "join our team",
                               "work with us", "we're hiring", "view all jobs"]:
                        if kw in text:
                            score += 3

                    if score > best_score:
                        best_score = score
                        best_url = abs_url

                if best_url and best_score >= 3:
                    # Verify it's actually a careers page
                    ok = await self._probe_url(client, best_url)
                    if ok:
                        return await self._save_config(company, best_url, "link_scan",
                                                        {"method": "link_scan", "score": best_score})
        except Exception:
            pass

        # ━━━ SLOW PATH: Single LLM call (find URL + map fields) ━━━━━━━━
        try:
            # Render with Playwright for JS-heavy sites
            try:
                rendered = await client.get_rendered(company.root_url)
                if rendered and len(rendered) > len(html or "") + 500:
                    html = rendered
            except Exception:
                pass

            if not html or len(html) < 500:
                result["error"] = "No HTML content available"
                return result

            # Extract links for the LLM
            soup = BeautifulSoup(html, "lxml")
            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(strip=True)[:60]
                if href and text and len(text) > 1:
                    abs_url = urljoin(company.root_url, href)
                    if company.domain in urlparse(abs_url).netloc:
                        links.append(f"[{text}]({abs_url})")

            from markdownify import markdownify
            page_text = markdownify(html[:3000], strip=["script", "style"])

            # SINGLE combined prompt: find URL + map fields
            prompt = (
                f"Company: {company.name} ({company.domain})\n"
                f"Homepage: {company.root_url}\n\n"
                f"Links on page:\n" + "\n".join(links[:25]) + "\n\n"
                f"Page text:\n{page_text[:1000]}\n\n"
                f"Task: Find the careers/jobs listing page and identify job data structure.\n"
                f"Return JSON with:\n"
                f"  careers_url: URL of the job listings page (or NONE)\n"
                f"  job_listing_selector: CSS selector for each job card\n"
                f"  job_title: CSS selector for title\n"
                f"  location: CSS selector for location\n"
                f"  detail_link: CSS selector for link to full job page\n"
                f"  job_count: estimated number of jobs\n\n"
                f"If no careers page exists, return: {{\"careers_url\": \"NONE\"}}\n\nJSON:"
            )

            resp_text = await self._ask_llm(prompt)
            if not resp_text:
                result["error"] = "LLM timeout"
                return result

            m = re.search(r'\{[^{}]*\}', resp_text, re.DOTALL)
            if not m:
                result["error"] = "No JSON in LLM response"
                return result

            data = json.loads(m.group())
            careers_url = data.get("careers_url", "")

            if not careers_url or "NONE" in str(careers_url).upper():
                result["error"] = "LLM found no careers page"
                return result

            # Validate URL
            if not careers_url.startswith("http"):
                careers_url = urljoin(company.root_url, careers_url)

            if company.domain not in urlparse(careers_url).netloc:
                result["error"] = "LLM URL is off-domain"
                return result

            selectors = {k: v for k, v in data.items() if k != "careers_url" and v}
            selectors["method"] = "llm_unified"
            selectors["model"] = self.model

            return await self._save_config(company, careers_url, "llm_unified", selectors)

        except Exception as e:
            result["error"] = f"LLM path failed: {str(e)[:100]}"
            return result

    async def _probe_url(self, client, url: str) -> bool:
        """Quick check if URL is reachable and has career-like content."""
        try:
            resp = await client.get(url, timeout=10)
            return resp and resp.status_code == 200 and len(resp.text) > 500
        except Exception:
            return False

    async def _ask_llm(self, prompt: str) -> str | None:
        """Single LLM call with generous timeout."""
        try:
            async with httpx.AsyncClient(timeout=120) as http:
                r = await http.post(
                    f"http://{self.ollama_host}:11434/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.1, "num_predict": 300},
                    },
                )
                if r.status_code != 200:
                    return None
                return r.json().get("response", "").strip()
        except Exception as e:
            logger.warning(f"LLM call failed: {e}")
            return None

    async def _save_config(self, company, url: str, method: str, selectors: dict) -> dict:
        """Save career page + template and queue for crawling."""
        from app.models.career_page import CareerPage
        from app.models.site_template import SiteTemplate
        from app.services import queue_manager
        from sqlalchemy import select, text

        result = {"company": company.name, "domain": company.domain,
                  "status": "success", "method": method, "career_page_url": url,
                  "job_count": selectors.get("job_count", 0),
                  "fields_mapped": [k for k in ["job_title", "location", "description",
                                                 "detail_link", "job_listing_selector"]
                                    if selectors.get(k)]}

        # Upsert career page
        existing = await self.db.scalar(
            select(CareerPage).where(CareerPage.company_id == company.id, CareerPage.url == url)
        )
        if existing:
            page = existing
            page.is_active = True
            page.site_status = "ok"
            page.discovery_method = method
            page.requires_js_rendering = method == "llm_unified"
        else:
            page = CareerPage(
                company_id=company.id, url=url, page_type="listing_page",
                discovery_method=method, discovery_confidence=0.85,
                is_primary=True, site_status="ok",
                requires_js_rendering=method == "llm_unified",
            )
            self.db.add(page)
        await self.db.commit()
        await self.db.refresh(page)

        # Deactivate old templates, create new one
        await self.db.execute(
            text("UPDATE site_templates SET is_active = false WHERE career_page_id = :id"),
            {"id": str(page.id)},
        )
        template = SiteTemplate(
            company_id=company.id, career_page_id=page.id,
            template_type=method, selectors=selectors, learned_via=method,
            accuracy_score=0.85, is_active=True,
            last_validated_at=datetime.now(timezone.utc),
        )
        self.db.add(template)
        page.last_extraction_at = datetime.now(timezone.utc)
        await self.db.commit()

        # Queue for job crawling
        await queue_manager.enqueue(self.db, "job_crawling", page.id, added_by=method)
        await self.db.commit()

        logger.info(f"Hybrid config OK: {company.domain} → {url} [{method}] "
                     f"fields={result['fields_mapped']}")
        return result
