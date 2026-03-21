"""
Site Structure Extractor — Heuristic Extractor for mapping job listing structure on a site.

Escalation ladder (stops as soon as structure is mapped):
  Layer 1: Extruct — JSON-LD / Microdata / RDFa structured data
  Layer 2: Repeating block detector — finds groups of structurally similar DOM elements
  Layer 3: Learned selector (existing site template)
  Layer 4: Instructor + Pydantic — structured LLM extraction
  Layer 5: 3B parameter LLM
  Layer 6: 8B parameter LLM + LLaVA screenshot analysis

After each layer:
  - If structure mapped → set site_status='ok'
  - If all exhausted → set site_status based on history
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

STATUS_OK = "ok"
STATUS_AT_RISK = "at_risk"
STATUS_NO_STRUCTURE_NEW = "no_structure_new"
STATUS_NO_STRUCTURE_BROKEN = "no_structure_broken"


class SiteStructureExtractor:
    """Orchestrates extraction layers to map job listing structure for a career page."""

    def __init__(self, db):
        self.db = db

    # ATS platforms that have working API extractors — skip structure detection,
    # mark as ok immediately so the job_crawling phase handles via ATS API.
    _ATS_API_PLATFORMS = frozenset([
        "greenhouse", "lever", "ashby", "workday", "bamboohr",
        "smartrecruiters", "jobvite", "icims", "pageup", "applynow",
    ])
    # ATS URL patterns for URL-based fast detection (no DB query needed)
    _ATS_URL_SIGNALS = [
        "boards.greenhouse.io", "job-boards.greenhouse.io",
        "jobs.lever.co", "jobs.ashbyhq.com", "myworkdayjobs.com",
        ".bamboohr.com", "jobs.jobvite.com", "careers.smartrecruiters.com",
        "icims.com", "pageuppeople.com", "applynow.net.au",
        ".teamtailor.com", "breezy.hr", "recruitee.com",
        "jobs.lever.co", "apply.workable.com",
    ]
    # JS-only ATS platforms — also set requires_js_rendering flag
    _JS_ONLY_ATS = frozenset(["workday", "icims", "taleo"])
    _JS_ONLY_URL_SIGNALS = ["myworkdayjobs.com", "icims.com", "taleo.net"]

    async def extract(self, career_page) -> bool:
        """
        Run all extraction layers. Returns True if structure was successfully mapped.
        Updates career_page.site_status on completion.
        """
        logger.info(f"SiteStructureExtractor: starting for {career_page.url}")

        # ── Layer 0: ATS API shortcut ─────────────────────────────────────────
        # If this page belongs to a known API-backed ATS, skip all structure
        # detection layers — jobs are extracted via the ATS API at crawl time.
        url_lower = (career_page.url or "").lower()
        url_is_ats = any(sig in url_lower for sig in self._ATS_URL_SIGNALS)
        ats_platform = None

        if not url_is_ats:
            # Check company ats_platform from DB
            try:
                from app.models.company import Company as _Company
                company = await self.db.get(_Company, career_page.company_id)
                if company:
                    ats_platform = company.ats_platform
                    if ats_platform in self._ATS_API_PLATFORMS:
                        url_is_ats = True
            except Exception as _e:
                logger.debug(f"Layer 0 company lookup failed: {_e}")

        if url_is_ats:
            # Mark requires_js_rendering for JS-only ATS platforms
            is_js_only = (
                ats_platform in self._JS_ONLY_ATS or
                any(sig in url_lower for sig in self._JS_ONLY_URL_SIGNALS)
            )
            if is_js_only and not getattr(career_page, "requires_js_rendering", False):
                from sqlalchemy import text as _text
                await self.db.execute(
                    _text("UPDATE career_pages SET requires_js_rendering = true WHERE id = :id"),
                    {"id": str(career_page.id)},
                )
                logger.info(f"Layer 0: set requires_js_rendering for JS-only ATS {career_page.url}")

            await self._set_status(career_page, STATUS_OK)
            logger.info(f"Layer 0: ATS shortcut → ok for {career_page.url}")
            # Auto-enqueue for job_crawling
            try:
                from app.services import queue_manager
                await queue_manager.enqueue(self.db, "job_crawling", career_page.id, added_by="ats_layer0")
                await self.db.commit()
            except Exception:
                pass
            return True

        # Fetch page HTML — use Playwright if requires_js flag is set
        requires_js = getattr(career_page, "requires_js_rendering", False)
        html = await self._fetch_html(career_page.url, requires_js=requires_js)
        if not html:
            await self._set_status(career_page, STATUS_NO_STRUCTURE_BROKEN
                                   if career_page.last_extraction_at else STATUS_NO_STRUCTURE_NEW)
            return False

        # Auto-detect JS-heavy pages and re-fetch with Playwright if needed
        if not requires_js and self._html_needs_js_rendering(html, career_page.url):
            logger.info(f"JS rendering auto-detected for {career_page.url}, re-fetching with Playwright")
            rendered = await self._fetch_html(career_page.url, requires_js=True)
            if rendered and len(rendered) > len(html) + 500:
                html = rendered
                requires_js = True
                # Persist flag so future crawls skip the plain fetch
                try:
                    from sqlalchemy import text as _text
                    await self.db.execute(
                        _text("UPDATE career_pages SET requires_js_rendering = true WHERE id = :id"),
                        {"id": str(career_page.id)},
                    )
                    await self.db.commit()
                except Exception:
                    pass

        # Layer 1: Structured data (JSON-LD, Microdata, RDFa) via extruct
        if await self._layer_extruct(career_page, html):
            await self._set_status(career_page, STATUS_OK)
            return True

        # Layer 2: Repeating block detector (fast heuristic)
        if await self._layer_repeating_blocks(career_page, html):
            await self._set_status(career_page, STATUS_OK)
            return True

        # Layer 3: Learned selector from existing templates
        if await self._layer_learned_selector(career_page, html):
            await self._set_status(career_page, STATUS_OK)
            return True

        # Layer 4: LLM + Playwright — comprehensive field mapping
        # Escalate to LLM for ALL pages that heuristics couldn't map.
        # The LLM uses Playwright-rendered HTML to handle JS-heavy sites
        # and maps all available fields (title, location, dept, type, etc.)
        from app.core.config import settings as _settings
        if getattr(_settings, "SITE_STRUCTURE_LLM_ENABLED", True):
            # Layer 4: LLM fast (3B) with Playwright
            if await self._layer_llm(career_page, html, model="qwen2.5:3b"):
                await self._set_status(career_page, STATUS_OK)
                return True

            # Layer 5: LLM full (8B) with Playwright
            if await self._layer_llm(career_page, html, model="llama3.1:8b"):
                await self._set_status(career_page, STATUS_OK)
                return True

            # Layer 6: LLM basic field validation — confirm the page contains real job data
            # even when no repeating listing structure is found (handles individual job detail pages).
            # Escalates: qwen2.5:3b → llama3.1:8b
            if await self._layer_llm_field_validation(career_page, html):
                await self._set_status(career_page, STATUS_OK)
                return True

        # All layers failed
        had_structure = career_page.last_extraction_at is not None
        status = STATUS_NO_STRUCTURE_BROKEN if had_structure else STATUS_NO_STRUCTURE_NEW
        await self._set_status(career_page, status)
        logger.warning(f"SiteStructureExtractor: no structure found for {career_page.url} → {status}")
        return False

    async def _fetch_html(self, url: str, requires_js: bool = False) -> str | None:
        try:
            from app.crawlers.http_client import ResilientHTTPClient
            client = ResilientHTTPClient(timeout=25)
            if requires_js:
                return await client.get_rendered(url)
            resp = await client.get(url)
            if not resp:
                return None
            return resp.text if hasattr(resp, "text") else resp.get("html", "")
        except Exception as e:
            logger.debug(f"Fetch failed for {url}: {e}")
            return None

    @staticmethod
    def _html_needs_js_rendering(html: str, url: str) -> bool:
        """Detect JS-heavy / SPA pages that need Playwright for meaningful content."""
        from urllib.parse import urlparse as _up
        # Known ATS domains that always require JS
        JS_DOMAINS = ["myworkdayjobs.com", "icims.com", "taleo.net", "ultipro.com",
                      "successfactors.com", "oraclecloud.com"]
        domain = _up(url).netloc.lower()
        if any(d in domain for d in JS_DOMAINS):
            return True
        # Very sparse visible text relative to HTML size signals a SPA shell
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "lxml")
            for tag in soup(["script", "style", "noscript", "head"]):
                tag.decompose()
            visible = soup.get_text(separator=" ", strip=True)
            if len(html) > 5000 and len(visible) < 200:
                return True
            # SPA framework root markers
            spa_markers = ["data-reactroot", "data-reactid", "__vue__", "data-v-app",
                           "__nuxt__", "__next", "id=\"app\"", "id=\"root\"", "id=\"__next\"",
                           "ng-app", "ng-version"]
            html_lower = html.lower()
            if sum(1 for m in spa_markers if m in html_lower) >= 2:
                return True
        except Exception:
            pass
        return False

    async def _layer_extruct(self, career_page, html: str) -> bool:
        """Layer 1: Extract JSON-LD / Microdata / RDFa structured job data."""
        try:
            import extruct
            from w3lib.html import get_base_url
            base_url = get_base_url(html, career_page.url)
            data = extruct.extract(html, base_url=base_url, syntaxes=['json-ld', 'microdata', 'rdfa'])

            job_items = []
            for syntax, items in data.items():
                for item in (items or []):
                    if isinstance(item, dict):
                        t = item.get('@type', '')
                        if 'JobPosting' in str(t) or 'Job' in str(t):
                            job_items.append(item)

            if not job_items:
                return False

            # We found structured job data — extract selectors from first item
            selectors = {
                "method": "json_ld",
                "job_count": len(job_items),
                "sample_title": job_items[0].get('title', '') if job_items else '',
            }
            await self._save_template(career_page, "json_ld", selectors, accuracy=0.92)
            logger.info(f"extruct found {len(job_items)} job postings at {career_page.url}")
            return True
        except Exception as e:
            logger.debug(f"extruct layer failed: {e}")
            return False

    async def _layer_repeating_blocks(self, career_page, html: str) -> bool:
        """Layer 2: Find repeating DOM blocks that likely contain job listings.

        False-positive prevention:
          - Strips nav/header/footer before analysis so menus don't match.
          - Requires at least 5 matching elements (raises from old threshold of 3).
          - Requires ≥70% of elements to contain a link (job cards almost always link
            to detail pages; nav items often don't link to detail pages in this pattern).
          - Requires minimum text length of 40 chars per element (nav items are short).
          - Requires a majority (≥4 of 8 sampled) to contain strong or multiple weak
            job-listing signals.
        """
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, 'lxml')

            # Remove chrome/navigation so we don't accidentally match menus, footers, etc.
            for tag in soup(["nav", "header", "footer", "script", "style", "noscript"]):
                tag.decompose()

            # Strong signals = keywords highly specific to job listings
            STRONG_SIGNALS = frozenset([
                'full-time', 'part-time', 'contract', 'permanent',
                'salary', 'qualifications', 'responsibilities', 'requirements',
                'benefits', 'hybrid', 'deadline', 'closing date',
            ])
            # Weak signals = common on job pages but also elsewhere
            WEAK_SIGNALS = frozenset(['apply', 'location', 'remote', 'experience', 'role', 'position'])

            best_selector = None
            best_count = 0

            for tag in ['li', 'div', 'article', 'tr']:
                # Group elements by their parent + class fingerprint
                groups: dict[str, list] = {}
                for el in soup.find_all(tag):
                    classes = ' '.join(sorted(el.get('class', [])))
                    parent_tag = el.parent.name if el.parent else ''
                    key = f"{parent_tag}>{tag}.{classes}"
                    if len(key) < 100:  # skip elements with huge class strings
                        groups.setdefault(key, []).append(el)

                for key, elements in groups.items():
                    if len(elements) < 5:
                        continue

                    # Require that most elements contain a link — job cards link to detail pages
                    elements_with_links = [el for el in elements if el.find('a')]
                    if len(elements_with_links) / len(elements) < 0.7:
                        continue

                    # Score each of the first 8 elements individually
                    job_like_count = 0
                    for el in elements[:8]:
                        text = el.get_text()
                        # Skip very short elements (nav items, tag badges, etc.)
                        if len(text.strip()) < 40:
                            continue
                        text_lower = text.lower()
                        strong = sum(1 for kw in STRONG_SIGNALS if kw in text_lower)
                        weak = sum(1 for kw in WEAK_SIGNALS if kw in text_lower)
                        # Element looks job-like: 1 strong signal OR 3+ weak signals
                        if strong >= 1 or weak >= 3:
                            job_like_count += 1

                    # Require majority (≥4 of 8 sampled) to look like job listings
                    if job_like_count >= 4 and len(elements) > best_count:
                        best_count = len(elements)
                        best_selector = key

            if not best_selector or best_count < 5:
                return False

            # Build CSS selector from the winning key
            parts = best_selector.split('>')
            classes = parts[-1].split('.')
            tag_name = classes[0].split(' ')[0].strip()
            css_class = classes[1] if len(classes) > 1 else ''
            selector = f"{tag_name}.{css_class}" if css_class else tag_name

            selectors = {
                "method": "repeating_block",
                "job_listing_selector": selector,
                "job_count_estimate": best_count,
            }

            # Try to detect a location selector within the matched elements
            _LOC_CANDIDATE_SELECTORS = (
                "[class*='location']", "[class*='city']", "[class*='place']",
                "[class*='job-location']", "[itemprop='jobLocation']",
                "[itemprop='addressLocality']", "[data-testid*='location']",
            )
            try:
                # Re-select elements using the winning selector
                test_elements = soup.select(selector)[:8]
                best_loc_sel = None
                best_loc_hits = 0
                for loc_sel in _LOC_CANDIDATE_SELECTORS:
                    hits = sum(1 for el in test_elements if el.select_one(loc_sel))
                    if hits > best_loc_hits:
                        best_loc_hits = hits
                        best_loc_sel = loc_sel
                # Also check class-based selectors dynamically from the elements
                if not best_loc_sel or best_loc_hits < len(test_elements) * 0.4:
                    # Scan for common class patterns across elements
                    from collections import Counter
                    class_counter = Counter()
                    for el in test_elements:
                        for child in el.find_all(True):
                            for cls in child.get("class", []):
                                cls_lower = cls.lower()
                                if any(kw in cls_lower for kw in ("loc", "city", "place", "region", "office")):
                                    css = f".{cls}"
                                    class_counter[css] += 1
                    if class_counter:
                        top_cls, top_count = class_counter.most_common(1)[0]
                        if top_count >= len(test_elements) * 0.4:
                            best_loc_sel = top_cls
                            best_loc_hits = top_count
                if best_loc_sel and best_loc_hits >= 2:
                    selectors["location_selector"] = best_loc_sel
                    logger.info(f"Detected location selector '{best_loc_sel}' ({best_loc_hits}/{len(test_elements)} hits)")
            except Exception as e:
                logger.debug(f"Location selector detection failed: {e}")

            await self._save_template(career_page, "repeating_block", selectors, accuracy=0.72)
            logger.info(f"Repeating block found {best_count} listings at {career_page.url}")
            return True
        except Exception as e:
            logger.debug(f"Repeating block layer failed: {e}")
            return False

    async def _layer_learned_selector(self, career_page, html: str) -> bool:
        """Layer 3: Try applying existing learned selectors from site_templates."""
        try:
            from sqlalchemy import select
            from app.models.site_template import SiteTemplate
            from bs4 import BeautifulSoup

            tmpl = await self.db.scalar(
                select(SiteTemplate).where(
                    SiteTemplate.career_page_id == career_page.id,
                    SiteTemplate.is_active == True,
                )
            )
            if not tmpl or not tmpl.selectors:
                return False

            # Try applying the selectors to see if they still work
            soup = BeautifulSoup(html, 'lxml')
            selector = (tmpl.selectors or {}).get('job_listing_selector', '')
            if not selector:
                return False

            matches = soup.select(selector)
            if len(matches) >= 2:
                # Template still works — update last_validated_at
                from sqlalchemy import text
                await self.db.execute(
                    text("UPDATE site_templates SET last_validated_at = NOW() WHERE id = :id"),
                    {"id": str(tmpl.id)}
                )
                await self.db.commit()
                logger.info(f"Learned selector still valid for {career_page.url}: {len(matches)} matches")
                return True
            return False
        except Exception as e:
            logger.debug(f"Learned selector layer failed: {e}")
            return False

    async def _layer_llm(self, career_page, html: str, model: str = "qwen2.5:3b") -> bool:
        """LLM-powered comprehensive field mapping.

        Uses Playwright-rendered HTML + LLM to identify:
        - Job listing container selector
        - Title, location, description, employment type, salary selectors
        - Individual job detail page URL pattern

        This is the most powerful extraction layer — handles any site structure.
        """
        try:
            import httpx, json, re
            from app.core.config import settings
            from markdownify import markdownify

            ollama_host = getattr(settings, 'OLLAMA_HOST', 'ollama')
            ollama_url = f"http://{ollama_host}:11434/api/generate"

            # Try Playwright-rendered HTML first (catches JS-rendered content)
            rendered_html = html
            try:
                from app.crawlers.http_client import ResilientHTTPClient
                client = ResilientHTTPClient(timeout=20)
                rendered = await client.get_rendered(career_page.url)
                if rendered and len(rendered) > len(html) + 500:
                    rendered_html = rendered
            except Exception:
                pass

            # Convert to readable markdown for better LLM comprehension
            page_md = markdownify(rendered_html[:10000], strip=["script", "style", "nav"])
            html_excerpt = rendered_html[:5000]

            prompt = (
                f"Analyze this careers page and identify the structure of job listings.\n"
                f"URL: {career_page.url}\n\n"
                f"Return a JSON object with these fields:\n"
                f"  job_listing_selector: CSS selector for each job card/row element\n"
                f"  title_selector: CSS selector for the job title within each card\n"
                f"  location_selector: CSS selector for location within each card\n"
                f"  link_selector: CSS selector for the link to the job detail page\n"
                f"  department_selector: CSS selector for department (if visible)\n"
                f"  employment_type_selector: CSS selector for job type (if visible)\n"
                f"  job_count: estimated number of jobs visible on the page\n"
                f"  detail_url_pattern: URL pattern for individual job pages (if identifiable)\n\n"
                f"Example response:\n"
                f'{{"job_listing_selector": "div.job-card", "title_selector": "h3.title", '
                f'"location_selector": "span.location", "link_selector": "a.job-link", '
                f'"job_count": 25, "detail_url_pattern": "/jobs/{{id}}-{{slug}}"}}\n\n'
                f"If this is NOT a job listing page, return: {{}}\n\n"
                f"HTML excerpt:\n{html_excerpt}\n\n"
                f"Page text:\n{page_md[:3000]}\n\nJSON:"
            )

            async with httpx.AsyncClient(timeout=30) as http:
                r = await http.post(ollama_url, json={
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1, "num_predict": 400},
                })
                if r.status_code != 200:
                    return False
                raw = r.json().get("response", "").strip()

            match = re.search(r'\{.*?\}', raw, re.DOTALL)
            if not match:
                return False
            selectors = json.loads(match.group())
            if not selectors or not selectors.get("job_listing_selector"):
                return False

            # Validate the selector actually matches elements
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(rendered_html, "lxml")
            try:
                matches = soup.select(selectors["job_listing_selector"])
                if len(matches) < 2:
                    logger.debug(f"LLM selector matched {len(matches)} elements — too few")
                    return False
                selectors["job_count"] = len(matches)
            except Exception:
                pass  # Invalid CSS selector — still save it, template extraction will handle

            selectors["method"] = f"llm_{model.replace(':', '_')}"
            selectors["used_playwright"] = rendered_html != html
            await self._save_template(career_page, "llm_bootstrapped", selectors, accuracy=0.75)
            logger.info(f"LLM ({model}) mapped {selectors.get('job_count', '?')} jobs with {len(selectors)} fields for {career_page.url}")
            return True
        except Exception as e:
            logger.warning(f"LLM layer ({model}) failed for {career_page.url}: {e}")
            return False

    async def _save_template(self, career_page, template_type: str, selectors: dict, accuracy: float):
        """Save or update site template and update career_page stats."""
        try:
            from sqlalchemy import text
            from sqlalchemy import select
            from app.models.site_template import SiteTemplate
            import json

            # Deactivate any existing templates for this page
            await self.db.execute(
                text("UPDATE site_templates SET is_active = false WHERE career_page_id = :id"),
                {"id": str(career_page.id)}
            )

            new_tmpl = SiteTemplate(
                company_id=career_page.company_id,
                career_page_id=career_page.id,
                template_type=template_type,
                selectors=selectors,
                learned_via=template_type,
                accuracy_score=accuracy,
                is_active=True,
                last_validated_at=datetime.now(timezone.utc),
            )
            self.db.add(new_tmpl)

            # Update last_extraction_at on career page
            await self.db.execute(
                text("UPDATE career_pages SET last_extraction_at = NOW() WHERE id = :id"),
                {"id": str(career_page.id)}
            )
            await self.db.commit()
            # Auto-enqueue: structure mapped → trigger job crawl for this page
            try:
                from app.services import queue_manager
                await queue_manager.enqueue(self.db, "job_crawling", career_page.id, added_by="structure_mapped")
                await self.db.commit()
            except Exception as qe:
                logger.debug(f"Failed to enqueue job_crawling after template save: {qe}")
        except Exception as e:
            logger.warning(f"Failed to save template: {e}")
            await self.db.rollback()

    async def _layer_llm_field_validation(self, career_page, html: str) -> bool:
        """Layer 6: Ask LLM to extract basic job fields (title, location, description).

        Used when structural analysis finds no repeating listing pattern — handles
        individual job detail pages and JS-heavy sites that render to plain HTML.
        Escalates from 3B to 8B model if the first attempt finds nothing.

        On success, saves extracted fields as a template so job_extractor can
        re-use them without repeating LLM inference.
        """
        for model in ["qwen2.5:3b", "llama3.1:8b"]:
            result = await self._try_llm_field_extract(career_page, html, model)
            if result:
                return True
        return False

    async def _try_llm_field_extract(self, career_page, html: str, model: str) -> bool:
        """Ask LLM to confirm and extract job fields from a page.

        Returns True if the LLM confirmed this is a real job posting with
        at least a title and saves the extracted fields to site_templates.
        """
        try:
            import httpx, json, re
            from markdownify import markdownify
            from app.core.config import settings

            ollama_host = getattr(settings, "OLLAMA_HOST", "ollama")
            ollama_url = f"http://{ollama_host}:11434/api/generate"

            # Convert HTML to readable markdown — trim to keep prompt manageable
            md = markdownify(html[:5000], strip=["script", "style"])

            prompt = (
                f"Analyze this web page and determine if it is a job posting.\n"
                f"URL: {career_page.url}\n\n"
                f"If it IS a job posting, return JSON with these keys:\n"
                f"  is_job: true\n"
                f"  title: job title (string)\n"
                f"  location: location text (string, empty if not found)\n"
                f"  description: first 300 chars of job description (string)\n"
                f"  employment_type: e.g. Full-time, Part-time, Contract (string, empty if not found)\n\n"
                f"If it is NOT a job posting (it's a listing page, homepage, error page, etc.), return:\n"
                f'  {{"is_job": false}}\n\n'
                f"Return ONLY the JSON object, nothing else.\n\n"
                f"Page content:\n{md}\n\nJSON:"
            )

            async with httpx.AsyncClient(timeout=30) as http:
                r = await http.post(
                    ollama_url,
                    json={
                        "model": model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.1, "num_predict": 400},
                    },
                )
                if r.status_code != 200:
                    logger.warning(f"LLM field validation: Ollama returned {r.status_code} for {model}")
                    return False
                raw = r.json().get("response", "").strip()

            # Extract JSON from response
            m = re.search(r"\{.*?\}", raw, re.DOTALL)
            if not m:
                logger.debug(f"LLM field validation ({model}): no JSON in response")
                return False

            data = json.loads(m.group())
            if not data.get("is_job") or not data.get("title"):
                logger.debug(f"LLM field validation ({model}): not a job page at {career_page.url}")
                return False

            # Confirmed as a job posting — save extracted fields as a template
            selectors = {
                "method": f"llm_field_validated_{model.replace(':', '_')}",
                "page_type": "single_job",
                "extracted_title": str(data.get("title", ""))[:200],
                "extracted_location": str(data.get("location", "") or ""),
                "extracted_description": str(data.get("description", "") or ""),
                "extracted_employment_type": str(data.get("employment_type", "") or ""),
            }
            await self._save_template(career_page, "llm_field_validated", selectors, accuracy=0.65)

            # Mark as single-job page type so job_extractor knows to treat it as one job
            from sqlalchemy import text
            await self.db.execute(
                text("UPDATE career_pages SET page_type = 'single_job' WHERE id = :id"),
                {"id": str(career_page.id)},
            )
            await self.db.commit()

            logger.info(
                f"LLM field validation ({model}) confirmed job at {career_page.url}: "
                f"'{data['title'][:60]}' / '{data.get('location', '')[:40]}'"
            )
            return True

        except Exception as e:
            logger.warning(f"LLM field validation ({model}) failed for {career_page.url}: {e}")
            return False

    async def _set_status(self, career_page, status: str):
        from sqlalchemy import text
        await self.db.execute(
            text("UPDATE career_pages SET site_status = :s WHERE id = :id"),
            {"s": status, "id": str(career_page.id)}
        )
        await self.db.commit()
