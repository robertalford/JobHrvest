"""
Career Page Discoverer — Stage 2 of the pipeline.

Discovers careers/jobs pages on a company website using multiple methods:
  2a. Heuristic URL & link analysis (depth-3 BFS with scoring)
  2b. ATS detection shortcut (canonical ATS URLs)
  2c. Subdomain probe (careers.company.com, jobs.company.com)
"""

import logging
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.core.config import settings
from app.crawlers.domain_blocklist import assert_not_blocked, is_blocked
from app.crawlers.http_client import ResilientHTTPClient
from app.models.career_page import CareerPage
from app.models.company import Company

logger = logging.getLogger(__name__)

# URL path keywords — strong signals for career page scoring
URL_KEYWORDS_HIGH = [
    "careers", "jobs", "vacancies", "openings", "opportunities",
    "join-us", "join-our-team", "work-with-us", "we-are-hiring",
    "current-openings", "job-listings", "job-board",
]
URL_KEYWORDS_MED = [
    "employment", "talent", "hiring", "positions", "roles",
    "recruitment", "apply", "working-here", "work-here",
]

LINK_TEXT_KEYWORDS_HIGH = [
    "careers", "jobs", "we're hiring", "we are hiring", "join our team",
    "open positions", "job openings", "view all jobs", "view openings",
    "search jobs", "current vacancies", "work with us",
]
LINK_TEXT_KEYWORDS_MED = [
    "join us", "employment", "vacancies", "opportunities", "apply",
    "talent", "grow with us",
]

# ATS-known career page URL templates
ATS_URL_TEMPLATES = {
    "greenhouse": "https://boards.greenhouse.io/{slug}",
    "lever": "https://jobs.lever.co/{slug}",
    "bamboohr": "https://{slug}.bamboohr.com/careers",
    "ashby": "https://jobs.ashbyhq.com/{slug}",
    "smartrecruiters": "https://careers.smartrecruiters.com/{slug}",
    "jobvite": "https://jobs.jobvite.com/{slug}",
    "workday": None,  # URL varies per company — discovered via heuristics
    "pageup": "https://careers.pageuppeople.com/{slug}/external/",
    "applynow": "https://{slug}.applynow.net.au/careers/opportunities/jobs",
}

# Common career subdomain prefixes to probe
CAREER_SUBDOMAINS = ["careers", "jobs", "talent", "work", "hiring", "apply"]

# Non-HTML file extensions to skip
SKIP_EXTENSIONS = {".pdf", ".doc", ".docx", ".xml", ".zip", ".png", ".jpg",
                   ".jpeg", ".gif", ".svg", ".mp4", ".mp3", ".css", ".js"}

# Max career pages to store per company — prevents crawl explosions on ATS-hosted sites
# (e.g. careers.footlocker.com had 1,287 individual job URLs saved as "listing pages")
MAX_PAGES_PER_COMPANY = 5

# Regex: URL patterns that strongly indicate an individual job detail page (not a listing)
_JOB_DETAIL_PATH_RE = re.compile(
    r'/jobs?/[^/?]{8,}$'           # /jobs/some-long-title-or-id
    r'|/careers?/[^/?]{8,}$'       # /careers/some-job-title
    r'|/positions?/[^/?]{8,}$'
    r'|/vacancies?/[^/?]{8,}$'
    r'|/openings?/[^/?]{8,}$'
    r'|/role/[^/?]{5,}$'
    r'|/job[-_]\d{4,}'             # /job-12345
    r'|/req[-_]?\d{4,}'            # /req12345
    r'|/posting[-_/][a-zA-Z0-9]{6,}$'  # /posting/abc123def
    r'|[?&](jobId|job_id|requisitionId|req_id|positionId)=',
    re.IGNORECASE,
)


class CareerPageDiscoverer:
    def __init__(self, db):
        self.db = db
        self.client = ResilientHTTPClient(timeout=20)

    async def discover(self, company: Company) -> list[CareerPage]:
        """Discover all career pages for a company. Returns list of saved CareerPage objects."""
        candidates: list[dict] = []

        # 2e: Self-referential — if the company domain itself looks like a career site, include root URL
        domain_lower = company.domain.lower()
        career_domain_prefixes = ("careers.", "jobs.", "talent.", "hiring.", "work.", "apply.", "recruitment.")
        career_domain_keywords = ("careers", "jobs", "vacancies", "recruitment", "talent")
        if (any(domain_lower.startswith(p) for p in career_domain_prefixes) or
                any(kw in domain_lower for kw in career_domain_keywords)):
            candidates.append({
                "url": company.root_url,
                "discovery_method": "domain_signal",
                "confidence": 0.80,
                "is_primary": True,
                "page_type": "listing_page",
            })

        # 2b: ATS shortcut — canonical ATS URL based on known platform
        if company.ats_platform and company.ats_platform not in ("unknown", "custom", None):
            ats_urls = self._ats_candidate_urls(company)
            candidates.extend(ats_urls)
            if any(c["confidence"] >= 0.9 for c in ats_urls):
                # High-confidence ATS URL — still run heuristics for completeness
                logger.info(f"ATS shortcut for {company.domain}: {company.ats_platform}")

        # 2c: Probe common career subdomains
        subdomain_results = await self._probe_career_subdomains(company)
        candidates.extend(subdomain_results)

        # 2a: Heuristic link analysis (BFS depth 3)
        heuristic_results = await self._heuristic_discovery(company.root_url, company.domain)
        candidates.extend(heuristic_results)

        # 2d: LLM-powered discovery — ALWAYS run to validate/improve heuristic results
        # Playwright renders the homepage, LLM identifies the correct job listing page
        llm_results = await self._llm_discovery(company)
        candidates.extend(llm_results)

        # Deduplicate by URL (keep highest confidence)
        seen: dict[str, dict] = {}
        for c in candidates:
            url = c["url"]
            if url not in seen or c["confidence"] > seen[url]["confidence"]:
                seen[url] = c

        # QUALITY GATE: Only save pages discovered/validated by LLM or ATS detection.
        # Heuristic-only candidates are used as input for LLM but never saved directly.
        TRUSTED_METHODS = {"llm_playwright", "ats_fingerprint", "ats_bulk"}
        pages = []
        sorted_candidates = sorted(seen.items(), key=lambda x: x[1]["confidence"], reverse=True)
        for url, meta in sorted_candidates:
            if len(pages) >= MAX_PAGES_PER_COMPANY:
                break
            method = meta.get("discovery_method", "")
            # Only save LLM-validated or ATS-detected pages
            if method not in TRUSTED_METHODS:
                # Exception: domain_signal with very high confidence (careers.company.com)
                if method == "domain_signal" and meta["confidence"] >= 0.80:
                    pass  # Allow through
                else:
                    continue
            if meta["confidence"] < 0.50:
                continue
            if _JOB_DETAIL_PATH_RE.search(urlparse(url).path + "?" + urlparse(url).query):
                continue
            page = await self._upsert_career_page(company, url, meta)
            if page:
                pages.append(page)

        # If LLM/ATS found nothing but heuristics did, try LLM validation on top heuristic candidates
        if not pages:
            heuristic_candidates = [(url, meta) for url, meta in sorted_candidates
                                     if meta.get("discovery_method") in ("heuristic", "domain_signal", "subdomain_probe")
                                     and meta["confidence"] >= 0.60]
            if heuristic_candidates:
                best_url, best_meta = heuristic_candidates[0]
                # Save the best heuristic candidate as a fallback with reduced confidence
                best_meta["discovery_method"] = "heuristic_fallback"
                best_meta["confidence"] = min(best_meta["confidence"], 0.60)
                page = await self._upsert_career_page(company, best_url, best_meta)
                if page:
                    pages.append(page)
                    logger.info(f"Fallback: saved top heuristic candidate for {company.domain}: {best_url}")

        logger.info(f"Discovered {len(pages)} career pages for {company.domain}")
        return pages

    def _ats_candidate_urls(self, company: Company) -> list[dict]:
        """Generate ATS-known career page URL candidates from company domain slug."""
        # Try domain slug variants
        domain = company.domain
        # Strip common prefixes like "careers.", "jobs.", "www."
        base = re.sub(r'^(careers\.|jobs\.|www\.)', '', domain)
        slug = base.split(".")[0]

        results = []
        template = ATS_URL_TEMPLATES.get(company.ats_platform)
        if template:
            for candidate_slug in [slug, domain.split(".")[0]]:
                url = template.format(slug=candidate_slug)
                if not is_blocked(url):
                    results.append({
                        "url": url,
                        "discovery_method": "ats_fingerprint",
                        "confidence": company.ats_confidence or 0.88,
                        "is_primary": True,
                        "page_type": "listing_page",
                    })
                    break
        return results

    async def _probe_career_subdomains(self, company: Company) -> list[dict]:
        """Probe careers.domain.com, jobs.domain.com etc."""
        # Extract root domain (strip subdomains)
        parts = company.domain.split(".")
        # Handle cases like careers.company.co.nz — root is company.co.nz
        root_domain = ".".join(parts[-3:]) if len(parts) >= 3 and len(parts[-1]) <= 3 else ".".join(parts[-2:])

        results = []
        for prefix in CAREER_SUBDOMAINS:
            subdomain_url = f"https://{prefix}.{root_domain}"
            if is_blocked(subdomain_url):
                continue
            # Skip if it's the same as the company's own domain
            if subdomain_url.rstrip("/") == company.root_url.rstrip("/"):
                continue
            try:
                resp = await self.client.get(subdomain_url)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "lxml")
                    title = (soup.title.get_text(strip=True) if soup.title else "").lower()
                    body_text = soup.get_text(strip=True)[:2000].lower()
                    # Validate this is actually a careers page
                    career_signals = sum(1 for kw in ["careers", "jobs", "vacancies", "openings", "apply"]
                                         if kw in title or kw in body_text[:500])
                    if career_signals >= 2:
                        results.append({
                            "url": subdomain_url,
                            "discovery_method": "subdomain_probe",
                            "confidence": 0.75 + (career_signals * 0.05),
                            "is_primary": True,
                            "page_type": "listing_page",
                        })
                        logger.info(f"Career subdomain found: {subdomain_url}")
                        break  # Found one, no need to try others
            except Exception:
                continue  # Subdomain doesn't exist or unreachable

        return results

    async def _heuristic_discovery(self, root_url: str, company_domain: str) -> list[dict]:
        """Crawl site to depth 2 and score each discovered URL for career relevance.

        Key design decisions:
        - BFS starts from the domain root (not a specific job URL) so we discover
          the careers section even when root_url is a single job posting.
        - Only follow links with score > 0 (career-relevant URLs) to avoid
          crawling the entire site (services, blog, team pages, etc.).
        - Hard cap of 40 pages visited to keep each company crawl fast.
        """
        # Always start BFS from the domain root, not from a specific job URL.
        # This ensures we discover /careers, /jobs, etc. even when root_url is
        # a single job posting imported from an aggregator.
        parsed_root = urlparse(root_url)
        bfs_start = f"{parsed_root.scheme}://{parsed_root.netloc}/"

        try:
            assert_not_blocked(bfs_start)
        except ValueError:
            return []

        # Extract root domain for loose same-domain matching (allows subdomains)
        root_parts = parsed_root.netloc.split(".")
        root_base = ".".join(root_parts[-2:]) if len(root_parts) >= 2 else parsed_root.netloc

        visited: set[str] = set()
        queue: list[tuple[str, int]] = [(bfs_start, 0)]
        scored: dict[str, float] = {}
        js_required: set[str] = set()
        MAX_PAGES = 40  # Cap to keep crawls fast

        while queue and len(visited) < MAX_PAGES:
            url, depth = queue.pop(0)
            if url in visited or depth > 2:
                continue
            visited.add(url)

            # Skip non-HTML resources
            path = urlparse(url).path.lower()
            if any(path.endswith(ext) for ext in SKIP_EXTENSIONS):
                continue

            try:
                resp = await self.client.get(url)
                if resp.status_code >= 400:
                    continue
                html = resp.text
            except Exception as e:
                logger.debug(f"Failed to fetch {url}: {e}")
                continue

            # Detect if this page needs JS rendering
            if self._detect_js_rendering_required(html, url):
                scored[url] = max(scored.get(url, 0), 0.6)
                js_required.add(url)
                logger.debug(f"JS rendering required detected: {url}")
                # Don't parse links from a SPA shell — server-side nav won't be present
                continue

            soup = BeautifulSoup(html, "lxml")
            base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"

            for a in soup.find_all("a", href=True):
                href = str(a["href"]).strip()
                if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
                    continue

                abs_url = urljoin(base, href)
                parsed = urlparse(abs_url)

                # Only follow links on same root domain (allows subdomains like careers.company.com)
                link_domain = parsed.netloc
                if not (link_domain.endswith("." + root_base) or link_domain == root_base):
                    continue

                # Skip blocked, skip already visited
                if is_blocked(abs_url) or abs_url in visited:
                    continue

                link_text = a.get_text(strip=True).lower()
                abs_url = self._normalize_discovered_url(abs_url)
                score = self._score_url(abs_url, link_text)

                if score > 0:
                    scored[abs_url] = max(scored.get(abs_url, 0), score)

                # IMPORTANT: only follow career-relevant links (score > 0.1) to avoid
                # crawling the entire site. Depth-1 from root gets nav links; depth-2
                # follows only links that look career-related.
                min_follow_score = 0.0 if depth == 0 else 0.15
                if depth < 2 and score >= min_follow_score:
                    queue.append((abs_url, depth + 1))

        results = []
        for url, score in scored.items():
            if score >= 0.3:
                # Skip individual job detail URLs — these are not listing/career pages
                if _JOB_DETAIL_PATH_RE.search(urlparse(url).path + "?" + urlparse(url).query):
                    logger.debug(f"Skipping job detail URL as career page: {url}")
                    continue
                results.append({
                    "url": url,
                    "discovery_method": "heuristic",
                    "confidence": min(score, 0.92),
                    "is_primary": score >= 0.65,
                    "page_type": "listing_page",
                    "requires_js": url in js_required,
                })
        return results

    async def _llm_discovery(self, company: Company) -> list[dict]:
        """Use Playwright + LLM to find the careers/jobs listing page.

        Renders the company homepage with Playwright, then asks the LLM to:
        1. Identify links to careers/jobs pages
        2. Return the best URL for the main job listings page

        This catches what heuristic BFS misses: JS-rendered navigation,
        single-page apps, non-standard URL patterns, hidden menus.
        """
        import httpx
        from app.core.config import settings
        from markdownify import markdownify

        results = []
        try:
            # Render the homepage with Playwright (captures JS-rendered content)
            html = await self.client.get_rendered(company.root_url)
            if not html or len(html) < 500:
                return []

            # Convert to readable text for the LLM
            md = markdownify(html[:15000], strip=["script", "style"])

            # Extract all links from the page for the LLM to analyze
            soup = BeautifulSoup(html, "lxml")
            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(strip=True)[:80]
                if href and text and len(text) > 1:
                    from urllib.parse import urljoin
                    abs_url = urljoin(company.root_url, href)
                    links.append(f"  [{text}]({abs_url})")

            links_text = "\n".join(links[:60])  # Cap at 60 links

            prompt = (
                f"You are analyzing a company website to find their careers/jobs page.\n"
                f"Company: {company.name} ({company.domain})\n"
                f"Homepage URL: {company.root_url}\n\n"
                f"Here are the links found on the page:\n{links_text}\n\n"
                f"Which URL is the main careers/jobs listing page where ALL open positions are listed?\n"
                f"Look for links containing: careers, jobs, vacancies, openings, positions, work-with-us, join-us\n"
                f"Return ONLY the full URL. If no careers page found, return NONE.\n\n"
                f"URL:"
            )

            ollama_host = getattr(settings, "OLLAMA_HOST", "ollama")
            async with httpx.AsyncClient(timeout=25) as http:
                r = await http.post(
                    f"http://{ollama_host}:11434/api/generate",
                    json={
                        "model": "qwen2.5:3b",
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.1, "num_predict": 100},
                    },
                )
                if r.status_code != 200:
                    return []
                raw = r.json().get("response", "").strip()

            # Parse the URL from LLM response
            import re
            url_match = re.search(r'https?://[^\s<>"\]]+', raw)
            if url_match:
                found_url = url_match.group(0).rstrip(".,;)")
                # Validate it's on the same domain
                from urllib.parse import urlparse
                if company.domain in urlparse(found_url).netloc:
                    # Normalize before adding
                    found_url = self._normalize_career_url(found_url)
                    if found_url:
                        results.append({
                            "url": found_url,
                            "discovery_method": "llm_playwright",
                            "confidence": 0.85,
                            "is_primary": True,
                            "page_type": "listing_page",
                            "requires_js": True,
                        })
                        logger.info(f"LLM discovered career page for {company.domain}: {found_url}")

        except Exception as e:
            logger.debug(f"LLM discovery failed for {company.domain}: {e}")

        return results

    def _normalize_discovered_url(self, url: str) -> str:
        """Normalize discovered URLs before scoring — redirect known patterns to canonical URLs."""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        host = parsed.netloc.lower()

        # TeamTailor: /stories, /departments, /connect → redirect to /jobs
        if '.teamtailor.com' in host:
            if parsed.path in ('/stories', '/departments', '/connect', '/people', '/'):
                return f"{parsed.scheme}://{parsed.netloc}/jobs"

        # NGA (Australian Gov ATS): normalize complex index.cfm URLs to base
        if '.nga.net.au' in host or '.nga.net.nz' in host:
            if 'index.cfm' in parsed.path:
                return f"{parsed.scheme}://{parsed.netloc}/cp/"

        return url

    def _score_url(self, url: str, link_text: str) -> float:
        """Score a URL on how likely it is to be a careers page. Returns 0.0–1.0."""
        score = 0.0
        path = urlparse(url).path.lower()
        text = link_text.lower()

        # Skip non-HTML resources immediately
        if any(path.endswith(ext) for ext in SKIP_EXTENSIONS):
            return 0.0

        # High-value URL path keywords (weight 0.45)
        for kw in URL_KEYWORDS_HIGH:
            if kw in path:
                score += 0.45
                break
        else:
            # Medium-value URL path keywords (weight 0.2)
            for kw in URL_KEYWORDS_MED:
                if kw in path:
                    score += 0.2
                    break

        # High-value link text keywords (weight 0.4)
        for kw in LINK_TEXT_KEYWORDS_HIGH:
            if kw in text:
                score += 0.4
                break
        else:
            # Medium-value link text keywords (weight 0.15)
            for kw in LINK_TEXT_KEYWORDS_MED:
                if kw in text:
                    score += 0.15
                    break

        # Boost: URL looks like a specific jobs listing page (not just /careers)
        if re.search(r'/jobs?/\w|/careers?/\w|/openings?/\w|/vacancies/\w', path):
            score += 0.1

        return min(score, 1.0)

    def _detect_js_rendering_required(self, html: str, url: str) -> bool:
        """Detect if a page is a SPA / JS-rendered page that needs Playwright.

        Signals:
        - Body has very little visible text despite a non-empty HTML document
        - Page contains known SPA framework markers
        - Known ATS platforms that always require JS (Workday, iCIMS, etc.)
        - noscript tag with significant content (JS-only page)
        """
        # Known ATS domains that always require JS rendering
        JS_REQUIRED_DOMAINS = [
            "myworkdayjobs.com", "icims.com", "taleo.net",
            "ultipro.com", "successfactors.com", "oraclecloud.com",
        ]
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lower()
        if any(d in domain for d in JS_REQUIRED_DOMAINS):
            return True

        soup = BeautifulSoup(html, "lxml")

        # Remove script/style and check visible text length
        for tag in soup(["script", "style", "noscript", "head"]):
            tag.decompose()
        visible_text = soup.get_text(separator=" ", strip=True)

        # Short visible text but non-trivial HTML → JS rendering needed
        html_len = len(html)
        text_len = len(visible_text)
        if html_len > 5000 and text_len < 200:
            return True

        # SPA framework markers in original HTML
        spa_markers = [
            'ng-app', 'ng-version', 'data-reactroot', 'data-reactid',
            '__vue__', 'data-v-app', '__nuxt__', '__next',
            'id="app"', 'id="root"', 'id="__next"',
        ]
        html_lower = html.lower()
        spa_count = sum(1 for m in spa_markers if m in html_lower)
        if spa_count >= 2:
            return True

        return False

    # URLs that should never be saved as career pages
    _BAD_URL_PATTERNS = [
        'show_more?page=', '&rmuh=', 'index.cfm?event=jobs.listjobs',
        'pagestamp=', 'in_organId=', 'in_sessionid=', 'posbrowser_resetto',
        '/login', '/signin', '/sign-in', '/register', '/forgot-password',
        'create_account', '/wechat/ShareJob', '/wechat/share',
        '/saved-jobs', '/job-alerts', 'mailto:', 'javascript:',
        '/passwordreset', '/alertregister', '/talentpool',
        '/alerts', '/vacancy/', '/subscribe', '/notification',
        '/bookmark', '/shortlist', '/watchlist',
        '#content', '/content/dam/', 'show_more?', '&in_create_account',
    ]

    def _normalize_career_url(self, url: str) -> str | None:
        """Normalize and validate a career page URL. Returns None if URL is bad."""
        from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

        # Reject bad patterns
        url_lower = url.lower()
        if any(p in url_lower for p in self._BAD_URL_PATTERNS):
            logger.debug(f"Rejected bad career page URL: {url[:80]}")
            return None

        # Strip common tracking/session params
        parsed = urlparse(url)
        if parsed.query:
            params = parse_qs(parsed.query, keep_blank_values=True)
            # Remove session/tracking params
            clean_params = {k: v for k, v in params.items()
                          if k.lower() not in ('rmuh', 'pagestamp', 'in_sessionid',
                                                'utm_source', 'utm_medium', 'utm_campaign',
                                                'utm_term', 'utm_content', 'fbclid', 'gclid')}
            clean_query = urlencode(clean_params, doseq=True)
            url = urlunparse(parsed._replace(query=clean_query))

        # Strip trailing fragments
        if '#' in url:
            url = url.split('#')[0]

        return url.rstrip('/')

    async def _upsert_career_page(self, company: Company, url: str, meta: dict) -> CareerPage:
        from sqlalchemy import select

        # Validate and normalize URL before saving
        url = self._normalize_career_url(url)
        if not url:
            return None

        existing = await self.db.scalar(
            select(CareerPage).where(CareerPage.company_id == company.id, CareerPage.url == url)
        )
        if existing:
            existing.discovery_confidence = max(existing.discovery_confidence or 0, meta["confidence"])
            existing.updated_at = datetime.now(timezone.utc)
            # Upgrade requires_js flag if newly detected
            if meta.get("requires_js") and not existing.requires_js_rendering:
                existing.requires_js_rendering = True
            await self.db.commit()
            return existing

        page = CareerPage(
            company_id=company.id,
            url=url,
            page_type=meta.get("page_type", "listing_page"),
            discovery_method=meta.get("discovery_method", "heuristic"),
            discovery_confidence=meta.get("confidence", 0.5),
            is_primary=meta.get("is_primary", False),
            requires_js_rendering=meta.get("requires_js", False),
        )
        self.db.add(page)
        await self.db.commit()
        await self.db.refresh(page)
        return page
