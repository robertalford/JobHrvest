"""
Career Page Finder — given only a company domain, discover the careers/jobs page URL.

Strategy (tried in order, stops at first success):
1. Common path probing: try ~30 well-known career page URL patterns
2. Homepage link crawl: fetch homepage, scan for career-related links
3. Sitemap scan: check /sitemap.xml for career-related URLs
"""

import logging
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx

logger = logging.getLogger(__name__)

# Career page URL paths — top 10 most common (covers 90%+ of sites)
_CAREER_PATHS = [
    "/careers",
    "/jobs",
    "/career",
    "/join-us",
    "/work-with-us",
    "/about/careers",
    "/opportunities",
    "/vacancies",
    "/en/careers",
    "/hiring",
]

# Link text patterns that indicate a careers page
_CAREER_LINK_TEXT = re.compile(
    r"\b(?:career|careers|jobs?|work with us|join us|join our team|"
    r"work for us|opportunities|vacancies|hiring|openings|"
    r"employment|we.re hiring|join the team|current vacancies|"
    r"job openings|positions|recru)\b",
    re.IGNORECASE,
)

# Link href patterns
_CAREER_HREF_PATTERN = re.compile(
    r"/(?:career|jobs?|work|join|vacanc|hiring|opening|opportunit|employment|recruit|talent)",
    re.IGNORECASE,
)

_CLIENT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class CareerPageFinder:
    """Discover career page URLs from a company domain."""

    def __init__(self, timeout: float = 6):
        self.timeout = timeout

    async def find(self, domain: str, company_name: str = "") -> dict:
        """Find career page(s) for a domain.

        Returns: {
            "url": str | None,          # Best career page URL found
            "method": str,              # How it was found
            "candidates": list[str],    # All candidate URLs found
            "html": str | None,         # HTML of the best URL (pre-fetched)
        }
        """
        base_url = f"https://{domain}"

        async with httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=True,
            headers=_CLIENT_HEADERS,
        ) as client:
            # Strategy 1: Probe common paths
            result = await self._probe_common_paths(client, base_url, domain)
            if result["url"]:
                return result

            # Strategy 2: Crawl homepage for career links
            result = await self._crawl_homepage(client, base_url, domain)
            if result["url"]:
                return result

            # Strategy 3: Check sitemap
            result = await self._check_sitemap(client, base_url, domain)
            if result["url"]:
                return result

            # Fallback: just return the homepage
            try:
                resp = await client.get(base_url)
                return {
                    "url": base_url,
                    "method": "homepage_fallback",
                    "candidates": [base_url],
                    "html": resp.text if len(resp.text) > 200 else None,
                }
            except Exception:
                return {"url": None, "method": "failed", "candidates": [], "html": None}

    async def _probe_common_paths(
        self, client: httpx.AsyncClient, base_url: str, domain: str
    ) -> dict:
        """Try well-known career page URL paths — parallel batches for speed."""
        import asyncio

        import asyncio

        async def _try_path(path: str) -> tuple[str, Optional[str], Optional[str]]:
            url = base_url + path
            try:
                resp = await client.get(url)
                if resp.status_code == 200 and len(resp.text) > 500:
                    if self._looks_like_careers_page(resp.text[:5000].lower()):
                        return (path, str(resp.url), resp.text)
                    return (path, str(resp.url), None)
            except Exception:
                pass
            return (path, None, None)

        # All paths in one parallel batch
        results = await asyncio.gather(*[_try_path(p) for p in _CAREER_PATHS])

        # First check for confirmed careers pages
        for path, found_url, html in results:
            if found_url and html:
                return {"url": found_url, "method": f"path_probe:{path}", "candidates": [found_url], "html": html}

        # Fall back to any valid page
        candidates = [url for _, url, _ in results if url]
        if candidates:
            return {"url": candidates[0], "method": "path_probe_best_guess", "candidates": candidates[:5], "html": None}

        return {"url": None, "method": "path_probe_none", "candidates": [], "html": None}

    async def _crawl_homepage(
        self, client: httpx.AsyncClient, base_url: str, domain: str
    ) -> dict:
        """Fetch homepage and scan for career-related links."""
        try:
            resp = await client.get(base_url)
            if resp.status_code != 200 or len(resp.text) < 200:
                return {"url": None, "method": "homepage_empty", "candidates": [], "html": None}

            from lxml import etree
            parser = etree.HTMLParser(encoding="utf-8")
            tree = etree.fromstring(resp.text.encode("utf-8", errors="replace"), parser)

            career_links = []
            for a_el in tree.iter("a"):
                href = (a_el.get("href") or "").strip()
                if not href or href.startswith("#") or href.startswith("javascript:"):
                    continue

                # Check link text
                try:
                    text = a_el.text_content().strip()
                except AttributeError:
                    text = etree.tostring(a_el, method="text", encoding="unicode").strip()

                full_url = urljoin(base_url, href)
                parsed = urlparse(full_url)

                # Must be same domain or known ATS
                if parsed.hostname and not parsed.hostname.endswith(domain):
                    # Allow known ATS domains
                    ats_domains = [
                        "greenhouse.io", "lever.co", "workday", "bamboohr.com",
                        "smartrecruiters.com", "icims.com", "applynow.net",
                        "livehire.com", "pageup", "jobvite.com",
                    ]
                    if not any(ats in parsed.hostname for ats in ats_domains):
                        continue

                # Score this link
                score = 0
                if _CAREER_LINK_TEXT.search(text):
                    score += 10
                if _CAREER_HREF_PATTERN.search(href):
                    score += 5

                if score > 0:
                    career_links.append((full_url, score, text[:60]))

            if not career_links:
                return {"url": None, "method": "homepage_no_links", "candidates": [], "html": None}

            # Sort by score, pick best
            career_links.sort(key=lambda x: x[1], reverse=True)
            best_url = career_links[0][0]
            all_candidates = [u for u, _, _ in career_links[:5]]

            # Fetch the best career page
            try:
                career_resp = await client.get(best_url)
                return {
                    "url": str(career_resp.url),
                    "method": f"homepage_link:{career_links[0][2][:30]}",
                    "candidates": all_candidates,
                    "html": career_resp.text if career_resp.status_code == 200 else None,
                }
            except Exception:
                return {
                    "url": best_url,
                    "method": "homepage_link_unfetched",
                    "candidates": all_candidates,
                    "html": None,
                }

        except Exception as e:
            return {"url": None, "method": f"homepage_error:{str(e)[:50]}", "candidates": [], "html": None}

    async def _check_sitemap(
        self, client: httpx.AsyncClient, base_url: str, domain: str
    ) -> dict:
        """Check sitemap.xml for career-related URLs."""
        try:
            resp = await client.get(base_url + "/sitemap.xml")
            if resp.status_code != 200:
                return {"url": None, "method": "sitemap_not_found", "candidates": [], "html": None}

            # Parse XML for career URLs
            career_urls = []
            for match in re.finditer(r"<loc>(https?://[^<]+)</loc>", resp.text):
                url = match.group(1)
                if _CAREER_HREF_PATTERN.search(url):
                    career_urls.append(url)

            if not career_urls:
                return {"url": None, "method": "sitemap_no_careers", "candidates": [], "html": None}

            best = career_urls[0]
            try:
                career_resp = await client.get(best)
                return {
                    "url": str(career_resp.url),
                    "method": "sitemap",
                    "candidates": career_urls[:5],
                    "html": career_resp.text if career_resp.status_code == 200 else None,
                }
            except Exception:
                return {"url": best, "method": "sitemap_unfetched", "candidates": career_urls[:5], "html": None}

        except Exception:
            return {"url": None, "method": "sitemap_error", "candidates": [], "html": None}

    @staticmethod
    def _looks_like_careers_page(html_lower: str) -> bool:
        """Quick check if HTML looks like a careers/jobs page."""
        career_signals = [
            "job listing", "job opening", "current vacanc", "open position",
            "join our team", "career opportunit", "we're hiring", "apply now",
            "job description", "job-listing", "job-card", "job-post",
            "joblist", "job_listing", "careers-page",
        ]
        return sum(1 for s in career_signals if s in html_lower) >= 2


def extract_domain(url: str) -> str:
    """Extract the registrable domain from a URL."""
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host.startswith("www."):
        host = host[4:]
    return host.lower()
