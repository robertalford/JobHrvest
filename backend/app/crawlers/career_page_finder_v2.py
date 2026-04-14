"""
Career Page Finder v2 — fixes the 6 issues identified in v1.3 testing:

1. ATS domain detection: known ATS domains returned directly (no probing)
2. Careers subdomain detection: careers.*/jobs.* used as-is
3. Always fetch HTML for best-guess URLs (no more "0 bytes" returns)
4. Homepage-first strategy: crawl homepage links BEFORE path probing
5. Fallback: try domain root if all else fails
6. Playwright for empty HTML (< 200 bytes)
"""

import logging
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx

logger = logging.getLogger(__name__)

# Known ATS platform domains — if the input domain contains any of these,
# the domain itself IS the careers page (just use root URL)
_ATS_DOMAINS = [
    "greenhouse.io", "lever.co", "smartrecruiters.com", "icims.com",
    "bamboohr.com", "pageuppeople.com", "livehire.com", "readycareers.io",
    "recruitmenthub.com", "applynow.net", "expr3ss.com", "elmotalent.com",
    "dayforcehcm.com", "taleo.net", "ripplehire.com", "bigredsky.com",
    "csod.com", "darwinbox.in", "gupy.io", "jobvite.com", "ashbyhq.com",
    "teamtailor.com", "myworkdayjobs.com", "workday.com",
    "careers-page.com", "applytojob.com", "theresumator.com",
    "deputy.com", "livevacancies.co.uk", "peopleadmin.com",
]

_CAREER_PATHS = [
    "/careers", "/jobs", "/career", "/join-us", "/work-with-us",
    "/about/careers", "/opportunities", "/vacancies", "/en/careers", "/hiring",
]

_CAREER_LINK_TEXT = re.compile(
    r"\b(?:career|careers|jobs?|work with us|join us|join our team|"
    r"opportunities|vacancies|hiring|openings|we.re hiring|positions)\b",
    re.IGNORECASE,
)

_CAREER_HREF_PATTERN = re.compile(
    r"/(?:career|jobs?|work|join|vacanc|hiring|opening|opportunit|employment|recruit)",
    re.IGNORECASE,
)

_CLIENT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class CareerPageFinderV2:
    """Discover career page URLs from a company domain — v2 with ATS/subdomain awareness."""

    def __init__(self, timeout: float = 6):
        self.timeout = timeout

    async def find(self, domain: str, company_name: str = "") -> dict:
        """Find career page for a domain. Returns {url, method, candidates, html}."""
        base_url = f"https://{domain}"

        # ── Fix 1: ATS domain detection ──
        # If domain is a known ATS platform, the root URL IS the careers page
        for ats_domain in _ATS_DOMAINS:
            if ats_domain in domain.lower():
                return await self._fetch_and_return(base_url, f"ats_domain:{ats_domain}")

        # ── Fix 2: Careers subdomain detection ──
        # If domain starts with careers.*/jobs.*, it's already a careers subdomain
        host_parts = domain.lower().split(".")
        if host_parts[0] in ("careers", "jobs", "career", "job", "recruiting", "recruitment", "talent"):
            return await self._fetch_and_return(base_url, f"careers_subdomain:{host_parts[0]}")

        async with httpx.AsyncClient(
            timeout=self.timeout, follow_redirects=True, headers=_CLIENT_HEADERS,
        ) as client:

            # ── Fix 4: Homepage-first strategy ──
            # Try homepage link crawl FIRST (finds non-standard paths like /green-jobs/)
            result = await self._crawl_homepage(client, base_url, domain)
            if result["url"] and result["html"] and len(result["html"]) > 200:
                return result

            # Standard path probing (parallel)
            result = await self._probe_common_paths(client, base_url)
            if result["url"] and result["html"] and len(result["html"]) > 200:
                return result

            # Sitemap
            result = await self._check_sitemap(client, base_url)
            if result["url"] and result["html"] and len(result["html"]) > 200:
                return result

            # ── Fix 3: Fetch best-guess URLs that didn't return HTML ──
            for prev_result in [result]:
                if prev_result["url"] and not prev_result.get("html"):
                    fetched = await self._try_fetch(client, prev_result["url"])
                    if fetched and len(fetched) > 200:
                        return {**prev_result, "html": fetched, "method": prev_result["method"] + "+fetched"}

            # ── Fix 5: Try domain root as fallback ──
            try:
                resp = await client.get(base_url)
                if resp.status_code == 200 and len(resp.text) > 200:
                    return {"url": str(resp.url), "method": "domain_root_fallback",
                            "candidates": [str(resp.url)], "html": resp.text}
            except Exception:
                pass

            # ── Fix 6: Last resort — Playwright on all candidate URLs ──
            # Collect every URL we found (even those with empty HTML) and try Playwright
            all_candidates = [r["url"] for r in [result] if r.get("url")]
            if not all_candidates:
                all_candidates = [base_url]
            for candidate_url in all_candidates[:3]:
                rendered = await self._try_playwright(candidate_url)
                if rendered and len(rendered) > 200:
                    return {"url": candidate_url, "method": "playwright_fallback",
                            "candidates": all_candidates, "html": rendered}

            return {"url": None, "method": "failed", "candidates": [], "html": None}

    async def _fetch_and_return(self, url: str, method: str) -> dict:
        """Fetch a URL and return the result dict. Used for ATS/subdomain shortcuts."""
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout, follow_redirects=True, headers=_CLIENT_HEADERS,
            ) as client:
                resp = await client.get(url)
                html = resp.text
                if len(html) > 200:
                    return {"url": str(resp.url), "method": method,
                            "candidates": [str(resp.url)], "html": html}

                # ── Fix 6: Playwright for empty HTML ──
                rendered = await self._try_playwright(url)
                if rendered and len(rendered) > 200:
                    return {"url": url, "method": method + "+playwright",
                            "candidates": [url], "html": rendered}

                return {"url": url, "method": method + "+empty",
                        "candidates": [url], "html": html if html else None}
        except Exception as e:
            return {"url": url, "method": method + "+error",
                    "candidates": [url], "html": None}

    async def _probe_common_paths(self, client: httpx.AsyncClient, base_url: str) -> dict:
        """Try common career paths in parallel."""
        import asyncio

        async def _try(path: str):
            try:
                resp = await client.get(base_url + path)
                if resp.status_code == 200 and len(resp.text) > 500:
                    if self._looks_like_careers(resp.text[:5000].lower()):
                        return (path, str(resp.url), resp.text)
                    return (path, str(resp.url), None)
            except Exception:
                pass
            return (path, None, None)

        results = await asyncio.gather(*[_try(p) for p in _CAREER_PATHS])

        # Return first confirmed careers page
        for path, url, html in results:
            if url and html:
                return {"url": url, "method": f"path_probe:{path}", "candidates": [url], "html": html}

        # ── Fix 3: Fetch best-guess URLs, try Playwright if empty ──
        candidates = [(url, path) for path, url, _ in results if url]
        if candidates:
            best_url, best_path = candidates[0]
            try:
                resp = await client.get(best_url)
                if resp.status_code == 200 and len(resp.text) > 200:
                    return {"url": best_url, "method": f"path_probe_fetched:{best_path}",
                            "candidates": [u for u, _ in candidates[:5]], "html": resp.text}
            except Exception:
                pass
            # Try Playwright on the best URL if plain fetch gave empty HTML
            rendered = await self._try_playwright(best_url)
            if rendered and len(rendered) > 200:
                return {"url": best_url, "method": f"path_probe_playwright:{best_path}",
                        "candidates": [u for u, _ in candidates[:5]], "html": rendered}
            return {"url": best_url, "method": "path_probe_best_guess",
                    "candidates": [u for u, _ in candidates[:5]], "html": None}

        return {"url": None, "method": "path_probe_none", "candidates": [], "html": None}

    async def _crawl_homepage(self, client: httpx.AsyncClient, base_url: str, domain: str) -> dict:
        """Fetch homepage, scan for career links."""
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

                try:
                    text = a_el.text_content().strip()
                except AttributeError:
                    text = etree.tostring(a_el, method="text", encoding="unicode").strip()

                full_url = urljoin(base_url, href)
                parsed = urlparse(full_url)

                # Allow same domain or known ATS domains
                if parsed.hostname and not parsed.hostname.endswith(domain):
                    if not any(ats in (parsed.hostname or "") for ats in _ATS_DOMAINS):
                        continue

                score = 0
                if _CAREER_LINK_TEXT.search(text):
                    score += 10
                if _CAREER_HREF_PATTERN.search(href):
                    score += 5

                if score > 0:
                    career_links.append((full_url, score, text[:60]))

            if not career_links:
                return {"url": None, "method": "homepage_no_links", "candidates": [], "html": None}

            career_links.sort(key=lambda x: x[1], reverse=True)
            best_url = career_links[0][0]

            try:
                career_resp = await client.get(best_url)
                html = career_resp.text if career_resp.status_code == 200 else None
                if html and len(html) > 200:
                    return {"url": str(career_resp.url), "method": f"homepage_link:{career_links[0][2][:30]}",
                            "candidates": [u for u, _, _ in career_links[:5]], "html": html}
            except Exception:
                pass

            return {"url": best_url, "method": "homepage_link_unfetched",
                    "candidates": [u for u, _, _ in career_links[:5]], "html": None}

        except Exception:
            return {"url": None, "method": "homepage_error", "candidates": [], "html": None}

    async def _check_sitemap(self, client: httpx.AsyncClient, base_url: str) -> dict:
        """Check sitemap.xml for career URLs."""
        try:
            resp = await client.get(base_url + "/sitemap.xml")
            if resp.status_code != 200:
                return {"url": None, "method": "sitemap_not_found", "candidates": [], "html": None}

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
                return {"url": str(career_resp.url), "method": "sitemap",
                        "candidates": career_urls[:5],
                        "html": career_resp.text if career_resp.status_code == 200 else None}
            except Exception:
                return {"url": best, "method": "sitemap_unfetched", "candidates": career_urls[:5], "html": None}

        except Exception:
            return {"url": None, "method": "sitemap_error", "candidates": [], "html": None}

    async def _try_fetch(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        """Try to fetch a URL, return HTML or None."""
        try:
            resp = await client.get(url)
            return resp.text if resp.status_code == 200 else None
        except Exception:
            return None

    @staticmethod
    async def _try_playwright(url: str) -> Optional[str]:
        """Render with Playwright if available. Hard 25s timeout to prevent hangs."""
        import asyncio
        try:
            return await asyncio.wait_for(_playwright_render(url), timeout=25)
        except (asyncio.TimeoutError, Exception):
            return None


async def _playwright_render(url: str) -> Optional[str]:
    """Inner Playwright render — separated so asyncio.wait_for can cancel it."""
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            try:
                await page.goto(url, wait_until="networkidle", timeout=15000)
                await page.wait_for_timeout(2000)
                return await page.content()
            except Exception:
                return None
            finally:
                await browser.close()
    except Exception:
        return None

    @staticmethod
    def _looks_like_careers(html_lower: str) -> bool:
        signals = ["job listing", "job opening", "current vacanc", "open position",
                   "join our team", "career opportunit", "we're hiring", "apply now",
                   "job description", "job-listing", "job-card", "job-post",
                   "joblist", "job_listing", "careers-page"]
        return sum(1 for s in signals if s in html_lower) >= 2


def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if host.startswith("www."):
        host = host[4:]
    return host.lower()
