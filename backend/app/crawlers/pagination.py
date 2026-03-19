"""
Pagination handler — Stage 3e.

Detects and handles all pagination types on career pages:
  - URL-based: ?page=2, ?offset=20, /page/2/
  - Load More buttons
  - Infinite scroll (Playwright)
  - Next/prev links
"""

import logging
import re
from typing import Optional, AsyncGenerator
from urllib.parse import urljoin, urlparse, urlencode, parse_qs, urlunparse

from bs4 import BeautifulSoup

from app.crawlers.http_client import ResilientHTTPClient

logger = logging.getLogger(__name__)

# Text patterns that identify "Load More" / "Next" buttons
LOAD_MORE_PATTERNS = re.compile(
    r"\b(load more|show more|view more|see more|more jobs|next page|next|›|»|→)\b",
    re.IGNORECASE,
)

URL_PAGINATION_PARAMS = ["page", "p", "pg", "offset", "start", "from", "skip"]


class PaginationHandler:
    """Iterates through all pages of a career listing, yielding HTML for each page."""

    def __init__(self, client: Optional[ResilientHTTPClient] = None):
        self.client = client or ResilientHTTPClient()

    async def iter_pages(self, url: str, html: str, requires_js: bool = False) -> AsyncGenerator[tuple[str, str], None]:
        """
        Yield (page_url, html) for every page of the listing.
        Always yields the first page immediately.
        """
        yield url, html

        pagination_type = self._detect_pagination_type(html, url)
        logger.debug(f"Pagination type for {url}: {pagination_type}")

        if pagination_type == "url_param":
            async for page_url, page_html in self._iter_url_param_pages(url, html):
                yield page_url, page_html

        elif pagination_type == "next_link":
            async for page_url, page_html in self._iter_next_link_pages(url, html):
                yield page_url, page_html

        elif pagination_type == "load_more_button" and requires_js:
            async for page_url, page_html in self._iter_load_more(url):
                yield page_url, page_html

        elif pagination_type == "infinite_scroll" and requires_js:
            async for page_url, page_html in self._iter_infinite_scroll(url):
                yield page_url, page_html

    def _detect_pagination_type(self, html: str, url: str) -> Optional[str]:
        """Identify which pagination mechanism this page uses."""
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(" ", strip=True).lower()

        # Check for URL-based pagination (most reliable)
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        if any(p in qs for p in URL_PAGINATION_PARAMS):
            return "url_param"

        # Check for page links in DOM
        page_links = soup.find_all("a", href=re.compile(r"[?&](page|p|pg|offset|start)=\d+"))
        if page_links:
            return "url_param"

        # Check for next/prev links
        next_links = soup.find_all("a", string=re.compile(r"next|›|»|→", re.IGNORECASE))
        next_rels = soup.find_all("a", rel="next")
        if next_links or next_rels:
            return "next_link"

        # Check for Load More buttons
        buttons = soup.find_all(["button", "a", "div", "span"])
        for btn in buttons:
            btn_text = btn.get_text(strip=True)
            if LOAD_MORE_PATTERNS.search(btn_text) and len(btn_text) < 50:
                return "load_more_button"

        # Heuristic: if total job count is in text but few listings visible, likely infinite scroll
        job_count_match = re.search(r"(\d+)\s+jobs?", text)
        if job_count_match:
            visible_jobs = len(soup.select(".job, .posting, .opening, [data-job-id], [class*='job-item']"))
            total = int(job_count_match.group(1))
            if total > visible_jobs * 2 and visible_jobs > 0:
                return "infinite_scroll"

        return "none"

    async def _iter_url_param_pages(self, base_url: str, first_html: str) -> AsyncGenerator[tuple[str, str], None]:
        """Follow URL-parameter-based pagination until no new content."""
        soup = BeautifulSoup(first_html, "lxml")
        seen_urls = {base_url}

        # Find all page number links
        page_links = soup.find_all("a", href=re.compile(r"[?&](page|p|pg|offset|start)=\d+"))
        all_page_urls = set()

        for link in page_links:
            href = link.get("href", "")
            abs_url = urljoin(base_url, href)
            if abs_url not in seen_urls:
                all_page_urls.add(abs_url)

        # Also try incrementing page param if we found any
        if not all_page_urls:
            parsed = urlparse(base_url)
            qs = parse_qs(parsed.query, keep_blank_values=True)
            for param in URL_PAGINATION_PARAMS:
                if param in qs:
                    page_num = int(qs[param][0]) + 1
                    for p in range(page_num, page_num + 20):
                        qs[param] = [str(p)]
                        new_query = urlencode(qs, doseq=True)
                        new_url = urlunparse(parsed._replace(query=new_query))
                        all_page_urls.add(new_url)
                    break

        for page_url in sorted(all_page_urls):
            if page_url in seen_urls:
                continue
            try:
                resp = await self.client.get(page_url)
                html = resp.text
                seen_urls.add(page_url)
                yield page_url, html
            except Exception as e:
                logger.warning(f"Failed to fetch page {page_url}: {e}")
                break

    async def _iter_next_link_pages(self, base_url: str, first_html: str) -> AsyncGenerator[tuple[str, str], None]:
        """Follow 'Next' links until exhausted."""
        current_html = first_html
        current_url = base_url
        seen = {base_url}

        for _ in range(50):  # max 50 pages
            soup = BeautifulSoup(current_html, "lxml")
            next_el = (
                soup.find("a", rel="next") or
                soup.find("a", string=re.compile(r"^next$|^›$|^»$|^→$", re.IGNORECASE)) or
                soup.find("a", {"aria-label": re.compile(r"next", re.IGNORECASE)})
            )
            if not next_el or not next_el.get("href"):
                break
            next_url = urljoin(current_url, next_el["href"])
            if next_url in seen:
                break
            try:
                resp = await self.client.get(next_url)
                current_html = resp.text
                current_url = next_url
                seen.add(next_url)
                yield next_url, current_html
            except Exception as e:
                logger.warning(f"Failed to follow next link {next_url}: {e}")
                break

    async def _iter_load_more(self, url: str) -> AsyncGenerator[tuple[str, str], None]:
        """Click 'Load More' buttons using Playwright until no more content loads."""
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
                page = await browser.new_page()
                await page.goto(url, wait_until="networkidle", timeout=30000)

                click_count = 0
                while click_count < 20:
                    load_more = await page.query_selector_all("button, a, [role='button']")
                    clicked = False
                    for el in load_more:
                        text = (await el.inner_text()).strip().lower()
                        if LOAD_MORE_PATTERNS.search(text) and len(text) < 50:
                            before_html = await page.content()
                            await el.click()
                            await page.wait_for_load_state("networkidle", timeout=10000)
                            after_html = await page.content()
                            if after_html != before_html:
                                clicked = True
                                click_count += 1
                                yield url, after_html
                            break
                    if not clicked:
                        break

                await browser.close()
        except Exception as e:
            logger.error(f"Load More pagination failed for {url}: {e}")

    async def _iter_infinite_scroll(self, url: str) -> AsyncGenerator[tuple[str, str], None]:
        """Scroll to bottom repeatedly until no new content loads."""
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
                page = await browser.new_page()
                await page.goto(url, wait_until="networkidle", timeout=30000)

                last_height = 0
                scroll_count = 0
                while scroll_count < 30:
                    new_height = await page.evaluate("document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(2000)
                    html = await page.content()
                    scroll_count += 1
                    yield url, html

                await browser.close()
        except Exception as e:
            logger.error(f"Infinite scroll failed for {url}: {e}")
