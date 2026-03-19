"""
Resilient HTTP client with rate limiting, retries, and TLS fingerprint mimicry.

Uses curl_cffi for sites that inspect TLS fingerprints, httpx for normal requests.
All requests are checked against the domain blocklist before execution.
"""

import asyncio
import logging
import time
from typing import Optional
from urllib.parse import urlparse

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.core.config import settings
from app.crawlers.domain_blocklist import assert_not_blocked

logger = logging.getLogger(__name__)

# Per-domain rate limit tracking (domain → last_request_time)
_rate_limit_state: dict[str, float] = {}
_rate_limit_lock = asyncio.Lock()


async def _enforce_rate_limit(domain: str) -> None:
    """Wait if we've hit the per-domain rate limit."""
    async with _rate_limit_lock:
        last = _rate_limit_state.get(domain, 0)
        elapsed = time.monotonic() - last
        wait = settings.CRAWL_RATE_LIMIT_SECONDS - elapsed
        if wait > 0:
            await asyncio.sleep(wait)
        _rate_limit_state[domain] = time.monotonic()


class ResilientHTTPClient:
    """
    Async HTTP client with:
    - Domain blocklist enforcement
    - Per-domain rate limiting
    - Automatic retry with exponential backoff
    - curl_cffi fallback for anti-bot protected sites
    """

    DEFAULT_HEADERS = {
        "User-Agent": settings.CRAWL_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
    }

    def __init__(self, timeout: int = None, rate_limit: float = None):
        self.timeout = timeout or settings.CRAWL_TIMEOUT_SECONDS
        self.rate_limit = rate_limit or settings.CRAWL_RATE_LIMIT_SECONDS

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        reraise=True,
    )
    async def get(self, url: str, use_curl_cffi: bool = False, **kwargs) -> httpx.Response:
        """
        Fetch a URL with rate limiting and blocklist enforcement.

        Args:
            url: URL to fetch
            use_curl_cffi: Use curl_cffi for TLS fingerprint mimicry (for protected sites)
        """
        assert_not_blocked(url)
        domain = urlparse(url).netloc
        await _enforce_rate_limit(domain)

        if use_curl_cffi:
            return await self._get_curl_cffi(url, **kwargs)

        async with httpx.AsyncClient(
            headers=self.DEFAULT_HEADERS,
            timeout=self.timeout,
            follow_redirects=True,
        ) as client:
            response = await client.get(url, **kwargs)
            response.raise_for_status()
            return response

    async def _get_curl_cffi(self, url: str, **kwargs) -> httpx.Response:
        """Fetch using curl_cffi to mimic a real browser TLS fingerprint."""
        try:
            from curl_cffi.requests import AsyncSession
            async with AsyncSession(impersonate="chrome120") as session:
                resp = await session.get(url, timeout=self.timeout, **kwargs)
                # Wrap in httpx.Response for consistent interface
                return httpx.Response(
                    status_code=resp.status_code,
                    headers=dict(resp.headers),
                    content=resp.content,
                )
        except ImportError:
            logger.warning("curl_cffi not available, falling back to httpx")
            return await self.get(url, use_curl_cffi=False, **kwargs)

    async def get_rendered(self, url: str) -> str:
        """
        Fetch a JS-rendered page using Playwright.
        Returns the full rendered HTML after JavaScript execution.
        """
        assert_not_blocked(url)
        domain = urlparse(url).netloc
        await _enforce_rate_limit(domain)

        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"],
                )
                page = await browser.new_page(
                    user_agent=settings.CRAWL_USER_AGENT,
                    extra_http_headers={"Accept-Language": "en-AU,en;q=0.9"},
                )
                await page.goto(url, wait_until="networkidle", timeout=self.timeout * 1000)
                html = await page.content()
                await browser.close()
                return html
        except Exception as e:
            logger.error(f"Playwright rendering failed for {url}: {e}")
            raise
