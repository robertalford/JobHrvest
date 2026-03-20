"""
ATS Fingerprinting Engine — detect which Applicant Tracking System a company uses.

Supported platforms: greenhouse, lever, workday, bamboohr, icims, taleo,
smartrecruiters, ashby, jobvite, jazzhr, custom, unknown
"""

import re
import logging
from typing import Optional
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from app.crawlers.domain_blocklist import assert_not_blocked
from app.core.config import settings

logger = logging.getLogger(__name__)

ATS_SIGNATURES: list[dict] = [
    {
        "platform": "greenhouse",
        "url_patterns": [r"boards\.greenhouse\.io", r"job-boards\.greenhouse\.io"],
        "dom_patterns": [r'greenhouse', r'gh_jboard'],
        "meta_patterns": [r'greenhouse'],
        "confidence_base": 0.95,
    },
    {
        "platform": "lever",
        "url_patterns": [r"jobs\.lever\.co"],
        "dom_patterns": [r'lever-jobs-container', r'lever-job'],
        "confidence_base": 0.95,
    },
    {
        "platform": "workday",
        "url_patterns": [r"myworkdayjobs\.com", r"wd\d+\.myworkdayjobs\.com"],
        "dom_patterns": [r'workday', r'wd-'],
        "script_patterns": [r'workday'],
        "confidence_base": 0.95,
    },
    {
        "platform": "bamboohr",
        "url_patterns": [r"bamboohr\.com/careers", r"\.bamboohr\.com"],
        "dom_patterns": [r'bamboo', r'BambooHR'],
        "confidence_base": 0.90,
    },
    {
        "platform": "icims",
        "url_patterns": [r"careers-\w+\.icims\.com", r"icims\.com"],
        "dom_patterns": [r'iCIMS', r'icims'],
        "confidence_base": 0.92,
    },
    {
        "platform": "taleo",
        "url_patterns": [r"taleo\.net"],
        "dom_patterns": [r'taleo', r'oracle.*recruit'],
        "confidence_base": 0.92,
    },
    {
        "platform": "smartrecruiters",
        "url_patterns": [r"careers\.smartrecruiters\.com"],
        "dom_patterns": [r'SmartRecruiters', r'smart-apply'],
        "confidence_base": 0.93,
    },
    {
        "platform": "ashby",
        "url_patterns": [r"jobs\.ashbyhq\.com"],
        "dom_patterns": [r'ashby', r'ashbyhq'],
        "confidence_base": 0.94,
    },
    {
        "platform": "jobvite",
        "url_patterns": [r"jobs\.jobvite\.com"],
        "dom_patterns": [r'jobvite'],
        "confidence_base": 0.93,
    },
    {
        "platform": "pageup",
        "url_patterns": [r"pageuppeople\.com"],
        "dom_patterns": [r"pageup", r"pageuppeople", r"pua-table"],
        "confidence_base": 0.90,
    },
    {
        "platform": "jazzhr",
        "url_patterns": [r"jazzhr\.com", r"app\.jazz\.co"],
        "dom_patterns": [r'jazzhr', r'jazz-'],
        "confidence_base": 0.90,
    },
]


class ATSFingerprinter:
    """Multi-signal ATS detection. Returns the detected platform and confidence."""

    def __init__(self):
        self.headers = {"User-Agent": settings.CRAWL_USER_AGENT}

    async def fingerprint(self, url: str) -> Optional[dict]:
        """
        Detect ATS for the given URL.
        Returns {"platform": str, "confidence": float} or None if undetected.
        """
        assert_not_blocked(url)

        # Phase 1: check URL alone — fast, no HTTP needed
        url_match = self._match_url(url)
        if url_match and url_match["confidence"] >= 0.9:
            return url_match

        # Phase 2: fetch page and inspect DOM
        try:
            async with httpx.AsyncClient(
                headers=self.headers,
                timeout=settings.CRAWL_TIMEOUT_SECONDS,
                follow_redirects=True,
            ) as client:
                resp = await client.get(url)
                html = resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url} for ATS fingerprinting: {e}")
            return url_match  # Return URL-only result if fetch fails

        dom_match = self._match_dom(url, html)
        if dom_match:
            return dom_match

        # Check for iframes pointing to known ATS domains
        iframe_match = self._check_iframes(html)
        if iframe_match:
            return iframe_match

        return url_match or {"platform": "unknown", "confidence": 0.3}

    def _match_url(self, url: str) -> Optional[dict]:
        for sig in ATS_SIGNATURES:
            for pattern in sig.get("url_patterns", []):
                if re.search(pattern, url, re.IGNORECASE):
                    return {"platform": sig["platform"], "confidence": sig["confidence_base"]}
        return None

    def _match_dom(self, url: str, html: str) -> Optional[dict]:
        soup = BeautifulSoup(html, "lxml")
        page_text = soup.get_text(" ", strip=True).lower()
        scripts = " ".join(s.get("src", "") for s in soup.find_all("script") if s.get("src"))
        links = " ".join(a.get("href", "") for a in soup.find_all("a") if a.get("href"))
        combined = f"{url} {page_text} {scripts} {links}"

        for sig in ATS_SIGNATURES:
            matches = 0
            for pat in sig.get("url_patterns", []):
                if re.search(pat, combined, re.IGNORECASE):
                    matches += 2
            for pat in sig.get("dom_patterns", []):
                if re.search(pat, combined, re.IGNORECASE):
                    matches += 1
            for pat in sig.get("script_patterns", []):
                if re.search(pat, scripts, re.IGNORECASE):
                    matches += 1
            if matches >= 2:
                confidence = min(sig["confidence_base"] + (matches - 2) * 0.01, 0.99)
                return {"platform": sig["platform"], "confidence": confidence}
        return None

    def _check_iframes(self, html: str) -> Optional[dict]:
        soup = BeautifulSoup(html, "lxml")
        for iframe in soup.find_all("iframe"):
            src = iframe.get("src", "")
            match = self._match_url(src)
            if match:
                return {**match, "confidence": match["confidence"] * 0.9}  # Slight confidence reduction for iframe
        return None
