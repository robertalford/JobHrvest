"""
Career Page Finder v3 — extends v2 with ATS company slug discovery.

When the input domain is a known ATS platform (e.g. careers-page.com, livehire.com,
job-boards.greenhouse.io), v2 just goes to the platform root which is WRONG — it
returns the ATS homepage, not the company's job board.

v3 fix: discover the company-specific path on the ATS domain by trying slugified
variants of the company name against known ATS URL patterns.
"""

import logging
import re
from typing import Optional

import httpx

from app.crawlers.career_page_finder_v2 import (
    CareerPageFinderV2,
    _ATS_DOMAINS,
    _CLIENT_HEADERS,
)

logger = logging.getLogger(__name__)

# Common company name suffixes to strip when generating slug variants
_COMPANY_SUFFIXES = re.compile(
    r"\b(?:ltd|limited|pty|inc|incorporated|corp|corporation|group|"
    r"co|company|holdings|enterprises|services|solutions|international|"
    r"australia|aust|nz|uk|us)\b",
    re.IGNORECASE,
)

# ATS-specific URL patterns: {domain} and {slug} are substituted at runtime.
# Order matters — more specific patterns first.
_ATS_URL_PATTERNS: list[dict] = [
    # Greenhouse embedded board
    {
        "domains": ["greenhouse.io"],
        "patterns": [
            "https://{domain}/embed/job_board?for={slug}",
            "https://boards.greenhouse.io/{slug}",
            "https://boards.greenhouse.io/embed/job_board?for={slug}",
        ],
    },
    # LiveHire widget
    {
        "domains": ["livehire.com"],
        "patterns": [
            "https://{domain}/widgets/job-listings/{slug}/public",
            "https://{domain}/{slug}",
        ],
    },
    # Generic pattern — works for most ATS platforms (careers-page.com, applytojob.com, etc.)
    {
        "domains": [],  # fallback for any ATS domain
        "patterns": [
            "https://{domain}/{slug}",
        ],
    },
]


def _slugify(text: str) -> str:
    """Convert text to a URL slug: lowercase, hyphens, no special chars."""
    text = text.lower().strip()
    # Replace non-alphanumeric chars (except spaces/hyphens) with nothing
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    # Collapse whitespace/hyphens into single hyphens
    text = re.sub(r"[\s-]+", "-", text)
    return text.strip("-")


def _generate_slug_variants(company_name: str) -> list[str]:
    """Generate multiple slug variants from a company name.

    Examples for "Star Anise Limited":
        - "star-anise-limited" (full slugified)
        - "star-anise" (without common suffixes)
        - "star-anise" (first two words)
        - "staranise" (no hyphens variant of suffix-stripped)
    """
    if not company_name or not company_name.strip():
        return []

    seen: set[str] = set()
    variants: list[str] = []

    def _add(slug: str) -> None:
        slug = slug.strip("-")
        if slug and slug not in seen and len(slug) >= 2:
            seen.add(slug)
            variants.append(slug)

    # Full name slugified
    full_slug = _slugify(company_name)
    _add(full_slug)

    # Without common suffixes
    stripped = _COMPANY_SUFFIXES.sub("", company_name)
    stripped = re.sub(r"\s+", " ", stripped).strip()
    stripped_slug = _slugify(stripped)
    _add(stripped_slug)

    # First two words only
    words = company_name.split()
    if len(words) >= 2:
        two_word_slug = _slugify(" ".join(words[:2]))
        _add(two_word_slug)

    # First word only (if it's long enough to be meaningful)
    if words and len(words[0]) >= 4:
        one_word_slug = _slugify(words[0])
        _add(one_word_slug)

    # No-hyphen variants (some ATS platforms use concatenated slugs)
    for slug in list(variants):
        no_hyphen = slug.replace("-", "")
        _add(no_hyphen)

    return variants


def _get_ats_patterns_for_domain(domain: str) -> list[str]:
    """Get URL patterns for a specific ATS domain, most specific first."""
    patterns: list[str] = []
    domain_lower = domain.lower()

    for entry in _ATS_URL_PATTERNS:
        if entry["domains"]:
            # Specific domain match
            if any(d in domain_lower for d in entry["domains"]):
                patterns.extend(entry["patterns"])
        else:
            # Fallback patterns (applies to any ATS domain)
            patterns.extend(entry["patterns"])

    return patterns


class CareerPageFinderV3(CareerPageFinderV2):
    """Discover career page URLs — v3 with ATS company slug discovery."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        """Find career page for a domain.

        When the domain is a known ATS platform, tries company-slug-based URL
        patterns before falling back to v2 behavior.
        """
        # Check if domain is a known ATS platform
        matched_ats = None
        for ats_domain in _ATS_DOMAINS:
            if ats_domain in domain.lower():
                matched_ats = ats_domain
                break

        if matched_ats and company_name:
            # Try ATS slug discovery first
            result = await self._try_ats_slug_discovery(domain, company_name, matched_ats)
            if result and result.get("html") and len(result["html"]) > 200:
                return result

        # Fall back to full v2 behavior (homepage crawl, path probe, etc.)
        return await super().find(domain, company_name)

    async def _try_ats_slug_discovery(
        self, domain: str, company_name: str, ats_domain: str
    ) -> Optional[dict]:
        """Try to discover the company-specific path on an ATS platform."""
        slugs = _generate_slug_variants(company_name)
        if not slugs:
            return None

        url_patterns = _get_ats_patterns_for_domain(domain)
        if not url_patterns:
            url_patterns = ["https://{domain}/{slug}"]

        # Build list of candidate URLs: pattern × slug combinations
        candidates: list[str] = []
        seen: set[str] = set()
        for pattern in url_patterns:
            for slug in slugs:
                url = pattern.format(domain=domain, slug=slug)
                if url not in seen:
                    seen.add(url)
                    candidates.append(url)

        logger.info(
            "v3 ATS slug discovery for '%s' on %s: trying %d URLs",
            company_name, domain, len(candidates),
        )

        # Try each candidate URL
        async with httpx.AsyncClient(
            timeout=self.timeout, follow_redirects=True, headers=_CLIENT_HEADERS,
        ) as client:
            for url in candidates:
                try:
                    resp = await client.get(url)
                    if resp.status_code == 200 and len(resp.text) > 200:
                        # Verify it's not just the ATS homepage/error page
                        if self._looks_like_company_page(resp.text, company_name):
                            logger.info(
                                "v3 ATS slug hit: %s → %s (%d bytes)",
                                company_name, url, len(resp.text),
                            )
                            return {
                                "url": str(resp.url),
                                "method": f"ats_slug:{ats_domain}",
                                "candidates": candidates[:5],
                                "html": resp.text,
                            }
                except Exception as e:
                    logger.debug("v3 ATS slug failed for %s: %s", url, e)
                    continue

        # If plain HTTP failed, try Playwright on the most likely candidate
        if candidates:
            best_candidate = candidates[0]
            rendered = await self._try_playwright(best_candidate)
            if rendered and len(rendered) > 200:
                if self._looks_like_company_page(rendered, company_name):
                    logger.info(
                        "v3 ATS slug hit (Playwright): %s → %s",
                        company_name, best_candidate,
                    )
                    return {
                        "url": best_candidate,
                        "method": f"ats_slug_playwright:{ats_domain}",
                        "candidates": candidates[:5],
                        "html": rendered,
                    }

        return None

    async def _fetch_and_return(self, url: str, method: str) -> dict:
        """Override v2: for ATS domains, still try slug discovery if we have context.

        Falls back to parent behavior since _fetch_and_return doesn't receive
        company_name — the slug discovery happens in find() before this is called.
        """
        return await super()._fetch_and_return(url, method)

    @staticmethod
    def _looks_like_company_page(html: str, company_name: str) -> bool:
        """Check if the HTML looks like a company-specific page (not generic ATS homepage).

        A company page should have either:
        - The company name (or part of it) in the content
        - Job listing signals (from v2's _looks_like_careers)
        - More than trivial content
        """
        html_lower = html[:10000].lower()

        # Check for company name presence (first two words)
        words = company_name.lower().split()
        name_words_found = sum(1 for w in words[:3] if len(w) >= 3 and w in html_lower)
        if name_words_found >= 1:
            return True

        # Check for job listing signals
        job_signals = [
            "job listing", "job opening", "current vacanc", "open position",
            "join our team", "career opportunit", "we're hiring", "apply now",
            "job description", "job-listing", "job-card", "job-post",
            "no jobs", "no current", "no open", "no vacanc",
        ]
        signal_count = sum(1 for s in job_signals if s in html_lower)
        if signal_count >= 1:
            return True

        return False
