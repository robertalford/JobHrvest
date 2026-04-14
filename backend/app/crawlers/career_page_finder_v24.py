"""
Career Page Finder v2.4 — direct from v1.6 finder base (CareerPageFinderV4).

High-impact changes:
1. Multilingual careers/link discovery (lowongan/loker/karir/kerjaya + EN patterns).
2. Localized path probing when discovery lands on weak root pages.
3. Company slug probing for careers subdomains (e.g. careers.<domain>/<company-slug>/).
"""

from __future__ import annotations

import logging
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from lxml import etree

from app.crawlers.career_page_finder_v2 import _CLIENT_HEADERS, _ATS_DOMAINS
from app.crawlers.career_page_finder_v4 import CareerPageFinderV4

logger = logging.getLogger(__name__)


_SUBPAGE_HREF_PATTERN_V24 = re.compile(
    r"/(?:"
    r"careers?|jobs?|job-openings?|openings?|opportunities|vacancies|"
    r"search(?:-jobs?)?|job-search|all-jobs|current-openings|"
    r"lowongan|loker|karir|karier|kerjaya|pekerjaan|jawatan|"
    r"vacantes?|empleo|empleos|trabajo|trabajos|vagas?|stellen|jobsuche"
    r")",
    re.IGNORECASE,
)

_SUBPAGE_TEXT_PATTERN_V24 = re.compile(
    r"(?:"
    r"view\s+all|see\s+all|all\s+jobs|current\s+openings|job\s+openings|"
    r"browse\s+jobs|search\s+jobs|open\s+positions|available\s+positions|"
    r"vacancies|careers|jobs?|"
    r"lowongan|loker|karir|karier|kerjaya|pekerjaan|jawatan|"
    r"vacantes?|empleo|trabajo|vagas?|stellenangebote"
    r")",
    re.IGNORECASE,
)

_LISTING_KEYWORD_PATTERN_V24 = re.compile(
    r"(?:"
    r"job|jobs|career|careers|vacanc|opening|position|hiring|recruit|apply|"
    r"lowongan|loker|karir|karier|kerjaya|pekerjaan|jawatan|"
    r"vacantes?|empleo|trabajo|vagas?|stellen|jobsuche|info\s+lengkap|lamar"
    r")",
    re.IGNORECASE,
)

_JOB_HEADING_HINT_PATTERN_V24 = re.compile(
    r"(?:"
    r"manager|engineer|developer|analyst|specialist|assistant|officer|"
    r"akuntan|influencer|fotografer|videografer|desainer|customer\s+service|"
    r"teacher|nurse|technician|operator|staff|coordinator|executive|sales"
    r")",
    re.IGNORECASE,
)


def _slugify_v24(text: str) -> str:
    text = (text or "").lower().strip()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"[\s-]+", "-", text)
    return text.strip("-")


def _company_slug_variants_v24(company_name: str) -> list[str]:
    if not company_name or not company_name.strip():
        return []

    suffix_re = re.compile(
        r"\b(?:ltd|limited|pty|inc|corp|corporation|group|company|co|"
        r"holdings|international|services|solutions|australia|nz|uk|us)\b",
        re.IGNORECASE,
    )
    words = company_name.split()
    variants: list[str] = []
    seen: set[str] = set()

    def _add(value: str) -> None:
        slug = _slugify_v24(value)
        if slug and slug not in seen and len(slug) >= 3:
            seen.add(slug)
            variants.append(slug)

    _add(company_name)
    _add(suffix_re.sub("", company_name))
    if len(words) >= 2:
        _add(" ".join(words[:2]))
    if words:
        _add(words[0])

    # Many hosted boards use compact slugs.
    for slug in list(variants):
        compact = slug.replace("-", "")
        if compact and compact not in seen and len(compact) >= 3:
            seen.add(compact)
            variants.append(compact)

    return variants[:8]


def _same_base_domain_v24(host_a: str, host_b: str) -> bool:
    if not host_a or not host_b:
        return False
    a_parts = host_a.lower().split(".")
    b_parts = host_b.lower().split(".")
    if len(a_parts) < 2 or len(b_parts) < 2:
        return host_a.lower() == host_b.lower()
    return ".".join(a_parts[-2:]) == ".".join(b_parts[-2:])


class CareerPageFinderV24(CareerPageFinderV4):
    """v2.4 finder: multilingual subpage + localized path + careers-subdomain slug probing."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        disc = await super().find(domain, company_name)
        if not disc.get("url"):
            return disc

        best = disc
        best_score = self._score_listing_density_v24(best.get("html") or "", best.get("url") or "")

        # Careers subdomains often need /<company-slug>/ paths to expose listings.
        if company_name and self._should_try_slug_probe_v24(best.get("url") or "", best.get("html") or ""):
            slug_hit = await self._try_company_slug_probe_v24(best, company_name, best_score)
            if slug_hit:
                best = slug_hit
                best_score = self._score_listing_density_v24(best.get("html") or "", best.get("url") or "")

        if best.get("html") and len(best["html"]) > 200:
            subpage_hit = await self._try_multilingual_subpage_discovery_v24(best, best_score)
            if subpage_hit:
                best = subpage_hit
                best_score = self._score_listing_density_v24(best.get("html") or "", best.get("url") or "")

        # If still weak, try localized career paths directly.
        path_hit = await self._probe_localized_paths_v24(best, best_score)
        if path_hit:
            best = path_hit

        return best

    @staticmethod
    def _should_try_slug_probe_v24(url: str, html_body: str) -> bool:
        parsed = urlparse(url or "")
        host = (parsed.hostname or "").lower()
        if not host.startswith(("careers.", "career.", "jobs.", "job.", "recruitment.", "recruiting.")):
            return False

        path = parsed.path.strip("/")
        if path and len(path.split("/")) >= 1:
            return False

        lower = (html_body or "").lower()
        shellish = bool(
            re.search(r'<div[^>]+id="(?:__next|root|app)"[^>]*>\s*</div>', lower)
            or ("__next_data__" in lower and lower.count("<a ") < 5)
        )
        return shellish or len(html_body or "") < 2500

    async def _try_company_slug_probe_v24(self, disc: dict, company_name: str, current_score: float) -> Optional[dict]:
        base_url = f"{urlparse(disc['url']).scheme}://{urlparse(disc['url']).netloc}"
        slugs = _company_slug_variants_v24(company_name)
        if not slugs:
            return None

        candidates = [urljoin(base_url, f"/{slug}/") for slug in slugs]
        best: Optional[dict] = None
        best_score = current_score

        async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS) as client:
            for candidate in candidates[:8]:
                try:
                    resp = await client.get(candidate)
                except Exception:
                    continue
                if resp.status_code != 200 or len(resp.text) < 200:
                    continue

                score = self._score_listing_density_v24(resp.text, str(resp.url))
                if score > best_score + 0.8:
                    best_score = score
                    best = {
                        "url": str(resp.url),
                        "method": disc["method"] + "+company_slug_v24",
                        "candidates": disc.get("candidates", []) + [candidate],
                        "html": resp.text,
                    }

        return best

    async def _try_multilingual_subpage_discovery_v24(self, disc: dict, current_score: float) -> Optional[dict]:
        parent_url = disc["url"]
        parent_html = disc["html"]
        parent_parsed = urlparse(parent_url)

        try:
            parser = etree.HTMLParser(encoding="utf-8")
            root = etree.fromstring(parent_html.encode("utf-8", errors="replace"), parser)
        except Exception:
            return None

        candidate_scores: list[tuple[str, float, str]] = []
        seen: set[str] = set()

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            try:
                text = a_el.text_content().strip()
            except AttributeError:
                text = etree.tostring(a_el, method="text", encoding="unicode").strip()

            full_url = urljoin(parent_url, href)
            parsed = urlparse(full_url)
            if not parsed.hostname:
                continue

            if parsed.hostname != parent_parsed.hostname:
                if not _same_base_domain_v24(parsed.hostname, parent_parsed.hostname):
                    if not any(ats in parsed.hostname.lower() for ats in _ATS_DOMAINS):
                        continue

            if full_url.rstrip("/") == parent_url.rstrip("/"):
                continue
            if full_url in seen:
                continue
            seen.add(full_url)

            score = 0.0
            reasons: list[str] = []

            if _SUBPAGE_HREF_PATTERN_V24.search(href):
                score += 5.0
                reasons.append("href")
            if _SUBPAGE_TEXT_PATTERN_V24.search(text):
                score += 5.0
                reasons.append("text")
            if _LISTING_KEYWORD_PATTERN_V24.search(full_url):
                score += 2.0
                reasons.append("url_kw")
            if "pageNumber=" in full_url and "embed-jobs" in full_url:
                # Query-heavy probe URLs are fragile; keep but de-prioritize.
                score -= 2.0
            if score > 0:
                candidate_scores.append((full_url, score, "+".join(reasons)))

        if not candidate_scores:
            return None
        candidate_scores.sort(key=lambda item: item[1], reverse=True)

        best: Optional[dict] = None
        best_score = current_score

        async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS) as client:
            for candidate_url, base_score, reason in candidate_scores[:6]:
                try:
                    resp = await client.get(candidate_url)
                except Exception:
                    continue
                if resp.status_code != 200 or len(resp.text) < 200:
                    continue

                listing_score = self._score_listing_density_v24(resp.text, str(resp.url)) + base_score
                if listing_score > best_score + 1.0:
                    best_score = listing_score
                    best = {
                        "url": str(resp.url),
                        "method": disc["method"] + f"+subpage_v24:{reason}",
                        "candidates": disc.get("candidates", []) + [candidate_url],
                        "html": resp.text,
                    }

        return best

    async def _probe_localized_paths_v24(self, disc: dict, current_score: float) -> Optional[dict]:
        parsed = urlparse(disc["url"])
        if not parsed.scheme or not parsed.netloc:
            return None
        base = f"{parsed.scheme}://{parsed.netloc}"

        paths = [
            "/lowongan",
            "/loker",
            "/karir",
            "/karier",
            "/kerjaya",
            "/pekerjaan",
            "/jawatan",
            "/careers",
            "/career",
            "/jobs",
            "/job-openings",
            "/vacancies",
            "/opportunities",
            "/join-us",
        ]

        best: Optional[dict] = None
        best_score = current_score

        async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS) as client:
            for path in paths:
                url = urljoin(base, path)
                try:
                    resp = await client.get(url)
                except Exception:
                    continue
                if resp.status_code != 200 or len(resp.text) < 200:
                    continue

                score = self._score_listing_density_v24(resp.text, str(resp.url))
                if score > best_score + 1.2:
                    best_score = score
                    best = {
                        "url": str(resp.url),
                        "method": disc["method"] + f"+probe_v24:path:{path}",
                        "candidates": disc.get("candidates", []) + [url],
                        "html": resp.text,
                    }

        return best

    def _score_listing_density_v24(self, html_body: str, page_url: str) -> float:
        if not html_body or len(html_body) < 200:
            return 0.0

        lower = html_body.lower()
        keyword_hits = len(_LISTING_KEYWORD_PATTERN_V24.findall(lower))
        anchor_hits = len(
            re.findall(
                r'href=["\'][^"\']*(?:job|career|vacanc|opening|lowongan|loker|karir|kerjaya|apply)[^"\']*["\']',
                lower,
            )
        )
        apply_hits = len(re.findall(r"apply|lamar|info\s+lengkap|selengkapnya|job\s+description", lower))
        title_hits = 0

        try:
            parser = etree.HTMLParser(encoding="utf-8")
            root = etree.fromstring(html_body.encode("utf-8", errors="replace"), parser)
            headings = root.xpath("//h1 | //h2 | //h3 | //h4")
            for h in headings[:120]:
                text = " ".join((h.text or "").split())
                if not text:
                    text = " ".join(etree.tostring(h, method="text", encoding="unicode").split())
                if _JOB_HEADING_HINT_PATTERN_V24.search(text):
                    title_hits += 1
        except Exception:
            pass

        url_bonus = 2.0 if _LISTING_KEYWORD_PATTERN_V24.search(page_url or "") else 0.0

        score = (
            min(keyword_hits, 25) * 0.35
            + min(anchor_hits, 20) * 0.55
            + min(apply_hits, 20) * 0.35
            + min(title_hits, 20) * 0.8
            + url_bonus
        )
        return score
