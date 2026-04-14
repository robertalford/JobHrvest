"""
Career Page Finder v3.8 — direct from CareerPageFinderV4.

High-impact improvements:
1. Bad-target rejection and score-based upgrades for weak discovery pages.
2. Canonical/OG listing URL promotion and strong subpage traversal from career hubs.
3. ATS-focused recovery probes for Greenhouse embed boards and Zoho Recruit portals.
4. Localized listing path probing (/ms/kerjaya, /lowongan, /career/job-openings, /careers/join-our-team).
5. Search-form query upgrades (e.g. /search -> ?search=) for empty listing shells.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import Optional
from urllib.parse import parse_qsl, urljoin, urlparse

import httpx
from lxml import etree

from app.crawlers.career_page_finder_v2 import _CLIENT_HEADERS
from app.crawlers.career_page_finder_v4 import CareerPageFinderV4

logger = logging.getLogger(__name__)


_LISTING_PATH_PATTERN_V38 = re.compile(
    r"/(?:career|careers|jobs?|vacanc|opening|openings|position|positions|"
    r"opportunit|employment|recruit|talent|search(?:-jobs?)?|job-openings|"
    r"current-vacancies|join-our-team|portal\.na|candidateportal|"
    r"kerjaya|karir|karier|lowongan|loker)",
    re.IGNORECASE,
)

_LISTING_TEXT_PATTERN_V38 = re.compile(
    r"\b(?:career|careers|jobs?|vacanc(?:y|ies)|open(?:\s+)?positions?|"
    r"job\s+openings?|current\s+vacancies|join\s+our\s+team|browse\s+jobs|"
    r"view\s+all\s+jobs?|all\s+jobs|current\s+jobs?|"
    r"kerjaya|jawatan\s+kosong|karir|lowongan|loker)\b",
    re.IGNORECASE,
)

_LISTING_CTA_PATTERN_V38 = re.compile(
    r"\b(?:job\s+openings?|current\s+vacancies|join\s+our\s+team|"
    r"browse\s+jobs|view\s+all\s+jobs?|search\s+jobs|"
    r"lowongan|kerjaya|karir)\b",
    re.IGNORECASE,
)

_REJECT_LINK_TEXT_PATTERN_V38 = re.compile(
    r"\b(?:privacy|terms|cookie|blog|news|media|investor|contact|about|"
    r"our\s+values|our\s+culture|our\s+ecosystem|our\s+leaders|"
    r"sign\s*in|sign\s*up|register|my\s+account|help|support|talent\s+stories?)\b",
    re.IGNORECASE,
)

_REJECT_LINK_HREF_PATTERN_V38 = re.compile(
    r"(?:mailto:|tel:|/privacy|/terms|/cookie|/news|/blog|/investor|"
    r"/contact|/about|/login|/logout|/register|/account|/culture(?:/|$)|"
    r"/our-culture(?:/|$)|/our-values(?:/|$)|/our-ecosystem(?:/|$)|"
    r"wp-json|feed|rss|\.pdf(?:$|\?)|\.docx?(?:$|\?))",
    re.IGNORECASE,
)

_ROLE_HINT_PATTERN_V38 = re.compile(
    r"\b(?:engineer|developer|manager|director|analyst|specialist|assistant|"
    r"consultant|coordinator|officer|administrator|accountant|technician|"
    r"designer|architect|operator|supervisor|advisor|executive|intern(?:ship)?|"
    r"nurse|teacher|driver|chef|sales|marketing|finance|hr|recruit(?:er|ment)?)\b",
    re.IGNORECASE,
)

_ERROR_PAGE_PATTERN_V38 = re.compile(
    r"(?:\b404\b|page\s+not\s+found|something\s+went\s+wrong|error\s+occurred|"
    r"access\s+denied|forbidden|temporarily\s+unavailable)",
    re.IGNORECASE,
)

_NON_LISTING_SECTION_PATTERN_V38 = re.compile(
    r"/(?:sectors?|services?|insights?|resources?|news|blog|about|team|culture|"
    r"our-culture|our-direction|our-values|talent-story)(?:/|$)",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN_V38 = re.compile(
    r"(?:job|position|vacanc|opening|requisition|career|posting|listing|accordion)",
    re.IGNORECASE,
)


def _slugify_v38(value: str) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"[\s-]+", "-", text)
    return text.strip("-")


def _slug_variants_v38(company_name: str, current_url: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []

    def _add(value: str) -> None:
        slug = (value or "").strip("-")
        if not slug or slug in seen:
            return
        seen.add(slug)
        out.append(slug)

    if company_name:
        _add(_slugify_v38(company_name))
        stripped = re.sub(
            r"\b(?:ltd|limited|pty|inc|corp|corporation|group|co|company|"
            r"holdings|services|solutions|technology|technologies)\b",
            "",
            company_name,
            flags=re.IGNORECASE,
        )
        _add(_slugify_v38(stripped))

    parsed = urlparse(current_url or "")
    for seg in (parsed.path or "").split("/"):
        seg = seg.strip().lower()
        if not seg:
            continue
        _add(_slugify_v38(seg))

    return [s for s in out if len(s) >= 2]


class CareerPageFinderV38(CareerPageFinderV4):
    """v3.8 finder with stronger listing URL recovery and ATS-aware probing."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        disc = await super().find(domain, company_name)
        if not disc.get("url"):
            return disc

        current = dict(disc)
        current_score = self._listing_page_score_v38(current.get("url", ""), current.get("html") or "")
        current_bad = self._is_bad_target_v38(current.get("url", ""), current.get("html") or "")

        async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS) as client:
            candidate = await self._canonical_upgrade_v38(client, current)
            if candidate:
                score = self._listing_page_score_v38(candidate.get("url", ""), candidate.get("html") or "")
                if current_bad or score > current_score + 0.15:
                    current, current_score = candidate, score
                    current_bad = self._is_bad_target_v38(current.get("url", ""), current.get("html") or "")

            upgraded = await self._probe_candidates_v38(
                client,
                current,
                self._build_candidate_urls_v38(current, domain, company_name),
                limit=34,
            )
            if upgraded:
                score = self._listing_page_score_v38(upgraded.get("url", ""), upgraded.get("html") or "")
                if current_bad or score > current_score + 0.2:
                    current, current_score = upgraded, score
                    current_bad = self._is_bad_target_v38(current.get("url", ""), current.get("html") or "")

            query_upgrade = await self._query_variant_upgrade_v38(client, current)
            if query_upgrade:
                score = self._listing_page_score_v38(query_upgrade.get("url", ""), query_upgrade.get("html") or "")
                if score > current_score + 0.15:
                    current, current_score = query_upgrade, score
                    current_bad = self._is_bad_target_v38(current.get("url", ""), current.get("html") or "")

            if current_bad:
                home_upgrade = await self._homepage_recovery_v38(client, domain, company_name)
                if home_upgrade:
                    score = self._listing_page_score_v38(home_upgrade.get("url", ""), home_upgrade.get("html") or "")
                    if score > current_score:
                        current = home_upgrade

        return current

    async def _canonical_upgrade_v38(self, client: httpx.AsyncClient, disc: dict) -> Optional[dict]:
        current_url = disc.get("url", "")
        html_body = disc.get("html") or ""
        if not current_url or len(html_body) < 200:
            return None

        urls: list[str] = []
        for pat in (
            r"<link[^>]+rel=['\"]canonical['\"][^>]+href=['\"]([^\"']+)['\"]",
            r"<meta[^>]+property=['\"]og:url['\"][^>]+content=['\"]([^\"']+)['\"]",
        ):
            m = re.search(pat, html_body, re.IGNORECASE)
            if not m:
                continue
            candidate = urljoin(current_url, (m.group(1) or "").strip())
            if candidate:
                urls.append(candidate)

        if not urls:
            return None

        deduped: list[str] = []
        seen: set[str] = set()
        for url in urls:
            norm = url.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
            deduped.append(url)

        base_score = self._listing_page_score_v38(current_url, html_body)
        for candidate_url in deduped[:3]:
            parsed_cur = urlparse(current_url)
            parsed_can = urlparse(candidate_url)
            if not parsed_can.netloc or parsed_cur.netloc.lower() != parsed_can.netloc.lower():
                continue
            if not _LISTING_PATH_PATTERN_V38.search(parsed_can.path):
                continue
            try:
                resp = await client.get(candidate_url)
            except Exception:
                continue
            body = resp.text or ""
            if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload_v38(body):
                continue
            score = self._listing_page_score_v38(str(resp.url), body)
            if score > base_score + 0.15:
                return {
                    "url": str(resp.url),
                    "method": disc.get("method", "") + "+canonical_v38",
                    "candidates": disc.get("candidates", []) + [candidate_url],
                    "html": body,
                }

        return None

    def _build_candidate_urls_v38(self, disc: dict, domain: str, company_name: str) -> list[tuple[str, str, float]]:
        current_url = disc.get("url") or f"https://{domain}"
        html_body = disc.get("html") or ""
        parsed = urlparse(current_url)
        base = f"{parsed.scheme or 'https'}://{parsed.netloc or domain}"

        candidates: list[tuple[str, str, float]] = []

        def _add(url: str, reason: str, weight: float) -> None:
            if not url:
                return
            full = url if re.match(r"^https?://", url, flags=re.IGNORECASE) else urljoin(current_url, url)
            if not full:
                return
            candidates.append((full, reason, weight))

        for path in (
            "/career/job-openings",
            "/careers/join-our-team",
            "/recruit/Portal.na",
            "/jobs/Careers",
            "/ms/kerjaya",
            "/en/career",
            "/lowongan",
            "/loker",
            "/job-openings",
            "/current-vacancies",
            "/jobs/search",
        ):
            _add(base + path, f"path:{path}", 4.0)

        for url, reason, weight in self._anchor_candidates_v38(current_url, html_body):
            _add(url, reason, weight)

        host = (parsed.hostname or "").lower()
        if "greenhouse" in host:
            for url, reason, weight in self._greenhouse_candidates_v38(current_url, html_body, company_name):
                _add(url, reason, weight)

        if "zohorecruit" in host:
            _add(base + "/recruit/Portal.na", "zoho:portal", 8.4)
            _add(base + "/jobs/Careers", "zoho:jobs-careers", 6.2)

        return candidates

    def _anchor_candidates_v38(self, page_url: str, html_body: str) -> list[tuple[str, str, float]]:
        if not html_body or len(html_body) < 200:
            return []

        try:
            root = etree.fromstring(html_body.encode("utf-8", errors="replace"), etree.HTMLParser(encoding="utf-8"))
        except Exception:
            return []

        out: list[tuple[str, str, float]] = []
        seen: set[str] = set()

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            text = self._safe_text_v38(a_el)
            if self._is_rejected_link_v38(href, text):
                continue

            full = urljoin(page_url, href)
            norm = full.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)

            score = 0.0
            reason: list[str] = []
            if _LISTING_PATH_PATTERN_V38.search(href):
                score += 5.0
                reason.append("href")
            if _LISTING_TEXT_PATTERN_V38.search(text):
                score += 5.0
                reason.append("text")
            if _LISTING_CTA_PATTERN_V38.search(text):
                score += 2.5
                reason.append("cta")
            if _NON_LISTING_SECTION_PATTERN_V38.search(full):
                score -= 5.5
                reason.append("section_penalty")

            if score > 0:
                out.append((full, f"anchor:{'+'.join(reason) or 'listing'}", score))

        return out

    def _greenhouse_candidates_v38(
        self,
        current_url: str,
        html_body: str,
        company_name: str,
    ) -> list[tuple[str, str, float]]:
        parsed = urlparse(current_url)
        host = parsed.netloc
        if not host:
            return []

        query = dict(parse_qsl(parsed.query))
        slugs: list[str] = []

        def _add_slug(value: str) -> None:
            slug = _slugify_v38(value)
            if not slug:
                return
            if slug not in slugs:
                slugs.append(slug)

        if query.get("for"):
            _add_slug(query.get("for", ""))

        for match in re.finditer(r"/[A-Za-z0-9_-]{2,40}/jobs/[0-9]{4,}", html_body or ""):
            frag = match.group(0).strip("/").split("/")
            if frag:
                _add_slug(frag[0])

        for slug in _slug_variants_v38(company_name, current_url):
            _add_slug(slug)

        out: list[tuple[str, str, float]] = []
        for slug in slugs[:6]:
            out.append((f"https://{host}/embed/job_board?for={slug}", f"greenhouse:embed:{slug}", 9.0))
            out.append((f"https://{host}/{slug}", f"greenhouse:host-root:{slug}", 6.8))
            out.append((f"https://boards.greenhouse.io/{slug}", f"greenhouse:boards:{slug}", 6.0))

        return out

    async def _probe_candidates_v38(
        self,
        client: httpx.AsyncClient,
        current: dict,
        candidates: list[tuple[str, str, float]],
        limit: int = 30,
    ) -> Optional[dict]:
        if not candidates:
            return None

        current_url = current.get("url", "")
        current_html = current.get("html") or ""
        current_score = self._listing_page_score_v38(current_url, current_html)
        current_bad = self._is_bad_target_v38(current_url, current_html)

        deduped: list[tuple[str, str, float]] = []
        seen: set[str] = set()
        for url, reason, weight in sorted(candidates, key=lambda x: x[2], reverse=True):
            norm = url.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
            deduped.append((url, reason, weight))

        best: Optional[dict] = None
        best_score = current_score

        for candidate_url, reason, weight in deduped[:limit]:
            try:
                resp = await client.get(candidate_url)
            except Exception:
                continue

            body = resp.text or ""
            if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload_v38(body):
                continue

            score = self._listing_page_score_v38(str(resp.url), body) + weight
            if current_bad or score > best_score + 0.05:
                best_score = score
                best = {
                    "url": str(resp.url),
                    "method": current.get("method", "") + f"+probe_v38:{reason}",
                    "candidates": current.get("candidates", []) + [candidate_url],
                    "html": body,
                }

        return best

    async def _query_variant_upgrade_v38(self, client: httpx.AsyncClient, disc: dict) -> Optional[dict]:
        current_url = disc.get("url", "")
        current_html = disc.get("html") or ""
        parsed = urlparse(current_url)
        if not parsed.netloc or parsed.query:
            return None
        if not re.search(r"(?:/search(?:/|$)|/jobs(?:/|$)|/vacanc|/opening|/career)", parsed.path or "", re.IGNORECASE):
            return None

        if not any(token in current_html.lower() for token in ("search", "keywords", "jobsearchbutton", "show all jobs")):
            return None

        base = current_url.rstrip("/")
        variants = [
            f"{base}?search=",
            f"{base}?search=&keywords=",
            f"{base}?keywords=",
            f"{base}?keyword=",
            f"{base}?q=",
        ]

        current_score = self._listing_page_score_v38(current_url, current_html)
        best: Optional[dict] = None
        best_score = current_score

        for candidate_url in variants:
            try:
                resp = await client.get(candidate_url)
            except Exception:
                continue

            body = resp.text or ""
            if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload_v38(body):
                continue

            score = self._listing_page_score_v38(str(resp.url), body)
            if score > best_score + 0.15:
                best_score = score
                best = {
                    "url": str(resp.url),
                    "method": disc.get("method", "") + "+query_variant_v38",
                    "candidates": disc.get("candidates", []) + [candidate_url],
                    "html": body,
                }

        return best

    async def _homepage_recovery_v38(
        self,
        client: httpx.AsyncClient,
        domain: str,
        company_name: str,
    ) -> Optional[dict]:
        home_url = f"https://{domain}"
        try:
            resp = await client.get(home_url)
        except Exception:
            return None

        body = resp.text or ""
        if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload_v38(body):
            return None

        base_disc = {
            "url": str(resp.url),
            "method": "homepage_recovery_v38",
            "candidates": [str(resp.url)],
            "html": body,
        }

        upgraded = await self._probe_candidates_v38(
            client,
            base_disc,
            self._build_candidate_urls_v38(base_disc, domain, company_name),
            limit=36,
        )
        return upgraded or base_disc

    def _listing_page_score_v38(self, url: str, html_body: str) -> float:
        if not html_body or len(html_body) < 200:
            return -20.0
        if self._is_non_html_payload_v38(html_body):
            return -30.0

        score = 0.0
        lower = html_body.lower()
        url_l = (url or "").lower()

        if _ERROR_PAGE_PATTERN_V38.search(lower[:12000]):
            score -= 20.0
        if _NON_LISTING_SECTION_PATTERN_V38.search(url_l):
            score -= 8.0
        if _LISTING_PATH_PATTERN_V38.search(url_l):
            score += 4.0
        if "greenhouse" in url_l and "embed/job_board?for=" in url_l:
            score += 9.0
        if "zohorecruit" in url_l and "/recruit/portal.na" in url_l:
            score += 8.0

        score += min(lower.count("apply now"), 10)
        score += min(lower.count("job"), 10)
        score += min(lower.count("career"), 8)

        try:
            root = etree.fromstring(html_body.encode("utf-8", errors="replace"), etree.HTMLParser(encoding="utf-8"))
        except Exception:
            return score

        listing_links = 0
        role_links = 0
        nav_links = 0
        row_groups: dict[str, int] = defaultdict(int)

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href:
                continue
            text = self._safe_text_v38(a_el)
            if _LISTING_PATH_PATTERN_V38.search(href) or _LISTING_TEXT_PATTERN_V38.search(text):
                listing_links += 1
            if _ROLE_HINT_PATTERN_V38.search(text):
                role_links += 1
            if _REJECT_LINK_TEXT_PATTERN_V38.search(text):
                nav_links += 1

        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag not in {"li", "div", "tr", "article", "section"}:
                continue
            cls = (el.get("class") or "").lower()
            if not cls:
                continue
            if _ROW_CLASS_PATTERN_V38.search(cls):
                row_groups[f"{tag}:{cls.split()[0]}"] += 1

        repeated_rows = sum(v for v in row_groups.values() if v >= 3)
        score += min(listing_links * 1.4, 24.0)
        score += min(role_links * 2.2, 20.0)
        score += min(repeated_rows * 2.0, 28.0)

        if listing_links <= 1 and role_links == 0:
            score -= 8.0
        if nav_links >= 10 and role_links == 0:
            score -= 8.0

        return score

    def _is_bad_target_v38(self, url: str, html_body: str) -> bool:
        if not html_body or len(html_body) < 200:
            return True
        if self._is_non_html_payload_v38(html_body):
            return True

        lower = (html_body or "").lower()
        if _ERROR_PAGE_PATTERN_V38.search(lower[:10000]):
            return True

        page_score = self._listing_page_score_v38(url, html_body)
        if page_score < 6.5:
            return True

        return False

    @staticmethod
    def _safe_text_v38(el: etree._Element) -> str:
        try:
            txt = el.text_content()
            if txt:
                return " ".join(txt.split())
        except Exception:
            pass
        try:
            txt = etree.tostring(el, method="text", encoding="unicode")
            return " ".join((txt or "").split())
        except Exception:
            return ""

    @staticmethod
    def _is_rejected_link_v38(href: str, text: str) -> bool:
        href_l = (href or "").lower()
        text_l = (text or "").lower()
        if _REJECT_LINK_HREF_PATTERN_V38.search(href_l):
            return True
        if _REJECT_LINK_TEXT_PATTERN_V38.search(text_l):
            return True
        return False

    @staticmethod
    def _is_non_html_payload_v38(body: Optional[str]) -> bool:
        if not body:
            return True
        sample = body[:900].lstrip()
        if sample.startswith("%PDF-"):
            return True
        low = sample.lower()
        if (low.startswith("{") or low.startswith("[")) and "<html" not in low[:300]:
            return True
        return False
