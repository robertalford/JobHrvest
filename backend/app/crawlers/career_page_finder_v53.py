"""
Career Page Finder v5.3 — direct from CareerPageFinderV4.

v5.3 adds:
1. Non-HTML/bad-target rejection (PDF/feed/login/root shell safeguards).
2. ATS-aware platform probing (Salesforce/Oracle/Zoho/Greenhouse + localized career paths).
3. Homepage hub-link recovery with stronger listing scoring.
"""

from __future__ import annotations

import asyncio
import logging
import re
from urllib.parse import parse_qsl, urljoin, urlparse

import httpx
from lxml import etree

from app.crawlers.career_page_finder_v2 import _CLIENT_HEADERS
from app.crawlers.career_page_finder_v4 import CareerPageFinderV4

logger = logging.getLogger(__name__)

_LOCALIZED_CAREER_PATHS_V53 = [
    "/jobs",
    "/careers",
    "/lowongan",
    "/kerjaya",
    "/karir",
    "/loker",
    "/jobs/search",
    "/careers/jobs",
    "/career",
    "/job-openings",
    "/join-our-team",
    "/openings",
    "/vacancies",
    "/positions",
    "/recruitment",
    "/peluang-karir",
]

_LISTING_TEXT_PATTERN_V53 = re.compile(
    r"\b(?:careers?|jobs?|job\s+openings?|open\s+positions?|vacanc(?:y|ies)|"
    r"join\s+our\s+team|search\s+jobs|current\s+jobs?|current\s+vacancies|"
    r"lowongan|loker|karir|kerjaya|peluang\s+karir|info\s+lengkap)\b",
    re.IGNORECASE,
)

_LISTING_HREF_PATTERN_V53 = re.compile(
    r"/(?:career|careers|jobs?|openings?|vacanc|position|recruit|lowongan|loker|karir|kerjaya)",
    re.IGNORECASE,
)

_BAD_TARGET_URL_PATTERN_V53 = re.compile(
    r"(?:downloadrssfeed|/feed(?:/|$|\?)|\.pdf(?:$|\?)|/login(?:/|$|\?)|"
    r"fscmUI/faces/AtkHomePageWelcome)",
    re.IGNORECASE,
)
_ERROR_SHELL_PATTERN_V53 = re.compile(
    r"(?:404\s+not\s+found|an\s+error\s+occurred|sorry\s+about\s+that,\s+something\s+went\s+wrong|"
    r"server\s+returned\s+a\s+\"404|transaction\s+id\s+#|page\s+not\s+found)",
    re.IGNORECASE,
)


class CareerPageFinderV53(CareerPageFinderV4):
    """v5.3 finder with platform-aware recovery and bad-target rejection."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        try:
            parent_disc = await asyncio.wait_for(super().find(domain, company_name), timeout=15.0)
        except asyncio.TimeoutError:
            logger.warning("v5.3 parent finder timeout for %s", domain)
            parent_disc = {"url": None, "method": "parent_timeout", "candidates": [], "html": None}
        except Exception:
            logger.exception("v5.3 parent finder failed for %s", domain)
            parent_disc = {"url": None, "method": "parent_error", "candidates": [], "html": None}

        needs_recovery = not self._is_usable_discovery_v53(parent_disc)
        if not needs_recovery:
            upgraded = await self._try_platform_upgrade_v53(parent_disc)
            if upgraded:
                return upgraded
            root_upgrade = await self._try_root_hub_upgrade_v53(parent_disc)
            if root_upgrade:
                return root_upgrade
            return parent_disc

        recovered = await self._probe_platform_paths_v53(domain, company_name, parent_disc)
        if recovered:
            return recovered

        hub_recovered = await self._homepage_hub_recovery_v53(domain)
        if hub_recovered:
            return hub_recovered

        return parent_disc

    async def _try_root_hub_upgrade_v53(self, disc: dict) -> dict | None:
        """If discovery landed on a homepage/root-like URL, promote a stronger listing subpage."""
        url = disc.get("url") or ""
        html_body = disc.get("html") or ""
        if not url or not html_body:
            return None

        parsed = urlparse(url)
        path = (parsed.path or "/").strip().lower()
        if path not in {"", "/", "/home", "/index", "/index.html"}:
            return None

        parent_score = self._score_listing_page_v53(url, html_body)
        parsed_domain = parsed.netloc or ""
        recovered = await self._homepage_hub_recovery_v53(parsed_domain)
        if not recovered:
            return None
        recovered_score = self._score_listing_page_v53(recovered.get("url") or "", recovered.get("html") or "")
        if recovered_score <= parent_score:
            return None

        recovered["method"] = f"{disc.get('method') or 'v4'}+root_hub_upgrade_v53"
        return recovered

    def _is_usable_discovery_v53(self, disc: dict) -> bool:
        url = disc.get("url") or ""
        body = disc.get("html") or ""
        if not url or not body or len(body) < 180:
            return False
        if self._is_bad_target_url_v53(url):
            return False
        if self._looks_non_html_payload_v53(body) and not self._looks_like_feed_xml_v53(body):
            return False
        if self._looks_like_error_shell_v53(body):
            return False

        score = self._score_listing_page_v53(url, body)
        return score >= 2

    async def _try_platform_upgrade_v53(self, disc: dict) -> dict | None:
        url = disc.get("url") or ""
        html_body = disc.get("html") or ""
        if not url:
            return None

        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        probe_candidates: list[str] = []

        if "oraclecloud.com" in host and "candidateexperience" not in (parsed.path or "").lower():
            probe_candidates.extend([
                f"{parsed.scheme or 'https'}://{parsed.netloc}/hcmUI/CandidateExperience/en/sites/CX/requisitions",
                f"{parsed.scheme or 'https'}://{parsed.netloc}/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions",
            ])

        if "salesforce-sites.com" in host and "fRecruit__ApplyJobList" not in url:
            probe_candidates.extend([
                f"{parsed.scheme or 'https'}://{parsed.netloc}/careers/fRecruit__ApplyJobList?portal=English",
                f"{parsed.scheme or 'https'}://{parsed.netloc}/careers/",
            ])

        if "zohorecruit" in host and "portal.na" not in url.lower():
            probe_candidates.append(f"{parsed.scheme or 'https'}://{parsed.netloc}/recruit/Portal.na")

        if "job-boards.greenhouse.io" in host:
            query = dict(parse_qsl(parsed.query))
            slug = (query.get("for") or "").strip()
            if slug:
                probe_candidates.append(f"https://job-boards.greenhouse.io/embed/job_board?for={slug}")
                probe_candidates.append(f"https://job-boards.greenhouse.io/{slug}")

        if "elmotalent.com" in host and self._looks_like_error_shell_v53(html_body):
            for slug in self._elmotalent_slug_candidates_v53(company_name="", page_url=url):
                probe_candidates.extend(
                    [
                        f"{parsed.scheme or 'https'}://{parsed.netloc}/careers/{slug}/jobs",
                        f"{parsed.scheme or 'https'}://{parsed.netloc}/careers/{slug}/job/search",
                        f"{parsed.scheme or 'https'}://{parsed.netloc}/careers/{slug}",
                    ]
                )

        if not probe_candidates:
            return None

        best = await self._fetch_best_probe_v53(probe_candidates)
        if not best:
            return None

        best_url, best_html, best_score = best
        if best_score <= self._score_listing_page_v53(url, html_body):
            return None

        return {
            "url": best_url,
            "method": f"{disc.get('method') or 'v4'}+platform_upgrade_v53",
            "candidates": [best_url] + list(disc.get("candidates") or [])[:4],
            "html": best_html,
        }

    async def _probe_platform_paths_v53(self, domain: str, company_name: str, parent_disc: dict) -> dict | None:
        base_url = f"https://{domain}"

        paths = list(_LOCALIZED_CAREER_PATHS_V53)

        if "salesforce-sites.com" in domain:
            paths.extend([
                "/careers/",
                "/careers/fRecruit__ApplyJobList?portal=English",
            ])

        if "oraclecloud.com" in domain:
            paths.extend([
                "/hcmUI/CandidateExperience/en/sites/CX/requisitions",
                "/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions",
            ])

        if "zohorecruit" in domain:
            paths.extend([
                "/recruit/Portal.na",
                "/recruit/",
            ])

        if "elmotalent.com" in domain:
            paths.extend(
                [
                    "/careers/jobs",
                    "/careers/job/search",
                    "/jobs/search",
                ]
            )
            parent_url = parent_disc.get("url") or ""
            for slug in self._elmotalent_slug_candidates_v53(company_name=company_name, page_url=parent_url):
                paths.extend(
                    [
                        f"/careers/{slug}/jobs",
                        f"/careers/{slug}/job/search",
                        f"/careers/{slug}",
                    ]
                )

        if "job-boards.greenhouse.io" in domain:
            slug = self._slugify_company_v53(company_name)
            if slug:
                paths.extend([
                    f"/embed/job_board?for={slug}",
                    f"/{slug}",
                ])

            parent_url = (parent_disc.get("url") or "")
            if parent_url:
                query = dict(parse_qsl(urlparse(parent_url).query))
                parent_slug = (query.get("for") or "").strip()
                if parent_slug:
                    paths.extend([
                        f"/embed/job_board?for={parent_slug}",
                        f"/{parent_slug}",
                    ])

        probes = []
        seen = set()
        for path in paths:
            url = path if path.startswith("http") else base_url + path
            norm = url.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
            probes.append(url)

        best = await self._fetch_best_probe_v53(probes)
        if not best:
            return None

        best_url, best_html, best_score = best
        if best_score < 2:
            return None

        return {
            "url": best_url,
            "method": "probe_v53",
            "candidates": probes[:6],
            "html": best_html,
        }

    async def _fetch_best_probe_v53(self, urls: list[str]) -> tuple[str, str, int] | None:
        if not urls:
            return None
        ranked_urls = sorted(
            urls,
            key=lambda u: self._probe_priority_v53(u),
            reverse=True,
        )

        async with httpx.AsyncClient(timeout=4.5, follow_redirects=True, headers=_CLIENT_HEADERS) as client:

            async def _try(url: str) -> tuple[str, str | None, int]:
                try:
                    resp = await client.get(url)
                except Exception:
                    return url, None, -999

                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 140:
                    return str(resp.url), None, -999

                resolved = str(resp.url)
                if self._is_bad_target_url_v53(resolved):
                    return resolved, None, -999
                if self._looks_non_html_payload_v53(body) and not self._looks_like_feed_xml_v53(body):
                    return resolved, None, -999
                if self._looks_like_error_shell_v53(body):
                    return resolved, None, -999

                score = self._score_listing_page_v53(resolved, body)
                return resolved, body, score

            results = await asyncio.gather(*[_try(url) for url in ranked_urls[:18]])

        valid = [(url, body, score) for url, body, score in results if body]
        if not valid:
            return None

        return max(valid, key=lambda item: item[2])

    async def _homepage_hub_recovery_v53(self, domain: str) -> dict | None:
        base_url = f"https://{domain}"

        try:
            async with httpx.AsyncClient(timeout=4.5, follow_redirects=True, headers=_CLIENT_HEADERS) as client:
                resp = await client.get(base_url)
                if resp.status_code != 200:
                    return None

                body = resp.text or ""
                if len(body) < 200 or self._looks_non_html_payload_v53(body):
                    return None

                root = etree.fromstring(body.encode("utf-8", errors="replace"), etree.HTMLParser(encoding="utf-8"))

                scored_links: list[tuple[int, str, str]] = []
                seen: set[str] = set()

                for a_el in root.xpath("//a[@href]")[:1800]:
                    href = (a_el.get("href") or "").strip()
                    if not href or href.startswith("#") or href.startswith("javascript:"):
                        continue

                    full_url = urljoin(base_url, href)
                    parsed = urlparse(full_url)
                    if not parsed.netloc:
                        continue

                    home_host = urlparse(base_url).netloc.lower()
                    link_host = parsed.netloc.lower()
                    if link_host != home_host:
                        base_a = ".".join(home_host.split(".")[-2:])
                        base_b = ".".join(link_host.split(".")[-2:])
                        if base_a != base_b:
                            continue

                    norm = full_url.rstrip("/")
                    if norm in seen:
                        continue
                    seen.add(norm)

                    text = self._safe_text_v53(a_el)
                    score = 0
                    if _LISTING_TEXT_PATTERN_V53.search(text):
                        score += 10
                    if _LISTING_HREF_PATTERN_V53.search(parsed.path or ""):
                        score += 8
                    if self._is_bad_target_url_v53(full_url):
                        score -= 8
                    if score > 0:
                        scored_links.append((score, full_url, text[:60]))

                if not scored_links:
                    return None

                scored_links.sort(key=lambda item: item[0], reverse=True)

                for _score, candidate_url, label in scored_links[:3]:
                    probe_urls = [candidate_url]
                    if "search" in candidate_url.lower() or "jobs" in candidate_url.lower():
                        joiner = "&" if "?" in candidate_url else "?"
                        probe_urls.append(f"{candidate_url}{joiner}search=")

                    for probe_url in probe_urls:
                        try:
                            sub_resp = await client.get(probe_url)
                        except Exception:
                            continue

                        sub_body = sub_resp.text or ""
                        if sub_resp.status_code != 200 or len(sub_body) < 180:
                            continue
                        if self._looks_non_html_payload_v53(sub_body) and not self._looks_like_feed_xml_v53(sub_body):
                            continue
                        if self._looks_like_error_shell_v53(sub_body):
                            continue

                        sub_score = self._score_listing_page_v53(str(sub_resp.url), sub_body)
                        if sub_score < 2:
                            continue

                        return {
                            "url": str(sub_resp.url),
                            "method": f"homepage_hub_v53:{label}",
                            "candidates": [u for _, u, _ in scored_links[:5]],
                            "html": sub_body,
                        }

        except Exception:
            logger.debug("v5.3 homepage hub recovery failed for %s", domain)

        return None

    def _score_listing_page_v53(self, page_url: str, html_body: str) -> int:
        lower = (html_body or "").lower()
        score = 0

        if any(tok in (page_url or "").lower() for tok in ("jobs", "careers", "lowongan", "loker", "karir", "kerjaya", "requisitions")):
            score += 2

        score += min(lower.count("apply"), 8)
        score += min(lower.count("job"), 10) // 2
        score += min(lower.count("vacanc"), 6)

        if "job-post" in lower:
            score += 6
        if "datarow" in lower and "frecruit__applyjob" in lower:
            score += 7
        if "jobdetailrow" in lower or "portaldetail.na" in lower:
            score += 6
        if "candidateexperience" in lower and "requisition" in lower:
            score += 5
        if "__next_data__" in lower and ("clientcode" in lower or "recruiterid" in lower):
            score += 3
        if "list-group-item" in lower and "job/view" in lower:
            score += 8
        if "thjmf-loop-job" in lower:
            score += 7
        if "elementor-inner-column" in lower and ("apply" in lower or "lowongan" in lower):
            score += 5
        if "accordion-item" in lower and ("kerjaya" in lower or "career" in lower):
            score += 5

        if "downloadrssfeed" in (page_url or "").lower():
            score -= 6
        if self._is_bad_target_url_v53(page_url):
            score -= 5
        if self._looks_like_error_shell_v53(html_body):
            score -= 15

        if lower.count("<a ") <= 2 and "requisition" not in lower:
            score -= 1

        return score

    @staticmethod
    def _looks_non_html_payload_v53(body: str) -> bool:
        if not body:
            return True
        sample = body[:900].lstrip()
        if sample.startswith("%PDF-"):
            return True
        low = sample.lower()
        if (low.startswith("{") or low.startswith("[")) and "<html" not in low[:320]:
            return True
        return False

    @staticmethod
    def _looks_like_feed_xml_v53(body: str) -> bool:
        sample = (body or "")[:3200].lstrip().lower()
        return sample.startswith("<?xml") or "<rss" in sample or "<feed" in sample

    @staticmethod
    def _is_bad_target_url_v53(url: str) -> bool:
        return bool(_BAD_TARGET_URL_PATTERN_V53.search((url or "").strip()))

    @staticmethod
    def _looks_like_error_shell_v53(body: str) -> bool:
        sample = (body or "")[:7000]
        if not sample:
            return True
        return bool(_ERROR_SHELL_PATTERN_V53.search(sample))

    @staticmethod
    def _probe_priority_v53(url: str) -> int:
        low = (url or "").lower()
        score = 0
        if any(tok in low for tok in ("/lowongan", "/kerjaya", "/karir", "/loker")):
            score += 14
        if "/careers/" in low and "/jobs" in low:
            score += 14
        if any(tok in low for tok in ("/jobs/search", "?search=", "fRecruit__ApplyJobList", "Portal.na")):
            score += 12
        if "/requisitions" in low:
            score += 10
        if "/jobs" in low or "/careers" in low:
            score += 6
        if "/career" in low:
            score += 4
        return score

    @staticmethod
    def _slugify_company_v53(company_name: str) -> str:
        value = (company_name or "").strip().lower()
        if not value:
            return ""
        value = re.sub(r"[^a-z0-9\s-]", "", value)
        value = re.sub(r"\s+", "-", value).strip("-")
        if len(value) < 3:
            return ""
        return value

    def _elmotalent_slug_candidates_v53(self, company_name: str, page_url: str) -> list[str]:
        candidates: list[str] = []

        slug = self._slugify_company_v53(company_name)
        if slug:
            candidates.append(slug)
            if "-" in slug:
                candidates.append(slug.split("-", 1)[0])

        parsed = urlparse(page_url or "")
        path_parts = [seg for seg in (parsed.path or "").split("/") if seg]
        for part in path_parts:
            cleaned = re.sub(r"[^a-z0-9-]", "", part.lower()).strip("-")
            if len(cleaned) < 3:
                continue
            candidates.append(cleaned)
            if "-" in cleaned:
                candidates.append(cleaned.split("-", 1)[0])

        if parsed.netloc:
            sub = parsed.netloc.split(".")[0].strip().lower()
            sub = re.sub(r"[^a-z0-9-]", "", sub)
            if len(sub) >= 3:
                candidates.append(sub)

        deduped: list[str] = []
        seen: set[str] = set()
        for item in candidates:
            if item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped[:6]

    @staticmethod
    def _safe_text_v53(el: etree._Element) -> str:
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
