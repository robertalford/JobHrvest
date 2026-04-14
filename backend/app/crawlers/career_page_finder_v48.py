"""
Career Page Finder v4.8 — direct from CareerPageFinderV4.

v4.8 preserves v4.7 discovery behavior and adds an Oracle tenant recovery pass
that upgrades generic CandidateExperience targets to tenant-specific requisitions
paths (for example CX -> CX_1001) when evidence supports it.
"""

from __future__ import annotations

import logging
import re
from urllib.parse import parse_qsl, urljoin, urlparse

import httpx

from app.crawlers.career_page_finder_v2 import _CLIENT_HEADERS
from app.crawlers.career_page_finder_v4 import CareerPageFinderV4
from app.crawlers.career_page_finder_v47 import CareerPageFinderV47

logger = logging.getLogger(__name__)


class CareerPageFinderV48(CareerPageFinderV4):
    """v4.8 finder wrapper with Oracle tenant-path upgrade."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        disc = await CareerPageFinderV47().find(domain, company_name)
        if not disc.get("url"):
            return disc

        improved = await self._oracle_tenant_upgrade_v48(disc)
        return improved or disc

    async def _oracle_tenant_upgrade_v48(self, disc: dict) -> dict | None:
        current_url = disc.get("url", "")
        current_html = disc.get("html") or ""
        url_lower = current_url.lower()

        if "oraclecloud.com" not in url_lower and "candidateexperience" not in url_lower and "candidateexperience" not in current_html.lower():
            return None

        parsed = urlparse(current_url)
        if not parsed.netloc:
            return None

        current_site = ""
        site_match = re.search(r"/sites/([A-Za-z0-9_]+)/", parsed.path, flags=re.IGNORECASE)
        if site_match:
            current_site = site_match.group(1)

        site_ids = self._oracle_site_ids_v48(current_url, current_html)
        if not site_ids:
            return None

        base_url = f"{parsed.scheme or 'https'}://{parsed.netloc}"
        candidates: list[tuple[str, str]] = []
        for site_id in site_ids[:10]:
            candidates.append((urljoin(base_url, f"/hcmUI/CandidateExperience/en/sites/{site_id}/requisitions"), site_id))
            candidates.append((urljoin(base_url, f"/hcmUI/CandidateExperience/en/sites/{site_id}/jobs"), site_id))

        baseline_score = self._oracle_listing_score_v48(current_url, current_html)
        best: dict | None = None
        best_score = baseline_score
        force_upgrade = current_site.upper() == "CX"

        try:
            async with httpx.AsyncClient(timeout=8.0, follow_redirects=True, headers=_CLIENT_HEADERS) as client:
                for candidate_url, candidate_site in candidates:
                    try:
                        resp = await client.get(candidate_url)
                    except Exception:
                        continue

                    body = resp.text or ""
                    if resp.status_code != 200 or len(body) < 200 or self._looks_non_html_payload_v48(body):
                        continue

                    score = self._oracle_listing_score_v48(str(resp.url), body)
                    if "/requisitions" in candidate_url:
                        score += 1.2
                    if re.search(r"_[0-9]+$", candidate_site):
                        score += 1.0
                    if re.search(rf"siteNumber['\"=: ]+{re.escape(candidate_site)}", body, re.IGNORECASE):
                        score += 0.8

                    should_upgrade = False
                    if score > best_score + 0.2:
                        should_upgrade = True
                    elif force_upgrade and re.search(r"_[0-9]+$", candidate_site) and score >= best_score - 0.15:
                        should_upgrade = True

                    if should_upgrade:
                        best_score = score
                        best = {
                            "url": str(resp.url),
                            "method": disc.get("method", "") + f"+oracle_tenant_v48:{candidate_site}",
                            "candidates": disc.get("candidates", []) + [candidate_url],
                            "html": body,
                        }
        except Exception:
            logger.debug("v4.8 oracle tenant upgrade failed for %s", current_url)

        return best

    @staticmethod
    def _oracle_listing_score_v48(url: str, html_body: str) -> float:
        lower = (html_body or "").lower()
        score = 0.0

        if "candidateexperience" in (url or "").lower():
            score += 2.0
        if "/requisitions" in (url or "").lower():
            score += 2.5

        score += min(lower.count("requisition"), 12) * 0.5
        score += min(lower.count("job"), 12) * 0.35
        score += min(lower.count("siteNumber".lower()), 8) * 0.4

        if re.search(r"(?:hcmRestApi/resources/latest/recruitingCEJobRequisitions|requisitionList)", html_body or "", re.IGNORECASE):
            score += 3.0
        if re.search(r"siteNumber\s*[:=]\s*['\"][A-Za-z0-9_]+", html_body or "", re.IGNORECASE):
            score += 1.0

        return score

    @staticmethod
    def _oracle_site_ids_v48(page_url: str, html_body: str) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []

        def _add(value: str) -> None:
            site = (value or "").strip()
            if site.lower() in {"coreassets", "allitems", "forms"}:
                return
            if not re.fullmatch(r"[A-Za-z0-9_]{2,24}", site):
                return
            if not site or site in seen:
                return
            seen.add(site)
            ordered.append(site)

        for match in re.finditer(r"/sites/([A-Za-z0-9_]+)/", page_url or "", re.IGNORECASE):
            _add(match.group(1))
        for match in re.finditer(
            r"(?:<base[^>]+href=['\"][^'\"]*/sites/|CandidateExperience/en/sites/)([A-Za-z0-9_]+)",
            html_body or "",
            re.IGNORECASE,
        ):
            _add(match.group(1))
        for match in re.finditer(r"siteNumber\s*[:=]\s*['\"]([A-Za-z0-9_]+)", html_body or "", re.IGNORECASE):
            _add(match.group(1))
        for match in re.finditer(r"siteNumber=([A-Za-z0-9_]+)", html_body or "", re.IGNORECASE):
            _add(match.group(1))

        query_pairs = dict(parse_qsl(urlparse(page_url or "").query))
        if query_pairs.get("siteNumber"):
            _add(query_pairs.get("siteNumber", ""))

        base_ids = list(ordered)
        for site_id in base_ids:
            if re.fullmatch(r"CX(?:_\d+)?", site_id, flags=re.IGNORECASE):
                root = site_id.split("_", 1)[0]
                _add(root)
                for suffix in ("1001", "1002", "1003", "1004"):
                    _add(f"{root}_{suffix}")

        if not ordered:
            for fallback in ("CX_1001", "CX_1002", "CX"):
                _add(fallback)

        ordered.sort(
            key=lambda site: (
                0 if re.search(r"_[0-9]+$", site) else 1,
                0 if site.upper().endswith("_1001") else 1,
                site.lower(),
            )
        )
        return ordered

    @staticmethod
    def _looks_non_html_payload_v48(body: str) -> bool:
        sample = (body or "")[:900].lstrip()
        if not sample:
            return True
        if sample.startswith("%PDF-"):
            return True
        low = sample.lower()
        if (low.startswith("{") or low.startswith("[")) and "<html" not in low[:300]:
            return True
        return False
