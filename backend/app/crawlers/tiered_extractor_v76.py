"""
Tiered Extraction Engine v7.6 — split-row ATS recovery + strong-URL multilingual cards.

Strategy:
1. Keep v7.5 extraction order and safety gates.
2. Recover Teamtailor rows with strong numeric detail URLs but multilingual short titles.
3. Recover Bootstrap-style `?id=` card listings (common on Indonesian career pages).
4. Improve PageUp split-row link association when title and detail link live in sibling columns.
5. Broaden Connx GridTable row parsing for non-`div` row markup variants.
6. Strip common skip-link boilerplate from cleaned descriptions.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Optional
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
from lxml import html as lxml_html

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v74 import _V73_LOCATION_TITLE_HINT, _V73_NAV_TITLE, _V73_NON_JOB_HEADING
from app.crawlers.tiered_extractor_v75 import TieredExtractorV75

logger = logging.getLogger(__name__)

_V76_TEAMTAILOR_DETAIL = re.compile(r"/jobs/\d+-[a-z0-9][^/?#]{1,}", re.IGNORECASE)
_V76_QUERY_ID_DETAIL = re.compile(r"[?&]id=\d{2,}\b", re.IGNORECASE)
_V76_QUERY_ID_PATH_HINT = re.compile(r"/(?:career|careers|job|jobs|vacancy|lowongan)\b", re.IGNORECASE)
_V76_CONNX_DETAIL_PATH = re.compile(r"/job/details/[^/?#]{3,}", re.IGNORECASE)
_V76_CONNX_ENDPOINT_HINT = re.compile(
    r"""["'](/[^"']{1,200}(?:api|career|job)[^"']*)["']""",
    re.IGNORECASE,
)
_V76_SKIP_LINK_NOISE = re.compile(
    r"\b(?:Skip to primary navigation|Skip to main content|Back to all positions)\b",
    re.IGNORECASE,
)
_V76_SHORT_TITLE_DENY = {
    "jobs",
    "job",
    "home",
    "menu",
    "team",
    "careers",
    "career",
    "openings",
    "positions",
}


class TieredExtractorV76(TieredExtractorV75):
    """v7.6 extractor: focused recovery for split-row ATS and strong-URL multilingual cards."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        # Preemptive high-precision ATS/layout extractors that v7.5 can under-score.
        teamtailor_jobs = self._extract_teamtailor_rows_v76(working_html, page_url)
        if self._passes_teamtailor_jobset_v76(teamtailor_jobs, page_url):
            return await self._finalize_jobs_v76(teamtailor_jobs, working_html, page_url)

        query_id_jobs = self._extract_query_id_cards_v76(working_html, page_url)
        if self._passes_query_id_jobset_v76(query_id_jobs, page_url):
            return await self._finalize_jobs_v76(query_id_jobs, working_html, page_url)

        connx_shell_jobs = await self._extract_connx_shell_jobs_v76(page_url, working_html)
        if len(connx_shell_jobs) >= 2:
            return await self._finalize_jobs_v76(connx_shell_jobs, working_html, page_url)

        return await super().extract(career_page, company, html)

    async def _finalize_jobs_v76(self, jobs: list[dict], html: str, page_url: str) -> list[dict]:
        finalized = self._postprocess_jobs_v73(jobs, html, page_url)

        if self._should_probe_nuxt_shell_v73(html, page_url, finalized):
            recovered = await self._probe_localized_nuxt_jobs_v73(page_url, html)
            if recovered:
                finalized = self._postprocess_jobs_v73(recovered, html, page_url)

        if self._should_enrich_fast_path_v73(finalized, page_url):
            try:
                finalized = await asyncio.wait_for(self._enrich_bounded_v64(finalized), timeout=12.0)
                finalized = self._dedupe(finalized, page_url)
            except asyncio.TimeoutError:
                logger.warning("v7.6 fast-path enrichment timeout for %s", page_url)
            except Exception:
                logger.exception("v7.6 fast-path enrichment failed for %s", page_url)

        return self._clean_jobs_v73(finalized)[:MAX_JOBS_PER_PAGE]

    def _extract_linked_job_cards_v67(self, html: str, page_url: str) -> list[dict]:
        base_jobs = super()._extract_linked_job_cards_v67(html, page_url)
        extras: list[dict] = []

        extras.extend(self._extract_teamtailor_rows_v76(html, page_url))
        extras.extend(self._extract_query_id_cards_v76(html, page_url))

        if not extras:
            return base_jobs
        return self._dedupe_basic_v66(base_jobs + extras)

    def _extract_pageup_listing_jobs_v74(self, html: str, page_url: str) -> list[dict]:
        if not self._is_pageup_listing_page_v74(page_url, html):
            return []

        root = _parse_html(html)
        if root is None:
            return []

        title_nodes = root.xpath("//h3[contains(@class,'list-title')]")
        if not title_nodes:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()

        for title_node in title_nodes[:1200]:
            raw_title = self._normalize_title(_text(title_node) or "")
            if not raw_title or not self._is_valid_pageup_title_v75(raw_title):
                continue

            row, link_node = self._find_pageup_row_and_link_v76(title_node)
            if link_node is None:
                continue

            source_url = _resolve_url((link_node.get("href") or "").strip(), page_url) or page_url
            source_url = source_url.split("#", 1)[0]
            if source_url in seen_urls or self._is_non_job_url(source_url):
                continue
            if source_url.rstrip("/") == page_url.rstrip("/"):
                continue

            has_strong = self._has_strong_card_detail_url_v73(source_url, page_url)
            if self._is_obvious_non_job_card_v73(raw_title, source_url, page_url, has_strong):
                continue

            location = None
            if row is not None:
                loc_nodes = row.xpath(".//span[contains(@class,'location')]|.//li[contains(@class,'location')]")
                for loc_node in loc_nodes[:6]:
                    loc_text = " ".join((_text(loc_node) or "").split()).strip(" ,|-")
                    if 2 <= len(loc_text) <= 120 and loc_text.lower() != raw_title.lower():
                        location = loc_text
                        break

            row_text = " ".join((_text(row) or "").split()) if row is not None else ""
            desc = row_text[:5000] if len(row_text) >= 80 else None

            seen_urls.add(source_url)
            jobs.append(
                {
                    "title": raw_title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": desc,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "ats_pageup_listing_v76",
                    "extraction_confidence": 0.92,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _find_pageup_row_and_link_v76(self, title_node) -> tuple[Optional[Any], Optional[Any]]:
        candidates: list[Any] = []
        seen: set[int] = set()

        def add(nodes: list[Any]) -> None:
            for node in nodes:
                if node is None:
                    continue
                marker = id(node)
                if marker in seen:
                    continue
                seen.add(marker)
                candidates.append(node)

        add(
            title_node.xpath("ancestor::div[contains(@class,'list-item row')][1]")
            or title_node.xpath("ancestor::div[contains(@class,'list-item') and contains(@class,'row')][1]")
            or title_node.xpath("ancestor::div[contains(@class,'list-item')][1]")
            or title_node.xpath("ancestor::article[1]")
            or []
        )
        add(title_node.xpath("ancestor::div[position()<=7]"))
        add(title_node.xpath("ancestor::article[position()<=4]"))

        for row in candidates:
            links = row.xpath(".//a[@href]")
            link_node = self._pick_pageup_link_node_v76(links)
            if link_node is not None:
                return row, link_node

        ancestor_links = title_node.xpath("ancestor::a[@href][1]")
        if ancestor_links:
            return None, ancestor_links[0]
        return None, None

    @staticmethod
    def _pick_pageup_link_node_v76(links: list[Any]) -> Optional[Any]:
        if not links:
            return None

        def score(link_node: Any) -> int:
            href = (link_node.get("href") or "").strip().lower()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                return -1
            if "/cw/en/job/" in href or "/job/" in href:
                return 3
            if re.search(r"[?&]jobid=\w+", href):
                return 2
            if "/listing/" in href:
                return 1
            return 0

        best = max(links, key=score)
        return best if score(best) >= 0 else None

    def _extract_connx_grid_jobs_v75(self, html: str, page_url: str) -> list[dict]:
        base_jobs = super()._extract_connx_grid_jobs_v75(html, page_url)

        root = _parse_html(html)
        if root is None:
            return base_jobs

        rows = root.xpath(
            "//div[contains(@class,'GridTable') and contains(@class,'GridTable--rows')]/*"
            "|//div[contains(@class,'GridTable__row')]"
            "|//a[contains(@class,'GridTable__row')]"
            "|//tr[.//a[@href] and .//*[contains(@class,'name')]]"
        )
        if not rows:
            return base_jobs

        extra_jobs: list[dict] = []
        seen_urls = {str(j.get("source_url") or "") for j in base_jobs}

        for row in rows[:1200]:
            title_text = " ".join((row.xpath("string(.//*[contains(@class,'name')][1])") or "").split())
            if not title_text and isinstance(getattr(row, "tag", None), str) and row.tag.lower() == "a":
                title_text = " ".join((row.xpath("string(.)") or "").split())
            title = self._normalize_title(title_text)
            if not title or not self._is_valid_title_v60(title):
                continue

            href = (row.xpath("string((.//a[@href][1])/@href)") or "").strip()
            if not href and isinstance(getattr(row, "tag", None), str) and row.tag.lower() == "a":
                href = (row.get("href") or "").strip()

            if not href:
                attr_nodes = [row] + row.xpath(".//*[@data-href or @data-url or @data-link]")[:4]
                for node in attr_nodes:
                    for attr in ("data-href", "data-url", "data-link"):
                        value = (node.get(attr) or "").strip()
                        if value:
                            href = value
                            break
                    if href:
                        break

            if not href:
                row_markup = lxml_html.tostring(row, encoding="unicode")
                match = _V76_CONNX_DETAIL_PATH.search(row_markup)
                if match:
                    href = match.group(0)
            if not href:
                continue

            source_url = (_resolve_url(href, page_url) or page_url).split("#", 1)[0]
            if source_url in seen_urls:
                continue
            if source_url.rstrip("/") == page_url.rstrip("/"):
                continue
            if self._is_non_job_url(source_url):
                continue

            location = " ".join((row.xpath("string(.//*[contains(@class,'location')][1])") or "").split())
            location = location if 2 <= len(location) <= 120 else None

            employment_type = " ".join(
                (row.xpath("string(.//*[contains(@class,'employmentType') or contains(@class,'employment')][1])") or "").split()
            )
            employment_type = employment_type if 2 <= len(employment_type) <= 80 else None

            seen_urls.add(source_url)
            extra_jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": self._extract_row_description_v73(row, title),
                    "salary_raw": None,
                    "employment_type": employment_type,
                    "extraction_method": "ats_connx_grid_v76",
                    "extraction_confidence": 0.9,
                }
            )

        return self._dedupe_basic_v66(base_jobs + extra_jobs)

    async def _extract_connx_shell_jobs_v76(self, page_url: str, html: str) -> list[dict]:
        if not self._is_connx_app_shell_v76(page_url, html):
            return []

        root = _parse_html(html)
        if root is None:
            return []

        script_urls = []
        for script in root.xpath("//script[@src]")[:6]:
            src = (script.get("src") or "").strip()
            if not src:
                continue
            full = urljoin(page_url, src)
            if (urlparse(full).netloc or "").lower() != (urlparse(page_url).netloc or "").lower():
                continue
            script_urls.append(full)

        default_paths = (
            "/api/jobs",
            "/api/job/search",
            "/api/careers/jobs",
            "/api/v1/jobs",
            "/jobs/search",
            "/job/search",
        )
        candidate_urls = [urljoin(page_url, p) for p in default_paths]
        best: list[dict] = []

        try:
            async with httpx.AsyncClient(
                timeout=3.8,
                follow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json,text/plain,*/*",
                },
            ) as client:
                for script_url in script_urls[:3]:
                    try:
                        resp = await client.get(script_url)
                    except Exception:
                        continue
                    if resp.status_code != 200:
                        continue
                    body = resp.text or ""
                    for match in _V76_CONNX_ENDPOINT_HINT.finditer(body):
                        raw = match.group(1).strip()
                        low = raw.lower()
                        if low.endswith(".js") or low.endswith(".css"):
                            continue
                        if "job/details/" in low:
                            continue
                        if not any(token in low for token in ("job", "career", "vacanc", "position", "api")):
                            continue
                        candidate_urls.append(urljoin(page_url, raw))

                ordered_urls: list[str] = []
                seen: set[str] = set()
                for endpoint in candidate_urls:
                    if endpoint in seen:
                        continue
                    seen.add(endpoint)
                    ordered_urls.append(endpoint)
                    if len(ordered_urls) >= 10:
                        break

                for endpoint in ordered_urls:
                    try:
                        resp = await client.get(endpoint)
                    except Exception:
                        continue
                    if resp.status_code != 200:
                        continue
                    body = resp.text or ""
                    if len(body) < 40:
                        continue

                    jobs = self._extract_connx_probe_payload_v76(resp, body, page_url)
                    if len(jobs) > len(best):
                        best = jobs
                    if len(best) >= 3:
                        break
        except Exception:
            return []

        if len(best) < 2:
            return []
        if not self._passes_jobset_validation(best, page_url):
            return []
        return self._dedupe_basic_v66(best)

    def _extract_connx_probe_payload_v76(self, response: httpx.Response, body: str, page_url: str) -> list[dict]:
        content_type = (response.headers.get("content-type") or "").lower()
        if "json" in content_type or body.lstrip().startswith(("{", "[")):
            try:
                payload = response.json()
            except Exception:
                try:
                    payload = json.loads(body)
                except Exception:
                    payload = None
            if payload is not None:
                jobs = self._extract_jobs_json_items_v74(payload, page_url)
                filtered = [
                    job
                    for job in jobs
                    if _V76_CONNX_DETAIL_PATH.search(str(job.get("source_url") or ""))
                    or _V76_QUERY_ID_DETAIL.search(str(job.get("source_url") or ""))
                ]
                if filtered:
                    return self._dedupe_basic_v66(filtered)
            return []

        if "gridtable" in body.lower() or "/job/details/" in body.lower():
            return self._extract_connx_grid_jobs_v75(body, page_url)
        return []

    @staticmethod
    def _is_connx_app_shell_v76(page_url: str, html: str) -> bool:
        host = (urlparse(page_url).netloc or "").lower()
        if "connxcareers.com" not in host:
            return False
        sample = (html or "")[:200000].lower()
        if "gridtable--rows" in sample or "job/details/" in sample:
            return False
        return "<div id=\"app\"" in sample and "<script" in sample

    def _extract_teamtailor_rows_v76(self, html: str, page_url: str) -> list[dict]:
        host = (urlparse(page_url).netloc or "").lower()
        if "teamtailor.com" not in host and "jobs_list_container" not in (html or ""):
            return []

        root = _parse_html(html)
        if root is None:
            return []

        anchors = root.xpath(
            "//ul[@id='jobs_list_container']//a[@href]"
            "|//*[contains(@class,'jobs-list-container')]//a[@href]"
        )
        if not anchors:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()
        for anchor in anchors[:1200]:
            href = (anchor.get("href") or "").strip()
            if not href:
                continue
            source_url = (_resolve_url(href, page_url) or page_url).split("#", 1)[0]
            if source_url in seen_urls:
                continue
            if not _V76_TEAMTAILOR_DETAIL.search(source_url):
                continue
            if self._is_non_job_url(source_url):
                continue

            title = self._normalize_title(_text(anchor) or "")
            if not self._is_valid_teamtailor_title_v76(title):
                continue
            if _V73_NON_JOB_HEADING.match(title) or _V73_NAV_TITLE.match(title):
                continue
            if _V73_LOCATION_TITLE_HINT.match(title):
                continue

            row = self._find_row_container_v73(anchor)
            location = None
            employment_type = None
            description = None
            if row is not None:
                loc_nodes = row.xpath(
                    ".//*[contains(@class,'location') or contains(@class,'office') or contains(@class,'city') "
                    "or contains(@data-testid,'location')]"
                )
                for node in loc_nodes[:4]:
                    loc_txt = " ".join((_text(node) or "").split()).strip(" ,|-")
                    if self._is_location_candidate_v73(loc_txt, title, class_hint=str(node.get("class") or "")):
                        location = loc_txt[:120]
                        break

                type_nodes = row.xpath(
                    ".//*[contains(@class,'employment') or contains(@class,'job-type') or contains(@class,'work-type')]"
                )
                for node in type_nodes[:3]:
                    type_txt = " ".join((_text(node) or "").split()).strip(" ,|-")
                    if 2 <= len(type_txt) <= 60 and type_txt.lower() != title.lower():
                        employment_type = type_txt
                        break

                description = self._extract_row_description_v73(row, title)

            seen_urls.add(source_url)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": description,
                    "salary_raw": None,
                    "employment_type": employment_type,
                    "extraction_method": "ats_teamtailor_rows_v76",
                    "extraction_confidence": 0.92,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _is_valid_teamtailor_title_v76(self, title: str) -> bool:
        if self._is_valid_title_v60(title):
            return True

        t = (title or "").strip()
        if not t:
            return False
        if _V73_NON_JOB_HEADING.match(t) or _V73_NAV_TITLE.match(t) or _V73_LOCATION_TITLE_HINT.match(t):
            return False
        if not re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]", t):
            return False

        words = t.split()
        if len(words) > 8:
            return False
        if len(words) == 1:
            token = re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿ0-9+&./'-]", "", words[0])
            if len(token) < 4:
                return False
            if token.lower() in _V76_SHORT_TITLE_DENY:
                return False
            return True
        return True

    def _extract_query_id_cards_v76(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        cards = root.xpath(
            "//*[contains(concat(' ', normalize-space(@class), ' '), ' col-lg-4 ') "
            "and contains(concat(' ', normalize-space(@class), ' '), ' mb-4 ') "
            "and .//a[contains(@href,'id=')] and (.//h2 or .//h3 or .//h4)]"
        )
        if len(cards) < 3:
            return []

        page_host = (urlparse(page_url).netloc or "").lower()
        jobs: list[dict] = []
        seen_urls: set[str] = set()

        for card in cards[:1200]:
            anchor = (card.xpath(".//a[contains(@href,'id=')][1]") or [None])[0]
            if anchor is None:
                continue
            href = (anchor.get("href") or "").strip()
            if not href:
                continue

            source_url = (_resolve_url(href, page_url) or page_url).split("#", 1)[0]
            parsed = urlparse(source_url)
            if page_host and (parsed.netloc or "").lower() != page_host:
                continue
            if source_url in seen_urls:
                continue
            if self._is_non_job_url(source_url):
                continue
            if not _V76_QUERY_ID_DETAIL.search(source_url):
                continue
            if not _V76_QUERY_ID_PATH_HINT.search(parsed.path or ""):
                continue

            title_text = " ".join((card.xpath("string(.//h3[1] | .//h2[1] | .//h4[1])") or "").split())
            if not title_text:
                title_text = " ".join((anchor.xpath("string(.)") or "").split())
            title = self._normalize_title(title_text)
            if not self._is_valid_query_id_title_v76(title):
                continue

            row_desc = self._extract_row_description_v73(card, title)
            location = None
            loc_nodes = card.xpath(".//*[contains(@class,'location')]|.//h3/following-sibling::div[1]")
            for node in loc_nodes[:3]:
                loc_txt = " ".join((_text(node) or "").split()).strip(" ,|-")
                if self._is_location_candidate_v73(loc_txt, title, class_hint=str(node.get("class") or "")):
                    location = loc_txt[:120]
                    break

            seen_urls.add(source_url)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": row_desc,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "tier2_query_id_cards_v76",
                    "extraction_confidence": 0.88,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _is_valid_query_id_title_v76(self, title: str) -> bool:
        if self._is_valid_title_v60(title):
            return True

        t = (title or "").strip()
        if not t:
            return False
        if _V73_NON_JOB_HEADING.match(t) or _V73_NAV_TITLE.match(t) or _V73_LOCATION_TITLE_HINT.match(t):
            return False
        if not re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]", t):
            return False

        words = t.split()
        if len(words) > 5:
            return False
        if len(words) == 1:
            token = re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿ0-9+&./'-]", "", words[0])
            if len(token) < 4:
                return False
            if token.lower() in _V76_SHORT_TITLE_DENY:
                return False
        return True

    def _passes_teamtailor_jobset_v76(self, jobs: list[dict], page_url: str) -> bool:
        if len(jobs) < 3:
            return False
        strong = sum(1 for job in jobs if _V76_TEAMTAILOR_DETAIL.search(str(job.get("source_url") or "")))
        return strong >= max(3, int(len(jobs) * 0.75))

    def _passes_query_id_jobset_v76(self, jobs: list[dict], page_url: str) -> bool:
        if len(jobs) < 3:
            return False

        strong_urls = 0
        for job in jobs:
            source_url = str(job.get("source_url") or "")
            if not _V76_QUERY_ID_DETAIL.search(source_url):
                continue
            parsed = urlparse(source_url)
            query = parse_qs(parsed.query or "")
            if "id" in query and any(re.match(r"^\d{2,}$", str(v or "")) for v in query.get("id", [])):
                strong_urls += 1
        return strong_urls >= max(3, int(len(jobs) * 0.7))

    def _clean_description_v73(self, value: Any) -> Optional[str]:
        cleaned = super()._clean_description_v73(value)
        if not cleaned:
            return cleaned

        text = _V76_SKIP_LINK_NOISE.sub(" ", cleaned)
        text = re.sub(r"\s{2,}", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text or None
