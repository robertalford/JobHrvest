"""
Tiered Extraction Engine v8.9 - structured card/table recovery + pagination fill.

Strategy:
1. Recover dense query-id card boards where titles are in non-heading title nodes.
2. Clean table-row title extraction for `tr.data-row` boards to avoid duplicated noise.
3. Expand validated non-ATS listings across `startrow`/`pp` pagination links.
"""

from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

import httpx

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v88 import TieredExtractorV88

_V89_CARD_MARKER = re.compile(r"(?:jobcard|job-card|card-container|searchresultslist|lowongan|vacanc)", re.IGNORECASE)
_V89_DETAIL_HINT = re.compile(
    r"(?:"
    r"(?:[?&](?:id|job_id|jobid|vacancyid)=)"
    r"|(?:/detailpekerjaan\.aspx)"
    r"|(?:/job_detail/)"
    r"|(?:/jobs?/[\w-]{8,})"
    r"|(?:/job/[^/?#]{3,}/\d{5,})"
    r")",
    re.IGNORECASE,
)
_V89_BAD_ACTION_URL = re.compile(
    r"(?:/apply(?:/|$)|resume-entry|favorite|share|login|register|signup|talent-community)",
    re.IGNORECASE,
)
_V89_BAD_TITLE = re.compile(
    r"^(?:view\s+details?|apply\s+now|search|select|new|bagikan|lamar|for\s+job\s+seekers?)$",
    re.IGNORECASE,
)
_V89_LOCATION_NOISE = re.compile(
    r"(?:\bbatas\s+lamar\b|view\s+details?|apply|bagikan|share|favorite|search|select|unspecified)",
    re.IGNORECASE,
)
_V89_TABLE_ROW_MARKER = re.compile(r"tr\s+class=[\"']data-row[\"']|jobTitle-link", re.IGNORECASE)
_V89_PAGINATION_QUERY = re.compile(
    r"[?&](?:page|paged|pp|startrow|offset|pagenumber|page_number)=(\d{1,6})\b",
    re.IGNORECASE,
)
_V89_PAGE_PATH = re.compile(r"/page/(\d{1,4})(?:/|$)", re.IGNORECASE)


class TieredExtractorV89(TieredExtractorV88):
    """v8.9 extractor: query-id card/table recovery plus broader pagination fill."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        queryid_jobs = self._extract_queryid_card_rows_v89(working_html, page_url)
        if self._passes_queryid_card_jobset_v89(queryid_jobs, page_url):
            expanded = await self._expand_queryid_card_rows_v89(working_html, page_url, queryid_jobs)
            if self._passes_queryid_card_jobset_v89(expanded, page_url):
                return self._finalize_strict_rows_v88(expanded, page_url)
            return self._finalize_strict_rows_v88(queryid_jobs, page_url)

        return await super().extract(career_page, company, working_html)

    def _extract_query_table_jobs_v80(self, html: str, page_url: str) -> list[dict]:
        if not _V89_TABLE_ROW_MARKER.search((html or "")[:240000]):
            return super()._extract_query_table_jobs_v80(html, page_url)

        root = _parse_html(html)
        if root is None:
            return super()._extract_query_table_jobs_v80(html, page_url)

        rows = root.xpath("//tr[contains(@class,'data-row') and .//a[contains(@class,'jobTitle-link') and @href]]")
        if len(rows) < 3:
            return super()._extract_query_table_jobs_v80(html, page_url)

        jobs: list[dict] = []
        seen_urls: set[str] = set()
        for row in rows[:1600]:
            source_url = self._pick_table_detail_url_v89(row, page_url, seen_urls)
            if not source_url:
                continue

            title = self._extract_table_title_v89(row)
            if not title:
                continue
            if not self._is_reasonable_table_title_v89(title, source_url, page_url):
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, True):
                continue

            location = self._extract_table_location_v89(row, title)
            seen_urls.add(source_url)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "tier2_query_table_rows_v81",
                    "extraction_confidence": 0.92,
                }
            )

        deduped = self._dedupe_basic_v66(jobs)
        if len(deduped) >= 3:
            return deduped
        return super()._extract_query_table_jobs_v80(html, page_url)

    def _pick_table_detail_url_v89(self, row, page_url: str, seen_urls: set[str]) -> str | None:
        anchors = row.xpath(".//a[contains(@class,'jobTitle-link') and @href]|.//a[@href]")
        best_score = -99
        best_url = ""
        for a_el in anchors[:12]:
            href = (a_el.get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url or source_url in seen_urls:
                continue
            if self._is_non_job_url(source_url):
                continue
            if source_url.rstrip("/") == page_url.rstrip("/"):
                continue

            score = 0
            a_class = (a_el.get("class") or "").lower()
            if "jobtitle-link" in a_class:
                score += 4
            if self._has_strong_card_detail_url_v73(source_url, page_url) or self._is_job_like_url(source_url):
                score += 3
            if _V89_DETAIL_HINT.search(source_url):
                score += 2
            if _V89_BAD_ACTION_URL.search(source_url):
                score -= 4

            text = " ".join((_text(a_el) or "").split())
            if len(text) <= 2:
                score -= 1
            if re.search(r"\b(?:view|details?|job)\b", text, re.IGNORECASE):
                score += 1

            if score > best_score:
                best_score = score
                best_url = source_url

        return best_url if best_score >= 3 else None

    def _extract_table_title_v89(self, row) -> str:
        nodes = row.xpath(
            ".//span[contains(@class,'jobTitle') and contains(@class,'hidden-phone')]//a[contains(@class,'jobTitle-link')][1]"
            "|.//a[contains(@class,'jobTitle-link')][1]"
            "|./td[1]//a[@href][1]"
        )
        if nodes:
            title = self._normalize_title(" ".join((_text(nodes[0]) or "").split()))
            if title:
                return title

        fallback = self._normalize_title(" ".join((_text(row.xpath("./td[1]")[0]) or "").split())) if row.xpath("./td[1]") else ""
        return fallback

    def _extract_table_location_v89(self, row, title: str) -> str | None:
        nodes = row.xpath(
            ".//td[contains(@class,'colLocation')]//span[contains(@class,'jobLocation')][1]"
            "|.//span[contains(@class,'jobLocation')][1]"
        )
        for node in nodes[:3]:
            loc = " ".join((_text(node) or "").split()).strip(" ,|-")
            if not loc or loc.lower() == title.lower() or len(loc) > 120:
                continue
            return loc
        return None

    def _is_reasonable_table_title_v89(self, title: str, source_url: str, page_url: str) -> bool:
        if self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title):
            return True
        if _V89_BAD_TITLE.match(title):
            return False
        if not (self._has_strong_card_detail_url_v73(source_url, page_url) or self._is_job_like_url(source_url)):
            return False

        words = title.split()
        if len(words) != 1:
            return False
        token = words[0].strip(" ,|-")
        if len(token) < 4 or len(token) > 40:
            return False
        if not re.search(r"[A-Za-z]", token):
            return False
        return token.lower() not in {"search", "filter", "jobs", "careers", "apply", "details"}

    def _extract_queryid_card_rows_v89(self, html: str, page_url: str) -> list[dict]:
        preview = (html or "")[:260000]
        if not (_V89_CARD_MARKER.search(preview) and _V89_DETAIL_HINT.search(preview)):
            return []

        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath(
            "//article[contains(@class,'job-card') and .//a[@href]]"
            "|//div[contains(concat(' ', normalize-space(@class), ' '), ' jobCard ') and .//a[@href]]"
            "|//a[@href and .//h5[contains(@class,'card-title')]]"
            "|//a[@href and .//*[contains(@class,'card-title')]]"
        )
        if len(rows) < 4:
            return []

        jobs: list[dict] = []
        for row in rows[:2400]:
            source_url = self._pick_queryid_detail_url_v89(row, page_url)
            if not source_url:
                continue

            title = self._extract_queryid_title_v89(row)
            if not title:
                continue
            if _V89_BAD_TITLE.match(title):
                continue
            if not (
                self._is_valid_title_v60(title)
                or self._is_reasonable_structured_title_v81(title)
                or self._is_reasonable_multilingual_title_v88(title)
            ):
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, True):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_queryid_location_v89(row, title),
                    "description": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "tier2_queryid_cards_v89",
                    "extraction_confidence": 0.9,
                }
            )

        return self._dedupe_title_url_v88(jobs, limit=MAX_JOBS_PER_PAGE * 2)

    def _pick_queryid_detail_url_v89(self, row, page_url: str) -> str | None:
        anchors = [row] if getattr(row, "tag", "").lower() == "a" else []
        anchors.extend(row.xpath(".//a[@href]"))
        best_score = -99
        best_url = ""
        for a_el in anchors[:14]:
            href = (a_el.get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url:
                continue
            if self._is_non_job_url(source_url):
                continue
            if source_url.rstrip("/") == page_url.rstrip("/"):
                continue

            score = 0
            if _V89_DETAIL_HINT.search(source_url):
                score += 5
            elif self._has_strong_card_detail_url_v73(source_url, page_url) or self._is_job_like_url(source_url):
                score += 3
            if _V89_BAD_ACTION_URL.search(source_url):
                score -= 5

            text = " ".join((_text(a_el) or "").split()).strip()
            if re.search(r"\b(?:view|details?)\b", text, re.IGNORECASE):
                score += 1
            if re.search(r"\b(?:apply|lamar|share|favorite)\b", text, re.IGNORECASE):
                score -= 2

            if score > best_score:
                best_score = score
                best_url = source_url

        return best_url if best_score >= 3 else None

    def _extract_queryid_title_v89(self, row) -> str:
        nodes = row.xpath(
            ".//*[contains(@class,'jobCard__title__title')][1]"
            "|.//*[contains(@class,'job-title')][1]"
            "|.//*[contains(@class,'card-title')][1]"
            "|.//h1[1]|.//h2[1]|.//h3[1]|.//h4[1]|.//h5[1]|.//h6[1]"
            "|.//a[contains(@class,'job-title-link')][1]"
        )
        if nodes:
            title = self._normalize_title(" ".join((_text(nodes[0]) or "").split()))
            if title:
                return title

        row_text = self._normalize_title(" ".join((_text(row) or "").split()))
        return row_text

    def _extract_queryid_location_v89(self, row, title: str) -> str | None:
        nodes = row.xpath(
            ".//i[contains(@class,'fa-map-marker')]/parent::*[1]"
            "|.//*[contains(@class,'jobCard__country')]//p[1]"
            "|.//*[contains(@class,'location')][1]"
            "|.//*[contains(@class,'country')][1]"
            "|.//*[contains(@class,'city')][1]"
            "|.//*[contains(@class,'card-text-lowongan')]"
        )
        for node in nodes[:10]:
            loc = " ".join((_text(node) or "").split()).strip(" ,|-")
            if not loc or loc.lower() == title.lower() or len(loc) > 140:
                continue
            if _V89_LOCATION_NOISE.search(loc):
                continue
            return loc
        return None

    def _passes_queryid_card_jobset_v89(self, jobs: list[dict], page_url: str) -> bool:
        if len(jobs) < 5:
            return False

        valid = 0
        titles: list[str] = []
        strong_urls = 0
        for job in jobs:
            title = self._normalize_title(str(job.get("title") or ""))
            source_url = str(job.get("source_url") or "")
            if not title or not source_url:
                continue
            if _V89_BAD_TITLE.match(title):
                continue
            if not (
                self._is_valid_title_v60(title)
                or self._is_reasonable_structured_title_v81(title)
                or self._is_reasonable_multilingual_title_v88(title)
            ):
                continue
            if self._is_non_job_url(source_url):
                continue

            strong = bool(_V89_DETAIL_HINT.search(source_url)) or self._has_strong_card_detail_url_v73(source_url, page_url)
            if not strong:
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, strong):
                continue

            valid += 1
            strong_urls += 1
            titles.append(title)

        if valid < 5:
            return False
        unique_ratio = len({t.lower() for t in titles}) / max(1, len(titles))
        if unique_ratio < 0.65:
            return False
        return strong_urls >= max(4, int(valid * 0.7))

    async def _expand_queryid_card_rows_v89(self, html: str, page_url: str, seed_jobs: list[dict]) -> list[dict]:
        if not seed_jobs:
            return []

        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return self._dedupe_title_url_v88(seed_jobs, limit=MAX_JOBS_PER_PAGE * 2)

        next_urls = self._collect_listing_pagination_urls_v89(html, page_url, max_pages=4)
        if not next_urls:
            return self._dedupe_title_url_v88(seed_jobs, limit=MAX_JOBS_PER_PAGE * 2)

        merged = list(seed_jobs)
        async with httpx.AsyncClient(timeout=4.8, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
            for target in next_urls:
                try:
                    resp = await client.get(target)
                except Exception:
                    continue
                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 500:
                    continue
                merged.extend(self._extract_queryid_card_rows_v89(body, str(resp.url)))

        return self._dedupe_title_url_v88(merged, limit=MAX_JOBS_PER_PAGE * 2)

    def _collect_listing_pagination_urls_v89(self, html: str, page_url: str, max_pages: int = 4) -> list[str]:
        root = _parse_html(html)
        if root is None:
            return []

        page_host = (urlparse(page_url).netloc or "").lower()
        ranked: list[tuple[int, int, str]] = []
        seen_urls: set[str] = set()
        for node in root.xpath("//a[@href]")[:500]:
            href = (node.get("href") or "").strip()
            if not href:
                continue
            full = urljoin(page_url, href).split("#", 1)[0]
            if full in seen_urls:
                continue
            seen_urls.add(full)

            parsed = urlparse(full)
            if (parsed.netloc or "").lower() != page_host:
                continue
            if full.rstrip("/") == page_url.rstrip("/"):
                continue

            query_match = _V89_PAGINATION_QUERY.search(full)
            value = None
            weight = 1
            if query_match:
                value = int(query_match.group(1))
                if "startrow=" in full.lower():
                    if value <= 0:
                        continue
                    weight = 0
                elif value <= 1:
                    continue
            else:
                path_match = _V89_PAGE_PATH.search(parsed.path or "")
                if path_match:
                    value = int(path_match.group(1))
                    if value <= 1:
                        continue
                else:
                    continue

            ranked.append((weight, value or 0, full))

        ranked.sort(key=lambda item: (item[0], item[1]))
        urls: list[str] = []
        for _, _, full in ranked:
            urls.append(full)
            if len(urls) >= max_pages:
                break
        return urls

    async def _expand_paginated_heuristic_jobs_v88(self, html: str, page_url: str, jobs: list[dict]) -> list[dict]:
        if not jobs or len(jobs) >= MAX_JOBS_PER_PAGE:
            return jobs

        methods = {str(j.get("extraction_method") or "") for j in jobs}
        if not methods or any(m.startswith("ats_") for m in methods):
            return jobs

        allowed_methods = {
            "tier2_heuristic_v16",
            "tier2_links",
            "tier2_heading_rows",
            "tier2_linked_cards_v67",
            "tier2_query_table_rows_v81",
            "tier2_queryid_cards_v89",
        }
        if not methods.issubset(allowed_methods):
            return jobs

        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return jobs

        next_urls = self._collect_listing_pagination_urls_v89(html, page_url, max_pages=4)
        if not next_urls:
            return jobs

        merged = list(jobs)
        async with httpx.AsyncClient(timeout=4.6, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
            for target in next_urls:
                try:
                    resp = await client.get(target)
                except Exception:
                    continue
                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 500:
                    continue

                if "tier2_query_table_rows_v81" in methods:
                    candidate = self._extract_query_table_jobs_v80(body, str(resp.url))
                    candidate = self._finalize_structured_jobs_v81(candidate, body, str(resp.url)) if candidate else []
                elif "tier2_queryid_cards_v89" in methods:
                    candidate = self._extract_queryid_card_rows_v89(body, str(resp.url))
                    candidate = self._finalize_strict_rows_v88(candidate, str(resp.url)) if candidate else []
                else:
                    candidate = self._extract_tier2_v16(str(resp.url), body) or []
                    candidate = self._postprocess_jobs_v73(candidate, body, str(resp.url))
                    candidate = self._clean_jobs_v73(candidate)

                if candidate:
                    merged.extend(candidate)

        deduped = self._dedupe_title_url_location_v84(merged, limit=MAX_JOBS_PER_PAGE)
        if len(deduped) <= len(jobs) + 1:
            return jobs

        if "tier2_query_table_rows_v81" in methods:
            if not self._passes_structured_row_jobset_v81(deduped, page_url):
                return jobs
        elif "tier2_queryid_cards_v89" in methods:
            if not self._passes_queryid_card_jobset_v89(deduped, page_url):
                return jobs
        elif not self._passes_jobset_validation(deduped, page_url):
            return jobs

        return deduped
