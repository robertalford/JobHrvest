"""
Tiered Extraction Engine v8.3 - precision-first fallback arbitration.

Strategy:
1. Run stronger parent/structured extractors first, and keep heading-action as a
   fallback path only (prevents partial/noisy early returns).
2. Add dedicated static-row recovery for Greenhouse embed tables and Elementor
   accordion vacancy blocks.
3. Expand validated row sets via bounded same-host pagination follow-up.
"""

from __future__ import annotations

import re
from urllib.parse import parse_qs, quote_plus, unquote_plus, urljoin, urlparse

import httpx

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v74 import _V73_NAV_TITLE, _V73_NON_JOB_HEADING
from app.crawlers.tiered_extractor_v81 import TieredExtractorV81

_V82_REGION_OPENINGS = re.compile(
    r"^job\s+openings?\s+in\s+[A-Za-z][A-Za-z .&'/-]{2,}$",
    re.IGNORECASE,
)
_V82_GENERIC_OPENINGS = re.compile(r"^(?:all\s+job\s+openings?|job\s+openings?)$", re.IGNORECASE)
_V82_ACTION_TEXT = re.compile(
    r"\b(?:view\s+job|apply(?:\s+now)?|details?|read\s+more|submit|email)\b",
    re.IGNORECASE,
)
_V82_SOCIAL_URL = re.compile(
    r"(?:linkedin\.com|facebook\.com|instagram\.com|twitter\.com|x\.com|youtube\.com|tiktok\.com)",
    re.IGNORECASE,
)
_V82_MAILTO_SUBJECT = re.compile(r"^mailto:[^?]+\?.*\bsubject=", re.IGNORECASE)
_V82_LANG_LABEL = re.compile(
    r"^(?:english|fran[çc]ais|deutsch|espa[ñn]ol|polski|русский|中文|ไทย|日本語|portugu[êe]s)\b",
    re.IGNORECASE,
)
_V82_NOISY_CONTAINER = re.compile(r"(?:\bnav\b|\bmenu\b|footer|header|language|locale|social)", re.IGNORECASE)
_V82_DETAILISH_PATH = re.compile(r"(?:/|-)(?:job|jobs|career|careers|position|vacanc)[a-z0-9-]*/", re.IGNORECASE)
_V83_GENERIC_HEADING_TITLE = re.compile(
    r"^(?:"
    r"new\s+opportunities|"
    r"explore\s+current\s+job\s+openings|"
    r"current\s+job\s+openings|"
    r"don't\s+see\s+your\s+dream\s+role\??|"
    r"don't\s+see\s+your\s+dream\s+job\??|"
    r"search\s+jobs|browse\s+jobs|view\s+all\s+jobs|"
    r"create\s+job\s+alert"
    r")$",
    re.IGNORECASE,
)
_V83_PAGINATION_URL_HINT = re.compile(
    r"(?:[?&](?:page|page_number|pagenumber|startrow|offset)=\d+|/page/\d+(?:/|$))",
    re.IGNORECASE,
)
_V83_GREENHOUSE_HOST = re.compile(r"(?:^|\.)(?:job-boards\.greenhouse\.io|boards\.greenhouse\.io)$", re.IGNORECASE)


class TieredExtractorV83(TieredExtractorV81):
    """v8.3 extractor: super-first arbitration + focused row and pagination recovery."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        # Dedicated static Greenhouse table rows are high-confidence and should
        # beat generic heading/action fallback.
        greenhouse_jobs = self._extract_greenhouse_embed_rows_v83(working_html, page_url)
        if self._passes_structured_row_jobset_v81(greenhouse_jobs, page_url):
            return self._finalize_structured_jobs_v81(greenhouse_jobs, working_html, page_url)

        jobs = await super().extract(career_page, company, html)

        superset = self._recover_parent_superset_v82(working_html, page_url, jobs)
        if superset:
            jobs = self._finalize_structured_jobs_v81(superset, working_html, page_url)

        accordion_jobs = self._extract_elementor_accordion_rows_v83(working_html, page_url)
        if (
            len(accordion_jobs) >= max(4, len(jobs) + 3)
            and self._passes_accordion_jobset_v83(accordion_jobs)
        ):
            return self._finalize_structured_jobs_v81(accordion_jobs, working_html, page_url)

        if len(jobs) >= 3:
            expanded_jobs = await self._expand_paginated_rows_v83(working_html, page_url, jobs)
            if len(expanded_jobs) >= len(jobs) + 2 and self._passes_jobset_validation(expanded_jobs, page_url):
                return self._finalize_structured_jobs_v81(expanded_jobs, working_html, page_url)
            return jobs

        # Run heading/action as a fallback when stronger paths under-deliver.
        heading_jobs = self._extract_heading_action_rows_v82(working_html, page_url)
        if len(heading_jobs) >= 3:
            mailto_jobs = [j for j in heading_jobs if str(j.get("source_url") or "").startswith("mailto:")]
            http_jobs = [j for j in heading_jobs if not str(j.get("source_url") or "").startswith("mailto:")]

            if len(http_jobs) >= 3 and self._passes_heading_action_jobset_v82(http_jobs, page_url):
                http_jobs = await self._expand_page_number_rows_v82(working_html, page_url, http_jobs)
                if self._passes_heading_action_jobset_v82(http_jobs, page_url):
                    return self._finalize_structured_jobs_v81(http_jobs, working_html, page_url)

            if self._passes_mailto_heading_jobset_v82(mailto_jobs):
                return self._finalize_structured_jobs_v81(mailto_jobs, working_html, page_url)

        return jobs

    def _is_valid_title_v60(self, title: str) -> bool:
        if not super()._is_valid_title_v60(title):
            return False
        t = (title or "").strip()
        if _V82_REGION_OPENINGS.match(t):
            return False
        if _V82_GENERIC_OPENINGS.match(t):
            return False
        return not _V83_GENERIC_HEADING_TITLE.match(t)

    def _is_non_job_url(self, src: str) -> bool:
        if not super()._is_non_job_url(src):
            return False

        path = (urlparse(src or "").path or "").lower()
        if "/jobdetail/" in path and re.search(r"/jobdetail/[^/]*account[^/]*", path):
            return False
        if re.search(r"/jobs?/(?:[^/]*account[^/]*(?:-|\\d)[^/]*)/?$", path):
            return False
        return True

    def _extract_greenhouse_embed_rows_v83(self, html: str, page_url: str) -> list[dict]:
        host = (urlparse(page_url).netloc or "").lower()
        if not _V83_GREENHOUSE_HOST.search(host) and "job-posts--table" not in (html or ""):
            return []

        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath("//tr[contains(@class,'job-post') and .//a[@href]]")
        if len(rows) < 3:
            return []

        jobs: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for row in rows[:700]:
            link_nodes = row.xpath(".//a[@href][1]")
            if not link_nodes:
                continue

            href = (link_nodes[0].get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url or self._is_non_job_url(source_url):
                continue

            title_nodes = row.xpath(".//p[contains(@class,'body--medium')]")
            raw_title = _text(title_nodes[0]) if title_nodes else (_text(link_nodes[0]) or "")
            title = self._normalize_title(raw_title or "")
            if not title:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue

            location = None
            for loc_node in row.xpath(".//p[contains(@class,'body__secondary') or contains(@class,'metadata')]")[:2]:
                loc_text = " ".join((_text(loc_node) or "").split()).strip()
                if loc_text and loc_text.lower() != title.lower():
                    location = loc_text[:180]
                    break

            key = (title.lower(), source_url.lower())
            if key in seen:
                continue
            seen.add(key)

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "ats_greenhouse_embed_rows_v83",
                    "extraction_confidence": 0.92,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _extract_elementor_accordion_rows_v83(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        items = root.xpath("//div[contains(@class,'elementor-accordion-item')]")
        if len(items) < 3:
            return []

        jobs: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for idx, item in enumerate(items[:600], start=1):
            title_node = None
            nodes = item.xpath(".//a[contains(@class,'elementor-accordion-title')]|.//h4[contains(@class,'elementor-tab-title')]")
            if nodes:
                title_node = nodes[0]
            if title_node is None:
                continue

            title = self._normalize_title(_text(title_node) or "")
            if not title:
                continue
            if _V83_GENERIC_HEADING_TITLE.match(title):
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue
            if _V73_NAV_TITLE.match(title) or _V73_NON_JOB_HEADING.match(title):
                continue

            source_url = ""
            for a_el in item.xpath(".//a[@href]")[:10]:
                href = (a_el.get("href") or "").strip()
                if not href or href.startswith("#") or href.lower().startswith("javascript:"):
                    continue
                resolved = (_resolve_url(href, page_url) or "").split("#", 1)[0]
                if not resolved:
                    continue
                # Keep extraction self-contained; avoid off-limits outbound domains.
                if "seek.com" in resolved.lower():
                    continue
                if self._is_non_job_url(resolved):
                    continue
                source_url = resolved
                break

            if not source_url:
                anchor_id = (
                    title_node.get("id")
                    or item.get("id")
                    or (item.xpath(".//div[contains(@class,'elementor-tab-content')]/@id") or [None])[0]
                    or f"elementor-accordion-{idx}"
                )
                source_url = f"{page_url.split('#', 1)[0]}#{anchor_id}"

            row_desc = None
            desc_nodes = item.xpath(".//div[contains(@class,'elementor-tab-content')]")
            if desc_nodes:
                desc_text = " ".join((_text(desc_nodes[0]) or "").split()).strip()
                if desc_text and desc_text.lower() != title.lower():
                    row_desc = desc_text[:5000]

            key = (title.lower(), source_url.lower())
            if key in seen:
                continue
            seen.add(key)

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_row_location_v73(item, title),
                    "description": row_desc,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "tier2_elementor_accordion_rows_v83",
                    "extraction_confidence": 0.88,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _passes_accordion_jobset_v83(self, jobs: list[dict]) -> bool:
        if len(jobs) < 4:
            return False
        titles = [self._normalize_title(str(j.get("title") or "")) for j in jobs]
        titles = [t for t in titles if t]
        if len(titles) < 4:
            return False
        unique_ratio = len({t.lower() for t in titles}) / max(1, len(titles))
        if unique_ratio < 0.6:
            return False
        valid = sum(1 for t in titles if self._is_valid_title_v60(t) or self._is_reasonable_structured_title_v81(t))
        return valid >= max(4, int(len(titles) * 0.75))

    def _extract_heading_action_rows_v82(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        jobs: list[dict] = []
        seen: set[tuple[str, str]] = set()
        headings = root.xpath("//h2|//h3|//h4|//h5")
        for heading in headings[:1200]:
            title = self._normalize_title(_text(heading) or "")
            if not title:
                continue
            if _V83_GENERIC_HEADING_TITLE.match(title):
                continue
            if _V73_NAV_TITLE.match(title) or _V73_NON_JOB_HEADING.match(title):
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue

            row = self._find_heading_action_row_v82(heading)
            if row is None:
                continue

            source_url = self._pick_heading_action_url_v82(row, page_url, title)
            if not source_url:
                continue
            source_url = source_url.split("#", 1)[0]

            if source_url.startswith("mailto:"):
                if not _V82_MAILTO_SUBJECT.search(source_url):
                    continue
            else:
                if self._is_non_job_url(source_url):
                    continue
                has_strong = self._has_strong_card_detail_url_v73(source_url, page_url) or self._is_job_like_url(source_url)
                if self._is_obvious_non_job_card_v73(title, source_url, page_url, has_strong):
                    continue

            key = (title.lower(), source_url.lower())
            if key in seen:
                continue
            seen.add(key)

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_row_location_v73(row, title),
                    "description": self._extract_row_description_v73(row, title),
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "tier2_heading_action_rows_v83",
                    "extraction_confidence": 0.9,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _find_heading_action_row_v82(self, heading):
        current = heading
        for _ in range(6):
            current = current.getparent()
            if current is None or not isinstance(current.tag, str):
                return None
            cls = (current.get("class") or "") + " " + (current.get("id") or "")
            if _V82_NOISY_CONTAINER.search(cls):
                continue
            links = current.xpath(".//a[@href]")
            if not links:
                continue
            row_text = " ".join((_text(current) or "").split())
            if len(row_text) < 24 or len(row_text) > 3500:
                continue
            if len(links) > 14:
                continue
            return current
        return None

    def _pick_heading_action_url_v82(self, row, page_url: str, title: str) -> str:
        best_url = ""
        best_score = -10
        for a_el in row.xpath(".//a[@href]")[:20]:
            href = (a_el.get("href") or "").strip()
            if not href:
                continue

            text = " ".join((_text(a_el) or a_el.get("title") or a_el.get("aria-label") or "").split()).strip()
            text_low = text.lower()

            if href.lower().startswith("mailto:"):
                score = 2
                if _V82_MAILTO_SUBJECT.search(href):
                    score += 3
                    subj = unquote_plus(href.split("subject=", 1)[1].split("&", 1)[0]) if "subject=" in href else ""
                    if title and title.lower().split()[0] in subj.lower():
                        score += 1
            else:
                source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
                if not source_url:
                    continue
                if _V82_SOCIAL_URL.search(source_url):
                    continue
                if self._is_non_job_url(source_url):
                    continue

                score = 0
                if self._has_strong_card_detail_url_v73(source_url, page_url):
                    score += 4
                elif self._is_job_like_url(source_url):
                    score += 2
                elif _V82_DETAILISH_PATH.search(urlparse(source_url).path or ""):
                    score += 2
                else:
                    score -= 2

                keys = {k.lower() for k in parse_qs(urlparse(source_url).query or "", keep_blank_values=True)}
                if "locale" in keys and len(keys) <= 2:
                    score -= 2

                href = source_url

            if text and _V82_ACTION_TEXT.search(text_low):
                score += 2
            if text and _V82_LANG_LABEL.search(text_low):
                score -= 3
            if _V82_REGION_OPENINGS.match(text):
                score -= 3

            if score > best_score:
                best_score = score
                best_url = href

        return best_url if best_score >= 2 else ""

    async def _expand_page_number_rows_v82(self, html: str, page_url: str, seed_jobs: list[dict]) -> list[dict]:
        if "page_number=" not in (html or "").lower():
            return self._dedupe_basic_v66(seed_jobs)
        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return self._dedupe_basic_v66(seed_jobs)

        queue = self._page_number_urls_v82(html, page_url)
        if not queue:
            return self._dedupe_basic_v66(seed_jobs)

        merged = list(seed_jobs)
        seen_pages = {page_url.rstrip("/")}
        async with httpx.AsyncClient(timeout=4.0, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
            fetch_count = 0
            while queue and fetch_count < 2:
                target = queue.pop(0)
                norm = target.rstrip("/")
                if norm in seen_pages:
                    continue
                seen_pages.add(norm)
                fetch_count += 1
                try:
                    resp = await client.get(target)
                except Exception:
                    continue
                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 400:
                    continue
                page_jobs = [
                    j
                    for j in self._extract_heading_action_rows_v82(body, str(resp.url))
                    if not str(j.get("source_url") or "").startswith("mailto:")
                ]
                merged.extend(page_jobs)
        return self._dedupe_basic_v66(merged)

    async def _expand_paginated_rows_v83(self, html: str, page_url: str, seed_jobs: list[dict]) -> list[dict]:
        if len(seed_jobs) < 3:
            return self._dedupe_basic_v66(seed_jobs)

        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return self._dedupe_basic_v66(seed_jobs)

        queue = self._collect_pagination_urls_v83(html, page_url)
        if not queue:
            return self._dedupe_basic_v66(seed_jobs)

        seed_method = str(seed_jobs[0].get("extraction_method") or "")
        merged = list(seed_jobs)
        seen_pages = {page_url.rstrip("/")}

        async with httpx.AsyncClient(timeout=4.0, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
            fetch_count = 0
            while queue and fetch_count < 4:
                target = queue.pop(0)
                norm = target.rstrip("/")
                if norm in seen_pages:
                    continue
                seen_pages.add(norm)
                fetch_count += 1
                try:
                    resp = await client.get(target)
                except Exception:
                    continue
                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 500:
                    continue
                page_jobs = self._extract_pagination_page_jobs_v83(body, str(resp.url), seed_method)
                merged.extend(page_jobs)
        return self._dedupe_basic_v66(merged)

    def _collect_pagination_urls_v83(self, html: str, page_url: str) -> list[str]:
        root = _parse_html(html)
        if root is None:
            return []

        page_host = (urlparse(page_url).netloc or "").lower()
        urls: list[str] = []
        seen: set[str] = set()
        nodes = root.xpath(
            "//ul[contains(@class,'pagination')]//a[@href]"
            "|//div[contains(@class,'pagination')]//a[@href]"
            "|//a[contains(@href,'startrow=') or contains(@href,'page_number=') or contains(@href,'page=') or contains(@href,'pagenumber=') or contains(@href,'offset=')]"
        )

        for node in nodes[:120]:
            href = (node.get("href") or "").strip()
            if not href:
                continue
            full = urljoin(page_url, href).split("#", 1)[0]
            parsed = urlparse(full)
            if (parsed.netloc or "").lower() != page_host:
                continue
            if full.rstrip("/") == page_url.rstrip("/"):
                continue

            lower = full.lower()
            if "locale=" in lower and not _V83_PAGINATION_URL_HINT.search(lower):
                continue
            if not _V83_PAGINATION_URL_HINT.search(lower):
                continue
            if full in seen:
                continue
            seen.add(full)
            urls.append(full)
            if len(urls) >= 6:
                break
        return urls

    def _extract_pagination_page_jobs_v83(self, html: str, page_url: str, seed_method: str) -> list[dict]:
        method = (seed_method or "").lower()

        if "greenhouse" in method:
            jobs = self._extract_greenhouse_embed_rows_v83(html, page_url)
            if self._passes_structured_row_jobset_v81(jobs, page_url):
                return jobs

        if "heading_action" in method:
            jobs = [j for j in self._extract_heading_action_rows_v82(html, page_url) if not str(j.get("source_url") or "").startswith("mailto:")]
            if self._passes_heading_action_jobset_v82(jobs, page_url):
                return jobs

        if "accordion" in method:
            jobs = self._extract_elementor_accordion_rows_v83(html, page_url)
            if self._passes_accordion_jobset_v83(jobs):
                return jobs

        tier2_jobs = self._extract_tier2_v16(page_url, html) or []
        if len(tier2_jobs) >= 3 and self._passes_jobset_validation(tier2_jobs, page_url):
            return tier2_jobs

        table_jobs = self._extract_query_table_jobs_v80(html, page_url)
        if self._passes_structured_row_jobset_v81(table_jobs, page_url):
            return table_jobs

        gh_jobs = self._extract_greenhouse_embed_rows_v83(html, page_url)
        if self._passes_structured_row_jobset_v81(gh_jobs, page_url):
            return gh_jobs

        return []

    def _page_number_urls_v82(self, html: str, page_url: str) -> list[str]:
        root = _parse_html(html)
        if root is None:
            return []
        page_host = (urlparse(page_url).netloc or "").lower()
        urls: list[str] = []
        seen: set[str] = set()
        for node in root.xpath("//ul[contains(@class,'pagination')]//a[@href]|//a[contains(@href,'page_number=') and @href]")[:40]:
            href = (node.get("href") or "").strip()
            if not href:
                continue
            full = urljoin(page_url, href)
            parsed = urlparse(full)
            if (parsed.netloc or "").lower() != page_host:
                continue
            if "page_number=" not in full:
                continue
            if full.rstrip("/") == page_url.rstrip("/") or full in seen:
                continue
            seen.add(full)
            urls.append(full)
            if len(urls) >= 3:
                break
        return urls

    def _passes_mailto_heading_jobset_v82(self, jobs: list[dict]) -> bool:
        if len(jobs) < 3:
            return False
        valid_titles = [str(j.get("title") or "").strip() for j in jobs if str(j.get("source_url") or "").startswith("mailto:")]
        if len(valid_titles) < 3:
            return False
        unique_ratio = len({t.lower() for t in valid_titles}) / max(1, len(valid_titles))
        return unique_ratio >= 0.6

    def _passes_heading_action_jobset_v82(self, jobs: list[dict], page_url: str) -> bool:
        if len(jobs) < 3:
            return False

        valid = 0
        url_hits = 0
        titles: list[str] = []
        for job in jobs:
            title = self._normalize_title(str(job.get("title") or ""))
            source_url = str(job.get("source_url") or "")
            if not title or not source_url:
                continue
            if not (self._is_valid_title_v60(title) or self._is_reasonable_structured_title_v81(title)):
                continue
            if _V73_NAV_TITLE.match(title) or _V73_NON_JOB_HEADING.match(title):
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, True):
                continue

            valid += 1
            titles.append(title)
            if (
                self._has_strong_card_detail_url_v73(source_url, page_url)
                or self._is_job_like_url(source_url)
                or _V82_DETAILISH_PATH.search(urlparse(source_url).path or "")
            ):
                url_hits += 1

        if valid < 3:
            return False
        unique_ratio = len({t.lower() for t in titles}) / max(1, len(titles))
        if unique_ratio < 0.6:
            return False
        return url_hits >= max(2, int(valid * 0.5))

    def _recover_parent_superset_v82(self, html: str, page_url: str, current_jobs: list[dict]) -> list[dict] | None:
        parent_jobs = self._extract_tier2_v16(page_url, html) or []
        if len(parent_jobs) < max(3, len(current_jobs) + 3):
            return None

        filtered: list[dict] = []
        for job in parent_jobs:
            title = self._normalize_title(str(job.get("title") or ""))
            source_url = str(job.get("source_url") or "").split("#", 1)[0]
            if not title or not source_url:
                continue
            if not self._is_valid_title_v60(title):
                continue
            has_strong = self._has_strong_card_detail_url_v73(source_url, page_url) or self._is_job_like_url(source_url)
            if not has_strong:
                continue
            if self._is_obvious_non_job_card_v73(title, source_url, page_url, has_strong):
                continue
            filtered.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": job.get("location_raw"),
                    "description": job.get("description"),
                    "salary_raw": job.get("salary_raw"),
                    "employment_type": job.get("employment_type"),
                    "extraction_method": "tier2_heuristic_v16",
                    "extraction_confidence": 0.86,
                }
            )

        filtered = self._dedupe_basic_v66(filtered)
        if len(filtered) < max(3, len(current_jobs) + 3):
            return None
        if not self._passes_jobset_validation(filtered, page_url):
            return None
        return filtered

    def _jobs2web_endpoint_candidates_v66(self, page_url: str, cfg: dict[str, str | None]) -> list[str]:
        candidates = list(super()._jobs2web_endpoint_candidates_v66(page_url, cfg))

        parsed = urlparse(page_url or "")
        base = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""
        lower_path = (parsed.path or "").lower()
        locale = quote_plus((cfg.get("locale") or "en_US").strip())

        if base and "/search" in lower_path:
            query = parse_qs(parsed.query or "", keep_blank_values=True)
            q = quote_plus((query.get("q") or [""])[0])
            loc = quote_plus((query.get("locationsearch") or [""])[0])

            candidates.extend(
                [
                    f"{base}/search/?q={q}&locationsearch={loc}&searchResultView=LIST&pageNumber=0&markerViewed=&carouselIndex=&facetFilters=%7B%7D&sortBy=date",
                    f"{base}/search/?q={q}&locationsearch={loc}&searchResultView=LIST&pageNumber=0&facetFilters=%7B%7D&sortBy=date&locale={locale}",
                    f"{base}/search/?q={q}&locationsearch={loc}&searchResultView=LIST",
                    f"{base}/search/?q={q}&locationsearch={loc}",
                ]
            )

        page_host = (parsed.netloc or "").lower()

        def _score(endpoint: str) -> int:
            p = urlparse(endpoint)
            host = (p.netloc or "").lower()
            path = (p.path or "").lower()
            low = endpoint.lower()

            score = 0
            if host == page_host and "/search/" in path:
                score += 150
            if "searchresultview=list" in low:
                score += 80
            if "pagenumber=" in low:
                score += 20
            if "facetfilters=" in low:
                score += 12
            if "locationsearch=" in low:
                score += 10
            if "locale=" in low:
                score += 6
            if "/career/jobsearch" in path:
                score += 40
            return score

        ranked = sorted(enumerate(candidates), key=lambda pair: (-_score(pair[1]), pair[0]))
        ordered: list[str] = []
        seen: set[str] = set()
        for _, endpoint in ranked:
            if not endpoint or endpoint in seen:
                continue
            seen.add(endpoint)
            ordered.append(endpoint)
        return ordered
