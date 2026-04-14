"""
Tiered Extraction Engine v1.3 — extends v1.2 with:

1. <article> tag support in container scoring
2. Expanded job keywords: work, employ, recruit, role, opport, talent
3. Anchor-as-row detection (container of <a> children with short text = job list)
4. Elementor site handling (prefer containers linking to /career/, /job/, /apply/)
5. Tier 2 confidence calibration (lower confidence when job count << children count)
6. Playwright timeout 20s → 30s

Based on analysis of 14,696 clean (OK-status) test data wrappers where 85% are
custom (non-ATS) sites — Tier 2 heuristic must handle the vast majority.
"""

import logging
import re
from typing import Optional
from urllib.parse import urljoin

from lxml import etree

from app.crawlers.tiered_extractor import (
    TieredExtractor,
    ATS_TEMPLATES,
    _parse_html,
    _text,
    _href,
    _resolve_url,
    _get_el_classes,
    _child_signature,
    _is_valid_title,
    _detect_spa,
    _JOB_URL_PATTERN,
    _TITLE_CLASS_PATTERN,
    _LOCATION_CLASS_PATTERN,
    _SALARY_CLASS_PATTERN,
    _TYPE_CLASS_PATTERN,
    _AU_LOCATIONS,
    _SALARY_PATTERN,
    _JOB_TYPE_PATTERN,
    MAX_JOBS_PER_PAGE,
    MIN_JOBS_FOR_SUCCESS,
)
from app.crawlers.tiered_extractor_v12 import (
    TieredExtractorV12,
    _NEW_ATS_TEMPLATES,
    _NEW_URL_PATTERNS,
    _ROW_CLASS_PATTERN_V12,
    _TITLE_CLASS_PATTERN_V12,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# v1.3: Further expanded patterns
# ---------------------------------------------------------------------------

# Change 2: Expanded job class keywords — added work, employ, recruit, role, opport, talent
_JOB_CLASS_PATTERN_V13 = re.compile(
    r"job|career|vacanc|position|listing|posting|opening|"
    r"list-data|accordion|opportunities|openings|results|"
    r"search-result|content-wrap|vacancies|recruitment|"
    r"work|employ|recruit|role|opport|talent",
    re.IGNORECASE,
)

# Change 2: Expanded row class keywords
_ROW_CLASS_PATTERN_V13 = re.compile(
    r"job|career|vacanc|position|listing|posting|opening|"
    r"media|panel|entry|record|data-row|item|card|tile|block|"
    r"search-result|accordion_in|list-group-item|"
    r"workRow|work-item|work-row|employ|role|opport",
    re.IGNORECASE,
)


class TieredExtractorV13(TieredExtractorV12):
    """v1.3 — extends v1.2 with article tags, expanded keywords, anchor-rows, Elementor."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # v1.2: SPA detection + Playwright rendering (with v1.3 longer timeout)
        is_spa = _detect_spa(html)
        if is_spa:
            rendered = await self._render_with_playwright_v13(url)
            if rendered and len(rendered) > len(html):
                logger.info("v1.3 Playwright rendered %s (%d → %d bytes)", url, len(html), len(rendered))
                html = rendered
            else:
                logger.info("v1.3 SPA detected but Playwright unavailable for %s", url)

        # Tier 1: ATS templates (v1.2 extended set)
        tier1 = self._extract_tier1_v12(url, html)
        if tier1 and len(tier1) >= MIN_JOBS_FOR_SUCCESS:
            return tier1[:MAX_JOBS_PER_PAGE]

        # Tier 2: v1.3 expanded heuristic
        tier2 = self._extract_tier2_v13(url, html)
        if tier2 and len(tier2) >= MIN_JOBS_FOR_SUCCESS:
            return tier2[:MAX_JOBS_PER_PAGE]

        # Tier 3: LLM (still deferred)
        tier3 = self._extract_tier3_llm(url, html)
        if tier3 and len(tier3) >= MIN_JOBS_FOR_SUCCESS:
            return tier3[:MAX_JOBS_PER_PAGE]

        for partial in (tier1, tier2):
            if partial:
                return partial[:MAX_JOBS_PER_PAGE]

        return []

    # ------------------------------------------------------------------
    # Change 6: Playwright with longer timeout (30s)
    # ------------------------------------------------------------------

    async def _render_with_playwright_v13(self, url: str) -> Optional[str]:
        """Render with Playwright — 30s timeout (up from 20s in v1.2)."""
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                try:
                    await page.goto(url, wait_until="networkidle", timeout=30000)
                    await page.wait_for_timeout(2000)
                    return await page.content()
                except Exception as e:
                    logger.debug("v1.3 Playwright failed for %s: %s", url, e)
                    return None
                finally:
                    await browser.close()
        except ImportError:
            return None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Tier 2 v1.3: All improvements combined
    # ------------------------------------------------------------------

    def _extract_tier2_v13(self, url: str, html: str) -> Optional[list[dict]]:
        """Tier 2 v1.3 with article tags, expanded keywords, anchor-rows, Elementor."""
        root = _parse_html(html)
        if root is None:
            return None

        is_elementor = "elementor" in html[:5000].lower()

        candidates = self._score_containers_v13(root, url, is_elementor)
        if not candidates:
            return None

        candidates.sort(key=lambda c: c[1], reverse=True)

        for container_el, score in candidates[:3]:
            jobs = self._extract_jobs_v13(container_el, url, score)
            if jobs and len(jobs) >= MIN_JOBS_FOR_SUCCESS:
                return jobs

        return None

    def _score_containers_v13(
        self,
        root: etree._Element,
        page_url: str,
        is_elementor: bool,
    ) -> list[tuple[etree._Element, int]]:
        """Container scoring with all v1.3 improvements."""
        candidates: list[tuple[etree._Element, int]] = []

        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag in ("script", "style", "head", "meta", "link", "noscript", "svg"):
                continue

            score = 0
            classes = _get_el_classes(el)
            el_id = (el.get("id") or "").lower()
            combined = f"{classes} {el_id}"

            # v1.3: Expanded job keyword scoring (+10)
            if _JOB_CLASS_PATTERN_V13.search(combined):
                score += 10

            children = [c for c in el if isinstance(c.tag, str) and c.tag.lower()
                        not in ("script", "style", "br", "hr")]

            if len(children) < 2:
                continue

            # Change 1: <article> tag bonus (+4) — semantic repeated content
            if tag == "article":
                score += 4
            # Container of <article> children gets bonus
            article_children = sum(1 for c in children if isinstance(c.tag, str) and c.tag.lower() == "article")
            if article_children >= 3:
                score += 6

            # Repeating structure (+5)
            if tag in ("ul", "ol", "table", "div", "section", "main", "tbody",
                       "article", "nav"):  # Change 1: article added
                if len(children) > 3:
                    sigs = [_child_signature(c) for c in children]
                    if sigs:
                        most_common = max(sigs.count(s) for s in set(sigs))
                        if most_common >= 3:
                            score += 5

            # Job URL patterns in children (+3)
            job_url_count = 0
            for child in children:
                for a_tag in child.iter("a"):
                    href = a_tag.get("href", "")
                    if _JOB_URL_PATTERN.search(href):
                        job_url_count += 1
                        break
            if job_url_count >= 3:
                score += 3

            # Structural similarity (+3)
            if len(children) > 3:
                sigs = [_child_signature(c) for c in children]
                if sigs:
                    most_common = max(sigs.count(s) for s in set(sigs))
                    if most_common > 3:
                        score += 3

            # v1.2: Link density scoring (+8)
            short_link_count = 0
            for child in children:
                for a_tag in child.iter("a"):
                    a_text = _text(a_tag)
                    if 10 <= len(a_text) <= 100 and _is_valid_title(a_text):
                        short_link_count += 1
                        break
            if short_link_count >= 5:
                score += 8
            elif short_link_count >= 3:
                score += 4

            # v1.2: Bootstrap layout (+6)
            if "row" in classes and tag == "div":
                col_children = [c for c in children if "col-" in _get_el_classes(c)]
                if len(col_children) >= 3:
                    cols_with_links = sum(
                        1 for c in col_children
                        if any(a.get("href") for a in c.iter("a"))
                    )
                    if cols_with_links >= 3:
                        score += 6

            # v1.2: Row class keyword bonus (+3)
            children_with_class = sum(
                1 for c in children
                if _ROW_CLASS_PATTERN_V13.search(_get_el_classes(c))
            )
            if children_with_class >= 3:
                score += 3

            # ── Change 3: Anchor-as-row detection (+10) ──
            # If element has many direct <a> children with short text, it's a job link list
            if tag in ("div", "ul", "ol", "nav", "section", "main"):
                a_children = [c for c in children
                              if isinstance(c.tag, str) and c.tag.lower() == "a"]
                if len(a_children) >= 3:
                    valid_link_children = sum(
                        1 for a in a_children
                        if _is_valid_title(_text(a)) and a.get("href")
                    )
                    if valid_link_children >= 3:
                        score += 10

            # ── Change 4: Elementor site handling ──
            if is_elementor and score >= 5:
                # Boost containers whose child links point to career/job URLs
                career_link_count = 0
                for child in children:
                    for a_tag in child.iter("a"):
                        href = (a_tag.get("href") or "").lower()
                        if any(kw in href for kw in ("/career", "/job", "/apply", "/position", "/vacanc")):
                            career_link_count += 1
                            break
                if career_link_count >= 3:
                    score += 5  # Boost elementor containers with career links

            if score >= 5:
                candidates.append((el, score))

        # Table heuristic (from v1.1)
        for table_el in root.iter("table"):
            if any(table_el is c for c, _ in candidates):
                continue
            tbody = table_el.find("tbody")
            target = tbody if tbody is not None else table_el
            trs = [c for c in target if isinstance(c.tag, str) and c.tag.lower() == "tr"]
            if len(trs) < 3:
                continue
            job_like_rows = 0
            for tr in trs:
                tr_text = _text(tr)
                anchors = [a for a in tr.iter("a") if a.get("href")]
                if 10 < len(tr_text) < 300 and anchors:
                    job_like_rows += 1
            if job_like_rows >= 3:
                s = 4 + min(job_like_rows, 10)
                candidates.append((target, s))

        return candidates

    def _extract_jobs_v13(
        self, container: etree._Element, base_url: str, container_score: int
    ) -> list[dict]:
        """Extract jobs with v1.3 improvements — including anchor-as-row and calibration."""
        children = [
            c for c in container
            if isinstance(c.tag, str)
            and c.tag.lower() not in ("script", "style", "br", "hr", "thead")
        ]
        if not children:
            return []

        # ── Change 3: Detect anchor-as-row pattern ──
        # If most children are <a> tags with valid titles, treat each <a> as a job row
        a_children = [c for c in children if isinstance(c.tag, str) and c.tag.lower() == "a"]
        if len(a_children) >= 3 and len(a_children) >= len(children) * 0.5:
            jobs = []
            for a_el in a_children:
                title = _text(a_el)
                if not _is_valid_title(title):
                    continue
                href = a_el.get("href")
                source_url = _resolve_url(href, base_url) or base_url

                # Try to extract location from within the anchor
                location = None
                for sub in a_el.iter():
                    if not isinstance(sub.tag, str):
                        continue
                    cls = _get_el_classes(sub)
                    if _LOCATION_CLASS_PATTERN.search(cls):
                        loc = _text(sub)
                        if loc and 2 < len(loc) < 200 and loc != title:
                            location = loc
                            break

                jobs.append({
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": None,
                    "extraction_method": "tier2_heuristic_v13",
                    "extraction_confidence": min(0.5 + container_score * 0.02, 0.85),
                })
                if len(jobs) >= MAX_JOBS_PER_PAGE:
                    break
            if len(jobs) >= MIN_JOBS_FOR_SUCCESS:
                return jobs

        # Standard repeating-block extraction (from parent)
        sigs = [_child_signature(c) for c in children]
        sig_counts: dict[str, int] = {}
        for s in sigs:
            sig_counts[s] = sig_counts.get(s, 0) + 1

        best_sig = max(sig_counts, key=sig_counts.get) if sig_counts else ""
        rows = [c for c, s in zip(children, sigs) if s == best_sig]

        if len(rows) < MIN_JOBS_FOR_SUCCESS:
            rows = children

        jobs: list[dict] = []
        for row in rows:
            job = self._extract_heuristic_job_v13(row, base_url, container_score)
            if job:
                jobs.append(job)
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        # ── Change 5: Confidence calibration ──
        # If we extracted much fewer jobs than rows exist, lower confidence
        if jobs and len(rows) > 0:
            extraction_ratio = len(jobs) / len(rows)
            if extraction_ratio < 0.3:
                for j in jobs:
                    j["extraction_confidence"] = max(0.3, j["extraction_confidence"] - 0.15)

        return jobs

    def _extract_heuristic_job_v13(
        self, row: etree._Element, base_url: str, container_score: int
    ) -> Optional[dict]:
        """Extract a single job — same as parent but tagged v1.3."""
        job = self._extract_heuristic_job(row, base_url, container_score)
        if job:
            job["extraction_method"] = "tier2_heuristic_v13"
        return job
