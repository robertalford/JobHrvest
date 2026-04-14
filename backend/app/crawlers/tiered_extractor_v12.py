"""
Tiered Extraction Engine v1.2 — extends v1.1 with:

1. Playwright JS rendering for SPA shells (biggest win: ~17 sites)
2. Expanded Tier 2 keyword vocabulary (container + row + title keywords)
3. Link density scoring (catches pages that are just lists of job links)
4. Bootstrap layout detection (div.row > div.col-* with anchors)
5. New Tier 1 platform templates (careers-page.com, expr3ss.com, gupy.io, etc.)
"""

import logging
import re
from typing import Optional

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
    _JOB_CLASS_PATTERN,
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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# v1.2: Extended keyword patterns
# ---------------------------------------------------------------------------

_JOB_CLASS_PATTERN_V12 = re.compile(
    r"job|career|vacanc|position|listing|posting|opening|"
    r"list-data|accordion|opportunities|openings|results|"
    r"search-result|content-wrap|vacancies|recruitment",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN_V12 = re.compile(
    r"job|career|vacanc|position|listing|posting|opening|"
    r"media|panel|entry|record|data-row|item|card|tile|block|"
    r"search-result|accordion_in|list-group-item",
    re.IGNORECASE,
)

_TITLE_CLASS_PATTERN_V12 = re.compile(
    r"title|heading|name|item-heading|post-title|link-text|job-title",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# v1.2: New Tier 1 platform templates
# ---------------------------------------------------------------------------

_NEW_ATS_TEMPLATES: dict[str, dict] = {
    "careers_page": {
        "requires_js": True,
        "url_pattern": "careers-page.com",
        "container": "ul.list-group",
        "boundary": "li.media",
        "title": "h5, .media-body h5 a",
        "link": ".media-body a",
        "location": ".media-body small, .text-muted",
    },
    "expr3ss": {
        "requires_js": False,
        "url_pattern": "expr3ss.com",
        "container": "table, tbody",
        "boundary": "tbody tr",
        "title": "td a, td:first-child a",
        "link": "td a",
        "location": "td:nth-child(2)",
    },
    "gupy": {
        "requires_js": True,
        "url_pattern": "gupy.io",
        "container": "div.job-list, div[class*='job']",
        "boundary": "div[class*='job-card'], li[class*='job']",
        "title": "a h3, a h4, a span",
        "link": "a",
        "location": "[class*='location'], span",
    },
    "livevacancies": {
        "requires_js": True,
        "url_pattern": "livevacancies.co.uk",
        "boundary_xpath": "//a[@class='jk--link--text']",
        "title_xpath": ".//span[contains(@class, 'jk--link--text')]",
        "link": None,  # boundary <a> IS the link
    },
    "bigredsky": {
        "requires_js": True,
        "url_pattern": "bigredsky.com",
        "container": "table.Report, table",
        "boundary": "tbody tr",
        "title": "td a, td:first-child",
        "link": "td a",
        "location": "td:nth-child(2)",
    },
    "darwinbox": {
        "requires_js": True,
        "url_pattern": "darwinbox.in",
        "container": "table, div.job-list",
        "boundary": "tr.hover-icon-wrapper, div.job-card",
        "title": "td a, a.job-title",
        "link": "td a, a.job-title",
        "location": "td:nth-child(2), .location",
    },
    "deputy": {
        "requires_js": True,
        "url_pattern": "deputy.com/jobs",
        "container": "div.__jobs",
        "boundary": "div.__jobs-job",
        "title": "div.__jobs-job-data div.__jobs-info div.__jobs-title",
        "link": "a",
        "location": "div.__jobs-location",
    },
    "webitrent": {
        "requires_js": False,
        "url_pattern": "webitrent.com",
        "container": "table",
        "boundary": "tbody tr, table tr",
        "title": "td a, td:first-child a",
        "link": "td a",
        "location": "td:nth-child(3)",
    },
}

# Merge with existing templates
_ALL_ATS_TEMPLATES = {**ATS_TEMPLATES, **_NEW_ATS_TEMPLATES}

# URL patterns for new templates
_NEW_URL_PATTERNS = [
    (platform, cfg["url_pattern"])
    for platform, cfg in _NEW_ATS_TEMPLATES.items()
    if "url_pattern" in cfg
]


# ---------------------------------------------------------------------------
# v1.2: TieredExtractor subclass
# ---------------------------------------------------------------------------

class TieredExtractorV12(TieredExtractor):
    """v1.2 — extends v1.1 with Playwright, expanded keywords, link density, Bootstrap."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # v1.2: If SPA shell detected, try Playwright rendering first
        is_spa = _detect_spa(html)
        if is_spa:
            rendered = await self._render_with_playwright(url)
            if rendered and len(rendered) > len(html):
                logger.info("v1.2 Playwright rendered %s (%d → %d bytes)", url, len(html), len(rendered))
                html = rendered
            else:
                logger.info("v1.2 SPA detected but Playwright unavailable for %s", url)

        # Tier 1: ATS templates (extended with new platforms)
        tier1 = self._extract_tier1_v12(url, html)
        if tier1 and len(tier1) >= MIN_JOBS_FOR_SUCCESS:
            return tier1[:MAX_JOBS_PER_PAGE]

        # Tier 2: Expanded heuristic
        tier2 = self._extract_tier2_v12(url, html)
        if tier2 and len(tier2) >= MIN_JOBS_FOR_SUCCESS:
            return tier2[:MAX_JOBS_PER_PAGE]

        # Tier 3: LLM (still deferred)
        tier3 = self._extract_tier3_llm(url, html)
        if tier3 and len(tier3) >= MIN_JOBS_FOR_SUCCESS:
            return tier3[:MAX_JOBS_PER_PAGE]

        # Return partial results if any
        for partial in (tier1, tier2):
            if partial:
                return partial[:MAX_JOBS_PER_PAGE]

        return []

    # ------------------------------------------------------------------
    # Playwright rendering
    # ------------------------------------------------------------------

    async def _render_with_playwright(self, url: str) -> Optional[str]:
        """Render a page with Playwright headless Chrome. Returns HTML or None."""
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                try:
                    await page.goto(url, wait_until="networkidle", timeout=20000)
                    await page.wait_for_timeout(2000)  # extra settle time for dynamic content
                    html = await page.content()
                    return html
                except Exception as e:
                    logger.debug("Playwright render failed for %s: %s", url, e)
                    return None
                finally:
                    await browser.close()
        except ImportError:
            logger.debug("Playwright not installed — skipping JS rendering")
            return None
        except Exception as e:
            logger.debug("Playwright error for %s: %s", url, e)
            return None

    # ------------------------------------------------------------------
    # Tier 1 v1.2: Extended ATS detection
    # ------------------------------------------------------------------

    def _extract_tier1_v12(self, url: str, html: str) -> Optional[list[dict]]:
        """Tier 1 with additional platform templates."""
        # First try existing templates via parent
        ats = self._detect_ats(url)
        if ats and ats in ATS_TEMPLATES:
            result = self._extract_tier1_ats(url, html)
            if result:
                return result

        # Try new v1.2 platform templates
        url_lower = url.lower()
        for platform, url_pat in _NEW_URL_PATTERNS:
            if url_pat in url_lower:
                template = _NEW_ATS_TEMPLATES[platform]
                result = self._apply_ats_template(url, html, template, f"tier1_ats_{platform}")
                if result and len(result) >= MIN_JOBS_FOR_SUCCESS:
                    logger.info("v1.2 Tier 1 (%s): %d jobs from %s", platform, len(result), url)
                    return result

        return None

    def _apply_ats_template(self, url: str, html: str, template: dict, method_name: str) -> list[dict]:
        """Apply an ATS template dict to HTML. Generic implementation."""
        root = _parse_html(html)
        if root is None:
            return []

        # Find boundary elements
        rows = []
        if "boundary_xpath" in template:
            try:
                rows = root.xpath(template["boundary_xpath"])
            except Exception:
                pass
        if not rows and "boundary" in template:
            for sel in template["boundary"].split(","):
                sel = sel.strip()
                if sel:
                    try:
                        rows = root.cssselect(sel)
                        if rows:
                            break
                    except Exception:
                        continue

        if len(rows) < MIN_JOBS_FOR_SUCCESS:
            return []

        jobs = []
        for row in rows[:MAX_JOBS_PER_PAGE]:
            title = None
            link_href = None

            # Title
            for key in ("title_xpath", "title"):
                if key not in template:
                    continue
                for sel in template[key].split(","):
                    sel = sel.strip()
                    try:
                        els = row.xpath(sel) if "xpath" in key else row.cssselect(sel)
                        if els:
                            title = _text(els[0])
                            link_href = _href(els[0])
                            break
                    except Exception:
                        continue
                if title:
                    break

            if not _is_valid_title(title or ""):
                continue

            # Link
            if not link_href:
                for key in ("link_xpath", "link"):
                    if key not in template or template[key] is None:
                        continue
                    for sel in template[key].split(","):
                        sel = sel.strip()
                        try:
                            els = row.xpath(sel) if "xpath" in key else row.cssselect(sel)
                            if els:
                                link_href = _href(els[0])
                                break
                        except Exception:
                            continue
                    if link_href:
                        break
            if not link_href:
                link_href = _href(row)  # boundary element itself might be a link

            # Location
            location = None
            for key in ("location_xpath", "location"):
                if key not in template:
                    continue
                for sel in template[key].split(","):
                    sel = sel.strip()
                    try:
                        els = row.xpath(sel) if "xpath" in key else row.cssselect(sel)
                        if els:
                            t = _text(els[0])
                            if t and 2 < len(t) < 200:
                                location = t
                                break
                    except Exception:
                        continue
                if location:
                    break

            jobs.append({
                "title": title,
                "source_url": _resolve_url(link_href, url) or url,
                "location_raw": location,
                "salary_raw": None,
                "employment_type": None,
                "description": None,
                "extraction_method": method_name,
                "extraction_confidence": 0.90,
            })

        return jobs

    # ------------------------------------------------------------------
    # Tier 2 v1.2: Expanded heuristic with link density + Bootstrap
    # ------------------------------------------------------------------

    def _extract_tier2_v12(self, url: str, html: str) -> Optional[list[dict]]:
        """Tier 2 with expanded scoring."""
        root = _parse_html(html)
        if root is None:
            return None

        candidates = self._score_containers_v12(root)
        if not candidates:
            return None

        candidates.sort(key=lambda c: c[1], reverse=True)

        for container_el, score in candidates[:3]:
            jobs = self._extract_jobs_from_container(container_el, url, score)
            if jobs and len(jobs) >= MIN_JOBS_FOR_SUCCESS:
                # v1.2: Tag method to distinguish from v1.1
                for j in jobs:
                    j["extraction_method"] = "tier2_heuristic_v12"
                return jobs

        return None

    def _score_containers_v12(self, root: etree._Element) -> list[tuple[etree._Element, int]]:
        """Extended container scoring with link density, Bootstrap, expanded keywords."""
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

            # v1.2: Expanded job keyword scoring (+10)
            if _JOB_CLASS_PATTERN_V12.search(combined):
                score += 10

            children = [c for c in el if isinstance(c.tag, str) and c.tag.lower()
                        not in ("script", "style", "br", "hr")]

            if len(children) < 2:
                continue

            # Repeating structure (+5)
            if tag in ("ul", "ol", "table", "div", "section", "main", "tbody"):
                if len(children) > 3:
                    sigs = [_child_signature(c) for c in children]
                    if sigs:
                        most_common = max(sigs.count(s) for s in set(sigs))
                        if most_common >= 3:
                            score += 5

            # Job URL patterns in children (+3)
            job_url_children = 0
            for child in children:
                for a_tag in child.iter("a"):
                    href = a_tag.get("href", "")
                    if _JOB_URL_PATTERN.search(href):
                        job_url_children += 1
                        break
            if job_url_children >= 3:
                score += 3

            # Structural similarity (+3)
            if len(children) > 3:
                sigs = [_child_signature(c) for c in children]
                if sigs:
                    most_common = max(sigs.count(s) for s in set(sigs))
                    if most_common > 3:
                        score += 3

            # ── v1.2 NEW: Link density scoring (+8) ──
            # Count anchors with short text (10-100 chars) — job listing signal
            short_link_count = 0
            for child in children:
                for a_tag in child.iter("a"):
                    a_text = _text(a_tag)
                    if 10 <= len(a_text) <= 100 and _is_valid_title(a_text):
                        short_link_count += 1
                        break  # one per child
            if short_link_count >= 5:
                score += 8
            elif short_link_count >= 3:
                score += 4

            # ── v1.2 NEW: Bootstrap layout detection (+6) ──
            # div.row > div.col-* with anchors containing short text
            if "row" in classes and tag == "div":
                col_children = [c for c in children if "col-" in _get_el_classes(c)]
                if len(col_children) >= 3:
                    cols_with_links = sum(
                        1 for c in col_children
                        if any(a.get("href") for a in c.iter("a"))
                    )
                    if cols_with_links >= 3:
                        score += 6

            # ── v1.2 NEW: Row class keyword bonus (+3) ──
            # If children themselves have job-related classes
            children_with_job_class = sum(
                1 for c in children
                if _ROW_CLASS_PATTERN_V12.search(_get_el_classes(c))
            )
            if children_with_job_class >= 3:
                score += 3

            if score >= 5:
                candidates.append((el, score))

        # v1.1 table heuristic (inherited)
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
                score = 4 + min(job_like_rows, 10)
                candidates.append((target, score))

        return candidates
