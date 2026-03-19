"""
Template Learning System — Stage 4d.

Key innovation: auto-generates CSS selector maps from LLM extraction results.

Process:
1. Run LLM extraction on 3-5 different job detail pages from the same site
2. For each page, store the full DOM
3. Compare DOMs: find which CSS selectors consistently contain the LLM-extracted data
4. Generate a selector map: {"title": "h1.job-title", "location": "span.location", ...}
5. Store in site_templates table
6. For subsequent crawls, use fast selector-based extraction instead of LLM
7. Periodically validate templates by running LLM on a sample and comparing

This turns expensive LLM calls into a one-time learning cost per site.
"""

import logging
import re
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Fields we try to find selectors for
TARGET_FIELDS = [
    "title", "location_raw", "employment_type", "salary_raw",
    "department", "description", "requirements", "benefits", "date_posted",
]


class TemplateLearner:
    """
    Auto-generates CSS selector maps from LLM extraction results + DOM comparison.
    """

    def __init__(self, db=None):
        self.db = db

    def generate_selector_map(self, pages: list[dict]) -> dict:
        """
        Given a list of {"html": str, "extracted": dict} pairs,
        find CSS selectors that consistently contain the extracted values.

        Returns {"field_name": "css_selector", ...}
        """
        if not pages:
            return {}

        # For each field, find selector candidates from each page
        field_candidates: dict[str, list[list[str]]] = {f: [] for f in TARGET_FIELDS}

        for page in pages:
            html = page.get("html", "")
            extracted = page.get("extracted", {})
            soup = BeautifulSoup(html, "lxml")

            for field in TARGET_FIELDS:
                value = extracted.get(field)
                if not value:
                    continue
                selectors = self._find_selectors_for_value(soup, str(value))
                if selectors:
                    field_candidates[field].append(selectors)

        # Find selectors that work across ALL pages
        selector_map = {}
        for field, per_page_candidates in field_candidates.items():
            if not per_page_candidates:
                continue
            # Find selectors common to all pages
            common = set(per_page_candidates[0])
            for candidates in per_page_candidates[1:]:
                common &= set(candidates)
            if common:
                # Pick the most specific selector (longest)
                best = max(common, key=len)
                selector_map[field] = best
                logger.debug(f"Learned selector for {field}: {best}")

        return selector_map

    def _find_selectors_for_value(self, soup: BeautifulSoup, value: str) -> list[str]:
        """
        Find CSS selectors for elements whose text closely matches `value`.
        Returns list of CSS selector strings.
        """
        if not value or len(value) < 3:
            return []

        # Normalize: lowercase, collapse whitespace
        target = re.sub(r"\s+", " ", value.lower().strip())[:100]
        selectors = []

        for el in soup.find_all(True):
            el_text = re.sub(r"\s+", " ", el.get_text(strip=True).lower())
            if not el_text:
                continue
            # Require significant overlap
            if target[:50] in el_text or el_text[:50] in target:
                selector = self._element_to_selector(el)
                if selector:
                    selectors.append(selector)

        # Deduplicate and prefer more specific selectors
        return list(dict.fromkeys(selectors))[:5]

    def _element_to_selector(self, el) -> Optional[str]:
        """Generate a CSS selector for a BeautifulSoup element."""
        if not el.name:
            return None

        parts = []
        current = el

        for _ in range(4):  # Max 4 levels deep
            if not current or not current.name or current.name in ("html", "body", "[document]"):
                break

            tag = current.name
            classes = current.get("class", [])
            el_id = current.get("id", "")

            if el_id:
                return f"#{el_id}"  # ID is always unique enough
            elif classes:
                # Use up to 2 most specific classes
                cls_selector = "." + ".".join(sorted(classes[:2]))
                parts.insert(0, f"{tag}{cls_selector}")
            else:
                parts.insert(0, tag)

            current = current.parent

        return " > ".join(parts) if parts else None

    def extract_with_template(self, html: str, selector_map: dict) -> dict:
        """
        Extract job data from HTML using a learned selector map.
        Returns dict with extracted fields. This is the fast path.
        """
        soup = BeautifulSoup(html, "lxml")
        result = {}

        for field, selector in selector_map.items():
            try:
                el = soup.select_one(selector)
                if el:
                    result[field] = el.get_text(strip=True)
            except Exception as e:
                logger.debug(f"Selector '{selector}' failed for field '{field}': {e}")

        if result:
            result["extraction_method"] = "template"
            result["extraction_confidence"] = 0.85

        return result

    def calculate_template_accuracy(self, template_result: dict, llm_result: dict) -> float:
        """
        Compare template extraction against LLM extraction.
        Returns accuracy score 0.0-1.0.
        """
        comparable_fields = [f for f in TARGET_FIELDS if f in template_result or f in llm_result]
        if not comparable_fields:
            return 0.0

        matches = 0
        for field in comparable_fields:
            t_val = str(template_result.get(field, "")).lower().strip()
            l_val = str(llm_result.get(field, "")).lower().strip()
            if not t_val and not l_val:
                matches += 1  # Both empty = agree
            elif t_val and l_val and (t_val in l_val or l_val in t_val):
                matches += 1

        return matches / len(comparable_fields)

    async def bootstrap_template(self, db, company, career_page, sample_urls: list[str]) -> Optional[dict]:
        """
        Bootstrap a template for a site by running LLM on 3-5 job pages.
        Returns the generated selector_map or None if insufficient data.
        """
        from app.crawlers.http_client import ResilientHTTPClient
        from app.extractors.llm_extractor import LLMJobExtractor
        from markdownify import markdownify

        client = ResilientHTTPClient()
        llm = LLMJobExtractor()
        pages = []

        for url in sample_urls[:5]:
            try:
                resp = await client.get(url)
                html = resp.text
                markdown = markdownify(html, strip=["script", "style"])
                extracted = await llm.extract(url, markdown)
                if extracted:
                    pages.append({"html": html, "extracted": extracted})
            except Exception as e:
                logger.warning(f"Failed to fetch/extract {url} for template learning: {e}")

        if len(pages) < 2:
            logger.info(f"Not enough pages ({len(pages)}) to learn template for {company.domain}")
            return None

        selector_map = self.generate_selector_map(pages)
        if not selector_map:
            return None

        # Persist template to database
        from app.models.site_template import SiteTemplate
        template = SiteTemplate(
            company_id=company.id,
            career_page_id=career_page.id,
            template_type="detail_page",
            selectors=selector_map,
            learned_via="llm_bootstrapped",
        )
        db.add(template)
        await db.commit()

        logger.info(f"Bootstrapped template for {company.domain} with {len(selector_map)} selectors")
        return selector_map
