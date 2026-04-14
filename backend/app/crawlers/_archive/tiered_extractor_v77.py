"""
Tiered Extraction Engine v7.7 — row-context quality recovery.

Strategy:
1. Keep v7.6 extraction order and ATS recoveries.
2. Improve row-container selection for backfill so sibling metadata
   (location/time/salary) is captured reliably.
3. Prefer semantic summary text (`<p>/<li>`) for row descriptions and trim
   CTA tails.
4. Clean glued title/location/apply description prefixes and trailing CTA noise.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from app.crawlers.tiered_extractor import _text
from app.crawlers.tiered_extractor_v74 import _V73_NOISE_TOKEN
from app.crawlers.tiered_extractor_v76 import TieredExtractorV76

_V77_ROW_CLASS_HINT = re.compile(
    r"(?:row|item|card|listing|result|opening|vacan|position|job|media|entry|post)",
    re.IGNORECASE,
)
_V77_ROW_META_HINT = re.compile(
    r"(?:location|city|region|office|workplace|time|employment|salary|money|meta|details)",
    re.IGNORECASE,
)
_V77_CTA_TAIL = re.compile(
    r"\b(?:apply(?:\s+now)?|read\s+more|view\s+more|view\s+details?)\b\s*$",
    re.IGNORECASE,
)
_V77_DESC_PREFIX = re.compile(
    r"^[A-Z][A-Za-z0-9/&+().,' -]{2,120}\s+[A-Z][A-Za-z .'-]{2,80}(?:,\s*[A-Z][A-Za-z .'-]{2,80}){0,3}\s+Apply\s+",
    re.IGNORECASE,
)


class TieredExtractorV77(TieredExtractorV76):
    """v7.7 extractor: row-context location/description quality fixes."""

    def _find_row_container_v73(self, node) -> Optional[Any]:
        current = node
        candidates: list[tuple[int, int, Any]] = []

        for depth in range(1, 9):
            current = current.getparent()
            if current is None or not isinstance(current.tag, str):
                break

            tag = current.tag.lower()
            if tag not in {"div", "li", "article", "tr", "section"}:
                continue

            row_text = " ".join((_text(current) or "").split())
            if len(row_text) < 30 or len(row_text) > 3000:
                continue

            link_count = len(current.xpath(".//a[@href]"))
            if link_count == 0 or link_count > 14:
                continue

            classes = str(current.get("class") or "")
            has_meta = bool(current.xpath(".//*[contains(@class,'location') or contains(@class,'city') or "
                                          "contains(@class,'region') or contains(@class,'office') or "
                                          "contains(@class,'workplace') or contains(@class,'time') or "
                                          "contains(@class,'employment') or contains(@class,'salary') or "
                                          "contains(@class,'money') or contains(@class,'meta') or "
                                          "contains(@class,'details')]"))
            has_summary = bool(current.xpath(".//p[normalize-space()]|.//li[normalize-space()]"))

            score = 0
            if tag in {"li", "article", "tr"}:
                score += 3
            if _V77_ROW_CLASS_HINT.search(classes):
                score += 2
            if has_meta or _V77_ROW_META_HINT.search(classes):
                score += 5
            if has_summary:
                score += 2
            if link_count <= 3:
                score += 2
            if link_count > 8:
                score -= 3
            if len(row_text) > 2400:
                score -= 2

            # Keep slightly favoring closer ancestors when scores are tied.
            score -= depth // 4
            candidates.append((score, -depth, current))

        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        return candidates[0][2]

    def _extract_row_description_v73(self, row, title: str) -> Optional[str]:
        if row is None:
            return None

        summary_nodes = row.xpath(
            ".//p[normalize-space() and not(ancestor::a)]|"
            ".//li[normalize-space() and not(ancestor::a)]"
        )
        for node in summary_nodes[:8]:
            text = " ".join((_text(node) or "").split())
            text = self._strip_title_prefix_v77(text, title)
            text = _V77_CTA_TAIL.sub("", text).strip(" |:-")
            if len(text) < 40:
                continue
            if _V73_NOISE_TOKEN.search(text.lower()):
                continue
            return text[:5000]

        return super()._extract_row_description_v73(row, title)

    @staticmethod
    def _strip_title_prefix_v77(text: str, title: str) -> str:
        cleaned = text or ""
        if title:
            cleaned = re.sub(re.escape(title), "", cleaned, count=1, flags=re.IGNORECASE).strip(" |-:")
        return cleaned

    def _clean_description_v73(self, value: Any) -> Optional[str]:
        cleaned = super()._clean_description_v73(value)
        if not cleaned:
            return cleaned

        text = re.sub(r"(?<=[a-z])(?=[A-Z][a-z])", " ", cleaned)
        text = _V77_DESC_PREFIX.sub("", text)
        text = _V77_CTA_TAIL.sub("", text).strip()
        text = re.sub(r"\s{2,}", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text or None
