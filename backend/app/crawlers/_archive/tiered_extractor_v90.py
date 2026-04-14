"""
Tiered Extraction Engine v9.0 - progressive pagination + stricter card titles.

Strategy:
1. Expand sparse pagination links into bounded progressive sequences to recover
   missed page-2+ volume (e.g. `?pp=6` or `/page/3` not linked on page 1).
2. Recover multilingual AWSM rows where strict English role-hint checks under-capture.
3. Reject editorial linked-card labels (e.g. "Career Guide") while keeping compact
   role titles when URL evidence is strong.
"""

from __future__ import annotations

import re
from collections import defaultdict

from app.crawlers.tiered_extractor_v89 import TieredExtractorV89

_V90_CARD_NON_JOB_TITLE = re.compile(
    r"^(?:career\s+guide|for\s+job\s+seekers?|for\s+compan(?:y|ies)|job\s+seekers?)$",
    re.IGNORECASE,
)
_V90_CARD_NON_ROLE_TOKEN = re.compile(
    r"\b(?:guide|seekers?|compan(?:y|ies)|about|contact|blog|news|search|filter|menu)\b",
    re.IGNORECASE,
)
_V90_AWSM_NON_ROLE_TITLE = re.compile(
    r"^(?:for\s+job\s+seekers?|for\s+compan(?:y|ies)|all\s+\w+|search|filter|more\s+details?)$",
    re.IGNORECASE,
)
_V90_ONE_WORD_ROLE_ALLOW = {
    "accountant",
    "administrator",
    "analyst",
    "architect",
    "assistant",
    "barista",
    "cashier",
    "chef",
    "clerk",
    "consultant",
    "coordinator",
    "developer",
    "driver",
    "electrician",
    "engineer",
    "manager",
    "mechanic",
    "nurse",
    "officer",
    "operator",
    "plumber",
    "recruiter",
    "specialist",
    "storeman",
    "storeperson",
    "supervisor",
    "surveyor",
    "teacher",
    "technician",
    "welder",
    "worker",
    "workers",
}
_V90_QUERY_CAPTURE = re.compile(
    r"([?&](page|paged|pp|startrow|offset|pagenumber|page_number)=)(\d{1,6})\b",
    re.IGNORECASE,
)
_V90_PATH_CAPTURE = re.compile(r"(/page/)(\d{1,4})(/|$)", re.IGNORECASE)


class TieredExtractorV90(TieredExtractorV89):
    """v9.0 extractor: bounded progressive pagination + title precision recovery."""

    def _is_valid_awsm_title_v66(self, title: str) -> bool:
        if super()._is_valid_awsm_title_v66(title):
            return True

        t = self._normalize_title(title)
        if not t or _V90_AWSM_NON_ROLE_TITLE.match(t):
            return False

        words = t.split()
        if len(words) < 1 or len(words) > 6:
            return False

        if len(words) == 1:
            return words[0].lower() in _V90_ONE_WORD_ROLE_ALLOW

        return self._is_reasonable_multilingual_title_v88(t)

    def _is_valid_card_title_v67(self, title: str, has_strong_job_path: bool) -> bool:
        t = self._normalize_title(title)
        if not t or _V90_CARD_NON_JOB_TITLE.match(t):
            return False

        if super()._is_valid_card_title_v67(t, has_strong_job_path):
            return True

        if not has_strong_job_path:
            return False
        if _V90_CARD_NON_ROLE_TOKEN.search(t):
            return False

        words = t.split()
        if len(words) < 1 or len(words) > 3:
            return False

        if len(words) == 1:
            return words[0].lower() in _V90_ONE_WORD_ROLE_ALLOW

        return self._is_reasonable_structured_title_v81(t) or self._is_reasonable_multilingual_title_v88(t)

    def _extract_card_title_v67(self, a_el):
        title = super()._extract_card_title_v67(a_el)
        if title:
            return title

        for piece in a_el.itertext():
            raw_piece = " ".join((piece or "").split())
            if not raw_piece:
                continue
            if re.search(r"\b(?:apply|read\s+more|view\s+details|learn\s+more)\b", raw_piece, re.IGNORECASE):
                continue

            candidate = self._normalize_title(raw_piece)
            if not candidate:
                continue
            if len(candidate) > 100:
                continue
            if _V90_CARD_NON_JOB_TITLE.match(candidate):
                continue
            if _V90_CARD_NON_ROLE_TOKEN.search(candidate):
                continue

            words = candidate.split()
            if len(words) == 1 and words[0].lower() in _V90_ONE_WORD_ROLE_ALLOW:
                return candidate
            if 1 < len(words) <= 3 and (
                self._is_reasonable_structured_title_v81(candidate)
                or self._is_reasonable_multilingual_title_v88(candidate)
            ):
                return candidate

        return None

    def _collect_listing_pagination_urls_v89(self, html: str, page_url: str, max_pages: int = 4) -> list[str]:
        # Pull a larger seed set, then shape it into progressive page increments.
        requested = max(1, int(max_pages or 4))
        seed_urls = super()._collect_listing_pagination_urls_v89(
            html,
            page_url,
            max_pages=max(8, requested * 2),
        )
        if not seed_urls:
            return []

        effective = min(6, requested + 2)
        expanded = self._expand_progressive_pagination_urls_v90(seed_urls, effective)
        return expanded[:effective]

    def _expand_progressive_pagination_urls_v90(self, seed_urls: list[str], max_pages: int) -> list[str]:
        grouped: dict[tuple[str, str, str], set[int]] = defaultdict(set)
        for url in seed_urls:
            info = self._pagination_signature_v90(url)
            if info is None:
                continue
            key, value = info
            if value > 1:
                grouped[key].add(value)

        ranked: list[tuple[int, str]] = []
        seen: set[str] = set()
        for (kind_key, prefix, suffix), values in grouped.items():
            ordered = sorted(v for v in values if v > 1)
            if not ordered:
                continue

            step = self._pagination_step_v90(kind_key, ordered)
            start = ordered[0]
            end = start + step * (max_pages + 1)
            for value in range(start, end + 1, step):
                candidate = f"{prefix}{value}{suffix}"
                if candidate in seen:
                    continue
                seen.add(candidate)
                ranked.append((value, candidate))

        ranked.sort(key=lambda item: item[0])
        urls = [url for _, url in ranked]
        if urls:
            return urls

        deduped: list[str] = []
        for url in seed_urls:
            if url not in deduped:
                deduped.append(url)
        return deduped

    @staticmethod
    def _pagination_signature_v90(url: str) -> tuple[tuple[str, str, str], int] | None:
        query_match = _V90_QUERY_CAPTURE.search(url)
        if query_match:
            key = query_match.group(2).lower()
            value = int(query_match.group(3))
            prefix = url[: query_match.start(3)]
            suffix = url[query_match.end(3) :]
            return (f"query:{key}", prefix, suffix), value

        path_match = _V90_PATH_CAPTURE.search(url)
        if path_match:
            value = int(path_match.group(2))
            prefix = url[: path_match.start(2)]
            suffix = url[path_match.end(2) :]
            return ("path:page", prefix, suffix), value

        return None

    @staticmethod
    def _pagination_step_v90(kind_key: str, values: list[int]) -> int:
        diffs = [b - a for a, b in zip(values, values[1:]) if b > a]
        if diffs:
            return max(1, min(diffs))

        if kind_key in {"query:startrow", "query:offset"}:
            return max(1, values[0])

        return 1
