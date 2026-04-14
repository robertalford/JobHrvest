"""Strict SEARCH/REPLACE diff parser and applier."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DiffApplyError(Exception):
    block_idx: int
    reason: str

    def __str__(self) -> str:
        return f"DiffApplyError(block_idx={self.block_idx}, reason={self.reason})"


def parse_search_replace_blocks(text: str) -> list[tuple[str, str]]:
    blocks: list[tuple[str, str]] = []
    idx = 0
    while True:
        start = text.find("<<<<<<< SEARCH", idx)
        if start == -1:
            break
        mid = text.find("=======", start)
        end = text.find(">>>>>>> REPLACE", mid)
        if mid == -1 or end == -1:
            raise DiffApplyError(len(blocks), "malformed diff block")
        search = text[start + len("<<<<<<< SEARCH"):mid].lstrip("\n")
        replace = text[mid + len("======="):end].lstrip("\n")
        if search and not search.endswith("\n"):
            search += "\n"
        if replace and not replace.endswith("\n"):
            replace += "\n"
        blocks.append((search, replace))
        idx = end + len(">>>>>>> REPLACE")
    if not blocks:
        raise DiffApplyError(0, "no SEARCH/REPLACE blocks found")
    return blocks


def apply_search_replace_blocks(source: str, diff_text: str) -> str:
    updated = source
    for block_idx, (search, replace) in enumerate(parse_search_replace_blocks(diff_text)):
        occurrences = updated.count(search)
        if occurrences == 0:
            raise DiffApplyError(block_idx, "SEARCH anchor not found")
        if occurrences > 1:
            raise DiffApplyError(block_idx, "SEARCH anchor is ambiguous")
        updated = updated.replace(search, replace, 1)
    return updated


__all__ = ["DiffApplyError", "apply_search_replace_blocks", "parse_search_replace_blocks"]
