"""Load learned ATS templates from storage/ats_templates.json.

The template file is populated offline by `scripts/build_ats_templates.py`,
which clusters Jobstream's hand-tuned `fixed_test_sites.known_selectors` by
detected ATS platform and keeps the modal selector per wrapper key.

At extraction time, the base TieredExtractor consults this loader as a
pre-Tier-1 hook: if an ATS is detected (by URL or DOM cue) AND a learned
template is present AND it produces >=3 valid jobs, we take that over the
hardcoded Tier 1 templates. The hardcoded templates remain as a fallback.

Design goals:
    - Zero startup cost — the JSON is read lazily on first lookup.
    - Hot-reload safe — file mtime is checked on every lookup; changes pick up
      without a restart.
    - Sync-only — intentionally no async / HTTP. Detection operates on (url,
      html) that the caller has already fetched.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULT_PATH = os.environ.get(
    "ATS_TEMPLATES_PATH",
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)
        )))),
        "storage",
        "ats_templates.json",
    ),
)


# URL/host fragments per ATS. Intentionally a superset of tiered_extractor's
# _ATS_URL_PATTERNS — this loader is useful even when the hardcoded Tier 1
# library doesn't include a platform (e.g. pageup, martianlogic).
_URL_FRAGMENTS: list[tuple[str, tuple[str, ...]]] = [
    ("greenhouse", ("greenhouse.io", "boards.greenhouse", "job-boards.greenhouse")),
    ("lever", ("lever.co", "jobs.lever")),
    ("workday", ("myworkdayjobs", "workday")),
    ("ashby", ("ashbyhq",)),
    ("bamboohr", ("bamboohr",)),
    ("smartrecruiters", ("smartrecruiters",)),
    ("icims", ("icims",)),
    ("taleo", ("taleo",)),
    ("successfactors", ("successfactors",)),
    ("jobvite", ("jobvite",)),
    ("breezyhr", ("breezy.hr", "breezyhr")),
    ("rippling", ("rippling.com/jobs", "ats.rippling")),
    ("pageup", ("pageuppeople",)),
    ("livehire", ("livehire",)),
    ("teamtailor", ("teamtailor",)),
    ("applynow", ("applynow",)),
    ("jazzhr", ("applytojob", "theresumator")),
    ("oracle_cx", ("oracle", "candidate-experience")),
    ("salesforce", ("salesforce",)),
    ("martianlogic", ("martianlogic", "myrecruitmentplus")),
]

# DOM fragments — only consulted if URL patterns miss. Intentionally loose; a
# tight match comes from the URL.
_DOM_FRAGMENTS: list[tuple[str, tuple[str, ...]]] = [
    ("greenhouse", ("grnhse_app", "gh_jboard")),
    ("lever", ("lever-jobs-container",)),
    ("workday", ("data-automation-id", "wd-job")),
    ("ashby", ("ashby_jid",)),
    ("smartrecruiters", ("smart-apply",)),
    ("bamboohr", ("ResJobList",)),
    ("icims", ("icims-job",)),
    ("pageup", ("pua-table",)),
    ("martianlogic", ("myrecruitmentplus",)),
]


class ATSTemplateLoader:
    """Singleton-friendly loader. Instantiated once; callers share it."""

    def __init__(self, path: str = _DEFAULT_PATH):
        self.path = path
        self._templates: dict = {}
        self._loaded_mtime: float = 0.0
        self._lock = threading.Lock()

    def _load_if_stale(self) -> None:
        try:
            mtime = os.path.getmtime(self.path)
        except OSError:
            # File doesn't exist yet (first run before build script) — keep
            # the existing cache (empty dict) and return silently.
            return
        if mtime == self._loaded_mtime:
            return
        with self._lock:
            if mtime == self._loaded_mtime:
                return
            try:
                with open(self.path, encoding="utf-8") as fh:
                    payload = json.load(fh) or {}
            except (OSError, json.JSONDecodeError) as e:
                logger.warning("ats_template_loader: could not load %s: %s", self.path, e)
                return
            self._templates = payload.get("templates", payload) or {}
            self._loaded_mtime = mtime
            logger.info(
                "ats_template_loader: loaded %d templates from %s",
                len(self._templates), self.path,
            )

    def detect(self, url: str, html: str | None = None) -> Optional[str]:
        """Classify (url, html) into an ATS key. Offline, no HTTP."""
        u = (url or "").lower()
        for ats, fragments in _URL_FRAGMENTS:
            for frag in fragments:
                if frag in u:
                    return ats
        if html:
            h = html.lower()[:200_000]
            for ats, fragments in _DOM_FRAGMENTS:
                for frag in fragments:
                    if frag.lower() in h:
                        return ats
        return None

    def lookup(self, ats: str) -> Optional[dict]:
        """Return the template for a given ATS key, or None."""
        if not ats:
            return None
        self._load_if_stale()
        return self._templates.get(ats)

    def detect_and_lookup(self, url: str, html: str | None = None) -> Optional[tuple[str, dict]]:
        """One-shot: detect + fetch. Returns (ats, template) or None."""
        ats = self.detect(url, html)
        if not ats:
            return None
        template = self.lookup(ats)
        if not template:
            return None
        return ats, template


# Module-level singleton — extractor code imports this directly.
default_loader = ATSTemplateLoader()


def detect_ats(url: str, html: str | None = None) -> Optional[str]:
    """Convenience wrapper for callers that only need detection."""
    return default_loader.detect(url, html)


def get_template(ats: str) -> Optional[dict]:
    return default_loader.lookup(ats)


__all__ = [
    "ATSTemplateLoader",
    "default_loader",
    "detect_ats",
    "get_template",
]
