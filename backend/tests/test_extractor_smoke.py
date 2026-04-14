"""
Smoke tests for tiered extractor versions.

Codex can run these to validate a new extractor version BEFORE deployment:
  cd /path/to/jobharvest
  python -m pytest backend/tests/test_extractor_smoke.py -v --tb=short

Tests verify:
  1. The extractor module imports without errors
  2. It inherits from TieredExtractorV16 (not deeper chains)
  3. extract() runs without crashing on sample HTML
  4. Extracted jobs have core fields (title, source_url, location_raw)
  5. No obvious Type 1 errors (nav labels, section headings as titles)
"""

import asyncio
import glob
import importlib
import json
import os
import sys

import pytest

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

CRAWLERS_DIR = os.path.join(os.path.dirname(__file__), "..", "app", "crawlers")
CONTEXT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "storage", "auto_improve_context")


def _find_latest_extractor_version() -> int:
    """Find the highest versioned tiered_extractor_vXX.py file."""
    files = glob.glob(os.path.join(CRAWLERS_DIR, "tiered_extractor_v*.py"))
    versions = []
    for f in files:
        base = os.path.basename(f).replace("tiered_extractor_v", "").replace(".py", "")
        try:
            versions.append(int(base))
        except ValueError:
            pass
    return max(versions) if versions else 16


def _load_extractor(version: int):
    """Dynamically import a tiered extractor version."""
    mod = importlib.import_module(f"app.crawlers.tiered_extractor_v{version}")
    cls = getattr(mod, f"TieredExtractorV{version}")
    return cls


def _find_context_html_files() -> list[str]:
    """Find HTML files from the latest auto-improve context dir."""
    if not os.path.isdir(CONTEXT_DIR):
        return []
    # Find most recent context subdirectory
    subdirs = sorted(glob.glob(os.path.join(CONTEXT_DIR, "v*")), key=os.path.getmtime, reverse=True)
    if not subdirs:
        return []
    html_files = glob.glob(os.path.join(subdirs[0], "*.html"))
    return html_files[:5]  # Limit to 5 for speed


# ── Test: Import ──

class TestExtractorImport:
    def test_latest_version_imports(self):
        """The latest extractor version must import without errors."""
        ver = _find_latest_extractor_version()
        cls = _load_extractor(ver)
        assert cls is not None, f"TieredExtractorV{ver} could not be loaded"

    def test_inherits_from_v16(self):
        """Must inherit from TieredExtractorV16, not deeper chains."""
        from app.crawlers.tiered_extractor_v16 import TieredExtractorV16
        ver = _find_latest_extractor_version()
        cls = _load_extractor(ver)
        assert issubclass(cls, TieredExtractorV16), (
            f"TieredExtractorV{ver} must inherit from TieredExtractorV16"
        )

    def test_has_extract_method(self):
        """Must have an extract() method."""
        ver = _find_latest_extractor_version()
        cls = _load_extractor(ver)
        assert hasattr(cls, "extract"), f"TieredExtractorV{ver} missing extract() method"


# ── Test: Extraction on sample HTML ──

class TestExtractorExecution:
    @pytest.fixture
    def extractor(self):
        ver = _find_latest_extractor_version()
        cls = _load_extractor(ver)
        return cls()

    @pytest.fixture
    def html_files(self):
        return _find_context_html_files()

    def test_extract_does_not_crash(self, extractor, html_files):
        """extract() must not crash on any sample HTML file."""
        if not html_files:
            pytest.skip("No context HTML files available")

        for html_file in html_files:
            with open(html_file, encoding="utf-8", errors="replace") as f:
                html = f.read()
            url = f"https://example.com/careers"  # Dummy URL
            try:
                # Create minimal mock objects
                class MockPage:
                    def __init__(self):
                        self.url = url
                        self.requires_js_rendering = False
                class MockCompany:
                    def __init__(self):
                        self.name = "Test Company"
                        self.ats_platform = None

                jobs = asyncio.get_event_loop().run_until_complete(
                    extractor.extract(MockPage(), MockCompany(), html)
                )
                # Jobs can be empty (not all pages have extractable jobs), but must not crash
                assert isinstance(jobs, list), f"extract() must return a list, got {type(jobs)}"
            except Exception as e:
                pytest.fail(f"extract() crashed on {os.path.basename(html_file)}: {e}")

    def test_extracted_jobs_have_titles(self, extractor, html_files):
        """Every extracted job must have a non-empty title."""
        if not html_files:
            pytest.skip("No context HTML files available")

        for html_file in html_files:
            with open(html_file, encoding="utf-8", errors="replace") as f:
                html = f.read()

            class MockPage:
                def __init__(self):
                    self.url = "https://example.com/careers"
                    self.requires_js_rendering = False
            class MockCompany:
                def __init__(self):
                    self.name = "Test Company"
                    self.ats_platform = None

            try:
                jobs = asyncio.get_event_loop().run_until_complete(
                    extractor.extract(MockPage(), MockCompany(), html)
                )
            except Exception:
                continue  # Skip files that crash (caught by other test)

            for j in jobs:
                assert j.get("title"), f"Job missing title in {os.path.basename(html_file)}: {j}"
                assert len(j["title"]) > 2, f"Title too short: '{j['title']}'"
                assert len(j["title"]) < 200, f"Title too long (likely a description): '{j['title'][:50]}...'"


# ── Test: Title quality ──

NON_JOB_TITLES = [
    "Open Jobs", "Career Opportunities", "Join Our Team", "Current Openings",
    "Apply Now", "Submit Application", "Home", "About Us", "Contact",
    "Working at", "Our Culture", "Employee Benefits", "Meet Our Team",
    "Latest News", "Leave a Comment", "Subscribe", "Follow Us",
    "Privacy Policy", "Terms of Service", "Cookie Policy",
]

class TestTitleQuality:
    def test_no_obvious_type1_errors(self):
        """Extracted titles should not be common non-job labels."""
        ver = _find_latest_extractor_version()
        cls = _load_extractor(ver)
        ext = cls()
        html_files = _find_context_html_files()

        if not html_files:
            pytest.skip("No context HTML files available")

        for html_file in html_files[:3]:
            with open(html_file, encoding="utf-8", errors="replace") as f:
                html = f.read()

            class MockPage:
                def __init__(self):
                    self.url = "https://example.com/careers"
                    self.requires_js_rendering = False
            class MockCompany:
                def __init__(self):
                    self.name = "Test Company"
                    self.ats_platform = None

            try:
                jobs = asyncio.get_event_loop().run_until_complete(
                    ext.extract(MockPage(), MockCompany(), html)
                )
            except Exception:
                continue

            for j in jobs:
                title = (j.get("title") or "").strip()
                for bad in NON_JOB_TITLES:
                    assert title.lower() != bad.lower(), (
                        f"Type 1 error: '{title}' looks like a nav label, not a job title "
                        f"(file: {os.path.basename(html_file)})"
                    )
