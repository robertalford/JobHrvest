"""Fixture harness — sub-second smoke evaluation for challengers.

Why it exists: the live A/B test takes 10-30 minutes per cycle because it
fetches pages, runs Playwright, and talks to databases. That's fine for the
once-per-iteration gate, but it blocks everything upstream — we can't generate
multiple candidates, or abort obviously-broken ones, when feedback is 30-min
away.

This harness replaces the inner feedback loop with an offline fixture test
that scores a challenger against frozen holdout HTML in <30s:

    from app.ml.champion_challenger.fixture_harness import FixtureHarness
    report = await FixtureHarness.from_storage().run(extractor)
    if report.composite < champion.composite - 2.0:
        raise AbortCycle("challenger regressed on fixtures")

Fixtures are frozen (GoldHoldoutSnapshot + GoldHoldoutJob) — the harness
reuses the same structures the holdout evaluator uses, just with a thinner,
sync-friendly wrapper around them.

Axes reported mirror the production composite as closely as possible given
we're working offline:
    - discovery:          100 (snapshots are pre-discovered)
    - quality_extraction: % of fixtures with ≥1 valid job
    - volume_accuracy:    symmetric penalty vs expected_job_count
    - field_completeness: avg fraction of 6 core fields present per job
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Protocol

logger = logging.getLogger(__name__)


# ---------- Data types -----------------------------------------------------


@dataclass
class Fixture:
    """A single offline evaluation case.

    `snapshot_path` is an HTML file on disk. `expected_titles` is an optional
    list used to compute title recall (silver labels when available). All
    other fields are metadata for stratification or debugging.
    """

    domain: str
    url: str
    snapshot_path: Path
    expected_titles: list[str] = field(default_factory=list)
    expected_job_count: Optional[int] = None
    ats_platform: Optional[str] = None


@dataclass
class FixtureResult:
    domain: str
    extracted_count: int
    matched_titles: int
    ats_platform: Optional[str]
    fields_score: float
    quality_passed: bool
    expected_job_count: Optional[int] = None
    error: Optional[str] = None


@dataclass
class FixtureReport:
    fixtures_total: int
    results: list[FixtureResult]
    elapsed_s: float

    @property
    def axes(self) -> dict[str, float]:
        valid = [r for r in self.results if r.error is None]
        if not valid:
            return {"discovery": 0.0, "quality_extraction": 0.0,
                    "field_completeness": 0.0, "volume_accuracy": 0.0}

        # discovery: fixtures are pre-discovered => effectively 100 if we
        # didn't crash, so we report error-free ratio.
        discovery = 100.0 * len(valid) / len(self.results)

        quality = 100.0 * sum(1 for r in valid if r.quality_passed) / len(valid)
        field_score = 100.0 * sum(r.fields_score for r in valid) / len(valid)

        # volume: per-fixture ratio (extracted / expected), symmetric penalty
        volumes = []
        for r in valid:
            expected = r.expected_job_count
            if expected is None and r.matched_titles:
                expected = max(r.matched_titles, 1)
            if expected is None and r.extracted_count == 0:
                volumes.append(0.0)
                continue
            if expected is None:
                volumes.append(100.0 if r.extracted_count >= 1 else 0.0)
                continue
            ratio = r.extracted_count / expected
            if ratio <= 1.0:
                volumes.append(100.0 * ratio)
            else:
                overshoot = max(0.0, ratio - 1.5)
                volumes.append(max(0.0, 100.0 - 100.0 * overshoot))
        volume = sum(volumes) / len(volumes) if volumes else 0.0

        return {
            "discovery": round(discovery, 1),
            "quality_extraction": round(quality, 1),
            "field_completeness": round(field_score, 1),
            "volume_accuracy": round(volume, 1),
        }

    @property
    def composite(self) -> float:
        a = self.axes
        return round(
            0.20 * a["discovery"]
            + 0.30 * a["quality_extraction"]
            + 0.25 * a["field_completeness"]
            + 0.25 * a["volume_accuracy"],
            2,
        )


# ---------- The harness ----------------------------------------------------


class ExtractorProtocol(Protocol):
    async def extract(self, career_page: Any, company: Any, html: str) -> list[dict]:
        ...


class _StubCareerPage:
    def __init__(self, url: str) -> None:
        self.url = url
        self.requires_js_rendering = False


class _StubCompany:
    def __init__(self, ats_platform: Optional[str] = None) -> None:
        self.name = "fixture"
        self.ats_platform = ats_platform


_CORE_FIELDS = ("title", "source_url", "location_raw", "salary_raw",
                "employment_type", "description")


def _fields_score(jobs: list[dict]) -> float:
    """Fraction of core fields present per job (avg across jobs, 0..1)."""
    if not jobs:
        return 0.0
    n = len(jobs)
    total = 0.0
    for j in jobs:
        present = sum(1 for k in _CORE_FIELDS if (j.get(k) or "").strip()) if isinstance(j, dict) else 0
        total += present / len(_CORE_FIELDS)
    return total / n


def _quality_passed(jobs: list[dict]) -> bool:
    """Heuristic: at least 2 jobs with a real-looking title + unique detail URL."""
    if not jobs or len(jobs) < 2:
        return False
    seen_urls: set[str] = set()
    real = 0
    for j in jobs:
        if not isinstance(j, dict):
            continue
        title = (j.get("title") or "").strip()
        url = (j.get("source_url") or "").strip()
        if 4 <= len(title) <= 140 and url and url not in seen_urls:
            seen_urls.add(url)
            real += 1
    return real >= 2


def _count_fuzzy_matches(expected: list[str], extracted_titles: list[str]) -> int:
    """Token-set ratio via rapidfuzz if available, else substring fallback."""
    if not expected:
        return 0
    try:
        from rapidfuzz import fuzz  # type: ignore
    except ImportError:
        lo_e = [e.lower() for e in extracted_titles if e]
        return sum(1 for g in expected if any(g.lower() in t or t in g.lower() for t in lo_e))

    matched = 0
    for g in expected:
        best = max((fuzz.token_set_ratio(g, t) for t in extracted_titles), default=0)
        if best >= 80:
            matched += 1
    return matched


class FixtureHarness:
    """Run an extractor over a list of fixtures and score it."""

    def __init__(self, fixtures: list[Fixture]) -> None:
        self.fixtures = fixtures

    # --- Factories ---------------------------------------------------------

    @classmethod
    def from_storage(
        cls,
        root: str = os.environ.get("FIXTURE_HARNESS_ROOT", "backend/tests/fixtures/extractor_smoke"),
        *,
        limit: Optional[int] = None,
    ) -> "FixtureHarness":
        """Discover fixtures by scanning a storage directory.

        Expected layout (produced by build_gold_holdout.py + build_silver_labels.py):
            storage/gold_holdout/<domain_id>/<hash>.html
            storage/gold_holdout/<domain_id>/meta.json       # optional

        When meta.json is absent we fall back to a minimal fixture with no
        expected_titles — the harness still computes quality/field/volume axes
        from the extractor's output.
        """
        root_path = Path(root)
        fixtures: list[Fixture] = []
        candidates = [
            root_path,
            Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "extractor_smoke",
            Path(__file__).resolve().parents[4] / root,
            Path("storage/gold_holdout"),
        ]
        resolved = next((candidate for candidate in candidates if candidate.exists()), None)
        if resolved is None:
            logger.warning("fixture_harness: %s does not exist", root_path)
            return cls(fixtures)
        root_path = resolved
        manifest_path = root_path / "manifest.json"
        if manifest_path.exists():
            fixtures = cls._fixtures_from_manifest(manifest_path, limit=limit)
            logger.info("fixture_harness: loaded %d fixtures from %s", len(fixtures), manifest_path)
            return cls(fixtures)
        for html_path in sorted(root_path.rglob("*.html")):
            meta_path = html_path.parent / "meta.json"
            meta = {}
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text())
                except Exception:  # noqa: BLE001
                    meta = {}
            fixtures.append(Fixture(
                domain=meta.get("domain") or html_path.parent.name,
                url=meta.get("url") or f"file://{html_path}",
                snapshot_path=html_path,
                expected_titles=list(meta.get("expected_titles") or []),
                expected_job_count=meta.get("expected_job_count"),
                ats_platform=meta.get("ats_platform"),
            ))
            if limit and len(fixtures) >= limit:
                break
        logger.info("fixture_harness: discovered %d fixtures under %s", len(fixtures), root_path)
        return cls(fixtures)

    @staticmethod
    def _fixtures_from_manifest(manifest_path: Path, *, limit: Optional[int]) -> list[Fixture]:
        payload = json.loads(manifest_path.read_text())
        root = manifest_path.parent
        fixtures: list[Fixture] = []
        for item in payload[:limit]:
            fixtures.append(
                Fixture(
                    domain=item["domain"],
                    url=item["url"],
                    snapshot_path=root / item["snapshot_path"],
                    expected_titles=list(item.get("expected_titles") or []),
                    expected_job_count=item.get("expected_job_count"),
                    ats_platform=item.get("ats_platform"),
                )
            )
        return fixtures

    # --- Execution ---------------------------------------------------------

    async def run(self, extractor: ExtractorProtocol) -> FixtureReport:
        import time
        start = time.monotonic()
        async def _run_fixture(fix: Fixture) -> FixtureResult:
            try:
                html = fix.snapshot_path.read_bytes().decode("utf-8", errors="replace")
            except Exception as e:  # noqa: BLE001
                return FixtureResult(
                    domain=fix.domain, extracted_count=0, matched_titles=0,
                    ats_platform=fix.ats_platform, fields_score=0.0,
                    quality_passed=False, expected_job_count=fix.expected_job_count,
                    error=f"load-failed: {e}",
                )

            try:
                jobs = await extractor.extract(
                    _StubCareerPage(fix.url),
                    _StubCompany(fix.ats_platform),
                    html,
                )
            except Exception as e:  # noqa: BLE001
                return FixtureResult(
                    domain=fix.domain, extracted_count=0, matched_titles=0,
                    ats_platform=fix.ats_platform, fields_score=0.0,
                    quality_passed=False, expected_job_count=fix.expected_job_count,
                    error=f"extract-failed: {e}",
                )

            extracted_titles = [(j.get("title") or "") for j in (jobs or []) if isinstance(j, dict)]
            matched = _count_fuzzy_matches(fix.expected_titles, extracted_titles)
            return FixtureResult(
                domain=fix.domain,
                extracted_count=len(jobs or []),
                matched_titles=matched,
                ats_platform=fix.ats_platform,
                fields_score=_fields_score(jobs or []),
                quality_passed=_quality_passed(jobs or []),
                expected_job_count=fix.expected_job_count,
            )

        results = list(await asyncio.gather(*(_run_fixture(fix) for fix in self.fixtures)))

        elapsed = time.monotonic() - start
        return FixtureReport(fixtures_total=len(self.fixtures), results=results, elapsed_s=elapsed)
