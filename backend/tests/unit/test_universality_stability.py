"""Unit tests for the oscillation detector + ever-passed ratchet.

These tests hit the DB layer. Each test spins up its own async engine with
NullPool so pytest-asyncio's per-test event loops don't cross-pollinate
asyncpg connections (the default engine pool is shared and bound to the
first loop it touches, which fails the second test).
"""

import uuid
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool


pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _session():
    """Open a fresh AsyncSession with a dedicated NullPool engine per test."""
    from app.core.config import settings
    engine = create_async_engine(
        settings.DATABASE_URL, poolclass=NullPool, echo=False,
    )
    Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with Session() as s:
            yield s
    finally:
        await engine.dispose()


async def _skip_if_no_tables():
    async with _session() as s:
        result = await s.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name IN ('ever_passed_sites', 'site_result_history')
        """))
        if (result.scalar() or 0) != 2:
            pytest.skip("migration 0028_universality_gate not applied")


async def _cleanup(urls: list[str]) -> None:
    async with _session() as s:
        await s.execute(
            text("DELETE FROM site_result_history WHERE url = ANY(:urls)"),
            {"urls": urls},
        )
        await s.execute(
            text("DELETE FROM ever_passed_sites WHERE url = ANY(:urls)"),
            {"urls": urls},
        )
        await s.commit()


def _entry(url: str, match: str, *, baseline_jobs=10, model_jobs=None, ats="workday"):
    jq = model_jobs if model_jobs is not None else (baseline_jobs if match in
                                                    ("model_equal_or_better",
                                                     "model_only") else 0)
    return {
        "url": url,
        "company": f"Co-{url}",
        "ats_platform": ats,
        "match": match,
        "baseline": {"jobs": baseline_jobs, "fields": {}},
        "model": {"jobs": jq, "jobs_quality": jq, "tier_used": "jsonld",
                  "url_found": url},
    }


async def test_ever_passed_upsert_ratchets_forward():
    """Ratchet only advances — bigger jobs_quality always wins."""
    from app.ml.champion_challenger import stability

    await _skip_if_no_tables()
    url = "https://stability-test-1.example.com/careers"
    await _cleanup([url])

    async with _session() as s:
        run_id = uuid.uuid4()
        # First pass: model finds 5 jobs
        results = [_entry(url, "model_equal_or_better", baseline_jobs=5, model_jobs=5)]
        await stability.upsert_ever_passed(
            s, run_id=run_id, model_name="v7.0", site_results=results,
        )
        await s.commit()

        row = (await s.execute(
            text("SELECT best_version_name, jobs_quality FROM ever_passed_sites WHERE url=:u"),
            {"u": url},
        )).first()
        assert row.best_version_name == "v7.0"
        assert row.jobs_quality == 5

        # Second pass: later version does worse — ratchet should NOT downgrade.
        results_worse = [_entry(url, "model_equal_or_better", baseline_jobs=5, model_jobs=2)]
        await stability.upsert_ever_passed(
            s, run_id=uuid.uuid4(), model_name="v7.1",
            site_results=results_worse,
        )
        await s.commit()
        row = (await s.execute(
            text("SELECT best_version_name, jobs_quality FROM ever_passed_sites WHERE url=:u"),
            {"u": url},
        )).first()
        assert row.best_version_name == "v7.0"
        assert row.jobs_quality == 5

        # Third pass: later version beats it.
        results_better = [_entry(url, "model_equal_or_better", baseline_jobs=10, model_jobs=10)]
        await stability.upsert_ever_passed(
            s, run_id=uuid.uuid4(), model_name="v7.2",
            site_results=results_better,
        )
        await s.commit()
        row = (await s.execute(
            text("SELECT best_version_name, jobs_quality FROM ever_passed_sites WHERE url=:u"),
            {"u": url},
        )).first()
        assert row.best_version_name == "v7.2"
        assert row.jobs_quality == 10

    await _cleanup([url])


async def test_ever_passed_regression_detection():
    """Ever-passed gate flags a site that previously passed but now fails."""
    from app.ml.champion_challenger import stability

    await _skip_if_no_tables()
    url = "https://stability-test-2.example.com/careers"
    await _cleanup([url])

    async with _session() as s:
        # Seed the ever-passed set with a previous win (10 jobs_quality).
        await stability.upsert_ever_passed(
            s, run_id=uuid.uuid4(), model_name="v7.0",
            site_results=[_entry(url, "model_equal_or_better",
                                  baseline_jobs=10, model_jobs=10)],
        )
        await s.commit()

        # Now test a current run where the challenger got 2 jobs (regression).
        current = [_entry(url, "partial", baseline_jobs=10, model_jobs=2)]
        regs = await stability.fetch_ever_passed_regressions(s, site_results=current)
        assert len(regs) == 1
        assert regs[0]["url"] == url
        assert regs[0]["prev_jobs_quality"] == 10
        assert regs[0]["cur_jobs_quality"] == 2
        assert regs[0]["prev_best_version"] == "v7.0"

    await _cleanup([url])


async def test_ever_passed_slack_tolerates_minor_drift():
    """Small drift (within EVER_PASSED_REGRESSION_SLACK_PCT) is NOT flagged."""
    from app.ml.champion_challenger import stability

    await _skip_if_no_tables()
    url = "https://stability-test-3.example.com/careers"
    await _cleanup([url])

    async with _session() as s:
        await stability.upsert_ever_passed(
            s, run_id=uuid.uuid4(), model_name="v7.0",
            site_results=[_entry(url, "model_equal_or_better",
                                  baseline_jobs=10, model_jobs=10)],
        )
        await s.commit()

        # Current: 9 jobs_quality (within 15% slack).
        current = [_entry(url, "model_equal_or_better", baseline_jobs=10, model_jobs=9)]
        regs = await stability.fetch_ever_passed_regressions(s, site_results=current)
        assert regs == []

    await _cleanup([url])


async def test_oscillation_detector_finds_flipping_site():
    """A site that alternates pass/fail/pass/fail gets flagged."""
    from app.ml.champion_challenger import stability

    await _skip_if_no_tables()
    url = "https://stability-test-4.example.com/careers"
    stable_url = "https://stability-test-5.example.com/careers"
    await _cleanup([url, stable_url])

    async with _session() as s:
        # Seed 5 alternating observations on `url` and 5 stable passes.
        sequences = [(url, [True, False, True, False, True]),
                     (stable_url, [True, True, True, True, True])]
        rows = []
        for u, states in sequences:
            for i, passed in enumerate(states):
                rows.append({
                    "url": u, "run_id": uuid.uuid4(), "model_id": None,
                    "model_name": f"v7.{i}", "ats_platform": "workday",
                    "match": "model_equal_or_better" if passed else "partial",
                    "passed": passed, "baseline_jobs": 10,
                    "model_jobs": 10 if passed else 2,
                    "jobs_quality": 10 if passed else 2,
                    "composite_pts": 100.0 if passed else 20.0,
                })
        await s.execute(
            text("""
                INSERT INTO site_result_history
                    (url, run_id, model_id, model_name, ats_platform,
                     match, passed, baseline_jobs, model_jobs, jobs_quality,
                     composite_pts)
                VALUES
                    (:url, :run_id, :model_id, :model_name, :ats_platform,
                     :match, :passed, :baseline_jobs, :model_jobs, :jobs_quality,
                     :composite_pts)
            """),
            rows,
        )
        await s.commit()

        flips = await stability.compute_flip_counts(
            s, urls=[url, stable_url], lookback=5,
        )
        assert flips.get(url, 0) >= 2
        assert stable_url not in flips

        unstable = await stability.unstable_site_urls(
            s, urls=[url, stable_url], min_flips=2,
        )
        assert url in unstable
        assert stable_url not in unstable

    await _cleanup([url, stable_url])


async def test_history_trims_to_keep_limit():
    """More than HISTORY_KEEP_PER_SITE rows get trimmed on next insert."""
    from app.ml.champion_challenger import stability

    await _skip_if_no_tables()
    url = "https://stability-test-6.example.com/careers"
    await _cleanup([url])

    async with _session() as s:
        entries = [_entry(url, "model_equal_or_better", baseline_jobs=10, model_jobs=10)]
        for _ in range(stability.HISTORY_KEEP_PER_SITE + 3):
            await stability.record_run_history(
                s, run_id=uuid.uuid4(), model_id=None,
                model_name="vX", site_results=entries,
            )
        await s.commit()

        count = (await s.execute(
            text("SELECT COUNT(*) FROM site_result_history WHERE url=:u"),
            {"u": url},
        )).scalar()
        assert count == stability.HISTORY_KEEP_PER_SITE

    await _cleanup([url])
