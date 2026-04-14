"""Verify a newly-written challenger extractor against offline fixtures.

Runs in < 30s vs the full A/B test's 10-30 min. The daemon invokes this
between "Codex wrote the file" and "trigger the live test" — if the
challenger tanks on frozen fixtures vs the champion, we abort and feed the
failure summary back into the next Codex prompt.

Exit codes:
    0  challenger PASSES (composite within `tolerance` of champion)
    1  challenger FAILS (composite significantly below champion)
    2  harness error (no fixtures, import failure, etc.)

Usage:
    python -m scripts.verify_challenger --version v92
    python -m scripts.verify_challenger --version v92 --tolerance 2.0
    python -m scripts.verify_challenger --champion v91 --challenger v92 --json

Writes a per-challenger report to
    storage/auto_improve_fixture_reports/<version>.json
so the auto-improve loop can include it in the next-iteration brief.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger("verify_challenger")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORT_DIR = Path(
    os.environ.get(
        "FIXTURE_REPORTS_DIR",
        os.path.join(os.path.dirname(PROJECT_DIR), "storage", "auto_improve_fixture_reports"),
    )
)


def _load_extractor_instance(version_tag: str):
    """Import the module app.crawlers.tiered_extractor_v{NN} and instantiate its class.

    `version_tag` can be 'v92' or '9.2'; both are normalised to the file number.
    """
    n = re.sub(r"[^\d]", "", version_tag)
    if not n:
        raise ValueError(f"Could not parse version {version_tag!r}")
    modname = f"app.crawlers.tiered_extractor_v{n}"
    clsname = f"TieredExtractorV{n}"
    module = importlib.import_module(modname)
    cls = getattr(module, clsname)
    return cls()


def _select_fixture_set(limit: Optional[int]):
    """Load fixtures from storage/gold_holdout and return a FixtureHarness."""
    from app.ml.champion_challenger.fixture_harness import FixtureHarness
    harness = FixtureHarness.from_storage(limit=limit)
    return harness


async def _run_one(version_tag: str, harness) -> tuple[dict, float]:
    extractor = _load_extractor_instance(version_tag)
    report = await harness.run(extractor)
    return {
        "version": version_tag,
        "fixtures": report.fixtures_total,
        "elapsed_s": round(report.elapsed_s, 2),
        "composite": report.composite,
        "axes": report.axes,
        "per_fixture": [
            {
                "domain": r.domain,
                "extracted": r.extracted_count,
                "fields": round(r.fields_score, 2),
                "quality_ok": r.quality_passed,
                "error": r.error,
            }
            for r in report.results
        ],
    }, report.composite


async def main() -> int:
    parser = argparse.ArgumentParser(description="Offline fixture-based verification for a challenger")
    parser.add_argument("--version", help="Challenger version tag, e.g. v92")
    parser.add_argument("--champion", help="Champion version tag for comparison, e.g. v91")
    parser.add_argument("--challenger", help="Alias for --version when --champion is also given")
    parser.add_argument("--tolerance", type=float, default=2.0,
                        help="Composite points below champion before we fail (default 2.0)")
    parser.add_argument("--limit", type=int, help="Limit fixtures to first N (for dry runs)")
    parser.add_argument("--json", action="store_true", help="Print the JSON report to stdout")
    args = parser.parse_args()

    sys.path.insert(0, os.path.join(os.path.dirname(PROJECT_DIR), "backend"))

    challenger_tag = args.version or args.challenger
    if not challenger_tag:
        logger.error("Must supply --version (or --challenger)")
        return 2

    try:
        harness = _select_fixture_set(args.limit)
    except Exception as e:  # noqa: BLE001
        logger.error("failed to build fixture harness: %s", e)
        return 2
    if not harness.fixtures:
        logger.warning(
            "no fixtures discovered — run build_gold_holdout + build_silver_labels first"
        )
        return 2

    t_start = time.monotonic()
    try:
        challenger_report, challenger_score = await _run_one(challenger_tag, harness)
    except Exception as e:  # noqa: BLE001
        logger.error("challenger %s failed to run: %s", challenger_tag, e)
        return 2

    champion_score: Optional[float] = None
    champion_report: Optional[dict] = None
    if args.champion:
        try:
            champion_report, champion_score = await _run_one(args.champion, harness)
        except Exception as e:  # noqa: BLE001
            logger.warning("champion %s could not run for comparison: %s", args.champion, e)

    elapsed = round(time.monotonic() - t_start, 2)
    payload = {
        "elapsed_s": elapsed,
        "tolerance": args.tolerance,
        "champion": champion_report,
        "challenger": challenger_report,
        "delta": round(challenger_score - champion_score, 2) if champion_score is not None else None,
    }

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    (REPORT_DIR / f"{challenger_tag}.json").write_text(json.dumps(payload, indent=2, default=str))

    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        logger.info(
            "challenger %s composite=%.2f (%.2fs, %d fixtures)",
            challenger_tag, challenger_score, elapsed, len(harness.fixtures),
        )
        if champion_score is not None:
            delta = challenger_score - champion_score
            logger.info(
                "champion %s composite=%.2f, delta=%+.2f (tolerance=%.2f)",
                args.champion, champion_score, delta, args.tolerance,
            )

    # Pass/fail verdict
    if champion_score is not None and challenger_score + args.tolerance < champion_score:
        logger.error("FAIL: challenger regressed beyond tolerance — aborting cycle")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
