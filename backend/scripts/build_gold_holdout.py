"""CLI: materialise a frozen GOLD holdout from lead_imports.

Usage (from inside a backend container):

    python -m scripts.build_gold_holdout \
        --name au_baseline_v1 \
        --market AU \
        --max-domains 100

Snapshots are written under SNAPSHOT_ROOT (default /storage/gold_holdout).
After this script finishes, the holdout set is FROZEN — its membership and
snapshots will not change. To re-build, supply a new --name (e.g. v2).

Manual follow-up: GoldHoldoutJob rows must be entered by a human verifier
before the holdout becomes useful for extraction-accuracy metrics. The
classifier-level metrics (precision/recall/F1) work without them, since
every domain in the holdout is by definition a known job-hosting site.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path

from app.crawlers.http_client import ResilientHTTPClient
from app.db.base import AsyncSessionLocal
from app.ml.champion_challenger.holdout_builder import GoldHoldoutBuilder

logger = logging.getLogger("build_gold_holdout")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class _SimpleHttpAdapter:
    """Adapt ResilientHTTPClient to the (status, body, content_type) tuple
    contract that GoldHoldoutBuilder expects.
    """

    def __init__(self) -> None:
        self.client = ResilientHTTPClient()

    async def fetch(self, url: str) -> tuple[int, bytes, str]:
        # Lightweight wrapper — production fetch logic lives in the resilient
        # client; we only need the response body and content_type for snapshot
        # storage.
        result = await self.client.get(url)  # ResilientHTTPClient must expose .get
        body = result.content if hasattr(result, "content") else (result.body or b"")
        ctype = ""
        if hasattr(result, "headers"):
            ctype = result.headers.get("content-type", "")
        return getattr(result, "status_code", 0), body, ctype


async def main() -> int:
    parser = argparse.ArgumentParser(description="Build a frozen GOLD holdout from lead_imports")
    parser.add_argument("--name", required=True, help="Holdout set name (e.g. au_baseline_v1)")
    parser.add_argument("--market", default="AU", help="Market id to source leads from")
    parser.add_argument("--max-domains", type=int, default=100)
    parser.add_argument("--snapshot-root", default=os.getenv("SNAPSHOT_ROOT", "/storage/gold_holdout"))
    parser.add_argument("--description", default=None)
    parser.add_argument("--no-require-expected-count", action="store_true",
                        help="Include leads even if expected_job_count is missing")
    args = parser.parse_args()

    builder = GoldHoldoutBuilder(
        snapshot_root=Path(args.snapshot_root),
        http_client=_SimpleHttpAdapter(),
    )

    async with AsyncSessionLocal() as session:
        report = await builder.build(
            session,
            name=args.name,
            market_id=args.market,
            max_domains=args.max_domains,
            require_expected_count=not args.no_require_expected_count,
            description=args.description,
        )

    logger.info(
        "Done — set_id=%s domains_added=%d snapshots=%d failed=%d skipped=%d",
        report.set_id, report.domains_added,
        report.snapshots_saved, report.snapshots_failed,
        report.skipped_duplicate,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
