"""Backfill `ever_passed_sites` + `site_result_history` from existing test runs.

After migration 0028 ships, the two universality-gate tables are empty. Every
promotion gate that consults them (L2 ever-passed + L4 oscillation) therefore
has no data and silently passes — giving challengers a free ride.

This script replays every completed `ml_model_test_runs.results_detail` into:
  - ever_passed_sites (monotonic 'any version has passed this' set), using
    `app.ml.champion_challenger.stability.upsert_ever_passed`.
  - site_result_history (append-only per-site verdict log), using
    `stability.record_run_history`.

Runs are replayed in chronological order so the ratchet picks the genuine best
per-site version, not whichever run was processed last.

Run inside the api container:
    docker exec jobharvest-api python -m scripts.backfill_ever_passed
"""

import asyncio
import logging
import sys
from uuid import UUID

from sqlalchemy import select

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("backfill_ever_passed")


async def _main() -> int:
    # Defer imports so the module can be loaded without app wiring for help text.
    from app.db.base import AsyncSessionLocal
    from app.models.ml_model import MLModelTestRun, MLModel
    from app.ml.champion_challenger import stability

    async with AsyncSessionLocal() as db:
        # Chronological order — earlier runs seed the set, later runs ratchet it.
        result = await db.execute(
            select(MLModelTestRun, MLModel)
            .join(MLModel, MLModel.id == MLModelTestRun.model_id)
            .where(MLModelTestRun.status == "completed")
            .order_by(MLModelTestRun.completed_at.asc())
        )
        rows = list(result.all())
        logger.info("Replaying %d completed test runs", len(rows))

        total_history = 0
        total_ratcheted = 0
        for run, model in rows:
            rd = run.results_detail or {}
            sites = rd.get("sites") or []
            if not sites:
                continue
            # Record history for every run.
            try:
                await stability.record_run_history(
                    db, run_id=run.id, model_id=model.id,
                    model_name=model.name, site_results=sites,
                )
                total_history += len(sites)
            except Exception as e:  # noqa: BLE001
                logger.warning("history failed for run %s: %s", run.id, e)

            # Ratchet the ever-passed set. Every promoted version is a validated
            # win by definition; legacy runs may not have promotion metadata,
            # so we upsert from every run and rely on the ratchet's monotonic
            # `best_composite` check to pick the winner per site.
            try:
                n = await stability.upsert_ever_passed(
                    db, run_id=run.id, model_name=model.name,
                    site_results=sites,
                )
                total_ratcheted += n
            except Exception as e:  # noqa: BLE001
                logger.warning("ratchet failed for run %s: %s", run.id, e)

            await db.commit()
            logger.info(
                "processed run %s (%s, %d sites)",
                str(run.id)[:8], model.name, len(sites),
            )

        logger.info(
            "Done. history rows appended: %d, ever-passed upserts: %d",
            total_history, total_ratcheted,
        )
        # Final sanity check — print counts.
        from sqlalchemy import text
        ever_count = (await db.execute(
            text("SELECT COUNT(*) FROM ever_passed_sites"),
        )).scalar()
        hist_count = (await db.execute(
            text("SELECT COUNT(*) FROM site_result_history"),
        )).scalar()
        logger.info("ever_passed_sites: %d rows", ever_count)
        logger.info("site_result_history: %d rows", hist_count)

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
