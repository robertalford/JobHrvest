"""Queue manager — helpers for enqueueing, claiming, and completing run queue items."""
import logging
import uuid

logger = logging.getLogger(__name__)


async def enqueue(db, queue_type: str, item_id: uuid.UUID = None, priority: int = 5, added_by: str = "system") -> bool:
    """Add item to queue if not already pending. Returns True if a new item was added."""
    from sqlalchemy import text
    try:
        await db.execute(text("""
            INSERT INTO run_queue (queue_type, item_id, item_type, priority, added_by)
            VALUES (:qt, :item_id, :item_type, :priority, :added_by)
            ON CONFLICT DO NOTHING
        """), {
            "qt": queue_type,
            "item_id": str(item_id) if item_id else None,
            "item_type": _item_type_for(queue_type),
            "priority": priority,
            "added_by": added_by,
        })
        return True
    except Exception as e:
        logger.debug(f"enqueue({queue_type}, {item_id}) skipped: {e}")
        return False


def _item_type_for(queue_type: str) -> str:
    return {
        "discovery": "aggregator_source",
        "company_config": "company",
        "site_config": "career_page",
        "job_crawling": "career_page",
    }.get(queue_type, "unknown")


async def claim_batch(db, queue_type: str, batch_size: int = 20):
    """Claim N pending items atomically (FOR UPDATE SKIP LOCKED). Returns list of rows."""
    from sqlalchemy import text
    result = await db.execute(text("""
        UPDATE run_queue
        SET status = 'processing', processing_started_at = NOW()
        WHERE id IN (
            SELECT id FROM run_queue
            WHERE queue_type = :qt AND status = 'pending'
            ORDER BY priority DESC, added_at ASC
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, item_id
    """), {"qt": queue_type, "batch_size": batch_size})
    return result.fetchall()


async def complete(db, queue_item_id: uuid.UUID):
    from sqlalchemy import text
    await db.execute(text("""
        UPDATE run_queue SET status = 'done', processing_completed_at = NOW()
        WHERE id = :id
    """), {"id": str(queue_item_id)})


async def fail(db, queue_item_id: uuid.UUID, error: str):
    from sqlalchemy import text
    await db.execute(text("""
        UPDATE run_queue SET status = 'failed', processing_completed_at = NOW(),
               error_message = :err
        WHERE id = :id
    """), {"id": str(queue_item_id), "err": error[:500]})


async def reset_stale_processing(db, stale_after_minutes: int = 120) -> int:
    """Delete items stuck in 'processing' — they'll be re-enqueued by the next beat cycle.
    Also resets any that have no pending duplicate. Returns count of rows affected."""
    from sqlalchemy import text
    result = await db.execute(text("""
        DELETE FROM run_queue
        WHERE status = 'processing'
          AND processing_started_at < NOW() - (:minutes * INTERVAL '1 minute')
        RETURNING id
    """), {"minutes": stale_after_minutes})
    count = len(result.fetchall())
    if count:
        logger.info(f"reset_stale_processing: removed {count} stale items (>{stale_after_minutes}m)")
    return count


async def get_stats(db) -> dict:
    """Return queue depths by type and status."""
    from sqlalchemy import text
    result = await db.execute(text("""
        SELECT queue_type, status, COUNT(*) as cnt
        FROM run_queue
        GROUP BY queue_type, status
        ORDER BY queue_type, status
    """))
    stats = {}
    for row in result:
        qt = row[0]
        status = row[1]
        cnt = row[2]
        if qt not in stats:
            stats[qt] = {"pending": 0, "processing": 0, "done": 0, "failed": 0}
        stats[qt][status] = cnt
    return stats
