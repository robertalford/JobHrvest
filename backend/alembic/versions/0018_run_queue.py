"""Persistent run queues for all 4 pipeline stages.

Each queue type stores pending items to process. Workers drain items continuously.
Scheduled population tasks add items every 2h (catching anything missed).
Auto-enqueue hooks add items on create/status change events.

Revision ID: 0018
"""
from alembic import op

revision = "0018"
down_revision = "0017"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE IF NOT EXISTS run_queue (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            queue_type TEXT NOT NULL,
            item_id UUID,
            item_type TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            priority INT NOT NULL DEFAULT 5,
            added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            processing_started_at TIMESTAMPTZ,
            processing_completed_at TIMESTAMPTZ,
            error_message TEXT,
            added_by TEXT DEFAULT 'system'
        )
    """)
    # Prevent duplicate pending items for same (queue_type, item_id)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ix_run_queue_unique_pending
          ON run_queue (queue_type, item_id)
          WHERE status = 'pending'
    """)
    # Index for draining
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_run_queue_drain
          ON run_queue (queue_type, priority DESC, added_at ASC)
          WHERE status = 'pending'
    """)
    # Populate initial queues from existing data
    # company_config: all non-ok active companies
    op.execute("""
        INSERT INTO run_queue (queue_type, item_id, item_type, added_by)
        SELECT 'company_config', id, 'company', 'migration_seed'
        FROM companies
        WHERE is_active = true AND company_status != 'ok'
        ON CONFLICT DO NOTHING
    """)
    # site_config: all non-ok active career pages
    op.execute("""
        INSERT INTO run_queue (queue_type, item_id, item_type, added_by)
        SELECT 'site_config', id, 'career_page', 'migration_seed'
        FROM career_pages
        WHERE is_active = true AND site_status != 'ok'
        ON CONFLICT DO NOTHING
    """)
    # job_crawling: all active career pages due for crawl
    op.execute("""
        INSERT INTO run_queue (queue_type, item_id, item_type, added_by)
        SELECT 'job_crawling', id, 'career_page', 'migration_seed'
        FROM career_pages
        WHERE is_active = true
        ON CONFLICT DO NOTHING
    """)
    # discovery: all active aggregator sources
    op.execute("""
        INSERT INTO run_queue (queue_type, item_id, item_type, added_by)
        SELECT 'discovery', id, 'aggregator_source', 'migration_seed'
        FROM aggregator_sources
        WHERE is_active = true
        ON CONFLICT DO NOTHING
    """)


def downgrade():
    op.execute("DROP INDEX IF EXISTS ix_run_queue_unique_pending")
    op.execute("DROP INDEX IF EXISTS ix_run_queue_drain")
    op.execute("DROP TABLE IF EXISTS run_queue")
