"""B-tree index on jobs for the default list query sort order.

The jobs list endpoint defaults to is_active=true, is_canonical=true,
ORDER BY first_seen_at DESC LIMIT 50. Without a supporting index,
PostgreSQL scans the full index or table and sorts in memory.

A composite partial index on (first_seen_at DESC) WHERE is_active AND is_canonical
lets the planner do a forward index scan in sorted order, stopping after LIMIT rows.

Revision ID: 0016
"""

from alembic import op

revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None


def upgrade():
    # Composite partial index for the default unfiltered jobs list query
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_jobs_active_canonical_seen
          ON jobs (first_seen_at DESC)
          WHERE is_active = true AND is_canonical = true
    """)

    op.execute("ANALYZE jobs")


def downgrade():
    op.execute("DROP INDEX IF EXISTS ix_jobs_active_canonical_seen")
