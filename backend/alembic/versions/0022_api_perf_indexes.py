"""Missing indexes for API response time — career_pages sort/join + analytics composites.

career_pages ORDER BY created_at DESC caused a full seq-scan + in-memory sort on every
Sites page load. The company_id and is_active indexes are needed for the JOIN and
WHERE filters respectively.

The jobs composite index covering (is_active, is_canonical, company_id) eliminates
the most expensive market-breakdown and quality-by-site aggregation scans.

Revision ID: 0022
Revises: 0021
"""

from alembic import op

revision = "0022"
down_revision = "0021"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("COMMIT")  # CONCURRENTLY requires no open transaction

    # ── career_pages — all indexes missing at baseline ────────────────────────
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_career_pages_created_at_desc
        ON career_pages (created_at DESC)
    """)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_career_pages_company_id
        ON career_pages (company_id)
    """)
    # Partial index for the common active-only filter
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_career_pages_active_created
        ON career_pages (created_at DESC)
        WHERE is_active = true
    """)

    # ── jobs — composite for market_breakdown / stats analytics ───────────────
    # Covers: WHERE is_active AND is_canonical  GROUP BY company_id
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_jobs_active_canonical_company
        ON jobs (company_id)
        WHERE is_active = true AND is_canonical = true
    """)

    # ── Refresh planner statistics ─────────────────────────────────────────────
    op.execute("ANALYZE career_pages")
    op.execute("ANALYZE jobs")


def downgrade():
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_career_pages_created_at_desc")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_career_pages_company_id")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_career_pages_active_created")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_jobs_active_canonical_company")
