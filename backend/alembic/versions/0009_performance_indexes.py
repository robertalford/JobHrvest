"""Add performance indexes for analytics and crawl queries.

Revision ID: 0009
Revises: 0008
Create Date: 2026-03-19
"""
from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None

# CONCURRENTLY indexes cannot run inside a transaction
# This flag disables auto-begin so each statement runs outside a transaction
def upgrade():
    op.execute("COMMIT")  # end the alembic transaction
    # ── Jobs table — critical missing indexes ──────────────────────────────────
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_jobs_is_active_quality_score
        ON jobs (quality_score)
        WHERE is_active = true
    """)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_jobs_is_active_first_seen
        ON jobs (first_seen_at DESC)
        WHERE is_active = true
    """)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_jobs_is_active_canonical
        ON jobs (is_canonical)
        WHERE is_active = true
    """)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_jobs_quality_score_not_null
        ON jobs (quality_score)
        WHERE quality_score IS NOT NULL AND is_active = true
    """)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_jobs_company_active
        ON jobs (company_id)
        WHERE is_active = true
    """)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_jobs_location_country_active
        ON jobs (location_country)
        WHERE is_active = true
    """)

    # ── Crawl logs — stats queries ─────────────────────────────────────────────
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_crawl_logs_status_started
        ON crawl_logs (status, started_at DESC)
    """)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_crawl_logs_started_at_desc
        ON crawl_logs (started_at DESC)
    """)

    # ── Companies — scheduling ─────────────────────────────────────────────────
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_companies_next_crawl_active
        ON companies (next_crawl_at)
        WHERE is_active = true
    """)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_companies_quality_score
        ON companies (quality_score)
        WHERE quality_score IS NOT NULL
    """)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_companies_market_code
        ON companies (market_code)
        WHERE is_active = true
    """)


def downgrade():
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_jobs_is_active_quality_score")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_jobs_is_active_first_seen")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_jobs_is_active_canonical")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_jobs_quality_score_not_null")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_jobs_company_active")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_jobs_location_country_active")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_crawl_logs_status_started")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_crawl_logs_started_at_desc")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_companies_next_crawl_active")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_companies_quality_score")
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_companies_market_code")
