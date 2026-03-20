"""Add unique constraint on career_pages.url and deduplicate.

Revision ID: 0019
Revises: 0018
Create Date: 2026-03-19
"""
from alembic import op

revision = "0019"
down_revision = "0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Delete duplicate career_pages keeping the oldest (lowest created_at) per URL
    op.execute("""
        DELETE FROM career_pages
        WHERE id IN (
            SELECT id FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY url
                           ORDER BY created_at ASC, id ASC
                       ) AS rn
                FROM career_pages
            ) ranked
            WHERE rn > 1
        )
    """)

    # 2. Drop existing non-unique index and replace with unique
    op.execute("DROP INDEX IF EXISTS ix_career_pages_url")
    op.execute("""
        CREATE UNIQUE INDEX ix_career_pages_url_unique ON career_pages(url)
    """)

    # 3. Add unique constraint on companies.domain if somehow missing (safety)
    # (Already exists as companies_domain_key, this is a no-op guard)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_career_pages_url_unique")
    op.execute("CREATE INDEX ix_career_pages_url ON career_pages(url)")
