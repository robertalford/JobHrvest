"""Add description_enriched_at to jobs for tracking enrichment state.

Revision ID: 0011
Revises: 0010
Create Date: 2026-03-19
"""
from alembic import op
import sqlalchemy as sa

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "jobs",
        sa.Column("description_enriched_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Index to efficiently find jobs needing enrichment
    op.create_index(
        "ix_jobs_description_enrichment",
        "jobs",
        ["description_enriched_at", "is_active"],
    )


def downgrade():
    op.drop_index("ix_jobs_description_enrichment", table_name="jobs")
    op.drop_column("jobs", "description_enriched_at")
