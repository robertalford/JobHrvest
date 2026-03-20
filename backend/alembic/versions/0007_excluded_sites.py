"""Add excluded_sites table.

Revision ID: 0007
Revises: 0006
Create Date: 2026-03-19
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "excluded_sites",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("domain", sa.Text(), nullable=False, unique=True),
        sa.Column("company_name", sa.Text()),
        sa.Column("site_url", sa.Text()),
        sa.Column("site_type", sa.String(50)),
        sa.Column("country_code", sa.String(10)),
        sa.Column("expected_job_count", sa.Integer()),
        sa.Column("reason", sa.Text()),
        sa.Column("source_file", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_excluded_sites_domain", "excluded_sites", ["domain"])


def downgrade() -> None:
    op.drop_index("ix_excluded_sites_domain", "excluded_sites")
    op.drop_table("excluded_sites")
