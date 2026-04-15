"""Add worker ownership to company enrichment rows.

Revision ID: 0031
Revises: 0030
Create Date: 2026-04-15
"""
from alembic import op
import sqlalchemy as sa

revision = "0031"
down_revision = "0030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("company_enrichment_rows", sa.Column("worker_id", sa.Text(), nullable=True))
    op.create_index("ix_company_enrichment_rows_worker_id", "company_enrichment_rows", ["worker_id"])


def downgrade() -> None:
    op.drop_index("ix_company_enrichment_rows_worker_id", table_name="company_enrichment_rows")
    op.drop_column("company_enrichment_rows", "worker_id")
