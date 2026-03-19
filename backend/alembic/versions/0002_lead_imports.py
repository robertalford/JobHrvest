"""Lead imports table

Revision ID: 0002
Revises: 0001
Create Date: 2026-03-19

"""
from typing import Sequence, Union
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB
from alembic import op

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "lead_imports",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        # Source data from CSV
        sa.Column("country_id", sa.String(10), nullable=False),
        sa.Column("advertiser_name", sa.Text, nullable=False),
        sa.Column("origin_domain", sa.Text, nullable=False),
        sa.Column("sample_linkout_url", sa.Text),
        sa.Column("ad_origin_category", sa.Text),
        sa.Column("expected_job_count", sa.Integer),
        sa.Column("origin_rank", sa.Integer),
        # Import outcome
        sa.Column("status", sa.Text, nullable=False, default="pending"),
        # pending | success | failed | skipped | blocked
        sa.Column("company_id", UUID(as_uuid=True), sa.ForeignKey("companies.id", ondelete="SET NULL")),
        sa.Column("career_pages_found", sa.Integer, default=0),
        sa.Column("jobs_extracted", sa.Integer, default=0),
        sa.Column("error_message", sa.Text),
        sa.Column("error_details", JSONB),
        sa.Column("skip_reason", sa.Text),
        # Timestamps
        sa.Column("imported_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_lead_imports_country_id", "lead_imports", ["country_id"])
    op.create_index("ix_lead_imports_status", "lead_imports", ["status"])
    op.create_index("ix_lead_imports_origin_domain", "lead_imports", ["origin_domain"])
    op.create_index("ix_lead_imports_category", "lead_imports", ["ad_origin_category"])


def downgrade() -> None:
    op.drop_index("ix_lead_imports_category")
    op.drop_index("ix_lead_imports_origin_domain")
    op.drop_index("ix_lead_imports_status")
    op.drop_index("ix_lead_imports_country_id")
    op.drop_table("lead_imports")
