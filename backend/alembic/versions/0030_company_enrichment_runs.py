"""Add company enrichment batch run tables.

Revision ID: 0030
Revises: 0029_evo_population
Create Date: 2026-04-15
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "0030"
down_revision = "0029_evo_population"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "company_enrichment_runs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("filename", sa.Text(), nullable=False),
        sa.Column("original_filename", sa.Text(), nullable=False),
        sa.Column("output_filename", sa.Text()),
        sa.Column("file_size_bytes", sa.Integer()),
        sa.Column("total_rows", sa.Integer(), server_default="0"),
        sa.Column("validation_status", sa.String(20), server_default="pending"),
        sa.Column("validation_errors", JSONB()),
        sa.Column("run_status", sa.String(20), server_default="pending"),
        sa.Column("completed_rows", sa.Integer(), server_default="0"),
        sa.Column("failed_rows", sa.Integer(), server_default="0"),
        sa.Column("skipped_rows", sa.Integer(), server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("run_started_at", sa.DateTime(timezone=True)),
        sa.Column("run_completed_at", sa.DateTime(timezone=True)),
        sa.Column("error_message", sa.Text()),
    )

    op.create_table(
        "company_enrichment_rows",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("run_id", UUID(as_uuid=True), sa.ForeignKey("company_enrichment_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("row_number", sa.Integer(), nullable=False),
        sa.Column("company", sa.Text(), nullable=False),
        sa.Column("country", sa.Text(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("job_page_url", sa.Text()),
        sa.Column("job_count", sa.Text()),
        sa.Column("comment", sa.Text()),
        sa.Column("raw_response_text", sa.Text()),
        sa.Column("raw_response_json", JSONB()),
        sa.Column("error_message", sa.Text()),
        sa.Column("attempt_count", sa.Integer(), server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
    )

    op.create_index("ix_company_enrichment_rows_run_id", "company_enrichment_rows", ["run_id"])
    op.create_index("ix_company_enrichment_rows_status", "company_enrichment_rows", ["status"])
    op.create_index("ix_company_enrichment_rows_company", "company_enrichment_rows", ["company"])
    op.create_index("ix_company_enrichment_rows_country", "company_enrichment_rows", ["country"])


def downgrade() -> None:
    op.drop_index("ix_company_enrichment_rows_country", table_name="company_enrichment_rows")
    op.drop_index("ix_company_enrichment_rows_company", table_name="company_enrichment_rows")
    op.drop_index("ix_company_enrichment_rows_status", table_name="company_enrichment_rows")
    op.drop_index("ix_company_enrichment_rows_run_id", table_name="company_enrichment_rows")
    op.drop_table("company_enrichment_rows")
    op.drop_table("company_enrichment_runs")
