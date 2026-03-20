"""Add lead_import_batches table and batch_id to lead_imports.

Revision ID: 0006
Revises: 0005
Create Date: 2026-03-19
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "0006"
down_revision = "0005_settings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "lead_import_batches",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("filename", sa.Text(), nullable=False),
        sa.Column("original_filename", sa.Text(), nullable=False),
        sa.Column("file_size_bytes", sa.Integer()),
        sa.Column("total_rows", sa.Integer()),
        sa.Column("validation_status", sa.String(20), server_default="pending"),
        sa.Column("validation_errors", JSONB()),
        sa.Column("import_status", sa.String(20), server_default="pending"),
        sa.Column("imported_leads", sa.Integer(), server_default="0"),
        sa.Column("failed_leads", sa.Integer(), server_default="0"),
        sa.Column("blocked_leads", sa.Integer(), server_default="0"),
        sa.Column("skipped_leads", sa.Integer(), server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("import_started_at", sa.DateTime(timezone=True)),
        sa.Column("import_completed_at", sa.DateTime(timezone=True)),
        sa.Column("error_message", sa.Text()),
    )

    op.add_column(
        "lead_imports",
        sa.Column("batch_id", UUID(as_uuid=True),
                  sa.ForeignKey("lead_import_batches.id", ondelete="SET NULL"),
                  nullable=True, index=True),
    )


def downgrade() -> None:
    op.drop_column("lead_imports", "batch_id")
    op.drop_table("lead_import_batches")
