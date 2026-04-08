"""Add codex_improvement_runs table for tracking auto-improve iterations.

Revision ID: 0025
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
import uuid

revision = "0025"
down_revision = "0024"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "codex_improvement_runs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("source_model_id", UUID(as_uuid=True),
                  sa.ForeignKey("ml_models.id", ondelete="SET NULL"), nullable=True),
        sa.Column("test_run_id", UUID(as_uuid=True),
                  sa.ForeignKey("ml_model_test_runs.id", ondelete="SET NULL"), nullable=True),
        sa.Column("output_model_id", UUID(as_uuid=True),
                  sa.ForeignKey("ml_models.id", ondelete="SET NULL"), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'analysing'")),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("source_model_name", sa.Text(), nullable=True),
        sa.Column("output_model_name", sa.Text(), nullable=True),
        sa.Column("test_winner", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_index("ix_codex_improvement_runs_source_model_id",
                    "codex_improvement_runs", ["source_model_id"])
    op.create_index("ix_codex_improvement_runs_started_at",
                    "codex_improvement_runs", ["started_at"])


def downgrade():
    op.drop_table("codex_improvement_runs")
