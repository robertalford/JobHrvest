"""Add review_feedback table and quality_override column to jobs.

Revision ID: 0008
Revises: 0007
Create Date: 2026-03-19
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # quality_override flag on jobs — user-confirmed good quality despite low score
    op.add_column("jobs", sa.Column("quality_override", sa.Boolean(), nullable=True))

    # review_feedback — stores human decisions for model training
    op.create_table(
        "review_feedback",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("review_type", sa.String(20), nullable=False),   # 'quality' | 'duplicate'
        sa.Column("decision", sa.String(20), nullable=False),       # 'confirm' | 'overrule'
        sa.Column("canonical_job_id", UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True),
        sa.Column("features_snapshot", JSONB),                      # signals used for the determination
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_review_feedback_job_id", "review_feedback", ["job_id"])
    op.create_index("ix_review_feedback_type", "review_feedback", ["review_type"])


def downgrade() -> None:
    op.drop_index("ix_review_feedback_type", "review_feedback")
    op.drop_index("ix_review_feedback_job_id", "review_feedback")
    op.drop_table("review_feedback")
    op.drop_column("jobs", "quality_override")
