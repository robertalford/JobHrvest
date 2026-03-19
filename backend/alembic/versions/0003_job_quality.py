"""Job quality scoring columns

Revision ID: 0003
Revises: 0002
Create Date: 2026-03-19

"""
from typing import Sequence, Union
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add quality columns to jobs
    op.add_column("jobs", sa.Column("quality_score", sa.Float, nullable=True))
    op.add_column("jobs", sa.Column("quality_completeness", sa.Float, nullable=True))
    op.add_column("jobs", sa.Column("quality_description", sa.Float, nullable=True))
    op.add_column("jobs", sa.Column("quality_issues", JSONB, nullable=True))
    op.add_column("jobs", sa.Column("quality_flags", JSONB, nullable=True))
    op.add_column("jobs", sa.Column("quality_scored_at", sa.DateTime(timezone=True), nullable=True))

    op.create_index("ix_jobs_quality_score", "jobs", ["quality_score"])

    # Add quality columns to companies
    op.add_column("companies", sa.Column("quality_score", sa.Float, nullable=True))
    op.add_column("companies", sa.Column("quality_scored_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("companies", "quality_scored_at")
    op.drop_column("companies", "quality_score")
    op.drop_index("ix_jobs_quality_score", table_name="jobs")
    op.drop_column("jobs", "quality_scored_at")
    op.drop_column("jobs", "quality_flags")
    op.drop_column("jobs", "quality_issues")
    op.drop_column("jobs", "quality_description")
    op.drop_column("jobs", "quality_completeness")
    op.drop_column("jobs", "quality_score")
