"""Job deduplication fields — canonical tracking.

Revision ID: 0004
Revises: 0003
Create Date: 2026-03-19
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add deduplication fields to jobs
    op.add_column("jobs", sa.Column(
        "canonical_job_id",
        UUID(as_uuid=True),
        sa.ForeignKey("jobs.id", ondelete="SET NULL"),
        nullable=True,
        comment="Points to the canonical (best) version of this job. NULL = this job IS canonical.",
    ))
    op.add_column("jobs", sa.Column(
        "is_canonical",
        sa.Boolean(),
        nullable=False,
        server_default="true",
        comment="True if this is the best/canonical version. False = a duplicate exists.",
    ))
    op.add_column("jobs", sa.Column(
        "duplicate_count",
        sa.Integer(),
        nullable=False,
        server_default="0",
        comment="On canonical jobs: how many lower-quality duplicates exist.",
    ))
    op.add_column("jobs", sa.Column(
        "dedup_score",
        sa.Float(),
        nullable=True,
        comment="Similarity score to canonical job (0.0-1.0). NULL if no dedup run.",
    ))

    # Indexes for fast dedup queries
    op.create_index("ix_jobs_canonical_job_id", "jobs", ["canonical_job_id"])
    op.create_index("ix_jobs_is_canonical", "jobs", ["is_canonical"])


def downgrade() -> None:
    op.drop_index("ix_jobs_is_canonical", "jobs")
    op.drop_index("ix_jobs_canonical_job_id", "jobs")
    op.drop_column("jobs", "dedup_score")
    op.drop_column("jobs", "duplicate_count")
    op.drop_column("jobs", "is_canonical")
    op.drop_column("jobs", "canonical_job_id")
