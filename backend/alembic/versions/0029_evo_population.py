"""evo population tables

Revision ID: 0029_evo_population
Revises: 0028
Create Date: 2026-04-15 01:20:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0029_evo_population"
down_revision = "0028"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "evo_individuals",
        sa.Column("version_tag", sa.Text(), primary_key=True),
        sa.Column("ml_model_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("ml_models.id", ondelete="SET NULL")),
        sa.Column("parent_tag", sa.Text(), nullable=True),
        sa.Column("island_id", sa.SmallInteger(), nullable=False),
        sa.Column("focus_axis", sa.Text(), nullable=False),
        sa.Column("behaviour_cell", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("fixture_composite", sa.Float(), nullable=True),
        sa.Column("ab_composite", sa.Float(), nullable=True),
        sa.Column("axes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("loc", sa.Integer(), nullable=True),
        sa.Column("file_path", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("idx_evo_individuals_island", "evo_individuals", ["island_id", "created_at"], unique=False)
    op.create_index("idx_evo_individuals_cell", "evo_individuals", ["behaviour_cell"], unique=False)

    op.create_table(
        "evo_cycles",
        sa.Column("cycle_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("n_candidates", sa.SmallInteger(), nullable=True),
        sa.Column("promoted_tag", sa.Text(), nullable=True),
        sa.Column("notes", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )

    op.create_table(
        "evo_population_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("cycle_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("evo_cycles.cycle_id", ondelete="CASCADE")),
        sa.Column("version_tag", sa.Text(), nullable=True),
        sa.Column("event", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("evo_population_events")
    op.drop_table("evo_cycles")
    op.drop_index("idx_evo_individuals_cell", table_name="evo_individuals")
    op.drop_index("idx_evo_individuals_island", table_name="evo_individuals")
    op.drop_table("evo_individuals")
