"""Add test data tables and ML model tables.

Revision ID: 0024
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid

revision = "0024"
down_revision = "0023"
branch_labels = None
depends_on = None


def upgrade():
    # -- Test data tables (raw CSV imports) --

    op.create_table(
        "crawler_test_data",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("external_id", sa.Text(), nullable=False),
        sa.Column("job_site_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("crawler_type", sa.Text(), nullable=False),
        sa.Column("country", sa.Text(), nullable=True),
        sa.Column("country_code", sa.Text(), nullable=True),
        sa.Column("frequency", sa.Integer(), nullable=True),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("current_status", sa.Text(), nullable=True),
        sa.Column("disabled", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("statistics_data", JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "job_site_test_data",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("external_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("site_type", sa.Text(), nullable=False),
        sa.Column("num_of_jobs", sa.Integer(), nullable=True),
        sa.Column("expected_job_count", sa.Integer(), nullable=True),
        sa.Column("disabled", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("uncrawlable_reason", sa.Text(), nullable=True),
        sa.Column("tags", JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "site_url_test_data",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("site_id", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "site_wrapper_test_data",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("external_id", sa.Text(), nullable=False),
        sa.Column("crawler_id", sa.Text(), nullable=False),
        sa.Column("selectors", JSONB(), nullable=False),
        sa.Column("paths_config", JSONB(), nullable=True),
        sa.Column("has_detail_page", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )

    # -- ML experiment tables --

    op.create_table(
        "ml_models",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("model_type", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("config", JSONB(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'new'")),
        sa.Column("version", sa.Integer(), server_default=sa.text("1")),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "ml_model_test_runs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("model_id", UUID(as_uuid=True), sa.ForeignKey("ml_models.id", ondelete="CASCADE"), nullable=False),
        sa.Column("test_name", sa.Text(), nullable=True),
        sa.Column("total_tests", sa.Integer(), server_default=sa.text("0")),
        sa.Column("tests_passed", sa.Integer(), server_default=sa.text("0")),
        sa.Column("tests_failed", sa.Integer(), server_default=sa.text("0")),
        sa.Column("accuracy", sa.Float(), nullable=True),
        sa.Column("precision_score", sa.Float(), nullable=True),
        sa.Column("recall", sa.Float(), nullable=True),
        sa.Column("f1_score", sa.Float(), nullable=True),
        sa.Column("test_config", JSONB(), nullable=True),
        sa.Column("results_detail", JSONB(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Indexes for common lookups
    op.create_index("ix_crawler_test_data_external_id", "crawler_test_data", ["external_id"])
    op.create_index("ix_job_site_test_data_external_id", "job_site_test_data", ["external_id"])
    op.create_index("ix_site_url_test_data_site_id", "site_url_test_data", ["site_id"])
    op.create_index("ix_site_wrapper_test_data_crawler_id", "site_wrapper_test_data", ["crawler_id"])
    op.create_index("ix_ml_model_test_runs_model_id", "ml_model_test_runs", ["model_id"])


def downgrade():
    op.drop_table("ml_model_test_runs")
    op.drop_table("ml_models")
    op.drop_table("site_wrapper_test_data")
    op.drop_table("site_url_test_data")
    op.drop_table("job_site_test_data")
    op.drop_table("crawler_test_data")
