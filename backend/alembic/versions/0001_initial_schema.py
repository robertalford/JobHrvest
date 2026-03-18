"""Initial schema — all tables

Revision ID: 0001
Revises:
Create Date: 2026-03-19

"""
from typing import Sequence, Union
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB
from alembic import op

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # markets
    op.create_table(
        "markets",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("code", sa.String(10), unique=True, nullable=False),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("is_active", sa.Boolean, default=True),
        sa.Column("default_currency", sa.String(10)),
        sa.Column("locale", sa.String(20)),
        sa.Column("salary_parsing_config", JSONB, default={}),
        sa.Column("location_parsing_config", JSONB, default={}),
        sa.Column("aggregator_search_queries", JSONB, default={}),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # blocked_domains
    op.create_table(
        "blocked_domains",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("domain", sa.Text, unique=True, nullable=False),
        sa.Column("reason", sa.Text),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # aggregator_sources
    op.create_table(
        "aggregator_sources",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("base_url", sa.Text, nullable=False),
        sa.Column("market", sa.String(10), nullable=False),
        sa.Column("is_active", sa.Boolean, default=True),
        sa.Column("purpose", sa.Text, default="link_discovery_only"),
        sa.Column("last_link_harvest_at", sa.DateTime(timezone=True)),
    )

    # companies
    op.create_table(
        "companies",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("domain", sa.Text, unique=True, nullable=False),
        sa.Column("root_url", sa.Text, nullable=False),
        sa.Column("market_code", sa.String(10), sa.ForeignKey("markets.code"), default="AU"),
        sa.Column("discovered_via", sa.Text),
        sa.Column("ats_platform", sa.Text),
        sa.Column("ats_confidence", sa.Float),
        sa.Column("crawl_priority", sa.Integer, default=5),
        sa.Column("crawl_frequency_hours", sa.Integer, default=24),
        sa.Column("last_crawl_at", sa.DateTime(timezone=True)),
        sa.Column("next_crawl_at", sa.DateTime(timezone=True)),
        sa.Column("is_active", sa.Boolean, default=True),
        sa.Column("requires_js_rendering", sa.Boolean, default=False),
        sa.Column("anti_bot_level", sa.Text, default="none"),
        sa.Column("notes", sa.Text),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_companies_domain", "companies", ["domain"])

    # career_pages
    op.create_table(
        "career_pages",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("company_id", UUID(as_uuid=True), sa.ForeignKey("companies.id", ondelete="CASCADE"), nullable=False),
        sa.Column("url", sa.Text, nullable=False),
        sa.Column("page_type", sa.Text),
        sa.Column("discovery_method", sa.Text),
        sa.Column("discovery_confidence", sa.Float),
        sa.Column("is_primary", sa.Boolean, default=False),
        sa.Column("is_paginated", sa.Boolean, default=False),
        sa.Column("pagination_type", sa.Text),
        sa.Column("pagination_selector", sa.Text),
        sa.Column("requires_js_rendering", sa.Boolean, default=False),
        sa.Column("last_content_hash", sa.Text),
        sa.Column("last_crawled_at", sa.DateTime(timezone=True)),
        sa.Column("last_extraction_at", sa.DateTime(timezone=True)),
        sa.Column("is_active", sa.Boolean, default=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_career_pages_company_id", "career_pages", ["company_id"])
    op.create_index("ix_career_pages_url", "career_pages", ["url"])

    # jobs
    op.create_table(
        "jobs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("company_id", UUID(as_uuid=True), sa.ForeignKey("companies.id", ondelete="CASCADE"), nullable=False),
        sa.Column("career_page_id", UUID(as_uuid=True), sa.ForeignKey("career_pages.id", ondelete="SET NULL")),
        sa.Column("source_url", sa.Text, nullable=False),
        sa.Column("external_id", sa.Text),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("description", sa.Text),
        sa.Column("description_html", sa.Text),
        sa.Column("location_raw", sa.Text),
        sa.Column("location_city", sa.Text),
        sa.Column("location_state", sa.Text),
        sa.Column("location_country", sa.Text),
        sa.Column("is_remote", sa.Boolean),
        sa.Column("remote_type", sa.Text),
        sa.Column("employment_type", sa.Text),
        sa.Column("seniority_level", sa.Text),
        sa.Column("department", sa.Text),
        sa.Column("team", sa.Text),
        sa.Column("salary_raw", sa.Text),
        sa.Column("salary_min", sa.Numeric(12, 2)),
        sa.Column("salary_max", sa.Numeric(12, 2)),
        sa.Column("salary_currency", sa.Text),
        sa.Column("salary_period", sa.Text),
        sa.Column("requirements", sa.Text),
        sa.Column("benefits", sa.Text),
        sa.Column("application_url", sa.Text),
        sa.Column("date_posted", sa.Date),
        sa.Column("date_expires", sa.Date),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("is_active", sa.Boolean, default=True),
        sa.Column("extraction_method", sa.Text),
        sa.Column("extraction_confidence", sa.Float),
        sa.Column("raw_data", JSONB),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_jobs_company_id", "jobs", ["company_id"])
    op.create_index("ix_jobs_is_active", "jobs", ["is_active"])
    op.create_index("ix_jobs_first_seen_at", "jobs", ["first_seen_at"])
    op.create_index("ix_jobs_last_seen_at", "jobs", ["last_seen_at"])
    op.create_index("ix_jobs_location", "jobs", ["location_country", "location_city"])
    # Full-text search index
    op.execute(
        "CREATE INDEX ix_jobs_fts ON jobs USING GIN (to_tsvector('english', coalesce(title, '') || ' ' || coalesce(description, '')))"
    )

    # job_tags
    op.create_table(
        "job_tags",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("tag_type", sa.Text, nullable=False),
        sa.Column("tag_value", sa.Text, nullable=False),
        sa.Column("confidence", sa.Float),
    )
    op.create_index("ix_job_tags_job_id", "job_tags", ["job_id"])

    # crawl_logs
    op.create_table(
        "crawl_logs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("company_id", UUID(as_uuid=True), sa.ForeignKey("companies.id", ondelete="SET NULL")),
        sa.Column("career_page_id", UUID(as_uuid=True), sa.ForeignKey("career_pages.id", ondelete="SET NULL")),
        sa.Column("crawl_type", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False, default="pending"),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("pages_crawled", sa.Integer, default=0),
        sa.Column("jobs_found", sa.Integer, default=0),
        sa.Column("jobs_new", sa.Integer, default=0),
        sa.Column("jobs_updated", sa.Integer, default=0),
        sa.Column("jobs_removed", sa.Integer, default=0),
        sa.Column("error_message", sa.Text),
        sa.Column("error_details", JSONB),
        sa.Column("method_used", sa.Text),
        sa.Column("duration_seconds", sa.Float),
    )
    op.create_index("ix_crawl_logs_company_started", "crawl_logs", ["company_id", "started_at"])

    # site_templates
    op.create_table(
        "site_templates",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("company_id", UUID(as_uuid=True), sa.ForeignKey("companies.id", ondelete="CASCADE"), nullable=False),
        sa.Column("career_page_id", UUID(as_uuid=True), sa.ForeignKey("career_pages.id", ondelete="CASCADE"), nullable=False),
        sa.Column("template_type", sa.Text, nullable=False),
        sa.Column("selectors", JSONB, default={}),
        sa.Column("learned_via", sa.Text, default="llm_bootstrapped"),
        sa.Column("accuracy_score", sa.Float),
        sa.Column("last_validated_at", sa.DateTime(timezone=True)),
        sa.Column("is_active", sa.Boolean, default=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # extraction_comparisons
    op.create_table(
        "extraction_comparisons",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("career_page_id", UUID(as_uuid=True), sa.ForeignKey("career_pages.id", ondelete="CASCADE"), nullable=False),
        sa.Column("method_a", sa.Text, nullable=False),
        sa.Column("method_b", sa.Text, nullable=False),
        sa.Column("method_a_result", JSONB, default={}),
        sa.Column("method_b_result", JSONB, default={}),
        sa.Column("agreement_score", sa.Float),
        sa.Column("resolved_result", JSONB),
        sa.Column("resolution_method", sa.Text),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("extraction_comparisons")
    op.drop_table("site_templates")
    op.drop_table("crawl_logs")
    op.drop_table("job_tags")
    op.drop_table("jobs")
    op.drop_table("career_pages")
    op.drop_table("companies")
    op.drop_table("aggregator_sources")
    op.drop_table("blocked_domains")
    op.drop_table("markets")
