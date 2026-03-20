"""Settings tables — word_filters, blocked_domains_config, system_settings.

Revision ID: 0005_settings
Revises: 0004
Create Date: 2026-03-19
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "0005_settings"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "word_filters",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("word", sa.Text(), nullable=False),
        sa.Column("filter_type", sa.Text(), nullable=False),
        sa.Column("markets", JSONB(), nullable=False, server_default="[]"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "blocked_domains_config",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("domain", sa.Text(), nullable=False, unique=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "system_settings",
        sa.Column("key", sa.Text(), primary_key=True),
        sa.Column("value", JSONB(), nullable=False, server_default="{}"),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # Seed default system settings
    op.execute(
        """
        INSERT INTO system_settings (key, value) VALUES
        (
            'markets',
            '{"items": [
                {"code": "AU", "name": "Australia", "is_active": true},
                {"code": "NZ", "name": "New Zealand", "is_active": false},
                {"code": "MY", "name": "Malaysia", "is_active": false},
                {"code": "PH", "name": "Philippines", "is_active": false},
                {"code": "ID", "name": "Indonesia", "is_active": false},
                {"code": "SG", "name": "Singapore", "is_active": false},
                {"code": "TH", "name": "Thailand", "is_active": false},
                {"code": "HK", "name": "Hong Kong", "is_active": false}
            ]}'::jsonb
        ),
        (
            'discovery_sources',
            '{"items": [
                {"name": "Indeed AU", "base_url": "https://au.indeed.com", "is_active": true, "priority": 1, "markets": ["AU"]},
                {"name": "LinkedIn Jobs", "base_url": "https://www.linkedin.com/jobs", "is_active": true, "priority": 2, "markets": ["AU", "NZ"]},
                {"name": "Glassdoor AU", "base_url": "https://www.glassdoor.com.au", "is_active": false, "priority": 3, "markets": ["AU"]},
                {"name": "Adzuna AU", "base_url": "https://www.adzuna.com.au", "is_active": false, "priority": 4, "markets": ["AU"]}
            ]}'::jsonb
        ),
        (
            'schedule',
            '{"full_crawl_interval_hours": 24, "aggregator_harvest_interval_hours": 48, "enabled": true, "mark_inactive_interval_hours": 168}'::jsonb
        )
        """
    )


def downgrade() -> None:
    op.drop_table("system_settings")
    op.drop_table("blocked_domains_config")
    op.drop_table("word_filters")
