"""Add app_users table for multi-user auth.

Revision ID: 0023
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
import uuid

revision = "0023"
down_revision = "0022"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "app_users",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("username", sa.String(255), unique=True, nullable=False),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Seed initial users
    op.execute("""
        INSERT INTO app_users (id, username, password_hash) VALUES
        ('a0000000-0000-0000-0000-000000000001', 'r.m.l.alford@gmail.com',
         '$2b$12$tE.dJ3YxEfDf4g/NVfoMe.EoTmApYy.VEg1GuycDqh5yEPkADIqSe'),
        ('a0000000-0000-0000-0000-000000000002', 'anshunahar@seekasia.com',
         '$2b$12$utjdlakAC0RSHRh.Be279OOYozKd7glLDMxK1fJyioIfpE6bz7.ZC')
    """)


def downgrade():
    op.drop_table("app_users")
