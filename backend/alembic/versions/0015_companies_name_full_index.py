"""Full (non-partial) B-tree index on companies.name for ORDER BY + LIMIT.

The existing ix_companies_active_name is a partial index (WHERE is_active=true).
PostgreSQL can only use a partial index when the query's WHERE clause includes the
index predicate. Since the companies list query uses a dynamic is_active filter
(sometimes absent), the planner falls back to seq scan + sort on all 27k rows.

A full index on companies(name) lets the planner do a forward index scan in name
order and stop after LIMIT rows — no full sort required. With LIMIT 50, this reads
~50 rows instead of 27,974.

Also adds an index on company_stats(company_id) (if not already a PK index) to
accelerate the LEFT JOIN on the gold table.

Revision ID: 0015
"""

from alembic import op

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade():
    # Full B-tree index on companies.name — used by ORDER BY c.name ASC with LIMIT
    # The planner prefers this over seq scan + sort when LIMIT is small (e.g. 50).
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_companies_name
          ON companies (name)
    """)

    # Run ANALYZE so the planner has up-to-date statistics for the new index
    op.execute("ANALYZE companies")
    op.execute("ANALYZE company_stats")


def downgrade():
    op.execute("DROP INDEX IF EXISTS ix_companies_name")
