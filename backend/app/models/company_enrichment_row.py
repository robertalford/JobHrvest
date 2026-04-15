"""SQLAlchemy model for a single company row in an enrichment batch."""

import uuid
from sqlalchemy import Column, String, Text, Integer, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from app.db.base import Base


class CompanyEnrichmentRow(Base):
    __tablename__ = "company_enrichment_rows"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("company_enrichment_runs.id", ondelete="CASCADE"), nullable=False, index=True)

    row_number = Column(Integer, nullable=False)
    company = Column(Text, nullable=False, index=True)
    country = Column(Text, nullable=False, index=True)
    status = Column(String(20), nullable=False, default="pending", index=True)

    job_page_url = Column(Text)
    job_count = Column(Text)
    comment = Column(Text)

    raw_response_text = Column(Text)
    raw_response_json = Column(JSONB)
    error_message = Column(Text)
    attempt_count = Column(Integer, default=0)
    worker_id = Column(Text, index=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
