"""SQLAlchemy model for Codex-backed company enrichment batch runs."""

import uuid
from sqlalchemy import Column, String, Text, Integer, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from app.db.base import Base


class CompanyEnrichmentRun(Base):
    __tablename__ = "company_enrichment_runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    filename = Column(Text, nullable=False)
    original_filename = Column(Text, nullable=False)
    output_filename = Column(Text)
    file_size_bytes = Column(Integer)
    total_rows = Column(Integer, default=0)

    validation_status = Column(String(20), default="pending")
    validation_errors = Column(JSONB)

    run_status = Column(String(20), default="pending")
    completed_rows = Column(Integer, default=0)
    failed_rows = Column(Integer, default=0)
    skipped_rows = Column(Integer, default=0)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    run_started_at = Column(DateTime(timezone=True))
    run_completed_at = Column(DateTime(timezone=True))
    error_message = Column(Text)
