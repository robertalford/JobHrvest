"""SQLAlchemy model for lead_import_batches — tracks file uploads and import runs."""

import uuid
from sqlalchemy import Column, String, Text, Integer, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from app.db.base import Base


class LeadImportBatch(Base):
    __tablename__ = "lead_import_batches"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    filename = Column(Text, nullable=False)           # stored filename on disk
    original_filename = Column(Text, nullable=False)  # as uploaded
    file_size_bytes = Column(Integer)
    total_rows = Column(Integer)

    # Validation
    validation_status = Column(String(20), default="pending")
    # pending | valid | invalid
    validation_errors = Column(JSONB)   # list of error strings

    # Import
    import_status = Column(String(20), default="pending")
    # pending | importing | completed | failed
    imported_leads = Column(Integer, default=0)
    failed_leads = Column(Integer, default=0)
    blocked_leads = Column(Integer, default=0)
    skipped_leads = Column(Integer, default=0)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    import_started_at = Column(DateTime(timezone=True))
    import_completed_at = Column(DateTime(timezone=True))
    error_message = Column(Text)
