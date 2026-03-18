"""Market model — represents a geographic/linguistic job market."""

import uuid
from datetime import datetime
from sqlalchemy import Boolean, DateTime, String, Text, func
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Market(Base):
    __tablename__ = "markets"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    code: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)  # "AU", "US", "UK"
    name: Mapped[str] = mapped_column(Text, nullable=False)  # "Australia"
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    default_currency: Mapped[str] = mapped_column(String(10))  # "AUD"
    locale: Mapped[str] = mapped_column(String(20))  # "en-AU"
    salary_parsing_config: Mapped[dict] = mapped_column(JSONB, default=dict)
    location_parsing_config: Mapped[dict] = mapped_column(JSONB, default=dict)
    aggregator_search_queries: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    companies: Mapped[list["Company"]] = relationship("Company", back_populates="market")
