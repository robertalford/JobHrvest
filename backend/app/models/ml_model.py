"""ML model definitions and test run tracking."""

import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, Text, func
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class MLModel(Base):
    __tablename__ = "ml_models"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    model_type: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    config: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="new")
    version: Mapped[int] = mapped_column(Integer, default=1)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    test_runs: Mapped[list["MLModelTestRun"]] = relationship("MLModelTestRun", back_populates="model", cascade="all, delete-orphan")


class MLModelTestRun(Base):
    __tablename__ = "ml_model_test_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    model_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("ml_models.id", ondelete="CASCADE"))
    test_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    total_tests: Mapped[int] = mapped_column(Integer, default=0)
    tests_passed: Mapped[int] = mapped_column(Integer, default=0)
    tests_failed: Mapped[int] = mapped_column(Integer, default=0)
    accuracy: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    precision_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    recall: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    f1_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    test_config: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    results_detail: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    model: Mapped["MLModel"] = relationship("MLModel", back_populates="test_runs")
    feedback: Mapped[list["MLTestFeedback"]] = relationship("MLTestFeedback", back_populates="test_run", cascade="all, delete-orphan")


class CodexImprovementRun(Base):
    __tablename__ = "codex_improvement_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # The model whose test results are being analysed (input)
    source_model_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("ml_models.id", ondelete="SET NULL"), nullable=True)
    # The test run that triggered this improvement
    test_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("ml_model_test_runs.id", ondelete="SET NULL"), nullable=True)
    # The new model created by this improvement (output)
    output_model_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("ml_models.id", ondelete="SET NULL"), nullable=True)
    # Status: analysing, running_codex, deploying, testing, completed, failed, skipped
    status: Mapped[str] = mapped_column(Text, nullable=False, default="analysing")
    # Human-readable summary of what changed
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # Source and output model names (denormalized for display even if models are deleted)
    source_model_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    output_model_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # Winner of the test run that was analysed
    test_winner: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # Error info if failed
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    source_model: Mapped[Optional["MLModel"]] = relationship("MLModel", foreign_keys=[source_model_id])
    output_model: Mapped[Optional["MLModel"]] = relationship("MLModel", foreign_keys=[output_model_id])
    test_run: Mapped[Optional["MLModelTestRun"]] = relationship("MLModelTestRun", foreign_keys=[test_run_id])


class MLTestFeedback(Base):
    __tablename__ = "ml_test_feedback"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    test_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("ml_model_test_runs.id", ondelete="CASCADE"))
    site_url: Mapped[str] = mapped_column(Text, nullable=False)  # which site this feedback is about
    comment: Mapped[str] = mapped_column(Text, nullable=False)
    screenshot_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # path to uploaded image
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    test_run: Mapped["MLModelTestRun"] = relationship("MLModelTestRun", back_populates="feedback")
