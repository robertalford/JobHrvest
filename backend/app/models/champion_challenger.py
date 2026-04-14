"""SQLAlchemy models for champion/challenger ML infrastructure.

See migration 0023 for the source-of-truth schema. These ORM models mirror
those tables and expose the relationships the orchestrator needs.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Boolean, DateTime, Double, ForeignKey, Integer, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class ModelVersion(Base):
    __tablename__ = "model_versions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    algorithm: Mapped[str] = mapped_column(Text, nullable=False)
    artifact_path: Mapped[Optional[str]] = mapped_column(Text)
    config: Mapped[dict] = mapped_column(JSONB, default=dict)
    feature_set: Mapped[list] = mapped_column(JSONB, default=list)
    training_corpus_hash: Mapped[Optional[str]] = mapped_column(Text)
    parent_version_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("model_versions.id", ondelete="SET NULL")
    )
    status: Mapped[str] = mapped_column(Text, default="candidate", nullable=False)
    trained_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    promoted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    retired_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    notes: Mapped[Optional[str]] = mapped_column(Text)


class GoldHoldoutSet(Base):
    __tablename__ = "gold_holdout_sets"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    source: Mapped[str] = mapped_column(Text, default="lead_imports", nullable=False)
    market_id: Mapped[Optional[str]] = mapped_column(Text)
    is_frozen: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    frozen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    domains: Mapped[list["GoldHoldoutDomain"]] = relationship(
        "GoldHoldoutDomain", back_populates="holdout_set", cascade="all, delete-orphan"
    )


class GoldHoldoutDomain(Base):
    __tablename__ = "gold_holdout_domains"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    holdout_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gold_holdout_sets.id", ondelete="CASCADE"), nullable=False
    )
    domain: Mapped[str] = mapped_column(Text, nullable=False)
    advertiser_name: Mapped[Optional[str]] = mapped_column(Text)
    expected_job_count: Mapped[Optional[int]] = mapped_column(Integer)
    market_id: Mapped[Optional[str]] = mapped_column(Text)
    ats_platform: Mapped[Optional[str]] = mapped_column(Text)
    source_lead_import_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("lead_imports.id", ondelete="SET NULL")
    )
    verification_status: Mapped[str] = mapped_column(Text, default="unverified", nullable=False)
    verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    verified_by: Mapped[Optional[str]] = mapped_column(Text)

    holdout_set: Mapped["GoldHoldoutSet"] = relationship("GoldHoldoutSet", back_populates="domains")
    snapshots: Mapped[list["GoldHoldoutSnapshot"]] = relationship(
        "GoldHoldoutSnapshot", back_populates="domain", cascade="all, delete-orphan"
    )
    jobs: Mapped[list["GoldHoldoutJob"]] = relationship(
        "GoldHoldoutJob", back_populates="domain", cascade="all, delete-orphan"
    )


class GoldHoldoutSnapshot(Base):
    __tablename__ = "gold_holdout_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    holdout_domain_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gold_holdout_domains.id", ondelete="CASCADE"), nullable=False
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    snapshot_path: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    content_type: Mapped[Optional[str]] = mapped_column(Text)
    http_status: Mapped[Optional[int]] = mapped_column(Integer)
    byte_size: Mapped[Optional[int]] = mapped_column(Integer)
    snapshotted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    domain: Mapped["GoldHoldoutDomain"] = relationship("GoldHoldoutDomain", back_populates="snapshots")


class GoldHoldoutJob(Base):
    __tablename__ = "gold_holdout_jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    holdout_domain_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gold_holdout_domains.id", ondelete="CASCADE"), nullable=False
    )
    title: Mapped[str] = mapped_column(Text, nullable=False)
    location: Mapped[Optional[str]] = mapped_column(Text)
    employment_type: Mapped[Optional[str]] = mapped_column(Text)
    apply_url: Mapped[Optional[str]] = mapped_column(Text)
    # gold = human-verified, silver = auto-labelled from baseline wrappers,
    # suspect = silver but baseline vs expected_job_count differ >2x,
    # unverified = placeholder until verification is performed.
    verification_status: Mapped[str] = mapped_column(Text, default="unverified", nullable=False)
    # 'manual' | 'baseline_wrapper' | 'llm' | etc.
    source: Mapped[str] = mapped_column(Text, default="manual", nullable=False)
    source_url: Mapped[Optional[str]] = mapped_column(Text)
    description_length: Mapped[Optional[int]] = mapped_column(Integer)
    verified_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    verified_by: Mapped[Optional[str]] = mapped_column(Text)
    notes: Mapped[Optional[str]] = mapped_column(Text)

    domain: Mapped["GoldHoldoutDomain"] = relationship("GoldHoldoutDomain", back_populates="jobs")


class Experiment(Base):
    __tablename__ = "experiments"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    model_name: Mapped[str] = mapped_column(Text, nullable=False)
    champion_version_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("model_versions.id", ondelete="SET NULL")
    )
    challenger_version_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("model_versions.id", ondelete="SET NULL")
    )
    holdout_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gold_holdout_sets.id", ondelete="RESTRICT"), nullable=False
    )
    strategy: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, default="pending", nullable=False)
    promotion_decision: Mapped[Optional[dict]] = mapped_column(JSONB)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class MetricSnapshot(Base):
    __tablename__ = "metric_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    model_version_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("model_versions.id", ondelete="CASCADE"), nullable=False
    )
    holdout_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gold_holdout_sets.id", ondelete="CASCADE"), nullable=False
    )
    experiment_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("experiments.id", ondelete="SET NULL")
    )
    stratum_key: Mapped[str] = mapped_column(Text, default="all", nullable=False)
    metric_name: Mapped[str] = mapped_column(Text, nullable=False)
    metric_value: Mapped[float] = mapped_column(Double, nullable=False)
    sample_size: Mapped[int] = mapped_column(Integer, nullable=False)
    ci_low: Mapped[Optional[float]] = mapped_column(Double)
    ci_high: Mapped[Optional[float]] = mapped_column(Double)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AtsPatternProposal(Base):
    __tablename__ = "ats_pattern_proposals"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ats_name: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, default="llm", nullable=False)
    sample_url: Mapped[Optional[str]] = mapped_column(Text)
    url_patterns: Mapped[list] = mapped_column(JSONB, default=list)
    html_patterns: Mapped[list] = mapped_column(JSONB, default=list)
    selectors: Mapped[dict] = mapped_column(JSONB, nullable=False)
    pagination: Mapped[Optional[dict]] = mapped_column(JSONB)
    confidence: Mapped[Optional[float]] = mapped_column(Double)
    status: Mapped[str] = mapped_column(Text, default="proposed", nullable=False)
    shadow_match_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    shadow_failure_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    shadow_first_seen: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    shadow_last_seen: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    promoted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    rejected_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    rejection_reason: Mapped[Optional[str]] = mapped_column(Text)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class DriftBaseline(Base):
    __tablename__ = "drift_baselines"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    model_name: Mapped[str] = mapped_column(Text, nullable=False)
    feature_name: Mapped[str] = mapped_column(Text, nullable=False)
    distribution: Mapped[dict] = mapped_column(JSONB, nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sample_size: Mapped[int] = mapped_column(Integer, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class InferenceMetricsHourly(Base):
    __tablename__ = "inference_metrics_hourly"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    model_version_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("model_versions.id", ondelete="CASCADE"), nullable=False
    )
    hour_bucket: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    latency_p50_ms: Mapped[Optional[float]] = mapped_column(Double)
    latency_p95_ms: Mapped[Optional[float]] = mapped_column(Double)
    latency_p99_ms: Mapped[Optional[float]] = mapped_column(Double)
    llm_escalation_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)


class EvoIndividual(Base):
    __tablename__ = "evo_individuals"

    version_tag: Mapped[str] = mapped_column(Text, primary_key=True)
    ml_model_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("ml_models.id", ondelete="SET NULL")
    )
    parent_tag: Mapped[Optional[str]] = mapped_column(Text)
    island_id: Mapped[int] = mapped_column(Integer, nullable=False)
    focus_axis: Mapped[str] = mapped_column(Text, nullable=False)
    behaviour_cell: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    fixture_composite: Mapped[Optional[float]] = mapped_column(Double)
    ab_composite: Mapped[Optional[float]] = mapped_column(Double)
    axes_json: Mapped[Optional[dict]] = mapped_column(JSONB)
    loc: Mapped[Optional[int]] = mapped_column(Integer)
    file_path: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class EvoCycle(Base):
    __tablename__ = "evo_cycles"

    id: Mapped[uuid.UUID] = mapped_column("cycle_id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    n_candidates: Mapped[Optional[int]] = mapped_column(Integer)
    promoted_tag: Mapped[Optional[str]] = mapped_column(Text)
    notes: Mapped[Optional[dict]] = mapped_column(JSONB)


class EvoPopulationEvent(Base):
    __tablename__ = "evo_population_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    cycle_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("evo_cycles.cycle_id", ondelete="CASCADE"))
    version_tag: Mapped[Optional[str]] = mapped_column(Text)
    event: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
