"""Thin async helpers around the model_versions table.

Centralises the common operations (register, get champion, list versions)
so the orchestrator and CLI scripts don't reach into ORM details.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.champion_challenger import ModelVersion


async def register_model_version(
    session: AsyncSession,
    *,
    name: str,
    algorithm: str,
    config: dict,
    feature_set: list[str],
    artifact_path: Optional[str] = None,
    training_corpus_hash: Optional[str] = None,
    parent_version_id: Optional[UUID] = None,
    notes: Optional[str] = None,
) -> ModelVersion:
    """Register a freshly-trained model. Auto-increments the version number."""
    next_version = await _next_version_number(session, name)
    mv = ModelVersion(
        name=name,
        version=next_version,
        algorithm=algorithm,
        config=config,
        feature_set=feature_set,
        artifact_path=artifact_path,
        training_corpus_hash=training_corpus_hash,
        parent_version_id=parent_version_id,
        status="candidate",
        notes=notes,
    )
    session.add(mv)
    await session.flush()
    return mv


async def get_champion(session: AsyncSession, *, name: str) -> Optional[ModelVersion]:
    return await session.scalar(
        select(ModelVersion)
        .where(ModelVersion.name == name, ModelVersion.status == "champion")
        .limit(1)
    )


async def get_version(session: AsyncSession, version_id: UUID) -> Optional[ModelVersion]:
    return await session.get(ModelVersion, version_id)


async def list_versions(
    session: AsyncSession,
    *,
    name: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
) -> list[ModelVersion]:
    stmt = select(ModelVersion)
    if name:
        stmt = stmt.where(ModelVersion.name == name)
    if status:
        stmt = stmt.where(ModelVersion.status == status)
    stmt = stmt.order_by(desc(ModelVersion.trained_at)).limit(limit)
    return list((await session.execute(stmt)).scalars().all())


async def crown_initial_champion(session: AsyncSession, version_id: UUID) -> None:
    """First-time bootstrap: there is no previous champion to retire.

    Use only when migrating an existing model into the registry, never after
    the orchestrator has run at least once (use the orchestrator's promotion
    path instead — it retires the old champion atomically).
    """
    mv = await session.get(ModelVersion, version_id)
    if mv is None:
        raise ValueError(f"ModelVersion {version_id} not found")
    existing = await get_champion(session, name=mv.name)
    if existing is not None:
        raise ValueError(
            f"A champion already exists for {mv.name!r}; use the orchestrator to swap"
        )
    mv.status = "champion"
    mv.promoted_at = datetime.now(timezone.utc)
    await session.flush()


async def _next_version_number(session: AsyncSession, name: str) -> int:
    current_max = await session.scalar(
        select(func.max(ModelVersion.version)).where(ModelVersion.name == name)
    )
    return (current_max or 0) + 1
