"""Per-model inference latency tracker.

Each inference reports its latency to a Redis sorted set keyed by
(model_version_id, hour_bucket). A periodic Celery task rolls those buckets
up into the inference_metrics_hourly table and trims the raw Redis data.

This is the place where a "the new model is 2x slower" regression gets
caught — promotion gates can read recent p95 latency from the rollup and
refuse to promote a model whose inference cost would tank the crawl
pipeline.

Redis schema (one key per (model_version, hour)):
  inference:lat:{model_version_id}:{YYYYMMDDHH}  -> ZSET of latency_ms entries
  inference:esc:{model_version_id}:{YYYYMMDDHH}  -> INT (LLM-escalation count)
  inference:err:{model_version_id}:{YYYYMMDDHH}  -> INT (error count)

Each ZSET member is a unique micro-token "{ts_ns}:{rand}" with score = latency.
"""

from __future__ import annotations

import logging
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


KEY_LAT = "inference:lat:{model}:{bucket}"
KEY_ESC = "inference:esc:{model}:{bucket}"
KEY_ERR = "inference:err:{model}:{bucket}"

RAW_TTL_SECONDS = 3 * 24 * 3600  # 3 days — enough for 2 rollup retries


@dataclass
class InferenceObservation:
    model_version_id: str
    latency_ms: float
    escalated_to_llm: bool = False
    errored: bool = False


def _bucket(now: Optional[datetime] = None) -> str:
    ts = now or datetime.now(timezone.utc)
    return ts.strftime("%Y%m%d%H")


async def record_observation(redis_client, obs: InferenceObservation) -> None:
    """Push a single inference observation to Redis. Best-effort, never raises."""
    bucket = _bucket()
    member = f"{time.time_ns()}:{secrets.token_hex(4)}"
    try:
        pipe = redis_client.pipeline()
        pipe.zadd(KEY_LAT.format(model=obs.model_version_id, bucket=bucket), {member: obs.latency_ms})
        pipe.expire(KEY_LAT.format(model=obs.model_version_id, bucket=bucket), RAW_TTL_SECONDS)
        if obs.escalated_to_llm:
            pipe.incr(KEY_ESC.format(model=obs.model_version_id, bucket=bucket))
            pipe.expire(KEY_ESC.format(model=obs.model_version_id, bucket=bucket), RAW_TTL_SECONDS)
        if obs.errored:
            pipe.incr(KEY_ERR.format(model=obs.model_version_id, bucket=bucket))
            pipe.expire(KEY_ERR.format(model=obs.model_version_id, bucket=bucket), RAW_TTL_SECONDS)
        await pipe.execute()
    except Exception as e:  # noqa: BLE001 — telemetry must not break inference
        logger.warning("latency_budget.record_observation failed: %s", e)


def percentiles(latencies_ms: list[float]) -> dict[str, float]:
    """Compute p50/p95/p99. Returns NaN-safe values for empty input."""
    if not latencies_ms:
        return {"p50": float("nan"), "p95": float("nan"), "p99": float("nan")}
    sorted_v = sorted(latencies_ms)
    n = len(sorted_v)

    def pct(p: float) -> float:
        if n == 1:
            return sorted_v[0]
        rank = p * (n - 1)
        lo = int(rank)
        hi = min(lo + 1, n - 1)
        frac = rank - lo
        return sorted_v[lo] * (1 - frac) + sorted_v[hi] * frac

    return {"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99)}


@dataclass
class BudgetCheck:
    """Result of comparing recent latency against a budget."""
    within_budget: bool
    p95_ms: float
    budget_ms: float
    sample_size: int
    reason: str


def check_budget(
    *,
    p95_ms: float,
    budget_ms: float,
    sample_size: int,
    min_sample_size: int = 100,
) -> BudgetCheck:
    """Decide whether the model is within its inference latency budget.

    A model with too few observations is given the benefit of the doubt — we
    can't promote on noise either way. The orchestrator should call this in
    its pre-promotion checks and refuse to promote a challenger whose recent
    p95 already busts the budget.
    """
    if sample_size < min_sample_size:
        return BudgetCheck(
            within_budget=True,
            p95_ms=p95_ms,
            budget_ms=budget_ms,
            sample_size=sample_size,
            reason=f"insufficient samples ({sample_size} < {min_sample_size}) — "
                   "skipping budget check",
        )
    within = p95_ms <= budget_ms
    return BudgetCheck(
        within_budget=within,
        p95_ms=p95_ms,
        budget_ms=budget_ms,
        sample_size=sample_size,
        reason=(
            f"p95 {p95_ms:.1f}ms vs budget {budget_ms:.1f}ms "
            f"({'OK' if within else 'OVER'})"
        ),
    )
