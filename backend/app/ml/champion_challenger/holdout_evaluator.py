"""Score a model_version against a frozen GOLD holdout.

The evaluator runs the supplied predictor over each domain in the holdout
set, computes both classifier metrics (precision/recall/F1) and extraction
metrics (job_coverage, title_accuracy), and writes one MetricSnapshot row
per (stratum, metric).

Stratification is mandatory — aggregate scores hide systematic failures.
We always emit:
  - 'all'                 : overall numbers
  - 'ats=<platform>'      : per-ATS scores (if domain has ats_platform set)
  - 'market=<market_id>'  : per-market scores

The predictor protocol is intentionally narrow so it can wrap any
underlying model — TF-IDF/LR, LightGBM, ensemble, even an LLM — without
the evaluator needing to know.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional, Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.ml.champion_challenger.domain_splitter import assert_holdout_isolation
from app.ml.champion_challenger.promotion import bootstrap_ci
from app.models.champion_challenger import (
    GoldHoldoutDomain,
    GoldHoldoutJob,
    GoldHoldoutSet,
    GoldHoldoutSnapshot,
    MetricSnapshot,
)

logger = logging.getLogger(__name__)


class Predictor(Protocol):
    """The evaluator only needs two operations from a model."""

    async def predict_career_page(self, *, url: str, html: str) -> float:
        """Return P(is_career_page) ∈ [0, 1]."""
        ...

    async def extract_jobs(self, *, url: str, html: str) -> list[dict]:
        """Return a list of extracted job dicts (must include 'title')."""
        ...


@dataclass
class _DomainResult:
    domain: str
    market_id: Optional[str]
    ats_platform: Optional[str]

    is_career_pred: int = 0       # binary prediction
    is_career_true: int = 1       # GOLD domains are by definition career hosts
    classifier_proba: float = 0.0

    expected_job_count: Optional[int] = None
    extracted_count: int = 0
    matched_titles: int = 0
    job_coverage: float = 0.0
    title_accuracy: float = 0.0

    error: Optional[str] = None


@dataclass
class EvaluationReport:
    holdout_set_id: UUID
    model_version_id: UUID
    domains_evaluated: int
    metrics_overall: dict[str, float] = field(default_factory=dict)
    metrics_per_stratum: dict[str, dict[str, float]] = field(default_factory=dict)
    sample_size_per_stratum: dict[str, int] = field(default_factory=dict)
    ci_overall: dict[str, tuple[float, float]] = field(default_factory=dict)


class HoldoutEvaluator:
    def __init__(
        self,
        *,
        snapshot_loader: Callable[[GoldHoldoutSnapshot], Awaitable[bytes]],
        career_page_threshold: float = 0.5,
        silver_weight: float = 0.5,
        include_suspect: bool = False,
    ):
        """Evaluator supports a mix of verification statuses.

        Args:
            snapshot_loader: async callable returning raw HTML bytes.
            career_page_threshold: threshold for the binary career-page classifier.
            silver_weight: weight applied to silver-labelled (auto-generated)
                ground-truth jobs when aggregating metrics. Defaults to 0.5
                so one gold job counts as two silver jobs. Set to 1.0 to
                treat them equally.
            include_suspect: when False (default) rows with verification_status
                == 'suspect' are excluded from scoring entirely. Flip on only
                for debugging.
        """
        self.load_snapshot = snapshot_loader
        self.threshold = career_page_threshold
        self.silver_weight = max(0.0, min(1.0, silver_weight))
        self.include_suspect = include_suspect

    async def evaluate(
        self,
        session: AsyncSession,
        *,
        model_version_id: UUID,
        holdout_set_id: UUID,
        predictor: Predictor,
        training_domains: Optional[list[str]] = None,
        experiment_id: Optional[UUID] = None,
    ) -> EvaluationReport:
        """Run the predictor over every snapshot, compute metrics, persist them."""
        holdout = await session.get(GoldHoldoutSet, holdout_set_id)
        if holdout is None:
            raise ValueError(f"GoldHoldoutSet {holdout_set_id} not found")

        domain_rows = await self._load_domains(session, holdout_set_id)

        # Hard guard against the most embarrassing leak.
        if training_domains is not None:
            assert_holdout_isolation(training_domains, [d.domain for d in domain_rows])

        results: list[_DomainResult] = []
        for domain in domain_rows:
            results.append(await self._score_domain(session, domain, predictor))

        report = self._aggregate(results, model_version_id, holdout_set_id)
        await self._persist(session, report, experiment_id=experiment_id)
        return report

    async def _load_domains(
        self, session: AsyncSession, holdout_set_id: UUID,
    ) -> list[GoldHoldoutDomain]:
        stmt = select(GoldHoldoutDomain).where(
            GoldHoldoutDomain.holdout_set_id == holdout_set_id
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def _score_domain(
        self,
        session: AsyncSession,
        domain: GoldHoldoutDomain,
        predictor: Predictor,
    ) -> _DomainResult:
        result = _DomainResult(
            domain=domain.domain,
            market_id=domain.market_id,
            ats_platform=domain.ats_platform,
            expected_job_count=domain.expected_job_count,
        )

        snapshot = await session.scalar(
            select(GoldHoldoutSnapshot)
            .where(GoldHoldoutSnapshot.holdout_domain_id == domain.id)
            .order_by(GoldHoldoutSnapshot.snapshotted_at.desc())
            .limit(1)
        )
        if snapshot is None:
            result.error = "no snapshot"
            return result

        try:
            html_bytes = await self.load_snapshot(snapshot)
            html = html_bytes.decode("utf-8", errors="replace")
        except Exception as e:  # noqa: BLE001
            result.error = f"snapshot load failed: {e}"
            return result

        try:
            proba = await predictor.predict_career_page(url=snapshot.url, html=html)
            result.classifier_proba = float(proba)
            result.is_career_pred = int(proba >= self.threshold)
            extracted = await predictor.extract_jobs(url=snapshot.url, html=html)
        except Exception as e:  # noqa: BLE001 — single-domain failure must not abort the run
            result.error = f"predict failed: {e}"
            return result

        result.extracted_count = len(extracted)

        # Compare against verified ground-truth jobs if any exist. Gold labels
        # are authoritative (weight 1.0); silver labels (auto-generated from
        # baseline wrappers) count fractionally so they multiply eval volume
        # without dominating the decision. Suspect rows are excluded by default.
        stmt = select(GoldHoldoutJob).where(GoldHoldoutJob.holdout_domain_id == domain.id)
        gold_jobs_all = list((await session.execute(stmt)).scalars().all())
        usable = [
            j for j in gold_jobs_all
            if j.verification_status != "suspect" or self.include_suspect
        ]
        if usable:
            gold_titles = [j.title for j in usable if j.verification_status == "gold"]
            silver_titles = [j.title for j in usable if j.verification_status != "gold"]
            extracted_titles = [j.get("title", "") for j in extracted]

            # Weighted matched/total counts: gold worth 1.0, silver worth silver_weight.
            matched_gold = _count_fuzzy_title_matches(gold_titles, extracted_titles)
            matched_silver = _count_fuzzy_title_matches(silver_titles, extracted_titles)
            weighted_matched = matched_gold + self.silver_weight * matched_silver
            weighted_total = len(gold_titles) + self.silver_weight * len(silver_titles)

            result.matched_titles = int(round(weighted_matched))
            result.title_accuracy = (
                weighted_matched / weighted_total if weighted_total > 0 else 0.0
            )
            result.job_coverage = min(
                result.extracted_count / weighted_total, 1.0
            ) if weighted_total > 0 else 0.0
        elif domain.expected_job_count and domain.expected_job_count > 0:
            # Fall back to the lead_import expected count as a soft signal
            result.job_coverage = min(
                result.extracted_count / domain.expected_job_count, 1.0
            )

        return result

    def _aggregate(
        self,
        results: list[_DomainResult],
        model_version_id: UUID,
        holdout_set_id: UUID,
    ) -> EvaluationReport:
        report = EvaluationReport(
            holdout_set_id=holdout_set_id,
            model_version_id=model_version_id,
            domains_evaluated=len(results),
        )

        # Group results for stratified scoring
        strata: dict[str, list[_DomainResult]] = defaultdict(list)
        strata["all"] = list(results)
        for r in results:
            if r.ats_platform:
                strata[f"ats={r.ats_platform}"].append(r)
            if r.market_id:
                strata[f"market={r.market_id}"].append(r)

        for stratum_key, group in strata.items():
            metrics = _compute_metrics(group)
            report.metrics_per_stratum[stratum_key] = metrics
            report.sample_size_per_stratum[stratum_key] = len(group)
            if stratum_key == "all":
                report.metrics_overall = metrics
                report.ci_overall = _compute_overall_cis(group)

        return report

    async def _persist(
        self,
        session: AsyncSession,
        report: EvaluationReport,
        *,
        experiment_id: Optional[UUID],
    ) -> None:
        for stratum_key, metrics in report.metrics_per_stratum.items():
            sample_size = report.sample_size_per_stratum[stratum_key]
            for metric_name, metric_value in metrics.items():
                if metric_value is None or (isinstance(metric_value, float) and math.isnan(metric_value)):
                    continue
                ci_low = ci_high = None
                if stratum_key == "all":
                    ci = report.ci_overall.get(metric_name)
                    if ci is not None:
                        ci_low, ci_high = ci
                session.add(MetricSnapshot(
                    model_version_id=report.model_version_id,
                    holdout_set_id=report.holdout_set_id,
                    experiment_id=experiment_id,
                    stratum_key=stratum_key,
                    metric_name=metric_name,
                    metric_value=float(metric_value),
                    sample_size=sample_size,
                    ci_low=ci_low,
                    ci_high=ci_high,
                ))
        await session.commit()


# ─── Metric helpers ────────────────────────────────────────────────────────

def _compute_metrics(results: list[_DomainResult]) -> dict[str, float]:
    if not results:
        return {}

    valid = [r for r in results if r.error is None]
    if not valid:
        return {"error_rate": 1.0}

    n = len(valid)
    tp = sum(1 for r in valid if r.is_career_pred == 1 and r.is_career_true == 1)
    fp = sum(1 for r in valid if r.is_career_pred == 1 and r.is_career_true == 0)
    fn = sum(1 for r in valid if r.is_career_pred == 0 and r.is_career_true == 1)

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    job_coverage = sum(r.job_coverage for r in valid) / n
    title_accuracy = sum(r.title_accuracy for r in valid) / n
    domain_success = sum(
        1 for r in valid
        if r.is_career_pred == 1 and r.job_coverage >= 0.8 and r.title_accuracy >= 0.7
    ) / n
    error_rate = (len(results) - len(valid)) / len(results)

    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "false_positive_rate": fp / max(n, 1),
        "job_coverage_rate": job_coverage,
        "title_accuracy": title_accuracy,
        "domain_success_rate": domain_success,
        "error_rate": error_rate,
    }


def _compute_overall_cis(results: list[_DomainResult]) -> dict[str, tuple[float, float]]:
    """Bootstrap 95% CIs for a few headline metrics."""
    valid = [r for r in results if r.error is None]
    if len(valid) < 5:
        return {}
    y_true = [r.is_career_true for r in valid]
    y_pred = [r.is_career_pred for r in valid]

    def _f1(yt, yp):
        import numpy as np
        yt = np.asarray(yt); yp = np.asarray(yp)
        tp = float(((yp == 1) & (yt == 1)).sum())
        fp = float(((yp == 1) & (yt == 0)).sum())
        fn = float(((yp == 0) & (yt == 1)).sum())
        if tp == 0:
            return 0.0
        p = tp / (tp + fp); r = tp / (tp + fn)
        return 2 * p * r / (p + r)

    point, low, high = bootstrap_ci(y_true, y_pred, _f1, n_iterations=500)
    return {"f1": (low, high)}


def _count_fuzzy_title_matches(gold: list[str], extracted: list[str], threshold: int = 80) -> int:
    """Token-set ratio match. Falls back to substring if rapidfuzz unavailable."""
    try:
        from rapidfuzz import fuzz
        matched = 0
        for g in gold:
            if not extracted:
                break
            best = max((fuzz.token_set_ratio(g, e) for e in extracted), default=0)
            if best >= threshold:
                matched += 1
        return matched
    except ImportError:
        gold_lower = [g.lower() for g in gold]
        extracted_lower = [e.lower() for e in extracted]
        matched = 0
        for g in gold_lower:
            if any(g in e or e in g for e in extracted_lower):
                matched += 1
        return matched
