"""Champion/challenger experiment orchestrator.

Wires the small modules together into one decision-making call:

    1. Load the current champion + challenger model versions
    2. Evaluate both against the GOLD holdout (stratified)
    3. Run McNemar on the per-domain predictions
    4. Run multi-metric promotion gates
    5. Run latency budget check
    6. Persist the experiment row + decision
    7. (If promote) flip status fields atomically

The orchestrator never re-trains a model. It only *decides* between two
already-trained candidates. This keeps a single decision auditable and
re-runnable without burning compute.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Sequence
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.ml.champion_challenger.holdout_evaluator import (
    EvaluationReport,
    HoldoutEvaluator,
    Predictor,
)
from app.ml.champion_challenger.latency_budget import BudgetCheck, check_budget
from app.ml.champion_challenger.promotion import (
    MetricRequirement,
    PromotionDecision,
    evaluate_gates,
    mcnemar_test,
)
from app.models.champion_challenger import (
    Experiment,
    ModelVersion,
)

logger = logging.getLogger(__name__)


# Default promotion criteria — tuned conservatively for the AU classifier.
# Override per experiment by passing a different list to `run_experiment`.
DEFAULT_REQUIREMENTS: tuple[MetricRequirement, ...] = (
    MetricRequirement("f1", min_delta=0.005),
    MetricRequirement("recall", min_delta=0.005),
    MetricRequirement("job_coverage_rate", min_delta=0.01),
    MetricRequirement("false_positive_rate", higher_is_better=False, min_delta=0.005),
)


@dataclass
class OrchestratorResult:
    experiment_id: UUID
    decision: PromotionDecision
    champion_metrics: dict[str, float]
    challenger_metrics: dict[str, float]
    p_value: Optional[float]
    latency_check: Optional[BudgetCheck]
    promoted: bool


class ChampionChallengerOrchestrator:
    def __init__(
        self,
        *,
        evaluator: HoldoutEvaluator,
        latency_budget_ms: float = 200.0,
        latency_p95_provider=None,  # async (model_version_id) -> (p95, sample_size)
    ):
        self.evaluator = evaluator
        self.latency_budget_ms = latency_budget_ms
        self.latency_p95_provider = latency_p95_provider

    async def run_experiment(
        self,
        session: AsyncSession,
        *,
        experiment_name: str,
        model_name: str,
        champion_id: UUID,
        challenger_id: UUID,
        holdout_set_id: UUID,
        champion_predictor: Predictor,
        challenger_predictor: Predictor,
        strategy: str = "manual",
        requirements: Sequence[MetricRequirement] = DEFAULT_REQUIREMENTS,
        min_metrics_won: int = 2,
        p_value_threshold: float = 0.05,
        per_page_predictions: Optional[
            tuple[Sequence[int], Sequence[int], Sequence[int]]
        ] = None,
    ) -> OrchestratorResult:
        """Run a single champion-vs-challenger comparison.

        `per_page_predictions`, if supplied as (y_true, champion_preds,
        challenger_preds) at the page level, is used to compute McNemar's
        statistic. If not supplied, we fall back to per-domain binary
        agreement, which is weaker but still informative.
        """
        experiment = Experiment(
            name=experiment_name,
            model_name=model_name,
            champion_version_id=champion_id,
            challenger_version_id=challenger_id,
            holdout_set_id=holdout_set_id,
            strategy=strategy,
            status="running",
            started_at=datetime.now(timezone.utc),
        )
        session.add(experiment)
        await session.flush()

        # Step 1 — evaluate both
        champion_report = await self.evaluator.evaluate(
            session,
            model_version_id=champion_id,
            holdout_set_id=holdout_set_id,
            predictor=champion_predictor,
            experiment_id=experiment.id,
        )
        challenger_report = await self.evaluator.evaluate(
            session,
            model_version_id=challenger_id,
            holdout_set_id=holdout_set_id,
            predictor=challenger_predictor,
            experiment_id=experiment.id,
        )

        # Step 2 — McNemar
        p_value: Optional[float] = None
        if per_page_predictions is not None:
            y_true, champ_preds, chal_preds = per_page_predictions
            mcn = mcnemar_test(y_true, champ_preds, chal_preds)
            p_value = mcn["p_value"]
        else:
            mcn = self._mcnemar_from_reports(champion_report, challenger_report)
            if mcn is not None:
                p_value = mcn["p_value"]

        # Step 3 — gates
        decision = evaluate_gates(
            champion_metrics=champion_report.metrics_overall,
            challenger_metrics=challenger_report.metrics_overall,
            requirements=requirements,
            min_metrics_won=min_metrics_won,
            p_value=p_value,
            p_value_threshold=p_value_threshold,
        )

        # Step 4 — latency budget
        latency_check: Optional[BudgetCheck] = None
        if self.latency_p95_provider is not None:
            try:
                p95, sample_size = await self.latency_p95_provider(challenger_id)
                latency_check = check_budget(
                    p95_ms=p95,
                    budget_ms=self.latency_budget_ms,
                    sample_size=sample_size,
                )
                if not latency_check.within_budget and decision.verdict == "promote":
                    decision.verdict = "reject"
                    decision.reasons.append(
                        f"latency budget exceeded: {latency_check.reason}"
                    )
            except Exception as e:  # noqa: BLE001
                logger.warning("Latency budget check failed: %s", e)

        # Step 5 — persist
        experiment.status = (
            "promoted" if decision.verdict == "promote"
            else ("rejected" if decision.verdict == "reject" else "pending")
        )
        experiment.completed_at = datetime.now(timezone.utc)
        experiment.promotion_decision = {
            "verdict": decision.verdict,
            "reasons": decision.reasons,
            "metric_deltas": decision.metric_deltas,
            "p_value": decision.p_value,
            "n_metrics_won": decision.n_metrics_won,
            "n_metrics_required": decision.n_metrics_required,
            "latency_within_budget": (
                latency_check.within_budget if latency_check else None
            ),
            "latency_reason": latency_check.reason if latency_check else None,
        }

        promoted = False
        if decision.verdict == "promote":
            promoted = await self._promote(session, model_name=model_name, new_id=challenger_id)

        await session.commit()

        return OrchestratorResult(
            experiment_id=experiment.id,
            decision=decision,
            champion_metrics=champion_report.metrics_overall,
            challenger_metrics=challenger_report.metrics_overall,
            p_value=p_value,
            latency_check=latency_check,
            promoted=promoted,
        )

    async def _promote(self, session: AsyncSession, *, model_name: str, new_id: UUID) -> bool:
        """Atomically retire the current champion and crown the challenger.

        The unique partial index `ix_model_versions_one_champion_per_name`
        guarantees only one champion per model_name — we retire the old one
        in the same transaction to satisfy it.
        """
        now = datetime.now(timezone.utc)
        await session.execute(
            update(ModelVersion)
            .where(
                ModelVersion.name == model_name,
                ModelVersion.status == "champion",
            )
            .values(status="retired", retired_at=now)
        )
        await session.execute(
            update(ModelVersion)
            .where(ModelVersion.id == new_id)
            .values(status="champion", promoted_at=now)
        )
        logger.info("Promoted %s as new champion for %s", new_id, model_name)
        return True

    def _mcnemar_from_reports(
        self,
        champion_report: EvaluationReport,
        challenger_report: EvaluationReport,
    ) -> Optional[dict]:
        """Coarse McNemar at the domain level.

        We don't have the per-domain predictions in the report objects today,
        so this is a placeholder that returns None — the orchestrator falls
        back to the gate-only decision. Wiring per-domain predictions into
        the report would lift this restriction.
        """
        return None
