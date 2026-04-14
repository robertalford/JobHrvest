"""Smoke test: importing every champion_challenger module + the new ORM
models must succeed. Catches the most common breakage (typos in column
types, missing imports, circular references) before it shows up at runtime
in a Celery worker.
"""


def test_import_models():
    from app.models.champion_challenger import (
        ModelVersion, GoldHoldoutSet, GoldHoldoutDomain,
        GoldHoldoutSnapshot, GoldHoldoutJob, Experiment,
        MetricSnapshot, AtsPatternProposal, DriftBaseline,
        InferenceMetricsHourly,
    )
    # Each ORM class must have a __tablename__ — guards against the dataclass
    # decorator accidentally swallowing the SQLAlchemy mapping.
    for cls in [
        ModelVersion, GoldHoldoutSet, GoldHoldoutDomain, GoldHoldoutSnapshot,
        GoldHoldoutJob, Experiment, MetricSnapshot, AtsPatternProposal,
        DriftBaseline, InferenceMetricsHourly,
    ]:
        assert hasattr(cls, "__tablename__")


def test_import_ml_modules():
    from app.ml.champion_challenger import (
        domain_splitter, promotion, drift_monitor, uncertainty,
        ats_quarantine, latency_budget, failure_analysis,
        holdout_builder, holdout_evaluator, orchestrator, registry,
    )
    # Each module must export at least one public symbol — guards against
    # an empty file from a botched checkout.
    for mod in [
        domain_splitter, promotion, drift_monitor, uncertainty,
        ats_quarantine, latency_budget, failure_analysis,
        holdout_builder, holdout_evaluator, orchestrator, registry,
    ]:
        public = [n for n in dir(mod) if not n.startswith("_")]
        assert public, f"{mod.__name__} has no public symbols"


def test_models_index_exposes_new_classes():
    from app.models import (
        ModelVersion, GoldHoldoutSet, Experiment, MetricSnapshot,
        AtsPatternProposal, DriftBaseline, InferenceMetricsHourly,
    )
    assert ModelVersion.__tablename__ == "model_versions"
    assert GoldHoldoutSet.__tablename__ == "gold_holdout_sets"
    assert Experiment.__tablename__ == "experiments"
    assert MetricSnapshot.__tablename__ == "metric_snapshots"
    assert AtsPatternProposal.__tablename__ == "ats_pattern_proposals"
    assert DriftBaseline.__tablename__ == "drift_baselines"
    assert InferenceMetricsHourly.__tablename__ == "inference_metrics_hourly"
