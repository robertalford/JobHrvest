"""Champion/challenger ML loop infrastructure.

This package contains the pure-logic and orchestration modules that sit on top
of the DB tables added by migration 0023. Each module is intentionally small
and independently testable.

Core modules:
  - domain_splitter   : split-by-domain enforcement (no per-page leakage)
  - promotion         : bootstrap CIs, McNemar's test, multi-metric gates
  - drift_monitor     : PSI for distribution drift detection
  - uncertainty       : route low-confidence predictions to review queue
  - ats_quarantine    : proposed -> shadow -> active promotion of ATS patterns
  - latency_budget    : Redis-backed per-page latency tracker + hourly rollup
  - failure_analysis  : Ollama-backed analysis of holdout failures
  - holdout_builder   : materialise a frozen GOLD holdout from lead_imports
  - holdout_evaluator : score a model_version against a holdout (stratified)
  - orchestrator      : tie everything together for a single experiment run
"""
