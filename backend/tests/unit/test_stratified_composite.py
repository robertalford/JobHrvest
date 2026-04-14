"""Unit tests for the stratified composite scorer + cluster-gate verdict.

Covers the L1 layer of the universality-first redesign:
  - `_composite_score_stratified` buckets results by ATS and returns per-
    stratum axes + gate_eligible flags.
  - `_cluster_gate_verdict` blocks promotion when any ≥3-site cluster
    regresses by >CLUSTER_REGRESSION_TOLERANCE points.
"""

import pytest

from app.api.v1.endpoints.ml_models import (
    _composite_score_standalone,
    _composite_score_stratified,
    _cluster_gate_verdict,
    _stratum_key,
    CLUSTER_REGRESSION_TOLERANCE,
    MIN_STRATUM_SITES_FOR_GATE,
)


def _entry(
    *,
    url: str,
    ats: str | None,
    baseline_jobs: int,
    model_jobs: int,
    jobs_quality: int | None = None,
    champion_jobs: int | None = None,
    url_found: str | None = None,
    quality_warning: bool = False,
) -> dict:
    """Build a minimal results_detail.sites[] entry for scoring."""
    jq = jobs_quality if jobs_quality is not None else model_jobs
    model = {
        "jobs": model_jobs,
        "jobs_quality": jq,
        "fields": {"_core_complete": jq, "title": jq, "source_url": jq,
                   "location_raw": jq, "description": jq},
        "url_found": url_found or url,
        "tier_used": "jsonld",
    }
    if quality_warning:
        model["quality_warning"] = "synthetic"
    champ = None
    if champion_jobs is not None:
        # Matched field structure with the challenger so field_completeness
        # comparisons aren't distorted by fixture asymmetry.
        champ = {
            "jobs": champion_jobs,
            "jobs_quality": champion_jobs,
            "fields": {"_core_complete": champion_jobs, "title": champion_jobs,
                       "source_url": champion_jobs, "location_raw": champion_jobs,
                       "description": champion_jobs},
            "url_found": url_found or url,
        }
    return {
        "url": url,
        "ats_platform": ats,
        "baseline": {"jobs": baseline_jobs,
                     "fields": {"_core_complete": baseline_jobs}},
        "model": model,
        "champion": champ,
        "match": "model_equal_or_better" if jq >= baseline_jobs * 0.9 else "partial",
    }


def test_stratum_key_prefers_explicit_ats():
    e = {"ats_platform": "workday", "url": "https://nope.example.com"}
    assert _stratum_key(e) == "workday"


def test_stratum_key_fallback_structural():
    e = {"url": "https://anything.example.com/careers",
         "baseline": {"full_wrapper": {}},
         "model": {"url_found": "https://anything.example.com/careers",
                   "tier_used": "__next_data__"}}
    assert _stratum_key(e) == "spa_shell"


def test_stratified_matches_global_on_single_bucket():
    """If every site shares one ATS, by_stratum['workday'] == all."""
    results = [
        _entry(url=f"https://site{i}.example.com", ats="workday",
               baseline_jobs=10, model_jobs=10)
        for i in range(4)
    ]
    strat = _composite_score_stratified(results, "model")
    assert "workday" in strat["by_stratum"]
    assert strat["by_stratum"]["workday"]["n"] == 4
    assert strat["by_stratum"]["workday"]["composite"] == strat["all"]["composite"]


def test_gate_eligibility_threshold():
    """Strata with <MIN_STRATUM_SITES_FOR_GATE (3) sites are reported but not gate-eligible."""
    results = [
        _entry(url="https://w1.example.com", ats="workday", baseline_jobs=10, model_jobs=10),
        _entry(url="https://w2.example.com", ats="workday", baseline_jobs=10, model_jobs=10),
        _entry(url="https://g1.example.com", ats="greenhouse", baseline_jobs=10, model_jobs=10),
        _entry(url="https://g2.example.com", ats="greenhouse", baseline_jobs=10, model_jobs=10),
        _entry(url="https://g3.example.com", ats="greenhouse", baseline_jobs=10, model_jobs=10),
    ]
    strat = _composite_score_stratified(results, "model")
    assert strat["by_stratum"]["workday"]["gate_eligible"] is False
    assert strat["by_stratum"]["greenhouse"]["gate_eligible"] is True
    assert strat["n_strata_gate_eligible"] == 1
    assert strat["worst_gate_eligible"]["stratum"] == "greenhouse"


def test_cluster_gate_blocks_on_regression():
    """Challenger keeps greenhouse high but tanks workday — gate blocks."""
    urls = [f"https://site{i}.example.com" for i in range(10)]
    # 5 workday + 5 greenhouse. Champion=all good. Challenger = workday broken.
    results = []
    for i in range(5):
        results.append(_entry(url=urls[i], ats="workday",
                              baseline_jobs=10, model_jobs=2,
                              champion_jobs=10))  # challenger=2 of 10
    for i in range(5, 10):
        results.append(_entry(url=urls[i], ats="greenhouse",
                              baseline_jobs=10, model_jobs=10,
                              champion_jobs=10))

    ch_strat = _composite_score_stratified(results, "model")
    cp_strat = _composite_score_stratified(results, "champion")

    # Global challenger composite may still beat champion on some axes,
    # but the cluster gate must block.
    verdict = _cluster_gate_verdict(ch_strat, cp_strat)
    assert verdict["passed"] is False
    regressed_strata = {r["stratum"] for r in verdict["regressions"]}
    assert "workday" in regressed_strata


def test_cluster_gate_passes_on_improvement():
    """Challenger beats champion on every cluster → gate passes."""
    urls = [f"https://site{i}.example.com" for i in range(6)]
    results = []
    for i in range(3):
        results.append(_entry(url=urls[i], ats="workday",
                              baseline_jobs=10, model_jobs=10,
                              champion_jobs=8))
    for i in range(3, 6):
        results.append(_entry(url=urls[i], ats="greenhouse",
                              baseline_jobs=10, model_jobs=10,
                              champion_jobs=7))

    ch_strat = _composite_score_stratified(results, "model")
    cp_strat = _composite_score_stratified(results, "champion")
    verdict = _cluster_gate_verdict(ch_strat, cp_strat)
    assert verdict["passed"] is True
    assert verdict["regressions"] == []


def test_small_cluster_regression_does_not_block():
    """Only 1 bespoke site regresses — n<3 so it's reported but not gate-blocking."""
    urls = [f"https://site{i}.example.com" for i in range(5)]
    # 4 greenhouse (stable) + 1 bespoke (regressed)
    results = [
        _entry(url=urls[0], ats="greenhouse", baseline_jobs=10,
               model_jobs=10, champion_jobs=10),
        _entry(url=urls[1], ats="greenhouse", baseline_jobs=10,
               model_jobs=10, champion_jobs=10),
        _entry(url=urls[2], ats="greenhouse", baseline_jobs=10,
               model_jobs=10, champion_jobs=10),
        _entry(url=urls[3], ats="greenhouse", baseline_jobs=10,
               model_jobs=10, champion_jobs=10),
        _entry(url=urls[4], ats="bespoke", baseline_jobs=10,
               model_jobs=1, champion_jobs=10),
    ]
    ch_strat = _composite_score_stratified(results, "model")
    cp_strat = _composite_score_stratified(results, "champion")
    verdict = _cluster_gate_verdict(ch_strat, cp_strat)
    # bespoke cluster n=1 → not gate-eligible → gate still passes
    assert verdict["passed"] is True


def test_tolerance_absorbs_noise():
    """Regression within CLUSTER_REGRESSION_TOLERANCE does not block."""
    urls = [f"https://site{i}.example.com" for i in range(3)]
    # Tiny, sub-tolerance regression per-site — aggregate composite stays within
    # CLUSTER_REGRESSION_TOLERANCE (2.0 pts default) of champion.
    results = [
        _entry(url=urls[i], ats="workday",
               baseline_jobs=100, model_jobs=99, champion_jobs=100)
        for i in range(3)
    ]
    ch_strat = _composite_score_stratified(results, "model")
    cp_strat = _composite_score_stratified(results, "champion")
    verdict = _cluster_gate_verdict(ch_strat, cp_strat, tolerance=CLUSTER_REGRESSION_TOLERANCE)
    assert verdict["passed"] is True


def test_worst_invariant_blocks_when_worst_drops():
    """Challenger shuffles wins: best cluster up, worst cluster down. Blocked.

    Champion: workday solid (100%), greenhouse solid too (100%).
    Challenger: workday still solid, greenhouse tanks to 0.
    Global composite may be dragged down by an unrelated axis, but the
    promotion gate must block on the worst-cluster invariant alone.
    """
    urls = [f"https://site{i}.example.com" for i in range(6)]
    results = []
    for i in range(3):
        results.append(_entry(url=urls[i], ats="workday",
                              baseline_jobs=10, model_jobs=10,
                              champion_jobs=10))
    # Greenhouse: champion passes 10/10; challenger fails hard (0).
    for i in range(3, 6):
        results.append(_entry(url=urls[i], ats="greenhouse",
                              baseline_jobs=10, model_jobs=0,
                              champion_jobs=10))
    ch_strat = _composite_score_stratified(results, "model")
    cp_strat = _composite_score_stratified(results, "champion")
    verdict = _cluster_gate_verdict(ch_strat, cp_strat)
    # worst cluster (greenhouse) regressed; gate blocks.
    assert verdict["passed"] is False
