"""Unit tests for ATS-pattern quarantine state machine."""

from datetime import datetime, timedelta, timezone

import pytest

from app.ml.champion_challenger.ats_quarantine import (
    ProposalState,
    ShadowPromotionCriteria,
    begin_shadow,
    record_shadow_observation,
    evaluate_promotion,
)


def _now() -> datetime:
    return datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)


class TestBeginShadow:
    def test_transitions_proposed_to_shadow(self):
        s = begin_shadow(ProposalState(status="proposed"), now=_now())
        assert s.status == "shadow"
        assert s.shadow_match_count == 0
        assert s.shadow_first_seen == _now()

    def test_invalid_from_active(self):
        with pytest.raises(ValueError):
            begin_shadow(ProposalState(status="active"))


class TestRecordObservation:
    def test_increments_match_count(self):
        s = begin_shadow(ProposalState(status="proposed"), now=_now())
        s = record_shadow_observation(s, matched=True, now=_now())
        s = record_shadow_observation(s, matched=True, now=_now())
        assert s.shadow_match_count == 2
        assert s.shadow_failure_count == 0

    def test_increments_failure_count(self):
        s = begin_shadow(ProposalState(status="proposed"), now=_now())
        s = record_shadow_observation(s, matched=False, now=_now())
        assert s.shadow_failure_count == 1

    def test_rejects_when_not_in_shadow(self):
        with pytest.raises(ValueError):
            record_shadow_observation(ProposalState(status="proposed"), matched=True)


class TestEvaluatePromotion:
    def _shadow(self, **overrides) -> ProposalState:
        defaults = dict(
            status="shadow",
            shadow_match_count=0,
            shadow_failure_count=0,
            shadow_first_seen=_now(),
            shadow_last_seen=_now(),
        )
        defaults.update(overrides)
        return ProposalState(**defaults)

    def test_keep_shadow_when_too_few_matches(self):
        state = self._shadow(shadow_match_count=5)
        nxt, reason = evaluate_promotion(state, now=_now() + timedelta(hours=48))
        assert nxt == "shadow"
        assert "more matches" in reason

    def test_promote_when_all_criteria_met(self):
        state = self._shadow(shadow_match_count=30, shadow_failure_count=1)
        nxt, _reason = evaluate_promotion(state, now=_now() + timedelta(hours=48))
        assert nxt == "active"

    def test_reject_high_failure_rate(self):
        # 50 fires, 30 failures → 60% failure rate, well above default 10%
        state = self._shadow(shadow_match_count=20, shadow_failure_count=30)
        nxt, reason = evaluate_promotion(state, now=_now() + timedelta(hours=48))
        assert nxt == "rejected"
        assert "failure rate" in reason

    def test_keep_shadow_until_window_elapses(self):
        # 30 matches, 0 failures, but only 1h elapsed → keep observing
        state = self._shadow(shadow_match_count=30, shadow_failure_count=0)
        nxt, reason = evaluate_promotion(state, now=_now() + timedelta(hours=1))
        assert nxt == "shadow"
        assert "window" in reason

    def test_custom_criteria(self):
        crit = ShadowPromotionCriteria(min_match_count=5, min_observation_window_hours=0.0)
        state = self._shadow(shadow_match_count=5, shadow_failure_count=0)
        nxt, _ = evaluate_promotion(state, criteria=crit, now=_now())
        assert nxt == "active"

    def test_rejects_unknown_status(self):
        with pytest.raises(ValueError):
            evaluate_promotion(ProposalState(status="proposed"))
