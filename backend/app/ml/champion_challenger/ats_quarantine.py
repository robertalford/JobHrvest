"""Quarantine layer for LLM-suggested ATS selectors.

When the LLM proposes a new ATS pattern, we don't trust it straight to
production. The lifecycle is:

  proposed → shadow → active
                    ↘ rejected

  - proposed:  selectors saved, but never matched against live pages
  - shadow:    selectors run alongside the existing extractor; results compared
               but NOT used. We track shadow_match_count and shadow_failure_count.
  - active:    selectors used in production after passing shadow gate
  - rejected:  failed shadow gate (too few successful matches, or failure rate
               too high)

Promotion thresholds live in `ShadowPromotionCriteria`. The defaults are
intentionally strict — promotion is permanent and a bad selector breaks
extraction for entire ATS platforms.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class ShadowPromotionCriteria:
    min_match_count: int = 25
    max_failure_rate: float = 0.10
    min_observation_window_hours: float = 24.0


@dataclass
class ProposalState:
    """Mutable snapshot of an AtsPatternProposal row.

    The orchestrator passes one of these in; this module returns the next
    state. Keeping the policy pure-functional makes the promotion logic
    trivially testable without touching the DB.
    """
    status: str                           # 'proposed' | 'shadow' | 'active' | 'rejected'
    shadow_match_count: int = 0
    shadow_failure_count: int = 0
    shadow_first_seen: datetime | None = None
    shadow_last_seen: datetime | None = None


def begin_shadow(state: ProposalState, *, now: datetime | None = None) -> ProposalState:
    """Move a 'proposed' proposal into shadow mode."""
    if state.status != "proposed":
        raise ValueError(f"begin_shadow only valid from 'proposed', got {state.status!r}")
    ts = now or datetime.now(timezone.utc)
    return ProposalState(
        status="shadow",
        shadow_match_count=0,
        shadow_failure_count=0,
        shadow_first_seen=ts,
        shadow_last_seen=ts,
    )


def record_shadow_observation(
    state: ProposalState,
    *,
    matched: bool,
    now: datetime | None = None,
) -> ProposalState:
    """Increment counters on each shadow-mode run.

    `matched=True` means the proposed selectors successfully extracted
    something with structure consistent with a job listing. `matched=False`
    means the selectors fired but produced empty/unusable output.
    """
    if state.status != "shadow":
        raise ValueError(f"record_shadow_observation requires 'shadow' status, got {state.status!r}")
    ts = now or datetime.now(timezone.utc)
    return ProposalState(
        status="shadow",
        shadow_match_count=state.shadow_match_count + (1 if matched else 0),
        shadow_failure_count=state.shadow_failure_count + (0 if matched else 1),
        shadow_first_seen=state.shadow_first_seen or ts,
        shadow_last_seen=ts,
    )


def evaluate_promotion(
    state: ProposalState,
    *,
    criteria: ShadowPromotionCriteria | None = None,
    now: datetime | None = None,
) -> tuple[str, str]:
    """Check whether a shadow proposal is ready for promotion or rejection.

    Returns (next_status, reason). `next_status` is one of:
      - 'shadow'  : keep observing
      - 'active'  : promote
      - 'rejected': reject

    Strict by default: a proposal must accumulate at least min_match_count
    successful matches AND have a low enough failure rate AND have been in
    shadow for the minimum observation window before it can be promoted.
    Failure rate is gated as soon as the denominator is large enough — a
    pattern that fires often but mostly misses gets rejected fast.
    """
    if state.status != "shadow":
        raise ValueError(f"evaluate_promotion requires 'shadow' status, got {state.status!r}")
    crit = criteria or ShadowPromotionCriteria()
    ts = now or datetime.now(timezone.utc)

    total = state.shadow_match_count + state.shadow_failure_count

    # Early rejection: enough fires to call a high failure rate
    if total >= max(10, crit.min_match_count // 2):
        failure_rate = state.shadow_failure_count / total
        if failure_rate > crit.max_failure_rate:
            return "rejected", (
                f"failure rate {failure_rate:.1%} > {crit.max_failure_rate:.1%} "
                f"after {total} observations"
            )

    if state.shadow_match_count < crit.min_match_count:
        return "shadow", (
            f"need {crit.min_match_count - state.shadow_match_count} more matches "
            f"(have {state.shadow_match_count})"
        )

    # Window check (uses first_seen so a sudden burst can't promote a fresh proposal)
    if state.shadow_first_seen is None:
        return "shadow", "no observation window started yet"
    elapsed_hours = (ts - state.shadow_first_seen).total_seconds() / 3600
    if elapsed_hours < crit.min_observation_window_hours:
        return "shadow", (
            f"observation window {elapsed_hours:.1f}h < required "
            f"{crit.min_observation_window_hours}h"
        )

    failure_rate = state.shadow_failure_count / max(total, 1)
    return "active", (
        f"promoted: {state.shadow_match_count} matches, "
        f"failure rate {failure_rate:.1%}, observed {elapsed_hours:.1f}h"
    )
