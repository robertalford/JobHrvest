"""Failure-mode analysis using the local Ollama instance.

After every experiment, the orchestrator collects the holdout cases the
challenger got wrong and asks the local LLM to surface common patterns and
suggest implementable feature improvements. Output is structured JSON so it
can drive automated next-iteration challenger generation.

We deliberately use Ollama (already running per CLAUDE.md) instead of the
Claude API — it keeps the loop offline-capable and free per iteration.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Optional

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)


FAILURE_ANALYSIS_PROMPT = """\
You are helping improve a machine learning model that classifies whether a web \
page is a careers/jobs page, and that extracts job listings from such pages.

Below are {n} cases the model got WRONG on the GOLD holdout set. Each case shows:
- the URL
- the page title
- the model's prediction and confidence
- the ground truth
- a few key features extracted from the page

Your task: identify common patterns in these failures and propose specific, \
implementable improvements.

Respond ONLY with valid JSON in this exact shape:
{{
  "patterns": [
    "<one-sentence pattern observed across multiple failures>",
    ...
  ],
  "missing_features": [
    "<a feature the model clearly isn't using that would help>",
    ...
  ],
  "suggested_features": [
    {{
      "name": "<snake_case feature name>",
      "description": "<one sentence>",
      "python_pseudocode": "<a few lines of pseudocode using BeautifulSoup or regex>"
    }},
    ...
  ],
  "edge_cases": [
    "<categories of pages the model will keep struggling with>",
    ...
  ]
}}

---
FAILED CASES:
{cases}
---
"""


@dataclass
class FailureCase:
    url: str
    title: str
    predicted_label: int
    predicted_confidence: float
    true_label: int
    features: dict

    @property
    def failure_type(self) -> str:
        if self.predicted_label == 1 and self.true_label == 0:
            return "false_positive"
        if self.predicted_label == 0 and self.true_label == 1:
            return "false_negative"
        return "correct"


def _format_cases(cases: list[FailureCase], max_cases: int = 30) -> str:
    """Render up to `max_cases` failures as a readable text block."""
    chunks = []
    for case in cases[:max_cases]:
        nonzero_features = {k: v for k, v in case.features.items() if v not in (0, 0.0, None, "", False)}
        chunks.append(
            f"URL: {case.url}\n"
            f"Title: {case.title}\n"
            f"Type: {case.failure_type}\n"
            f"Confidence: {case.predicted_confidence:.3f}\n"
            f"Key features: {json.dumps(nonzero_features, default=str)[:600]}"
        )
    return "\n\n".join(chunks)


async def analyze_failures(
    cases: list[FailureCase],
    *,
    model: Optional[str] = None,
    timeout_s: int = 120,
) -> dict:
    """Send a batch of failure cases to the local LLM and parse its analysis.

    Returns a dict with keys: patterns, missing_features, suggested_features,
    edge_cases. On any error returns an empty-skeleton dict — the orchestrator
    treats "no analysis produced" as a non-fatal soft failure.
    """
    if not cases:
        return _empty_analysis()

    prompt = FAILURE_ANALYSIS_PROMPT.format(
        n=min(len(cases), 30),
        cases=_format_cases(cases),
    )

    target_model = model or settings.OLLAMA_MODEL

    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            resp = await client.post(
                f"{settings.OLLAMA_BASE_URL}/api/generate",
                json={
                    "model": target_model,
                    "prompt": prompt,
                    "format": "json",
                    "stream": False,
                    "options": {"temperature": 0.2, "num_predict": 2000},
                },
            )
            resp.raise_for_status()
            payload = resp.json()
            raw = payload.get("response", "")
    except (httpx.HTTPError, json.JSONDecodeError) as e:
        logger.warning("failure_analysis: Ollama call failed: %s", e)
        return _empty_analysis()

    parsed = _safe_parse_json(raw)
    if not parsed:
        logger.warning("failure_analysis: LLM returned non-JSON output")
        return _empty_analysis()

    # Coerce missing keys to empty lists for predictable downstream use
    return {
        "patterns": parsed.get("patterns") or [],
        "missing_features": parsed.get("missing_features") or [],
        "suggested_features": parsed.get("suggested_features") or [],
        "edge_cases": parsed.get("edge_cases") or [],
    }


def _safe_parse_json(text: str) -> Optional[dict]:
    """Parse JSON tolerant of fenced code blocks and trailing prose."""
    if not text:
        return None
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.MULTILINE)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # Try to extract the first {...} block
        match = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None


def _empty_analysis() -> dict:
    return {"patterns": [], "missing_features": [], "suggested_features": [], "edge_cases": []}
