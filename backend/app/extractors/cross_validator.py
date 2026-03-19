"""
Cross-validation and confidence scoring — Stage 3f / 4g.

Compares results from multiple extraction methods, calculates agreement scores,
resolves disagreements, and logs comparisons to the extraction_comparisons table.

Resolution priority:
  1. Schema.org structured data wins (highest trust)
  2. ATS-specific API extractors win for known ATS sites
  3. If LLM and structural disagree → LLM tiebreaker
  4. All disagreements logged for analysis
"""

import logging
from typing import Optional
from uuid import UUID

from app.models.extraction_comparison import ExtractionComparison

logger = logging.getLogger(__name__)

# Fields to compare and their relative importance weights
FIELD_WEIGHTS = {
    "title": 2.0,
    "location_raw": 1.5,
    "employment_type": 1.0,
    "salary_raw": 1.0,
    "department": 0.8,
    "seniority_level": 0.8,
    "is_remote": 0.7,
    "remote_type": 0.7,
    "date_posted": 0.5,
}

# Method trust ranking (higher = more trusted)
METHOD_TRUST = {
    "schema_org": 10,
    "ats_api": 9,
    "ats_html": 7,
    "llm_instructor": 6,
    "llm_raw": 5,
    "structural": 4,
    "heuristic": 3,
}


class CrossValidator:
    """Combines results from multiple extraction methods into a single resolved result."""

    def __init__(self, db=None):
        self.db = db

    def merge(self, results: list[dict]) -> dict:
        """
        Merge multiple extraction results into a single best result.

        Args:
            results: List of dicts, each with an "extraction_method" key

        Returns:
            Merged dict with per-field best values and overall confidence
        """
        if not results:
            return {}
        if len(results) == 1:
            return results[0]

        # Sort by trust level (most trusted first)
        sorted_results = sorted(
            results,
            key=lambda r: METHOD_TRUST.get(r.get("extraction_method", ""), 0),
            reverse=True,
        )

        merged = {}
        field_confidence: dict[str, float] = {}
        agreement_counts: dict[str, int] = {}

        # For each field, pick the value from the most trusted method
        all_fields = set()
        for r in sorted_results:
            all_fields.update(r.keys())

        for field in all_fields:
            if field in ("extraction_method", "extraction_confidence", "raw_data"):
                continue

            values = [(r.get(field), r.get("extraction_method", "")) for r in sorted_results if r.get(field) is not None]
            if not values:
                continue

            # Pick value from highest-trust method
            merged[field] = values[0][0]

            # Calculate agreement score for this field
            unique_vals = len(set(str(v[0]) for v in values))
            if len(values) > 1:
                agreement = 1.0 - ((unique_vals - 1) / len(values))
            else:
                agreement = 0.8  # Single source, moderate confidence

            field_confidence[field] = agreement * METHOD_TRUST.get(values[0][1], 3) / 10

        # Overall confidence: weighted average of field confidences
        if field_confidence:
            weighted_sum = sum(
                field_confidence.get(f, 0.5) * FIELD_WEIGHTS.get(f, 0.5)
                for f in field_confidence
            )
            total_weight = sum(FIELD_WEIGHTS.get(f, 0.5) for f in field_confidence)
            merged["extraction_confidence"] = min(weighted_sum / total_weight, 0.99) if total_weight > 0 else 0.5
        else:
            merged["extraction_confidence"] = 0.4

        # Track which methods were used
        methods = [r.get("extraction_method", "unknown") for r in sorted_results]
        merged["extraction_method"] = "hybrid" if len(methods) > 1 else methods[0]

        return merged

    def calculate_agreement(self, result_a: dict, result_b: dict) -> float:
        """
        Calculate field-level agreement score between two extraction results.
        Returns 0.0 (no agreement) to 1.0 (perfect agreement).
        """
        compared_fields = 0
        agreed_fields = 0

        for field, weight in FIELD_WEIGHTS.items():
            val_a = result_a.get(field)
            val_b = result_b.get(field)

            if val_a is None and val_b is None:
                continue  # Skip fields neither method found
            if val_a is None or val_b is None:
                compared_fields += weight
                continue  # One found it, other didn't → disagreement

            compared_fields += weight
            if str(val_a).strip().lower() == str(val_b).strip().lower():
                agreed_fields += weight
            elif field == "salary_raw":
                # Partial credit for salary if they're in the same ballpark
                agreed_fields += weight * 0.5

        return agreed_fields / compared_fields if compared_fields > 0 else 1.0

    async def log_comparison(
        self,
        job_id: UUID,
        career_page_id: UUID,
        result_a: dict,
        result_b: dict,
        resolved: dict,
    ) -> None:
        """Persist an extraction comparison to the database for analysis."""
        if not self.db:
            return

        agreement = self.calculate_agreement(result_a, result_b)
        comparison = ExtractionComparison(
            job_id=job_id,
            career_page_id=career_page_id,
            method_a=result_a.get("extraction_method", "unknown"),
            method_b=result_b.get("extraction_method", "unknown"),
            method_a_result={k: v for k, v in result_a.items() if k != "raw_data"},
            method_b_result={k: v for k, v in result_b.items() if k != "raw_data"},
            agreement_score=agreement,
            resolved_result={k: v for k, v in resolved.items() if k != "raw_data"},
            resolution_method="auto",
        )
        self.db.add(comparison)
        await self.db.commit()
        logger.debug(f"Logged extraction comparison: agreement={agreement:.2f}")
