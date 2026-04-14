"""Bulk Domain Processor — standalone Champion-model runner.

Takes a list of domains, runs the current champion site-config model over each,
and returns a CSV with extraction selectors. Selector columns are blanked when
the model's confidence is below the configured threshold so the output can be
imported directly into the production system without manual review of low-
confidence rows.

The pure CSV parse/build functions in this module are covered by unit tests
(see tests/unit/test_bulk_domain_processor.py) because the column schema is a
contract with the external import system.
"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

from app.extractors.template_learner import TARGET_FIELDS

logger = logging.getLogger(__name__)


DEFAULT_CONFIDENCE_THRESHOLD = 0.8


CSV_OUTPUT_FIELDS: list[str] = [
    "domain",
    "careers_url",
    "listing_url",
    "pagination_type",
    "pagination_selector",
    "requires_js_rendering",
    *[f"selector_{field}" for field in TARGET_FIELDS],
    "confidence",
    "status",
    "error",
]


@dataclass
class DomainResult:
    """One row of the output CSV."""
    domain: str
    careers_url: Optional[str] = None
    listing_url: Optional[str] = None
    pagination_type: Optional[str] = None
    pagination_selector: Optional[str] = None
    requires_js_rendering: bool = False
    selectors: dict[str, str] = field(default_factory=dict)
    confidence: float = 0.0
    status: str = "pending"  # ok | low_confidence | failed | pending
    error: Optional[str] = None


def parse_input_csv(csv_text: str) -> list[str]:
    """Parse a user-uploaded CSV into a list of normalised domains.

    Accepts single-column CSVs with or without a `domain` header. Strips
    scheme/path, lowercases, and deduplicates while preserving first-seen order.
    """
    domains: list[str] = []
    seen: set[str] = set()
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    if not rows:
        raise ValueError("no domains supplied")

    # Skip a header row when the first cell is literally "domain".
    start = 1 if rows and rows[0] and rows[0][0].strip().lower() == "domain" else 0

    for row in rows[start:]:
        if not row:
            continue
        raw = row[0].strip()
        if not raw:
            continue
        normalised = _normalise_domain(raw)
        if not normalised or normalised in seen:
            continue
        seen.add(normalised)
        domains.append(normalised)

    if not domains:
        raise ValueError("no domains supplied")
    return domains


def _normalise_domain(value: str) -> str:
    """Strip scheme/path and lowercase. `https://WWW.Atlassian.com/careers` -> `atlassian.com`."""
    value = value.strip().lower()
    if not value:
        return ""
    # urlparse needs a scheme to populate netloc
    to_parse = value if "://" in value else f"http://{value}"
    host = urlparse(to_parse).netloc or value
    # Strip leading www.
    if host.startswith("www."):
        host = host[4:]
    # Drop port
    host = host.split(":")[0]
    return host


def build_output_csv(
    results: list[DomainResult],
    confidence_threshold: float = DEFAULT_CONFIDENCE_THRESHOLD,
) -> str:
    """Serialise results to CSV using the fixed column schema.

    Selector columns are emitted only when `confidence >= threshold`. Rows with
    lower confidence keep their domain + careers_url + status + error but have
    every `selector_*` column blanked — the external import system can then
    safely ingest the whole file.
    """
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_OUTPUT_FIELDS)
    writer.writeheader()
    for r in results:
        emit_selectors = r.confidence >= confidence_threshold and r.status == "ok"
        row = {
            "domain": r.domain,
            "careers_url": r.careers_url or "",
            "listing_url": r.listing_url or "",
            "pagination_type": r.pagination_type or "",
            "pagination_selector": r.pagination_selector or "",
            "requires_js_rendering": "true" if r.requires_js_rendering else "false",
            "confidence": f"{r.confidence:.2f}" if r.confidence else "0.00",
            "status": r.status,
            "error": r.error or "",
        }
        for field_name in TARGET_FIELDS:
            row[f"selector_{field_name}"] = (
                r.selectors.get(field_name, "") if emit_selectors else ""
            )
        writer.writerow(row)
    return buf.getvalue()


async def process_domains(
    domains: list[str],
    confidence_threshold: float = DEFAULT_CONFIDENCE_THRESHOLD,
) -> list[DomainResult]:
    """Run the current champion site-config model over each domain.

    This is a thin orchestrator that calls into existing discovery + extraction
    services. The heavy pipeline (careers discovery, layered structure extractor,
    template learner) lives in app.services / app.extractors. For now we return
    a not-yet-run placeholder per domain so the UI + CSV round-trip is usable;
    wiring the real pipeline is the next increment.
    """
    results: list[DomainResult] = []
    for domain in domains:
        try:
            result = await _run_champion_for_domain(domain)
        except Exception as exc:  # noqa: BLE001
            logger.exception("bulk champion run failed for %s", domain)
            result = DomainResult(
                domain=domain,
                status="failed",
                error=str(exc)[:200],
            )
        results.append(result)
    return results


async def _run_champion_for_domain(domain: str) -> DomainResult:
    """Invoke the current champion model for one domain.

    TODO: wire to ChampionChallengerOrchestrator + SiteStructureExtractor. This
    placeholder returns a pending row so the end-to-end CSV round-trip works
    while the model-serving path is built out in a follow-up.
    """
    return DomainResult(
        domain=domain,
        status="pending",
        error="champion model serving not yet wired — run queued",
    )
