"""Build storage/ats_templates.json from Jobstream wrappers.

Clusters `fixed_test_sites.known_selectors` by detected ATS platform, then
for each cluster keeps the modal value of each wrapper key. The output is a
JSON file Codex + the extractor consume at runtime via ats_template_loader.

Running idempotent; overwriting the file in place. Re-run after new sites are
added to fixed_test_sites or when baseline wrappers are updated.

Usage (from inside a backend container):

    python -m scripts.build_ats_templates
    python -m scripts.build_ats_templates --out /storage/ats_templates.json

The script prints a per-ATS coverage summary and exits 0.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import text

from app.db.base import AsyncSessionLocal

logger = logging.getLogger("build_ats_templates")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# URL → ATS classification. Kept in sync with ats_template_loader so a site
# classified as `greenhouse` here is lookupable there.
_URL_FRAGMENTS: list[tuple[str, tuple[str, ...]]] = [
    ("greenhouse", ("greenhouse.io", "boards.greenhouse", "job-boards.greenhouse")),
    ("lever", ("lever.co", "jobs.lever")),
    ("workday", ("myworkdayjobs", "workday")),
    ("ashby", ("ashbyhq",)),
    ("bamboohr", ("bamboohr",)),
    ("smartrecruiters", ("smartrecruiters",)),
    ("icims", ("icims",)),
    ("taleo", ("taleo",)),
    ("successfactors", ("successfactors",)),
    ("jobvite", ("jobvite",)),
    ("breezyhr", ("breezy.hr", "breezyhr")),
    ("rippling", ("rippling.com/jobs", "ats.rippling")),
    ("pageup", ("pageuppeople",)),
    ("livehire", ("livehire",)),
    ("teamtailor", ("teamtailor",)),
    ("applynow", ("applynow",)),
    ("jazzhr", ("applytojob", "theresumator")),
    ("oracle_cx", ("oracle", "candidate-experience")),
    ("salesforce", ("salesforce",)),
    ("martianlogic", ("martianlogic", "myrecruitmentplus")),
]

# Wrapper keys we aggregate — these are the selector fields Jobstream uses.
_LIST_KEYS = (
    "record_boundary_path", "job_title_path", "min_container_path",
    "row_details_page_link_path", "row_location_paths", "row_description_paths",
    "row_salary_paths", "row_job_type_paths", "row_listed_date_path",
    "row_closing_date_path", "job_title_url_pattern",
)
_DETAIL_KEYS = (
    "details_page_description_paths", "details_page_location_paths",
    "details_page_salary_path", "details_page_job_type_paths",
    "details_page_title_path", "details_page_apply_url_path",
)
_PAGINATION_KEYS = ("pagination",)


def _classify_ats(url: str) -> str | None:
    u = (url or "").lower()
    for ats, fragments in _URL_FRAGMENTS:
        for frag in fragments:
            if frag in u:
                return ats
    return None


def _normalise_value(v: Any) -> Any:
    """Hash-friendly representation for modal-voting.

    Lists get frozen to tuples so they're usable as dict keys. Empty / null /
    falsy values are dropped so they never win a tie.
    """
    if v is None or v == "" or v == [] or v == {}:
        return None
    if isinstance(v, list):
        return tuple(str(x) for x in v if x)
    return str(v) if not isinstance(v, (int, float, bool)) else v


def _modal(values: list[Any]) -> Any | None:
    """Pick the most-common non-null value. Ties are broken by first-seen."""
    candidates = [v for v in (_normalise_value(x) for x in values) if v]
    if not candidates:
        return None
    counter = Counter(candidates)
    top, _ = counter.most_common(1)[0]
    # If we froze a list to a tuple, unfreeze so the JSON output is natural
    if isinstance(top, tuple):
        return list(top)
    return top


def _build_template(cluster_rows: list[dict]) -> dict:
    """Collapse a list of wrapper dicts into a single template.

    Each key gets its modal value across the cluster. Low-cardinality keys
    usually converge within 2-3 samples; high-cardinality keys (description
    paths) fall back to the most common one.
    """
    list_selectors: dict = {}
    for k in _LIST_KEYS:
        modal = _modal([row.get(k) for row in cluster_rows])
        if modal:
            list_selectors[k] = modal

    detail_selectors: dict = {}
    for k in _DETAIL_KEYS:
        modal = _modal([row.get(k) for row in cluster_rows])
        if modal:
            detail_selectors[k] = modal

    pagination: dict = {}
    for k in _PAGINATION_KEYS:
        modal = _modal([row.get(k) for row in cluster_rows])
        if modal:
            pagination[k] = modal

    return {
        "list_selectors": list_selectors,
        "detail_selectors": detail_selectors,
        "pagination": pagination,
        "sample_count": len(cluster_rows),
    }


async def load_wrapper_rows() -> list[tuple[str, dict]]:
    """Return [(url, selectors_dict), ...] from fixed_test_sites."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(text(
            "SELECT url, known_selectors FROM fixed_test_sites "
            "WHERE known_selectors IS NOT NULL"
        ))
        rows = []
        for url, selectors in result.all():
            if isinstance(selectors, str):
                try:
                    selectors = json.loads(selectors)
                except json.JSONDecodeError:
                    continue
            if isinstance(selectors, dict) and selectors:
                rows.append((url, selectors))
    return rows


def build_templates(rows: list[tuple[str, dict]]) -> dict:
    clusters: dict[str, list[dict]] = defaultdict(list)
    sample_hosts: dict[str, list[str]] = defaultdict(list)
    skipped_no_ats = 0
    for url, selectors in rows:
        ats = _classify_ats(url)
        if not ats:
            skipped_no_ats += 1
            continue
        clusters[ats].append(selectors)
        host = urlparse(url).netloc.lstrip("www.")
        if host and host not in sample_hosts[ats]:
            sample_hosts[ats].append(host)

    templates: dict[str, dict] = {}
    for ats, cluster in clusters.items():
        tpl = _build_template(cluster)
        tpl["sample_domains"] = sample_hosts[ats][:5]
        templates[ats] = tpl

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "fixed_test_sites.known_selectors",
        "source_row_count": len(rows),
        "skipped_no_ats": skipped_no_ats,
        "templates": templates,
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description="Build ats_templates.json from baseline wrappers")
    parser.add_argument(
        "--out",
        default=os.environ.get(
            "ATS_TEMPLATES_PATH",
            os.path.join(os.getcwd(), "storage", "ats_templates.json"),
        ),
    )
    args = parser.parse_args()

    rows = await load_wrapper_rows()
    logger.info("loaded %d wrapper rows", len(rows))
    payload = build_templates(rows)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, default=str)
    logger.info("wrote %d ATS templates to %s", len(payload["templates"]), args.out)
    for ats, tpl in payload["templates"].items():
        logger.info(
            "  %-18s samples=%d  domains=%s",
            ats, tpl.get("sample_count", 0),
            ", ".join(tpl.get("sample_domains", []) or []),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
