"""
Job deduplication service.

Finds duplicate job listings for the same company — same role posted on multiple sources
(e.g., Greenhouse board AND company careers page) — and marks which is canonical (best).

Deduplication strategy:
- Group jobs within a company by normalized title similarity
- Within each group, score candidates on description overlap (Jaccard) + location + date proximity
- The canonical job is the one with the richest data and highest-trust extraction source
- Duplicates are marked with canonical_job_id pointing to the winner
- Both are kept in the DB for redundancy/source tracking
"""

import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Extraction method trust order (higher = more trustworthy)
METHOD_TRUST = {
    "schema_org": 6,
    "ats_api": 5,
    "ats_html": 4,
    "hybrid": 4,
    "llm": 3,
    "structural": 2,
    "root_fallback": 1,
    "domain_signal": 1,
}

STOPWORDS = frozenset({
    "a", "an", "the", "and", "or", "of", "in", "at", "to", "for", "with",
    "on", "is", "are", "we", "you", "our", "your", "this", "that", "as",
    "be", "will", "have", "has", "from", "by", "it", "its", "we're",
})


def _extract_url_job_id(url: Optional[str]) -> Optional[str]:
    """Extract a probable job identifier from the URL path.

    Looks for the last numeric-dominant or hex-like path segment that looks like
    a job/folder ID (e.g. /FolderDetail/Senior-Spa-Therapist/427333 → '427333',
    or /job/P25-319090-1 → 'p25-319090-1').

    Returns None if no clear ID is found.
    """
    if not url:
        return None
    try:
        path = urlparse(url).path
    except Exception:
        return None
    segments = [s for s in path.split("/") if s]
    # Walk segments from the end — find the first that looks like an ID
    # An ID segment: mostly digits, or an alphanumeric code with dashes/letters
    _id_pat = re.compile(r'^[\w]{2,}[-\w]*$')  # at least 2 chars, word chars + dashes
    _mostly_digits = re.compile(r'^\d+$')
    for seg in reversed(segments):
        seg_lower = seg.lower()
        # Pure numeric → strong ID signal
        if _mostly_digits.match(seg):
            return seg_lower
        # Alphanumeric code (like P25-319090-1, 7AD7F9B2949F..., R-062707)
        if _id_pat.match(seg) and re.search(r'\d', seg) and len(seg) >= 4:
            # Exclude common non-ID slugs
            if seg_lower not in {
                "jobs", "careers", "apply", "job", "position", "detail",
                "search", "listing", "vacancy", "opportunity", "folder",
            }:
                return seg_lower
    return None


def _normalize_title(title: str) -> str:
    """Normalize a job title for comparison."""
    t = title.lower().strip()
    # Remove common noise patterns
    t = re.sub(r'\(.*?\)', '', t)           # Remove parenthetical notes
    t = re.sub(r'\[.*?\]', '', t)           # Remove bracket notes
    t = re.sub(r'\s*[-–—/|]\s*.*$', '', t)  # Remove sub-titles after separators
    t = re.sub(r'\b(ref|req|id|#)\s*[\w\d]+', '', t, flags=re.I)  # Remove reference IDs
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def _tokenize(text: str) -> frozenset[str]:
    """Tokenize text into a set of significant words."""
    words = re.findall(r'[a-z]+', text.lower())
    return frozenset(w for w in words if w not in STOPWORDS and len(w) > 2)


def _jaccard_similarity(a: frozenset, b: frozenset) -> float:
    """Jaccard similarity between two token sets."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _title_similarity(t1: str, t2: str) -> float:
    """Fuzzy title similarity — exact normalized match = 1.0, token overlap otherwise."""
    n1 = _normalize_title(t1)
    n2 = _normalize_title(t2)
    if n1 == n2:
        return 1.0
    tok1 = _tokenize(n1)
    tok2 = _tokenize(n2)
    return _jaccard_similarity(tok1, tok2)


def _desc_similarity(d1: Optional[str], d2: Optional[str]) -> float:
    """Description similarity — token-based Jaccard on first 1000 chars.

    Both NULL → 0.5 (neutral/unknown — we cannot assert similarity from absence of data).
    One NULL  → 0.0 (asymmetric data, can't confirm match).
    Both present → Jaccard token overlap.
    """
    if not d1 and not d2:
        return 0.5   # Neutral: missing data on both sides — neither confirms nor denies
    if not d1 or not d2:
        return 0.0   # One side has data, the other doesn't — suspicious, penalise
    tok1 = _tokenize(d1[:1000])
    tok2 = _tokenize(d2[:1000])
    return _jaccard_similarity(tok1, tok2)


def _date_proximity(d1: Optional[str], d2: Optional[str]) -> float:
    """Date proximity score — same date = 1.0, within 90 days = 0.5, otherwise 0."""
    if not d1 or not d2:
        return 0.5  # Unknown — give benefit of doubt
    try:
        dt1 = datetime.fromisoformat(str(d1)[:10])
        dt2 = datetime.fromisoformat(str(d2)[:10])
        diff = abs((dt1 - dt2).days)
        if diff == 0:
            return 1.0
        if diff <= 14:
            return 0.8
        if diff <= 90:
            return 0.5
        return 0.0
    except Exception:
        return 0.5


def _location_similarity(loc1: Optional[str], loc2: Optional[str]) -> float:
    """Location similarity — fuzzy match on location strings.

    Both NULL → 0.5 (neutral — absence of location data doesn't mean same location).
    One NULL  → 0.5 (benefit of doubt, could be same role different source).
    Both present → Jaccard token overlap.
    """
    if not loc1 and not loc2:
        return 0.5   # Neutral: we don't know — don't assert identical
    if not loc1 or not loc2:
        return 0.5   # Benefit of doubt: one source may not publish location
    tok1 = _tokenize(loc1)
    tok2 = _tokenize(loc2)
    return _jaccard_similarity(tok1, tok2)


def _data_richness(job_dict: dict) -> int:
    """Score a job dict on data richness. More fields = higher score."""
    score = 0
    if job_dict.get("description") and len(job_dict["description"]) > 100:
        score += 3
    if job_dict.get("salary_raw"):
        score += 2
    if job_dict.get("location_raw"):
        score += 1
    if job_dict.get("employment_type"):
        score += 1
    if job_dict.get("requirements"):
        score += 2
    if job_dict.get("benefits"):
        score += 1
    if job_dict.get("date_posted"):
        score += 1
    if job_dict.get("external_id"):
        score += 1
    return score


def _pick_canonical(jobs: list[dict]) -> dict:
    """Given a group of duplicate jobs, pick the canonical (best) one.

    Ranking: extraction method trust > data richness > extraction_confidence
    """
    def rank(j: dict):
        trust = METHOD_TRUST.get(j.get("extraction_method") or "", 0)
        richness = _data_richness(j)
        confidence = j.get("extraction_confidence") or 0.0
        return (trust, richness, confidence)

    return max(jobs, key=rank)


def deduplicate_jobs(jobs: list[dict]) -> list[tuple[dict, list[dict]]]:
    """
    Given a list of job dicts for one company, group into canonical + duplicate pairs.

    Returns: list of (canonical_job, [duplicate_jobs]) tuples.
    Jobs that have no duplicates appear as (job, []).
    """
    if not jobs:
        return []

    # Build similarity groups using union-find
    n = len(jobs)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        parent[find(x)] = find(y)

    # Compare all pairs (O(n²) — acceptable for per-company batches of hundreds)
    for i in range(n):
        for j in range(i + 1, n):
            ji, jj = jobs[i], jobs[j]

            # Fast reject: if career_page is the same, likely same job already
            if ji.get("career_page_id") == jj.get("career_page_id"):
                continue

            # External ID exact match (same job, different source)
            if (ji.get("external_id") and jj.get("external_id") and
                    ji["external_id"] == jj["external_id"]):
                union(i, j)
                continue

            # URL-derived job ID match — catches cross-brand portals with shared folder/job IDs
            # e.g. Rosewood /en_US/careers/FolderDetail/427333 vs /en_US/newworld/FolderDetail/427333
            ui = ji.get("_url_id")
            uj = jj.get("_url_id")
            if ui and uj and ui == uj:
                union(i, j)
                continue

            # Title similarity threshold
            title_sim = _title_similarity(
                ji.get("title", ""), jj.get("title", "")
            )
            if title_sim < 0.75:
                continue

            # Description similarity
            desc_sim = _desc_similarity(
                ji.get("description"), jj.get("description")
            )
            # Location similarity
            loc_sim = _location_similarity(
                ji.get("location_raw"), jj.get("location_raw")
            )
            # Date proximity
            date_prox = _date_proximity(
                ji.get("date_posted"), jj.get("date_posted")
            )

            # Composite duplicate score
            dup_score = (title_sim * 0.4) + (desc_sim * 0.35) + (loc_sim * 0.15) + (date_prox * 0.1)

            # When jobs come from different career pages (different sites/regions),
            # require stronger evidence to avoid false positives like "Package Handler"
            # at 200 different FedEx facilities being collapsed into one canonical.
            different_pages = (
                ji.get("career_page_id") and jj.get("career_page_id") and
                ji["career_page_id"] != jj["career_page_id"]
            )
            threshold = 0.72 if different_pages else 0.65

            if dup_score >= threshold:
                union(i, j)

    # Collect groups
    groups: dict[int, list[int]] = {}
    for i in range(n):
        root = find(i)
        groups.setdefault(root, []).append(i)

    result = []
    for _, indices in groups.items():
        group_jobs = [jobs[i] for i in indices]
        canonical = _pick_canonical(group_jobs)
        duplicates = [j for j in group_jobs if j is not canonical]
        result.append((canonical, duplicates))

    return result


async def run_company_dedup(db, company_id: uuid.UUID) -> dict:
    """
    Run deduplication for all active jobs of a company.
    Updates is_canonical / canonical_job_id / duplicate_count in the DB.

    Returns: {"canonical": int, "duplicates": int, "groups": int}
    """
    from sqlalchemy import select, update
    from app.models.job import Job

    jobs = list(await db.scalars(
        select(Job).where(Job.company_id == company_id, Job.is_active == True)
    ))

    if len(jobs) < 2:
        return {"canonical": len(jobs), "duplicates": 0, "groups": len(jobs)}

    # Convert to dicts for dedup algorithm
    job_dicts = []
    for j in jobs:
        job_dicts.append({
            "_id": j.id,
            "title": j.title or "",
            "description": j.description,
            "location_raw": j.location_raw,
            "date_posted": str(j.date_posted) if j.date_posted else None,
            "external_id": j.external_id,
            "extraction_method": j.extraction_method,
            "extraction_confidence": j.extraction_confidence,
            "salary_raw": j.salary_raw,
            "employment_type": j.employment_type,
            "requirements": j.requirements,
            "benefits": j.benefits,
            "career_page_id": str(j.career_page_id) if j.career_page_id else None,
            "source_url": j.source_url,
            "_url_id": _extract_url_job_id(j.source_url),
        })

    groups = deduplicate_jobs(job_dicts)

    canonical_count = 0
    duplicate_count = 0

    # Build lookup: job _id → Job object
    job_map = {j.id: j for j in jobs}

    # Reset all jobs to canonical=True first
    await db.execute(
        update(Job)
        .where(Job.company_id == company_id, Job.is_active == True)
        .values(is_canonical=True, canonical_job_id=None, duplicate_count=0)
    )

    for canonical_dict, duplicate_dicts in groups:
        canonical_id = canonical_dict["_id"]
        n_dups = len(duplicate_dicts)

        if n_dups > 0:
            # Update canonical job's duplicate count
            await db.execute(
                update(Job).where(Job.id == canonical_id).values(
                    is_canonical=True,
                    canonical_job_id=None,
                    duplicate_count=n_dups,
                )
            )
            # Mark duplicates
            for dup_dict in duplicate_dicts:
                dup_id = dup_dict["_id"]
                # Compute score for this duplicate vs canonical
                score = _title_similarity(canonical_dict["title"], dup_dict["title"])
                await db.execute(
                    update(Job).where(Job.id == dup_id).values(
                        is_canonical=False,
                        canonical_job_id=canonical_id,
                        dedup_score=round(score, 3),
                    )
                )
                duplicate_count += 1

        canonical_count += 1

    await db.commit()
    logger.info(f"Dedup complete for {company_id}: {canonical_count} canonical, {duplicate_count} duplicates")
    return {
        "canonical": canonical_count,
        "duplicates": duplicate_count,
        "groups": len(groups),
    }
