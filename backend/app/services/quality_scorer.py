"""
Job quality scorer — deterministic, rule-based scoring.

Score bands:
  80–100 : Excellent
  60–79  : Good
  40–59  : Fair
  20–39  : Poor
  0–19   : Disqualified (scam / harmful)

Penalties cap the final score — they override positive scores.
"""

import re
from dataclasses import dataclass, field
from typing import Optional


# ─── Scam patterns ────────────────────────────────────────────────────────────

SCAM_PATTERNS = [
    r"\bwire transfer\b",
    r"\bsend money\b",
    r"\bbank account (details|number|info)\b",
    r"\bno interview required\b",
    r"\bguaranteed (income|salary|earnings)\b",
    r"\bearn \$[\d,]+[kK]? (per|a) (week|month|day) (from home|at home|working from)\b",
    r"\bwork from home.{0,30}earn\b",
    r"\bpay for (training|your kit|equipment|materials)\b",
    r"\bmulti.?level marketing\b",
    r"\bmlm\b",
    r"\bpyramid (scheme|selling)\b",
    r"\bsend your (cv|resume).{0,40}(gmail|yahoo|hotmail|outlook)\b",
    r"\bno experience (required|necessary|needed).{0,50}earn\b",
    r"\brecruitment fee\b",
    r"\bprocessing fee\b",
]

SCAM_RE = [re.compile(p, re.IGNORECASE) for p in SCAM_PATTERNS]

# ─── Bad words ────────────────────────────────────────────────────────────────
# Curated — common English profanity/obscenity likely to appear in spam jobs
_BAD_WORDS = [
    r"\bf[*u]ck(ing|ed|er|s)?\b",
    r"\bsh[*i]t(ty|s)?\b",
    r"\basshole\b",
    r"\bbastard\b",
    r"\bbitch(es|y)?\b",
    r"\bc[*u]nt\b",
    r"\bdick(head)?\b",
    r"\bdamn\b",
    r"\bwhor(e|es|ing)\b",
    r"\bslut\b",
    r"\bcocksucker\b",
    r"\bmotherfucker\b",
]
BAD_WORD_RE = [re.compile(p, re.IGNORECASE) for p in _BAD_WORDS]

# ─── Discrimination patterns ──────────────────────────────────────────────────

DISCRIMINATION_PATTERNS = [
    # Age
    r"\b(under|below|no older than|maximum age of?)\s+\d{2}\b",
    r"\b(must be|aged?|age requirement)\s+\d{2}[-–]\d{2}\b",
    r"\byoung (graduate|professional|candidate)\b",
    # Gender
    r"\b(female|male|woman|man|ladies) (only|preferred|candidates)\b",
    r"\b(must be|only) (male|female)\b",
    r"\bgentlemen only\b",
    r"\bfamily man\b",
    r"\bsingle (male|female|man|woman)\b",
    # Ethnicity / nationality
    r"\b(chinese|indian|filipino|korean|japanese|malay)\s+(national|citizen|only|preferred)\b",
    r"\b(nationals? only|citizens? only)\b",
    r"\b(chinese|caucasian|asian|european|african)\s+preferred\b",
    # Religion
    r"\b(christian|muslim|hindu|buddhist|jewish)\s+(preferred|only|candidates|values required)\b",
    r"\bmust (be|practice|follow)\s+\w+\s+religion\b",
    # Physical appearance (common in some markets)
    r"\bheight\s+(requirement|minimum|at least)\b",
    r"\bweight\s+(limit|requirement|maximum)\b",
    r"\bgood[-\s]looking\b",
    r"\battractive (appearance|look)\b",
]
DISCRIMINATION_RE = [re.compile(p, re.IGNORECASE) for p in DISCRIMINATION_PATTERNS]

# ─── Clickbait title patterns (not a real job title) ─────────────────────────

CLICKBAIT_TITLE_PATTERNS = [
    r"^amazing opportunity",
    r"^incredible (job|role|position|opportunity)",
    r"^dream (job|career|opportunity)",
    r"join our (team|family|growing team)\s*$",
    r"we are hiring\s*[!]*$",
    r"urgent (hiring|requirement|vacancy)",
    r"work from home.{0,20}earn",
    r"be your own boss",
    r"unlimited (earning|income) potential",
    r"change your life",
]
CLICKBAIT_RE = [re.compile(p, re.IGNORECASE) for p in CLICKBAIT_TITLE_PATTERNS]


@dataclass
class QualityResult:
    score: float = 0.0
    completeness_score: float = 0.0
    description_score: float = 0.0
    issues: list = field(default_factory=list)
    scam_detected: bool = False
    bad_words_detected: bool = False
    discrimination_detected: bool = False

    @property
    def band(self) -> str:
        if self.score >= 80:
            return "excellent"
        if self.score >= 60:
            return "good"
        if self.score >= 40:
            return "fair"
        if self.score >= 20:
            return "poor"
        return "disqualified"

    def to_dict(self) -> dict:
        return {
            "score": round(self.score, 1),
            "completeness_score": round(self.completeness_score, 1),
            "description_score": round(self.description_score, 1),
            "issues": self.issues,
            "flags": {
                "scam_detected": self.scam_detected,
                "bad_words_detected": self.bad_words_detected,
                "discrimination_detected": self.discrimination_detected,
            },
            "band": self.band,
        }


def score_job(
    title: Optional[str],
    description: Optional[str],
    location_raw: Optional[str],
    employment_type: Optional[str],
    date_posted=None,
    salary_raw: Optional[str] = None,
    requirements: Optional[str] = None,
) -> QualityResult:
    """Score a single job and return a QualityResult."""
    result = QualityResult()
    issues = []
    completeness = 0.0

    # ── 1. Field completeness (0–50) ─────────────────────────────────────────

    # Title (10 pts)
    if title and len(title.strip()) >= 3:
        is_clickbait = any(r.search(title) for r in CLICKBAIT_RE)
        if is_clickbait:
            issues.append("title_clickbait")
            completeness += 3  # partial credit
        else:
            completeness += 10
    else:
        issues.append("title_missing_or_short")

    # Location (8 pts)
    if location_raw and len(location_raw.strip()) >= 2:
        completeness += 8
    else:
        issues.append("location_missing")

    # Employment type (7 pts)
    if employment_type:
        completeness += 7
    else:
        issues.append("employment_type_missing")

    # Description length — partial credit (10 pts)
    desc_len = len((description or "").strip())
    if desc_len >= 500:
        completeness += 10
    elif desc_len >= 150:
        completeness += 6
    elif desc_len >= 50:
        completeness += 3
    else:
        issues.append("description_very_short")

    # Date posted (5 pts)
    if date_posted:
        completeness += 5
    else:
        issues.append("date_posted_missing")

    # Salary / compensation (5 pts)
    if salary_raw and len(salary_raw.strip()) >= 2:
        completeness += 5
    else:
        issues.append("salary_missing")

    # Requirements / responsibilities (5 pts)
    if requirements and len(requirements.strip()) >= 50:
        completeness += 5
    else:
        issues.append("requirements_missing")

    result.completeness_score = completeness  # 0–50

    # ── 2. Description quality (0–20) ─────────────────────────────────────────
    desc_quality = 0.0
    if desc_len >= 500:
        desc_quality = 20.0
    elif desc_len >= 150:
        desc_quality = 10.0
    elif desc_len >= 50:
        desc_quality = 0.0
    else:
        desc_quality = -5.0  # penalty for near-empty

    result.description_score = desc_quality

    # ── 3. Build pre-penalty score ────────────────────────────────────────────
    raw_score = max(0.0, completeness + desc_quality)

    # ── 4. Scam detection (cap at 10) ─────────────────────────────────────────
    full_text = " ".join(filter(None, [title, description, requirements]))
    scam_matches = [r.pattern for r in SCAM_RE if r.search(full_text)]
    if scam_matches:
        result.scam_detected = True
        issues.append(f"scam_patterns_detected: {', '.join(scam_matches[:3])}")

    # ── 5. Bad words (cap at 15) ──────────────────────────────────────────────
    bad_word_matches = [r.pattern for r in BAD_WORD_RE if r.search(full_text)]
    if bad_word_matches:
        result.bad_words_detected = True
        issues.append("inappropriate_language_detected")

    # ── 6. Discrimination (cap at 10) ─────────────────────────────────────────
    disc_matches = [r.pattern for r in DISCRIMINATION_RE if r.search(full_text)]
    if disc_matches:
        result.discrimination_detected = True
        issues.append(f"discrimination_language: {', '.join(disc_matches[:3])}")

    # ── 7. Apply caps in order of severity ────────────────────────────────────
    final = min(100.0, raw_score)
    if result.scam_detected:
        final = min(final, 10.0)
    if result.discrimination_detected:
        final = min(final, 10.0)
    if result.bad_words_detected:
        final = min(final, 15.0)

    result.score = max(0.0, final)
    result.issues = issues
    return result


def compute_site_quality(job_scores: list[float], has_scam: bool, has_discrimination: bool) -> float:
    """
    Compute aggregate site quality from a list of job scores.
    Not a simple average — extremes matter, and flags apply hard caps.
    """
    if not job_scores:
        return 0.0

    n = len(job_scores)
    # Weighted average (sorted ascending so worst jobs get seen)
    sorted_scores = sorted(job_scores)
    # Give bottom 25% double weight (penalise bad sites more)
    weights = []
    for i, s in enumerate(sorted_scores):
        rank = i / max(n - 1, 1)
        w = 0.5 if rank < 0.25 else 1.0
        weights.append(w)

    total_w = sum(weights)
    weighted_avg = sum(s * w for s, w in zip(sorted_scores, weights)) / total_w

    # % of disqualified jobs (score < 20)
    pct_disqualified = sum(1 for s in job_scores if s < 20) / n
    weighted_avg -= pct_disqualified * 30  # severe penalty for high disqualified rate

    site_score = max(0.0, min(100.0, weighted_avg))

    # Hard caps for severe issues
    if has_scam:
        site_score = min(site_score, 20.0)
    if has_discrimination:
        site_score = min(site_score, 25.0)

    return round(site_score, 1)
