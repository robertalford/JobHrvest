"""
Job quality scorer — deterministic, rule-based scoring.

Designed for real-world career page crawl data where structured fields like
location, employment_type, salary, and date_posted are frequently absent.
Primary signal is description quality; secondary signals inferred from text.

Score bands:
  80–100 : Excellent
  60–79  : Good
  40–59  : Fair
  20–39  : Poor
  0–19   : Disqualified (scam / harmful / empty)

v2 improvements:
  - CJK-aware description length (East Asian chars count as 3x)
  - URL job-ID detection bonus
  - Description structure bonus (bullets, sections)
  - Better international location signals
  - Less harsh short-description penalty
  - Salary and date down-weighted (rarely available in career crawls)
  - Navigation-text description detection
"""

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Optional

from app.utils.profanity_wordlist import PROFANITY_WORDLIST


# ─── CJK-aware length ────────────────────────────────────────────────────────

def _cjk_weighted_len(text: str) -> int:
    """
    Return a weighted character count where CJK / full-width characters
    count as 3 (they carry ~3x the information density of ASCII chars).
    This prevents CJK job descriptions from being unfairly scored as 'very short'.
    """
    count = 0
    for ch in text:
        if unicodedata.east_asian_width(ch) in ('W', 'F'):
            count += 3
        else:
            count += 1
    return count


# ─── Location signals ─────────────────────────────────────────────────────────
# Major cities across AU, NZ, SG, MY, HK, PH, ID, TH markets + global hubs
_CITIES = [
    # AU
    r"\bsydney\b", r"\bmelbourne\b", r"\bbrisbane\b", r"\bperth\b", r"\badelaide\b",
    r"\bcanberra\b", r"\bgold coast\b", r"\bnewcastle\b", r"\bwollongong\b",
    r"\bhobart\b", r"\bdarwin\b", r"\bgeelong\b", r"\btownsville\b", r"\bcairns\b",
    r"\btoowoomba\b", r"\bballarat\b", r"\bbendigo\b", r"\bsunshine coast\b",
    r"\baustralia[-\s]wide\b", r"\bnationwide\b", r"\bnsw\b", r"\bvic\b",
    r"\bqld\b", r"\bwa\b", r"\bsa\b", r"\bact\b", r"\bnt\b", r"\btas\b",
    # NZ
    r"\bauckland\b", r"\bwellington\b", r"\bchristchurch\b", r"\bhamilton\b",
    r"\btauranga\b", r"\bdunedin\b", r"\bnew zealand\b",
    # SG
    r"\bsingapore\b",
    # MY
    r"\bkuala lumpur\b", r"\bklcc\b", r"\bpetaling jaya\b", r"\bjohor\b",
    r"\bpenang\b", r"\bsubang jaya\b", r"\bshah alam\b", r"\bmalaysia\b",
    r"\bkl\b",
    # HK
    r"\bhong kong\b", r"\bkowloon\b", r"\bcentral hong kong\b",
    # PH
    r"\bmanila\b", r"\bcebu\b", r"\bquezon city\b", r"\bphilippines\b",
    r"\bmakati\b", r"\bbgc\b", r"\bbonifacio\b", r"\btaguig\b",
    # ID
    r"\bjakarta\b", r"\bbandung\b", r"\bsurabaya\b", r"\bindonesia\b",
    r"\bbali\b", r"\byogyakarta\b", r"\bmedan\b",
    # TH
    r"\bbangkok\b", r"\bthailand\b", r"\bchiang mai\b", r"\bpattaya\b",
    # Remote / flexible
    r"\bremote\b", r"\bwork from home\b", r"\bwfh\b", r"\bhybrid\b",
    r"\bon-?site\b", r"\bflexible location\b", r"\banywhere\b",
    # Global cities (for multi-market companies)
    r"\blondon\b", r"\bnew york\b", r"\bsan francisco\b", r"\bberlin\b",
    r"\btokyo\b", r"\bseoul\b", r"\bshanghai\b", r"\bbeijing\b",
    r"\bsao paulo\b", r"\bparis\b", r"\btoronto\b", r"\bdubai\b",
    r"\buae\b", r"\bindia\b", r"\bbangalore\b", r"\bmumbai\b",
]
_CITY_RE = [re.compile(p, re.IGNORECASE) for p in _CITIES]

# ─── Employment type signals ──────────────────────────────────────────────────
_EMP_TYPE_PATTERNS = [
    r"\bfull[- ]time\b", r"\bpart[- ]time\b", r"\bcontract(or|ing)?\b",
    r"\bcasual\b", r"\btemporary\b", r"\bpermanent\b", r"\bfixed[- ]term\b",
    r"\bfreelance\b", r"\binternship\b", r"\bgraduate (role|position|program)\b",
    r"\bpermanent full[- ]time\b", r"\bpermanent part[- ]time\b",
    r"\btemporary full[- ]time\b", r"\btemporary part[- ]time\b",
]
_EMP_TYPE_RE = [re.compile(p, re.IGNORECASE) for p in _EMP_TYPE_PATTERNS]

# ─── Salary signals ───────────────────────────────────────────────────────────
_SALARY_PATTERNS = [
    r"\$[\d,]+(?:k|K)?(?:\s*[-–]\s*\$?[\d,]+(?:k|K)?)?\s*(?:per|/|p\.?a\.?|p\.?h\.?)",
    r"\$[\d,]+(?:k|K)?\s*(?:salary|package|remuneration|compensation)",
    r"(?:salary|package|remuneration)[\s:]+\$[\d,]+",
    r"\b\d{2,3}k\b.*(?:salary|package|pa|per annum)",
    r"(?:competitive|attractive)\s+(?:salary|package|remuneration)",
    r"(?:salary|compensation)\s+(?:up to|from|between|range)",
    r"\bbase salary\b",
    r"\bRM\s*[\d,]+\b",          # Malaysian Ringgit
    r"\bSGD\s*[\d,]+\b",         # Singapore Dollar
    r"\bHKD\s*[\d,]+\b",         # Hong Kong Dollar
    r"\bIDR\s*[\d,]+\b",         # Indonesian Rupiah
    r"\bTHB\s*[\d,]+\b",         # Thai Baht
    r"\bPHP\s*[\d,]+\b",         # Philippine Peso
]
_SALARY_RE = [re.compile(p, re.IGNORECASE) for p in _SALARY_PATTERNS]

# ─── Requirements signals in description ─────────────────────────────────────
_REQUIREMENTS_KEYWORDS = [
    r"\brequirements?\b", r"\bqualifications?\b", r"\bskills?\s+required\b",
    r"\bwhat (we're|we are) looking for\b", r"\byou (will|must|should) have\b",
    r"\bminimum \d+ years?\b", r"\bexperience (in|with)\b",
    r"\bproven (track record|experience|ability)\b",
    r"\bbachelor['']?s?\s+degree\b", r"\bcertification\b",
    r"\bkey (responsibilities|duties|requirements)\b",
    r"\bessen?tials?\b", r"\bdesireable\b", r"\bdesirable\b",
    r"\bwhat you['']ll (bring|need|have)\b",
    r"\babout (the|you|this) role\b", r"\byour (responsibilities|duties)\b",
    r"\bmust have\b", r"\bnice to have\b", r"\bwho (you are|we need)\b",
    r"\bthe (role|position|job)\s+requires\b",
]
_REQ_RE = [re.compile(p, re.IGNORECASE) for p in _REQUIREMENTS_KEYWORDS]

# ─── Date signals in description ──────────────────────────────────────────────
_DATE_PATTERNS = [
    r"\bposted\s+(?:on\s+)?\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}\b",
    r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\.?\s+\d{1,2},?\s+\d{4}\b",
    r"\bapply by\s+\d{1,2}\s+\w+\b",
    r"\bclosing date\b",
    r"\bapplications close\b",
    r"\bposted:\s*\d{1,2}\s+\w+\s+\d{4}\b",
    r"\bdate posted\b",
]
_DATE_RE = [re.compile(p, re.IGNORECASE) for p in _DATE_PATTERNS]

# ─── URL job-ID pattern — indicates this is an individual job listing ─────────
# URLs with a numeric or slug job ID are much more likely to be real jobs
_JOB_URL_RE = re.compile(
    r"""(
        /jobs?/\d+                          # /job/12345
      | /jobs?/[\w\-]+\d{3,}              # /jobs/senior-eng-98765
      | /careers?/\d+                      # /careers/12345
      | /careers?/[\w\-]+-\d{3,}          # /careers/role-123
      | /opening[s]?/\d+                   # /openings/12345
      | /position[s]?/\d+                  # /positions/12345
      | /vacancies?/\d+                    # /vacancy/12345
      | [?&]jid=[\w\d]+                    # ?jid=12345
      | [?&]job_id=[\w\d]+                 # ?job_id=abc
      | [?&]jobId=[\w\d]+                  # ?jobId=abc
      | [?&]id=\d{4,}                      # ?id=12345 (4+ digits)
      | /[\w\-]+-\d{4,}(?:/|$)            # /job-title-12345
    )""",
    re.VERBOSE | re.IGNORECASE,
)

# ─── Description structure signals ───────────────────────────────────────────
# Bullet lists, numbered lists, section headers indicate structured job content
_STRUCTURE_RE = re.compile(
    r"""(
        ^\s*[\u2022\u2023\u25e6\u2043\u2219•\-\*]\s+\w  # Unicode/ASCII bullet
      | ^\s*\d+\.\s+\w                                    # Numbered list
      | ^\s*[A-Z][A-Za-z\s]{3,30}:\s*$                   # Section header (e.g. "Requirements:")
      | ^\s*(about|key|what|your|the|responsibilities|requirements|skills|benefits)\b
    )""",
    re.VERBOSE | re.MULTILINE | re.IGNORECASE,
)

# ─── Navigation-text description detector ────────────────────────────────────
# Descriptions that are just card metadata (title + location + emp type + date)
# rather than actual job content
_NAV_TEXT_PATTERNS = re.compile(
    r"apply now[!.]?\s*$"                  # ends with "Apply Now!"
    r"|^[\w\s\-&]+\n[\w\s\-&,]+\n"        # Category\nCity\n pattern
    r"|\blocated\s+in\b"
    r"|\bview (all )?jobs?\b",
    re.IGNORECASE,
)

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
    r"\badvance fee\b",
    r"\bupfront (payment|investment|fee)\b",
]
SCAM_RE = [re.compile(p, re.IGNORECASE) for p in SCAM_PATTERNS]

# ─── Bad words — curated patterns only ──────────────────────────────────────
# We use a carefully curated list rather than the full profanity_wordlist to
# avoid false positives from:
#  - Short words that are also common acronyms (LAN, CB, etc.)
#  - Words with different meanings in other contexts (gin, curry, etc.)
#  - Scam phrases already covered by SCAM_RE
#
# Market-specific severe slurs (5+ chars, low false-positive risk) are included.

_BAD_WORD_PATTERNS = [
    # ── Core English profanity ─────────────────────────────────────────────────
    r"\bf[*u]ck(ing|ed|er|s|face)?\b",
    r"\bsh[*i]t(ty|s|bag|head|face|hole)?\b",
    r"\basshole\b",
    r"\barsehole\b",
    r"\bbastard\b",
    r"\bbitch(es|y|ass)?\b",
    r"\bc[*u]nt(s|ing|y)?\b",
    r"\bdick(head|face|s)?\b",
    r"\bwhor(e|es|ing)\b",
    r"\bslut(s|ty)?\b",
    r"\bcock(sucker|s)?\b",
    r"\bmotherfucker\b",
    r"\bfaggot\b",
    r"\bretard(ed)?\b",
    r"\bspastic\b",
    r"\bnigger\b",
    r"\bnigga\b",
    r"\bchink\b",
    r"\bgook\b",

    # ── AU/NZ specific slurs (5+ chars, low collision risk) ───────────────────
    r"\bdeadshit\b",
    r"\bboong\b",
    r"\bpoofter\b",
    r"\bpoofta\b",
    r"\bdrongos?\b",
    r"\bgronks?\b",
    r"\bwanker\b",
    r"\bpissed\s+off\b",
    r"\bget\s+stuffed\b",

    # ── SG/MY severe profanity (multi-char, specific) ─────────────────────────
    r"\bkanina\b",
    r"\bkannina\b",
    r"\bkan\s*ni\s*na\b",
    r"\bchee\s*bye\b",
    r"\bchibai\b",
    r"\bchi\s*bai\b",
    r"\bpukimak\b",
    r"\bpuki\s*mak\b",
    r"\bkeling\b",

    # ── HK/Cantonese (5+ char compounds, specific) ────────────────────────────
    r"\bdiu\s*nei\b",
    r"\bpuk\s*gai\b",
    r"\bpuk\s*kai\b",
    r"\bham\s*ka\s*chan\b",
    r"\bham\s*ka\s*ling\b",

    # ── PH severe profanity (5+ chars) ────────────────────────────────────────
    r"\bputang\s+ina\b",
    r"\bputangina\b",
    r"\btangina\b",
    r"\btarantado\b",

    # ── ID severe profanity (5+ chars) ────────────────────────────────────────
    r"\bbangsat\b",
    r"\bngentot\b",
    r"\bkontol\b",

    # ── Common English slurs/severe offensive (5+ chars) ─────────────────────
    r"\btwat\b",
    r"\bprick\b",
    r"\bwanker\b",
    r"\bpedophile\b",
    r"\bpaedophile\b",
]
BAD_WORD_RE = [re.compile(p, re.IGNORECASE) for p in _BAD_WORD_PATTERNS]

# ─── Discrimination patterns ──────────────────────────────────────────────────
DISCRIMINATION_PATTERNS = [
    r"\b(under|below|no older than|maximum age of?)\s+\d{2}\b",
    r"\b(must be|aged?|age requirement)\s+\d{2}[-–]\d{2}\b",
    r"\b(female|male|woman|man|ladies) (only|preferred|candidates)\b",
    r"\b(must be|only) (male|female)\b",
    r"\bgentlemen only\b",
    r"\bfamily man\b",
    r"\bsingle (male|female|man|woman)\b",
    r"\b(chinese|indian|filipino|korean|japanese|malay)\s+(national|citizen|only|preferred)\b",
    r"\b(nationals? only|citizens? only)\b",
    r"\b(chinese|caucasian|asian|european|african)\s+preferred\b",
    r"\b(christian|muslim|hindu|buddhist|jewish)\s+(preferred|only|candidates|values required)\b",
    r"\bmust (be|practice|follow)\s+\w+\s+religion\b",
    r"\bheight\s+(requirement|minimum|at least)\b",
    r"\bweight\s+(limit|requirement|maximum)\b",
    r"\bgood[-\s]looking\b",
    r"\battractive (appearance|look)\b",
]
DISCRIMINATION_RE = [re.compile(p, re.IGNORECASE) for p in DISCRIMINATION_PATTERNS]

# ─── Clickbait title patterns ─────────────────────────────────────────────────
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
    r"^no job title",  # placeholder titles from failed extraction
]
CLICKBAIT_RE = [re.compile(p, re.IGNORECASE) for p in CLICKBAIT_TITLE_PATTERNS]

# ─── Generic/useless page titles (pages that aren't job listings) ─────────────
_GENERIC_TITLES = {
    "careers", "jobs", "job opportunities", "work with us", "join us",
    "join our team", "current openings", "open positions", "vacancies",
    "opportunities", "employment", "apply now", "browse jobs",
    "our team", "talent", "people", "culture", "life at", "working here",
    "explore careers", "find your role", "find a job", "search jobs",
    "job search", "how to apply",
}


def _has_location_signal(text: str) -> bool:
    return any(r.search(text) for r in _CITY_RE)


def _has_employment_type_signal(text: str) -> bool:
    return any(r.search(text) for r in _EMP_TYPE_RE)


def _has_salary_signal(text: str) -> bool:
    return any(r.search(text) for r in _SALARY_RE)


def _has_date_signal(text: str) -> bool:
    return any(r.search(text) for r in _DATE_RE)


def _has_requirements_signal(text: str) -> bool:
    return sum(1 for r in _REQ_RE if r.search(text)) >= 2


def _is_generic_title(title: str) -> bool:
    return title.lower().strip().rstrip("!.?") in _GENERIC_TITLES


def _has_description_structure(text: str) -> bool:
    """Return True if description has bullet points, numbered lists, or section headers."""
    return bool(_STRUCTURE_RE.search(text))


def _has_job_id_url(url: str) -> bool:
    """Return True if the URL pattern suggests an individual job listing."""
    return bool(_JOB_URL_RE.search(url)) if url else False


def _is_navigation_text(text: str) -> bool:
    """Return True if description looks like card navigation text, not real content."""
    return bool(_NAV_TEXT_PATTERNS.search(text))


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
    location_raw: Optional[str] = None,
    employment_type: Optional[str] = None,
    date_posted=None,
    salary_raw: Optional[str] = None,
    requirements: Optional[str] = None,
    source_url: Optional[str] = None,
) -> QualityResult:
    """
    Score a single job. Returns QualityResult with score 0–100.

    Scoring is practical for career-page crawl data:
    - Description is the primary signal (up to 40 pts)
    - Title quality matters (up to 15 pts)
    - Structured fields give bonus pts; missing ones inferred from description text
    - CJK characters weighted 3x to prevent unfair penalisation of Asian-market jobs
    - Scam/discrimination/bad-word detection applies hard caps
    """
    result = QualityResult()
    issues = []
    score = 0.0

    desc = (description or "").strip()
    desc_weighted_len = _cjk_weighted_len(desc)
    full_text = " ".join(filter(None, [title, desc, requirements]))

    # ── 1. Title (0–15 pts) ───────────────────────────────────────────────────
    title_clean = (title or "").strip()
    if not title_clean or len(title_clean) < 3:
        issues.append("title_missing_or_short")
    elif _is_generic_title(title_clean):
        issues.append("title_generic_page")
        score += 2  # near-useless title
    elif any(r.search(title_clean) for r in CLICKBAIT_RE):
        issues.append("title_clickbait")
        score += 4
    else:
        score += 15

    # ── 1b. URL job-ID bonus (0–5 pts) ────────────────────────────────────────
    # Specific job URLs strongly signal this is an individual listing, not a page
    if _has_job_id_url(source_url or ""):
        score += 5

    # ── 2. Description quality (0–40 pts) — primary signal ───────────────────
    # Uses CJK-weighted length so Asian-language jobs aren't unfairly penalised.
    if _is_navigation_text(desc) and desc_weighted_len < 300:
        # Description is just card metadata, not actual job content
        issues.append("description_is_navigation_text")
        score += 2  # minimal credit
    elif desc_weighted_len >= 800:
        score += 40
    elif desc_weighted_len >= 500:
        score += 33
    elif desc_weighted_len >= 300:
        score += 24
    elif desc_weighted_len >= 150:
        score += 16
    elif desc_weighted_len >= 50:
        score += 8
    else:
        issues.append("description_very_short")
        score -= 3  # reduced penalty (was -5)

    # Bonus for structured description (bullet points, numbered list, sections)
    if desc_weighted_len >= 100 and _has_description_structure(desc):
        score += 5
        issues.append("description_well_structured")  # positive signal

    result.description_score = score  # track description contribution

    # ── 3. Location (0–10 pts) ────────────────────────────────────────────────
    if location_raw and len(location_raw.strip()) >= 2:
        score += 10
    elif _has_location_signal(full_text):
        score += 6  # inferred from description
        issues.append("location_inferred_from_description")
    else:
        issues.append("location_missing")

    # ── 4. Employment type (0–8 pts) ──────────────────────────────────────────
    if employment_type:
        score += 8
    elif _has_employment_type_signal(full_text):
        score += 5  # inferred
        issues.append("employment_type_inferred_from_description")
    else:
        issues.append("employment_type_missing")

    # ── 5. Salary / compensation (0–5 pts, down from 7) ──────────────────────
    # Salary is legitimately absent in most career-page crawls — lower weight.
    if salary_raw and len(salary_raw.strip()) >= 2:
        score += 5
    elif _has_salary_signal(full_text):
        score += 3
        issues.append("salary_inferred_from_description")
    else:
        issues.append("salary_missing")

    # ── 6. Date posted (0–3 pts, down from 5) ────────────────────────────────
    # Date is almost never available from career page crawls — minimal weight.
    if date_posted:
        score += 3
    elif _has_date_signal(full_text):
        score += 2
    else:
        issues.append("date_posted_missing")

    # ── 7. Requirements / responsibilities (0–15 pts) ─────────────────────────
    if requirements and len(requirements.strip()) >= 50:
        score += 15  # dedicated requirements field
    elif desc_weighted_len >= 300 and _has_requirements_signal(full_text):
        score += 10  # requirements embedded in a substantive description
        issues.append("requirements_embedded_in_description")
    elif desc_weighted_len >= 150:
        score += 5   # partial credit for a reasonable description
    else:
        issues.append("requirements_missing")

    result.completeness_score = round(score, 1)  # pre-penalty score
    raw_score = max(0.0, score)

    # ── 8. Scam detection (cap at 10) ─────────────────────────────────────────
    scam_matches = [r.pattern for r in SCAM_RE if r.search(full_text)]
    if scam_matches:
        result.scam_detected = True
        issues.append(f"scam_patterns_detected: {', '.join(scam_matches[:3])}")

    # ── 9. Bad words (cap at 15) ──────────────────────────────────────────────
    bad_word_matches = [r.pattern for r in BAD_WORD_RE if r.search(full_text)]
    if bad_word_matches:
        result.bad_words_detected = True
        issues.append("inappropriate_language_detected")

    # ── 10. Discrimination (cap at 10) ────────────────────────────────────────
    disc_matches = [r.pattern for r in DISCRIMINATION_RE if r.search(full_text)]
    if disc_matches:
        result.discrimination_detected = True
        issues.append(f"discrimination_language: {', '.join(disc_matches[:3])}")

    # ── 11. Apply caps ─────────────────────────────────────────────────────────
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
    Aggregate site quality from job scores.
    Bottom 25% of jobs get double weight to penalise bad sites.
    """
    if not job_scores:
        return 0.0

    n = len(job_scores)
    sorted_scores = sorted(job_scores)
    weights = []
    for i in range(n):
        rank = i / max(n - 1, 1)
        weights.append(0.5 if rank < 0.25 else 1.0)

    total_w = sum(weights)
    weighted_avg = sum(s * w for s, w in zip(sorted_scores, weights)) / total_w

    pct_disqualified = sum(1 for s in job_scores if s < 20) / n
    weighted_avg -= pct_disqualified * 30

    site_score = max(0.0, min(100.0, weighted_avg))
    if has_scam:
        site_score = min(site_score, 20.0)
    if has_discrimination:
        site_score = min(site_score, 25.0)

    return round(site_score, 1)
