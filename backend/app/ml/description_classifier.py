"""
TF-IDF + Logistic Regression job description block classifier.

This is Layer 4.5 in the extraction pyramid — sits between content-density
(Layer 4) and the slow LLM layers (5/6), and handles the bulk of extraction
cases in < 1ms instead of 3-8 minutes of LLM inference.

Architecture:
  FeatureUnion of:
    - TF-IDF word n-grams (1-2)  — captures job-specific vocabulary patterns
    - TF-IDF char n-grams (3-5)  — handles domain-specific terminology/abbreviations
    - Engineered numeric features — text structure signals (list density, length, etc.)
  LogisticRegression with balanced class weights

Training data:
  Positive: jobs.description where quality_score >= 60 AND len >= 300
  Negative: disqualified jobs with short/wrong descriptions (navigation text, boilerplate)

Model persisted to /storage/models/description_classifier.joblib
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

MODEL_PATH = Path("/storage/models/description_classifier.joblib")

# Vocabulary of job-related terms used for feature scoring
JOB_TERMS = frozenset({
    # Role/responsibility language
    "responsible", "responsibilities", "accountable", "manage", "coordinate",
    "develop", "design", "implement", "maintain", "support", "deliver",
    "lead", "collaborate", "partner", "drive", "execute", "oversee",
    "ensure", "provide", "work", "assist", "report", "build", "create",
    # Requirements language
    "required", "requirements", "qualifications", "preferred", "experience",
    "degree", "bachelor", "master", "certification", "skills", "proficiency",
    "knowledge", "ability", "proven", "demonstrated", "strong",
    # Time signals
    "years", "minimum", "plus", "equivalent",
    # Benefits/compensation
    "salary", "compensation", "benefits", "package", "annual", "hourly",
    "equity", "bonus", "vacation", "insurance", "401k", "superannuation",
    # Common job sections
    "about", "role", "position", "opportunity", "team", "department",
    "reporting", "scope", "mission", "vision", "goals", "objectives",
    # Location/work mode
    "remote", "hybrid", "onsite", "office", "location", "travel",
    # Action verbs commonly in JDs
    "analyze", "create", "review", "monitor", "assess", "evaluate",
    "prepare", "present", "communicate", "negotiate", "plan", "schedule",
    "prioritize", "identify", "resolve", "troubleshoot", "test", "deploy",
})


def _sentence_count(text: str) -> int:
    return len(re.findall(r"[A-Z][^.!?]{10,}[.!?]", text))


def _list_density(text: str) -> float:
    """Fraction of lines that look like bullet-point list items."""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return 0.0
    list_lines = sum(
        1 for ln in lines
        if ln.startswith(("•", "-", "–", "◦", "*", "▪"))
        or re.match(r"^\d+[\.\)]\s", ln)
    )
    return list_lines / len(lines)


def _link_density(text: str) -> float:
    """Crude link density proxy: ratio of URL-like tokens to word count."""
    words = text.split()
    if not words:
        return 0.0
    links = sum(1 for w in words if "http" in w or w.startswith("www."))
    return links / len(words)


def _job_term_density(text: str) -> float:
    """Fraction of unique job terms present in the text (capped at 1.0)."""
    text_lower = text.lower()
    hits = sum(1 for t in JOB_TERMS if t in text_lower)
    return min(1.0, hits / 20)


def extract_features(text: str) -> list[float]:
    """Extract numeric features from a text block.

    Returns a fixed-length feature vector used alongside TF-IDF.
    """
    length = len(text)
    words = text.split()
    word_count = len(words)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    avg_word_len = (
        sum(len(w) for w in words) / word_count if word_count else 0.0
    )
    alpha_ratio = sum(1 for c in text if c.isalpha()) / max(1, length)
    digit_ratio = sum(1 for c in text if c.isdigit()) / max(1, length)
    upper_ratio = sum(1 for c in text if c.isupper()) / max(1, length)

    sentence_ct = _sentence_count(text)
    sentence_ratio = sentence_ct / max(1, word_count / 10)

    list_dens = _list_density(text)
    link_dens = _link_density(text)
    job_term_dens = _job_term_density(text)

    has_salary = 1.0 if re.search(
        r"\$[\d,]+|\b\d{2,3}[kK]\b|salary|compensation", text, re.I
    ) else 0.0
    has_years_exp = 1.0 if re.search(r"\d+\+?\s*years?\s+of", text, re.I) else 0.0
    has_responsibilities = 1.0 if re.search(
        r"responsibilit|you will|your role|what you.ll do", text, re.I
    ) else 0.0
    has_requirements = 1.0 if re.search(
        r"qualifi|require|you (must|have|need)|ideal candidate", text, re.I
    ) else 0.0
    has_apply = 1.0 if re.search(r"apply now|submit.{0,20}application", text, re.I) else 0.0

    # Normalize length to 0-1 (cap at 10k chars)
    norm_length = min(1.0, length / 10_000)
    # Short-line nav penalty: if most lines are very short, likely navigation
    short_line_ratio = (
        sum(1 for ln in lines if len(ln.split()) <= 3) / max(1, len(lines))
    )

    return [
        norm_length,
        word_count / 2000,           # normalized word count
        avg_word_len / 10,           # normalized avg word length
        alpha_ratio,
        digit_ratio,
        upper_ratio,
        sentence_ratio,
        list_dens,
        link_dens,
        job_term_dens,
        has_salary,
        has_years_exp,
        has_responsibilities,
        has_requirements,
        has_apply,
        short_line_ratio,
    ]


def extract_text_blocks(html: str, min_len: int = 100) -> list[str]:
    """Extract candidate text blocks from HTML DOM.

    Decomposes script/style/nav/etc. then extracts text from block-level
    elements. Returns blocks sorted by length (longest first — most likely
    to contain the full description).
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    soup = BeautifulSoup(html, "lxml")

    # Remove non-content elements
    for tag in soup(["script", "style", "noscript", "nav", "header",
                     "footer", "aside", "form", "button", "select",
                     "iframe", "svg", "picture"]):
        tag.decompose()

    seen: set[str] = set()
    blocks: list[str] = []

    for el in soup.find_all(["div", "section", "article", "main", "td", "p"]):
        text = el.get_text(separator="\n", strip=True)
        if len(text) < min_len:
            continue
        # Deduplicate (parent often contains same text as child)
        # Keep a fingerprint of the first 200 chars
        fp = text[:200].strip()
        if fp in seen:
            continue
        seen.add(fp)
        blocks.append(text)

    # Sort longest-first; large descriptions tend to score higher
    blocks.sort(key=len, reverse=True)
    # Limit to top 50 blocks to keep inference fast
    return blocks[:50]


class EngineeringFeatures:
    """sklearn-compatible transformer that converts text blocks to numeric features."""

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return np.array([extract_features(text) for text in X], dtype=np.float32)

    def fit_transform(self, X, y=None):
        return self.fit(X, y).transform(X)


def build_pipeline():
    """Construct the TF-IDF + Logistic Regression sklearn pipeline."""
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline, FeatureUnion
    from sklearn.preprocessing import StandardScaler
    from scipy.sparse import hstack, csr_matrix

    # We compose manually so we can mix sparse (TF-IDF) + dense (engineered)
    # features without a sklearn dependency on a custom estimator wrapper.
    # The pipeline here is: TfidfWord + TfidfChar + Engineering → LR.
    # See DescriptionClassifier.predict_proba() for how these are combined.
    word_tfidf = TfidfVectorizer(
        analyzer="word",
        ngram_range=(1, 2),
        max_features=50_000,
        sublinear_tf=True,
        min_df=2,
        strip_accents="unicode",
    )
    char_tfidf = TfidfVectorizer(
        analyzer="char_wb",
        ngram_range=(3, 5),
        max_features=30_000,
        sublinear_tf=True,
        min_df=3,
    )
    lr = LogisticRegression(
        C=1.0,
        class_weight="balanced",
        max_iter=1000,
        solver="lbfgs",
    )
    return word_tfidf, char_tfidf, lr


class DescriptionClassifier:
    """Wraps the trained TF-IDF + LR model with block scoring and lazy loading."""

    _instance: Optional["DescriptionClassifier"] = None
    _loaded: bool = False

    def __init__(self):
        self.word_tfidf = None
        self.char_tfidf = None
        self.lr = None
        self.eng = EngineeringFeatures()
        self.available = False

    @classmethod
    def get(cls) -> "DescriptionClassifier":
        """Singleton accessor — loads model lazily on first call."""
        if cls._instance is None:
            cls._instance = cls()
            cls._instance._load()
        return cls._instance

    def _load(self) -> None:
        """Load model from disk. Silently marks unavailable if file absent."""
        if not MODEL_PATH.exists():
            logger.debug(
                f"[classifier] model not found at {MODEL_PATH} — Layer 4.5 disabled"
            )
            self.available = False
            return
        try:
            import joblib
            state = joblib.load(MODEL_PATH)
            self.word_tfidf = state["word_tfidf"]
            self.char_tfidf = state["char_tfidf"]
            self.lr = state["lr"]
            self.available = True
            logger.info(f"[classifier] model loaded from {MODEL_PATH}")
        except Exception as e:
            logger.warning(f"[classifier] failed to load model: {e}")
            self.available = False

    def predict_proba(self, texts: list[str]) -> np.ndarray:
        """Return positive-class probabilities for each text block."""
        if not self.available or not texts:
            return np.zeros(len(texts))

        from scipy.sparse import hstack, csr_matrix
        X_word = self.word_tfidf.transform(texts)
        X_char = self.char_tfidf.transform(texts)
        X_eng = csr_matrix(self.eng.transform(texts))
        X = hstack([X_word, X_char, X_eng])
        return self.lr.predict_proba(X)[:, 1]

    def best_block(
        self, blocks: list[str], threshold: float = 0.65
    ) -> Optional[tuple[str, float]]:
        """Return (best_block_text, score) if any block scores above threshold.

        Picks the longest block among those at or above 90% of the top score,
        avoiding short fragments that score high on vocabulary but lack content.
        """
        if not self.available or not blocks:
            return None

        probs = self.predict_proba(blocks)
        top_prob = float(probs.max())

        if top_prob < threshold:
            return None

        # Among blocks with probability >= 90% of top, prefer longest
        cutoff = top_prob * 0.90
        candidates = [
            (blocks[i], float(probs[i]))
            for i in range(len(blocks))
            if probs[i] >= cutoff
        ]
        best_text, best_score = max(candidates, key=lambda x: len(x[0]))
        return best_text, best_score
