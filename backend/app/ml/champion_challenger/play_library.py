"""Play library — remember what worked so Codex stops re-deriving it.

Each time a challenger is promoted, we record:
    - version (e.g. v92)
    - one-paragraph summary of what the change did
    - per-axis composite deltas (discovery, quality, volume, fields)
    - the diff-summary's content-fingerprint

Later, when a new iteration starts, we retrieve the top-K semantically-
similar past plays and include them in the Codex prompt as exemplars. This
is deliberately lightweight RAG — no vector DB, no LLM, sklearn's built-in
TF-IDF + nearest-neighbours.

Why TF-IDF and not embeddings? The corpus is tiny (<200 plays) and domain-
specific ("Workday", "__NEXT_DATA__"). TF-IDF on that corpus retrieves
well enough, adds no dependencies, and loads in <50ms. If a future iteration
wants semantic search, plug in sentence-transformers — the retrieve()
contract won't change.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _resolve_default_dir() -> Path:
    """Resolve the play library directory.

    Priority:
      1. ``PLAY_LIBRARY_DIR`` env var (explicit override).
      2. ``/storage/play_library`` if the bind mount exists (Docker container).
      3. Repo-root ``storage/play_library`` (host runs / unit tests).
    """
    explicit = os.environ.get("PLAY_LIBRARY_DIR")
    if explicit:
        return Path(explicit)
    if os.path.isdir("/storage"):
        return Path("/storage/play_library")
    return Path(os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)
        )))),
        "storage",
        "play_library",
    ))


_DEFAULT_DIR = _resolve_default_dir()


@dataclass
class Play:
    version: str
    summary: str
    axis_deltas: dict[str, float]
    composite_delta: float
    ats_clusters_fixed: list[str] = field(default_factory=list)
    diff_keywords: list[str] = field(default_factory=list)
    recorded_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    notes: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


class PlayLibrary:
    def __init__(self, root: Path = _DEFAULT_DIR):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Record / load
    # ------------------------------------------------------------------

    def record(self, play: Play) -> Path:
        """Persist a Play to disk. One file per version."""
        path = self.root / f"{play.version.replace('.', '_')}.json"
        path.write_text(json.dumps(play.to_dict(), indent=2, default=str))
        logger.info("play_library: recorded %s (Δcomposite=%+.2f)", play.version, play.composite_delta)
        return path

    def load_all(self) -> list[Play]:
        plays: list[Play] = []
        for file in sorted(self.root.glob("*.json")):
            try:
                data = json.loads(file.read_text())
                plays.append(Play(**data))
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("play_library: skipping malformed %s: %s", file.name, e)
        return plays

    # ------------------------------------------------------------------
    # Retrieve — TF-IDF cosine similarity against `summary` + keywords
    # ------------------------------------------------------------------

    def retrieve(self, query: str, *, k: int = 3, min_composite_delta: float = 0.5) -> list[Play]:
        """Return the top-k plays most similar to `query`.

        Only plays with composite_delta >= `min_composite_delta` are returned
        — we don't want to surface marginal wins as exemplars.
        """
        plays = [p for p in self.load_all() if p.composite_delta >= min_composite_delta]
        if not plays:
            return []
        if not query or not query.strip():
            # No query → surface the biggest wins
            return sorted(plays, key=lambda p: -p.composite_delta)[:k]

        corpus = [self._play_text(p) for p in plays]
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.metrics.pairwise import cosine_similarity
        except ImportError:
            return self._keyword_fallback(query, plays, k)

        vec = TfidfVectorizer(ngram_range=(1, 2), min_df=1, max_features=4000)
        try:
            matrix = vec.fit_transform(corpus + [query])
        except ValueError:
            return self._keyword_fallback(query, plays, k)
        sims = cosine_similarity(matrix[-1:], matrix[:-1]).ravel()
        order = sims.argsort()[::-1]
        top: list[Play] = []
        for idx in order:
            if sims[idx] <= 0.0:
                continue
            top.append(plays[int(idx)])
            if len(top) >= k:
                break
        return top

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _play_text(p: Play) -> str:
        """Flatten a play into a corpus entry for TF-IDF."""
        parts = [p.summary or ""]
        if p.ats_clusters_fixed:
            parts.append(" ".join(p.ats_clusters_fixed))
        if p.diff_keywords:
            parts.append(" ".join(p.diff_keywords))
        if p.notes:
            parts.append(p.notes)
        return " \n ".join(parts).lower()

    @staticmethod
    def _keyword_fallback(query: str, plays: list[Play], k: int) -> list[Play]:
        """Very cheap substring ranking when sklearn isn't available."""
        q = query.lower()
        scored = []
        for p in plays:
            text = PlayLibrary._play_text(p)
            score = sum(1 for tok in q.split() if tok and tok in text)
            if score > 0:
                scored.append((score, p))
        scored.sort(key=lambda x: -x[0])
        return [p for _, p in scored[:k]]


# Module-level singleton for easy use from anywhere
default_library = PlayLibrary()


def format_plays_for_prompt(plays: list[Play]) -> str:
    """Render plays as a compact Markdown block for inclusion in the Codex prompt."""
    if not plays:
        return ""
    lines = ["### Past plays that worked (top matches by similarity)", ""]
    for p in plays:
        delta_str = ", ".join(f"{k}={v:+.1f}" for k, v in (p.axis_deltas or {}).items() if v)
        lines.append(
            f"- **{p.version}** (Δcomposite={p.composite_delta:+.2f}) — {p.summary}"
        )
        if delta_str:
            lines.append(f"  - per-axis: {delta_str}")
        if p.ats_clusters_fixed:
            lines.append(f"  - ATS clusters fixed: {', '.join(p.ats_clusters_fixed)}")
    lines.append("")
    lines.append(
        "Reuse the general principle of these past plays when relevant — but "
        "don't copy blindly. If your current failures have a different shape, "
        "the past play won't help."
    )
    lines.append("")
    return "\n".join(lines)


__all__ = [
    "Play",
    "PlayLibrary",
    "default_library",
    "format_plays_for_prompt",
]
