"""Geocoder service — resolves raw location text → structured GeoLocation.

Resolution ladder (most → least confident):
  1. Cache hit
  2. SQL exact match (name / ascii_name / alt_names)
  3. SQL pg_trgm fuzzy match (GIN-indexed, threshold 0.35)
  4. Multi-term split (comma-separated parts tried individually)
  5. LLM reasoning (Ollama) — extracts normalised terms then re-runs 2-3
  6. Cache the result (including failures) for next time
"""

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger(__name__)

# ISO country codes for each supported market
MARKET_COUNTRY: dict[str, str] = {
    "AU": "AU", "NZ": "NZ", "SG": "SG", "MY": "MY",
    "HK": "HK", "PH": "PH", "ID": "ID", "TH": "TH",
}

# Tokens that indicate the job is remote — no physical location to geocode
_REMOTE_TOKENS = frozenset([
    "remote", "wfh", "work from home", "anywhere", "virtual",
    "distributed", "telecommute", "home-based", "home based",
    "fully remote", "remote first", "remote-first", "remote only",
    "hybrid", "flexible", "on-site", "onsite", "in-office",
    "work from anywhere", "globally remote", "open to remote",
])

_FUZZY_THRESHOLD = 0.35

# Text that looks like a location but is actually a placeholder or non-place
_PLACEHOLDER_TOKENS = frozenset([
    "location", "details", "multiple locations", "various", "various locations",
    "tbc", "tbd", "see description", "see job description", "n/a", "na",
    "not specified", "to be confirmed", "multiple", "nationwide", "global",
])

# Market detection from location text — used to override company market for geocoding
_MARKET_TEXT_HINTS: list[tuple[str, list[str]]] = [
    ("AU", ["nsw", "new south wales", "victoria", "vic", "qld", "queensland",
            "south australia", " sa ", "western australia", " wa ", "act ",
            "northern territory", " nt ", "tasmania", "tas", "canberra",
            "melbourne", "sydney", "brisbane", "perth", "adelaide", "darwin",
            "gold coast", "newcastle", "wollongong", "geelong", "hobart",
            "australia", "australian"]),
    ("NZ", ["new zealand", "auckland", "wellington", "christchurch",
            "hamilton", "tauranga", "napier", "hastings", "dunedin",
            "palmerston north", " nz "]),
    ("SG", ["singapore", " sg "]),
    ("HK", ["hong kong", " hk ", "kowloon", "new territories"]),
    ("MY", ["malaysia", "kuala lumpur", " kl ", "penang", "johor",
            "selangor", "sabah", "sarawak", "malaysian"]),
    ("PH", ["philippines", "manila", "cebu", "davao", "quezon",
            "makati", "taguig", "filipino"]),
    ("ID", ["indonesia", "jakarta", "bali", "surabaya", "bandung",
            "medan", "semarang", "indonesian"]),
    ("TH", ["thailand", "bangkok", "phuket", "chiang mai", "thai"]),
]


def _detect_market(text: str) -> Optional[str]:
    """Detect the most likely market from location text itself."""
    t = " " + text.lower() + " "
    for market, hints in _MARKET_TEXT_HINTS:
        for hint in hints:
            if hint in t:
                return market
    return None


def _is_placeholder(text: str) -> bool:
    return text.strip().lower() in _PLACEHOLDER_TOKENS


@dataclass
class GeoResult:
    geo_location_id: str
    level: int
    name: str
    full_path: str        # "Suburb, City, Region, Country"
    lat: Optional[float]
    lng: Optional[float]
    country_code: str
    market_code: str
    confidence: float
    method: str           # exact | fuzzy | llm | llm_fuzzy | cached


# ── text normalisation ────────────────────────────────────────────────────────

def _normalise(text: str) -> str:
    """Lowercase, ASCII-fold, collapse whitespace."""
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = re.sub(r"[|/\\·•]", ",", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _is_remote(text: str) -> bool:
    lower = text.lower()
    return any(tok in lower for tok in _REMOTE_TOKENS)


def _split_terms(text: str) -> list[str]:
    """Return candidate search terms, most-specific first.

    "Sydney, NSW, Australia" → ["sydney", "nsw", "australia"]
    "Bondi Beach Sydney"     → ["bondi beach sydney", "bondi beach", "bondi"]
    """
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if len(parts) > 1:
        return parts

    words = text.split()
    terms = [text]
    for i in range(len(words) - 1, 0, -1):
        terms.append(" ".join(words[:i]))
    return terms


# ── SQL helpers ───────────────────────────────────────────────────────────────

async def _build_path(db: AsyncSession, geo_id, name: str, parent_id) -> str:
    """Walk parent chain and return "Name, Parent, Grandparent, ..." string."""
    parts = [name]
    current = parent_id
    visited: set = set()
    while current and current not in visited:
        visited.add(current)
        r = await db.execute(
            text("SELECT name, parent_id FROM geo_locations WHERE id = :id"),
            {"id": str(current)},
        )
        row = r.fetchone()
        if not row:
            break
        parts.append(row[0])
        current = row[1]
    return ", ".join(parts)


def _row_to_result_sync(row, path: str, confidence: float, method: str) -> GeoResult:
    geo_id, level, name, ascii_name, lat, lng, country_code, market_code = row[:8]
    return GeoResult(
        geo_location_id=str(geo_id),
        level=level,
        name=name,
        full_path=path,
        lat=float(lat) if lat is not None else None,
        lng=float(lng) if lng is not None else None,
        country_code=country_code or "",
        market_code=market_code or "",
        confidence=confidence,
        method=method,
    )


# ── main service ──────────────────────────────────────────────────────────────

class GeocoderService:
    """Market-aware geocoder with SQL exact + fuzzy + LLM fallback."""

    async def geocode(
        self, db: AsyncSession, raw_text: str, market_code: str = "AU"
    ) -> Optional[GeoResult]:
        """Resolve *raw_text* to the most-specific GeoLocation for *market_code*.

        Returns None when the text is remote-only or cannot be resolved.
        """
        if not raw_text or not raw_text.strip():
            return None

        market_code = (market_code or "AU").upper()
        normalised = _normalise(raw_text)
        if not normalised or _is_remote(normalised) or _is_placeholder(normalised):
            return None

        # Override market_code if the location text reveals a different market
        detected_market = _detect_market(normalised)
        effective_market = detected_market or market_code

        # 1. Cache (use effective market for lookup)
        cached = await self._get_cache(db, raw_text, effective_market)
        if cached is not None:
            return cached  # False → cached failure, None → miss

        # 2 & 3. Try each candidate term with effective market
        terms = _split_terms(normalised)
        result = await self._try_terms(db, terms, effective_market, "exact")
        if result:
            await self._set_cache(db, raw_text, effective_market, result)
            return result

        result = await self._try_terms(db, terms, effective_market, "fuzzy")
        if result:
            await self._set_cache(db, raw_text, effective_market, result)
            return result

        # 3b. Multi-market fallback: try all other markets if detection failed
        if not detected_market:
            for try_market in MARKET_COUNTRY:
                if try_market == effective_market:
                    continue
                result = await self._try_terms(db, terms, try_market, "exact")
                if result:
                    result.method = "exact"
                    await self._set_cache(db, raw_text, effective_market, result)
                    return result

        # 4. LLM → re-try exact + fuzzy on LLM-extracted terms
        llm_terms = await self._llm_extract(normalised, effective_market)
        if llm_terms:
            result = await self._try_terms(db, llm_terms, effective_market, "llm")
            if result:
                result.method = "llm"
                await self._set_cache(db, raw_text, effective_market, result)
                return result

            result = await self._try_terms(db, llm_terms, effective_market, "fuzzy")
            if result:
                result.method = "llm_fuzzy"
                result.confidence *= 0.9
                await self._set_cache(db, raw_text, effective_market, result)
                return result

        # 5. Cache as unresolvable
        logger.debug("geocoder: unresolvable '%s' (%s→%s)", raw_text, market_code, effective_market)
        await self._set_cache(db, raw_text, effective_market, None)
        return None

    # ── internal resolution ───────────────────────────────────────────────────

    async def _try_terms(
        self, db: AsyncSession, terms: list[str], market_code: str, mode: str
    ) -> Optional[GeoResult]:
        country_code = MARKET_COUNTRY.get(market_code)
        for term in terms:
            if mode == "exact":
                result = await self._exact(db, term, country_code, market_code)
            else:
                result = await self._fuzzy(db, term, country_code, market_code)
            if result:
                return result
        return None

    async def _exact(
        self, db: AsyncSession, term: str, country_code: Optional[str], market_code: str
    ) -> Optional[GeoResult]:
        r = await db.execute(text("""
            SELECT g.id, g.level, g.name, g.ascii_name, g.lat, g.lng,
                   g.country_code, g.market_code, g.parent_id
            FROM geo_locations g
            WHERE g.is_active = true
              AND (g.country_code = :cc OR g.market_code = :mc)
              AND (
                  lower(g.name) = :term
                  OR lower(g.ascii_name) = :term
                  OR :term = ANY(
                      SELECT lower(x) FROM unnest(g.alt_names) x
                  )
              )
            ORDER BY g.level DESC, g.population DESC NULLS LAST
            LIMIT 1
        """), {"term": term, "cc": country_code, "mc": market_code})
        row = r.fetchone()
        if not row:
            return None
        path = await _build_path(db, row[0], row[2], row[8])
        return _row_to_result_sync(row, path, 1.0, "exact")

    async def _fuzzy(
        self, db: AsyncSession, term: str, country_code: Optional[str], market_code: str
    ) -> Optional[GeoResult]:
        r = await db.execute(text("""
            SELECT g.id, g.level, g.name, g.ascii_name, g.lat, g.lng,
                   g.country_code, g.market_code, g.parent_id,
                   similarity(lower(COALESCE(g.ascii_name, g.name)), :term) AS sim
            FROM geo_locations g
            WHERE g.is_active = true
              AND (g.country_code = :cc OR g.market_code = :mc)
              AND lower(COALESCE(g.ascii_name, g.name)) % :term
            ORDER BY sim DESC, g.level DESC, g.population DESC NULLS LAST
            LIMIT 3
        """), {"term": term, "cc": country_code, "mc": market_code})
        rows = r.fetchall()
        if not rows:
            return None
        best = rows[0]
        confidence = float(best[9])
        if confidence < _FUZZY_THRESHOLD:
            return None
        path = await _build_path(db, best[0], best[2], best[8])
        return _row_to_result_sync(best, path, confidence, "fuzzy")

    # ── LLM fallback ──────────────────────────────────────────────────────────

    async def _llm_extract(self, text: str, market_code: str) -> list[str]:
        """Ask Ollama to extract/normalise location terms from free text."""
        market_names = {
            "AU": "Australia", "NZ": "New Zealand", "SG": "Singapore",
            "MY": "Malaysia", "HK": "Hong Kong", "PH": "Philippines",
            "ID": "Indonesia", "TH": "Thailand",
        }
        market_name = market_names.get(market_code, market_code)
        prompt = (
            f'You are a geocoding assistant for {market_name}. '
            f'Given this job location text: "{text}" '
            f'extract specific place names as they appear in a map database. '
            f'Return ONLY a JSON array of strings, most specific first. '
            f'Example: ["Surry Hills", "Sydney", "New South Wales"] '
            f'If no place can be determined return []. No other output.'
        )
        try:
            import httpx, json
            async with httpx.AsyncClient(timeout=12.0) as client:
                resp = await client.post(
                    "http://ollama:11434/api/generate",
                    json={"model": "llama3.1:8b", "prompt": prompt, "stream": False},
                )
                if resp.status_code == 200:
                    raw = resp.json().get("response", "").strip()
                    match = re.search(r"\[.*?\]", raw, re.DOTALL)
                    if match:
                        terms = json.loads(match.group())
                        return [_normalise(t) for t in terms if t and t.strip()]
        except Exception as exc:
            logger.debug("LLM geocode error: %s", exc)
        return []

    # ── cache ──────────────────────────────────────────────────────────────────

    async def _get_cache(
        self, db: AsyncSession, raw_text: str, market_code: str
    ):
        """Return GeoResult on hit, False on cached-failure, None on miss."""
        r = await db.execute(text("""
            SELECT gc.id, gc.geo_location_id, gc.confidence, gc.resolution_method,
                   g.id, g.level, g.name, g.ascii_name, g.lat, g.lng,
                   g.country_code, g.market_code, g.parent_id
            FROM geocode_cache gc
            LEFT JOIN geo_locations g ON g.id = gc.geo_location_id
            WHERE lower(gc.raw_text) = lower(:txt)
              AND COALESCE(gc.market_code, '') = COALESCE(:mc, '')
            LIMIT 1
        """), {"txt": raw_text, "mc": market_code})
        row = r.fetchone()
        if row is None:
            return None   # cache miss

        cache_id = row[0]
        await db.execute(text("""
            UPDATE geocode_cache
            SET last_used_at = NOW(), use_count = use_count + 1
            WHERE id = :id
        """), {"id": str(cache_id)})
        try:
            await db.commit()
        except Exception:
            await db.rollback()

        geo_id = row[1]
        if geo_id is None:
            return False   # cached failure

        geo_row = row[4:]
        path = await _build_path(db, geo_row[0], geo_row[2], geo_row[8])
        return _row_to_result_sync(geo_row, path, float(row[2] or 0.5), row[3] or "cached")

    async def _set_cache(
        self,
        db: AsyncSession,
        raw_text: str,
        market_code: str,
        result: Optional[GeoResult],
    ) -> None:
        geo_id = result.geo_location_id if result else None
        confidence = result.confidence if result else 0.0
        method = result.method if result else "unresolved"
        try:
            await db.execute(text("""
                INSERT INTO geocode_cache
                    (raw_text, market_code, geo_location_id, confidence, resolution_method)
                VALUES (:txt, :mc, :geo_id, :conf, :method)
                ON CONFLICT (lower(raw_text), COALESCE(market_code, ''))
                DO UPDATE SET
                    geo_location_id   = EXCLUDED.geo_location_id,
                    confidence        = EXCLUDED.confidence,
                    resolution_method = EXCLUDED.resolution_method,
                    last_used_at      = NOW(),
                    use_count         = geocode_cache.use_count + 1
            """), {"txt": raw_text, "mc": market_code,
                   "geo_id": geo_id, "conf": confidence, "method": method})
            await db.commit()
        except Exception as exc:
            logger.debug("cache write error: %s", exc)
            await db.rollback()


# Module-level singleton
geocoder_service = GeocoderService()
