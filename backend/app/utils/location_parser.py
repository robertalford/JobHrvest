"""
Location parser — Stage 4e.

Parses raw location strings into structured components.
AU-first with configurable market-specific rules.

Handles:
  - "Sydney, NSW" → city=Sydney, state=New South Wales, country=Australia
  - "Melbourne VIC 3000" → city=Melbourne, state=Victoria
  - "Brisbane, Queensland" → city=Brisbane, state=Queensland
  - "CBD" → city=Sydney CBD (ambiguous → flag)
  - "Remote - Australia" → is_remote=True, remote_type=fully_remote
  - "WFH" → is_remote=True
  - "Hybrid - Sydney" → city=Sydney, is_remote=True, remote_type=hybrid
"""

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class ParsedLocation:
    city: Optional[str] = None
    state: Optional[str] = None
    state_abbr: Optional[str] = None
    country: Optional[str] = None
    country_code: Optional[str] = None
    postcode: Optional[str] = None
    is_remote: Optional[bool] = None
    remote_type: Optional[str] = None  # fully_remote, hybrid, onsite, flexible
    is_ambiguous: bool = False
    raw: str = ""


AU_STATES = {
    "NSW": "New South Wales",
    "VIC": "Victoria",
    "QLD": "Queensland",
    "WA": "Western Australia",
    "SA": "South Australia",
    "TAS": "Tasmania",
    "ACT": "Australian Capital Territory",
    "NT": "Northern Territory",
}

AU_STATE_NAMES = {v.lower(): k for k, v in AU_STATES.items()}

# Known AU cities (incomplete — used for heuristics)
AU_MAJOR_CITIES = {
    "sydney", "melbourne", "brisbane", "perth", "adelaide",
    "canberra", "darwin", "hobart", "gold coast", "newcastle",
    "wollongong", "geelong", "townsville", "cairns", "toowoomba",
    "ballarat", "bendigo", "albury", "launceston", "mackay",
}

# Remote work indicators
REMOTE_PATTERNS = [
    (re.compile(r"\b(fully?\s*remote|100%\s*remote|all\s*remote)\b", re.I), "fully_remote"),
    (re.compile(r"\b(remote\s*[\-–]\s*australia|work\s*from\s*home|wfh|anywhere\s*in\s*australia|remote\s*ok)\b", re.I), "fully_remote"),
    (re.compile(r"\b(hybrid|part[\s\-]remote|partially\s*remote)\b", re.I), "hybrid"),
    (re.compile(r"\b(flexible|flex\s*work|flexible\s*work)\b", re.I), "flexible"),
    (re.compile(r"\b(on[\s\-]?site|in[\s\-]office|office[\s\-]based)\b", re.I), "onsite"),
]

AU_POSTCODE_RE = re.compile(r"\b(\d{4})\b")
AU_STATE_RE = re.compile(
    r"\b(" + "|".join(AU_STATES.keys()) + r")\b",
    re.IGNORECASE,
)


class AULocationParser:
    """Australian-market location parser. Config-driven via the markets table."""

    def parse(self, raw: str) -> ParsedLocation:
        if not raw or not raw.strip():
            return ParsedLocation(raw=raw)

        result = ParsedLocation(raw=raw, country="Australia", country_code="AU")
        text = raw.strip()

        # 1. Detect remote type
        for pattern, remote_type in REMOTE_PATTERNS:
            if pattern.search(text):
                result.is_remote = True
                result.remote_type = remote_type
                break

        if result.remote_type == "onsite":
            result.is_remote = False

        # 2. Extract postcode
        pc_match = AU_POSTCODE_RE.search(text)
        if pc_match:
            result.postcode = pc_match.group(1)

        # 3. Extract state abbreviation
        state_match = AU_STATE_RE.search(text)
        if state_match:
            abbr = state_match.group(1).upper()
            result.state_abbr = abbr
            result.state = AU_STATES.get(abbr, abbr)

        # 4. Extract state from full name
        if not result.state:
            text_lower = text.lower()
            for full_name, abbr in AU_STATE_NAMES.items():
                if full_name in text_lower:
                    result.state = AU_STATES[abbr]
                    result.state_abbr = abbr
                    break

        # 5. Extract city
        city = self._extract_city(text, result.state_abbr)
        if city:
            result.city = city

        # 6. Detect ambiguous CBD
        if "cbd" in text.lower() and not result.city:
            result.city = "CBD"
            result.is_ambiguous = True

        # 7. If only "Australia" or similar, no specific city
        if text.lower().strip() in ("australia", "au", "remote", "nationwide", "various"):
            result.city = None
            if text.lower().strip() in ("remote", "nationwide"):
                result.is_remote = True
                result.remote_type = result.remote_type or "fully_remote"

        return result

    def _extract_city(self, text: str, state_abbr: Optional[str]) -> Optional[str]:
        """Extract city name using simple heuristics."""
        # Pattern: "City, STATE" or "City STATE" or "City - STATE"
        city_state_re = re.compile(
            r"^([A-Z][a-zA-Z\s]+?)(?:\s*[,\-–]\s*|\s+)(?:" + "|".join(AU_STATES.keys()) + r")\b",
            re.IGNORECASE,
        )
        m = city_state_re.match(text.strip())
        if m:
            city = m.group(1).strip().rstrip(",- ")
            if len(city) > 1:
                return city.title()

        # Check for known cities in the string
        text_lower = text.lower()
        for city in sorted(AU_MAJOR_CITIES, key=len, reverse=True):
            if city in text_lower:
                return city.title()

        return None


class LocationNormalizer:
    """Top-level location normalizer — selects the right market parser."""

    _parsers = {"AU": AULocationParser()}

    def normalize(self, raw: str, market_code: str = "AU") -> ParsedLocation:
        parser = self._parsers.get(market_code, self._parsers["AU"])
        return parser.parse(raw)

    def to_dict(self, parsed: ParsedLocation) -> dict:
        return {
            "location_city": parsed.city,
            "location_state": parsed.state,
            "location_country": parsed.country,
            "is_remote": parsed.is_remote,
            "remote_type": parsed.remote_type,
        }


# Module-level singleton
location_normalizer = LocationNormalizer()
