"""
Salary parser — Stage 4f.

Parses raw salary strings into structured min/max/currency/period.
AU-first: bare "$" defaults to AUD for AU-market jobs.

Handles AU formats:
  "$80,000 - $120,000"       → min=80000, max=120000, currency=AUD, period=annual
  "$80K - $120K"             → min=80000, max=120000, currency=AUD, period=annual
  "$40/hr"                   → min=40, currency=AUD, period=hourly
  "$800/day"                 → min=800, currency=AUD, period=daily
  "A$90,000 - A$110,000 p.a." → min=90000, max=110000, currency=AUD, period=annual
  "$120,000 + super"         → min=120000, currency=AUD, period=annual (super noted)
  "$100K-$120K + super + bonus" → min=100000, max=120000
  "Competitive"              → None (not parseable)
  "DOE"                      → None (not parseable)
"""

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class ParsedSalary:
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    currency: Optional[str] = None
    period: Optional[str] = None  # annual, monthly, weekly, daily, hourly
    includes_super: bool = False
    raw: str = ""
    is_parseable: bool = True


# Currency patterns (order matters — longer/more specific first)
CURRENCY_PATTERNS = [
    (re.compile(r"A\$|AU\$|AUD\s*\$?", re.I), "AUD"),
    (re.compile(r"NZ\$|NZD\s*\$?", re.I), "NZD"),
    (re.compile(r"US\$|USD\s*\$?", re.I), "USD"),
    (re.compile(r"£|GBP", re.I), "GBP"),
    (re.compile(r"€|EUR", re.I), "EUR"),
    (re.compile(r"S\$|SGD", re.I), "SGD"),
    (re.compile(r"\$"), "AUD"),  # Default bare $ to AUD
]

# Period patterns
PERIOD_PATTERNS = [
    (re.compile(r"/\s*hr|per\s*hour|p\.?h\.?|hourly", re.I), "hourly"),
    (re.compile(r"/\s*day|per\s*day|daily|p\.?d\.?", re.I), "daily"),
    (re.compile(r"/\s*week|per\s*week|weekly|p\.?w\.?", re.I), "weekly"),
    (re.compile(r"/\s*month|per\s*month|monthly|p\.?m\.?", re.I), "monthly"),
    (re.compile(r"per\s*annum|p\.?a\.?|annual|yearly|per\s*year|salary", re.I), "annual"),
]

# Number patterns
K_SUFFIX_RE = re.compile(r"(\d+(?:\.\d+)?)\s*[Kk]")
NUMBER_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")

# Unparseable salary strings
UNPARSEABLE = re.compile(
    r"^(competitive|doe|negotiable|tbd|tba|market\s*rate|depending\s*on\s*experience|"
    r"attractive|excellent|generous|great|above\s*award|award\s*rate)$",
    re.I,
)

# Superannuation indicator
SUPER_RE = re.compile(r"\bsuper(?:annuation)?\b", re.I)


class AUSalaryParser:
    """Australian salary parser — defaults currency to AUD."""

    def parse(self, raw: str, default_currency: str = "AUD") -> ParsedSalary:
        if not raw or not raw.strip():
            return ParsedSalary(raw=raw, is_parseable=False)

        text = raw.strip()

        # Check for explicitly unparseable strings
        if UNPARSEABLE.match(text):
            return ParsedSalary(raw=raw, is_parseable=False)

        result = ParsedSalary(raw=raw, currency=default_currency)

        # Detect superannuation mention
        result.includes_super = bool(SUPER_RE.search(text))

        # Detect currency
        for pattern, currency in CURRENCY_PATTERNS:
            if pattern.search(text):
                result.currency = currency
                break

        # Detect period
        for pattern, period in PERIOD_PATTERNS:
            if pattern.search(text):
                result.period = period
                break
        if not result.period:
            result.period = "annual"  # Default to annual for AU job market

        # Extract numbers (handle K suffix)
        normalized = K_SUFFIX_RE.sub(lambda m: str(int(float(m.group(1)) * 1000)), text)
        numbers = [float(n.replace(",", "")) for n in NUMBER_RE.findall(normalized)]

        # Filter out obviously non-salary numbers (e.g. "2024" years, postcodes)
        salary_numbers = [n for n in numbers if 10 < n < 10_000_000]

        if not salary_numbers:
            result.is_parseable = False
            return result

        if len(salary_numbers) == 1:
            result.min_value = salary_numbers[0]
        elif len(salary_numbers) >= 2:
            result.min_value = min(salary_numbers[:2])
            result.max_value = max(salary_numbers[:2])

        # Sanity check: convert hourly to annual for comparison
        if result.period == "hourly" and result.min_value and result.min_value > 1000:
            # Probably annual despite /hr in string — recheck
            result.period = "annual"

        return result

    def to_annual(self, parsed: ParsedSalary) -> ParsedSalary:
        """Normalize any period to annual equivalent."""
        multipliers = {
            "hourly": 52 * 38,   # 38 hour week AU standard
            "daily": 52 * 5,
            "weekly": 52,
            "monthly": 12,
            "annual": 1,
        }
        m = multipliers.get(parsed.period or "annual", 1)
        if parsed.min_value:
            parsed.min_value = parsed.min_value * m
        if parsed.max_value:
            parsed.max_value = parsed.max_value * m
        parsed.period = "annual"
        return parsed

    def to_dict(self, parsed: ParsedSalary) -> dict:
        return {
            "salary_min": parsed.min_value,
            "salary_max": parsed.max_value,
            "salary_currency": parsed.currency,
            "salary_period": parsed.period,
        }


class SalaryNormalizer:
    """Top-level salary normalizer."""

    _parsers = {"AU": AUSalaryParser()}

    def normalize(self, raw: str, market_code: str = "AU") -> ParsedSalary:
        parser = self._parsers.get(market_code, self._parsers["AU"])
        return parser.parse(raw)

    def to_dict(self, parsed: ParsedSalary) -> dict:
        return {
            "salary_min": parsed.min_value,
            "salary_max": parsed.max_value,
            "salary_currency": parsed.currency,
            "salary_period": parsed.period,
        }


# Module-level singleton
salary_normalizer = SalaryNormalizer()
