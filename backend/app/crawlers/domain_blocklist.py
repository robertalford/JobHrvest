"""
Domain blocklist — hard-blocked at the lowest level of the crawl stack.

CRITICAL: Check every outbound URL against this list before making ANY HTTP request.
Jora, SEEK, Jobstreet, and JobsDB must NEVER be crawled under any circumstances.
"""

from urllib.parse import urlparse

# Hardcoded emergency blocklist — always enforced regardless of DB state
_HARDCODED_BLOCKED_DOMAINS = frozenset([
    # SEEK
    "seek.com.au",
    "www.seek.com.au",
    "talent.seek.com.au",
    "employer.seek.com.au",
    "seek.com",
    # Jora
    "jora.com",
    "au.jora.com",
    "www.jora.com",
    # Jobstreet
    "jobstreet.com",
    "jobstreet.com.au",
    "www.jobstreet.com",
    "jobstreet.com.my",
    "jobstreet.co.id",
    "jobstreet.com.ph",
    "jobstreet.com.sg",
    # JobsDB
    "jobsdb.com",
    "www.jobsdb.com",
    "hk.jobsdb.com",
    "th.jobsdb.com",
])

# Runtime cache populated from DB at startup
_db_blocked_domains: set[str] = set()


def is_blocked(url: str) -> bool:
    """Return True if the URL's domain is on the blocklist. Checked before every request."""
    try:
        domain = urlparse(url).netloc.lower().lstrip("www.")
        full_domain = urlparse(url).netloc.lower()
    except Exception:
        return True  # Malformed URL → block it

    # Check hardcoded list first (no DB dependency)
    if full_domain in _HARDCODED_BLOCKED_DOMAINS or domain in _HARDCODED_BLOCKED_DOMAINS:
        return True

    # Check subdomain containment — e.g. "foo.seek.com.au" is blocked
    for blocked in _HARDCODED_BLOCKED_DOMAINS:
        if full_domain.endswith(f".{blocked}") or domain.endswith(f".{blocked}"):
            return True

    # Check DB-loaded list
    if full_domain in _db_blocked_domains or domain in _db_blocked_domains:
        return True

    return False


def load_from_db(domains: list[str]) -> None:
    """Load additional blocked domains from the database into the runtime cache."""
    global _db_blocked_domains
    _db_blocked_domains = set(d.lower() for d in domains)


def assert_not_blocked(url: str) -> None:
    """Raise ValueError if URL is blocked. Use this at the crawl entrypoint."""
    if is_blocked(url):
        raise ValueError(f"URL is on the blocked domain list and must not be crawled: {url}")
