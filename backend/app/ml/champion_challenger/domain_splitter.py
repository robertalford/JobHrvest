"""Domain-aware train/test splitter.

Why this exists: two pages from the same careers site share vocabulary, ATS
markup and DOM structure. A naive per-row split leaks information across the
boundary and inflates every metric. This module enforces the invariant that
**no domain appears in more than one split**.

The splitter is deterministic given a seed — re-running it produces the same
partition, which is required for reproducible experiment comparisons.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable, Sequence
from urllib.parse import urlparse


@dataclass(frozen=True)
class SplitResult:
    train_indices: tuple[int, ...]
    val_indices: tuple[int, ...]
    test_indices: tuple[int, ...]
    train_domains: frozenset[str]
    val_domains: frozenset[str]
    test_domains: frozenset[str]

    def assert_no_leakage(self) -> None:
        """Raise AssertionError if any domain crosses split boundaries."""
        overlap_tv = self.train_domains & self.val_domains
        overlap_tt = self.train_domains & self.test_domains
        overlap_vt = self.val_domains & self.test_domains
        if overlap_tv or overlap_tt or overlap_vt:
            raise AssertionError(
                f"Domain leakage detected — train∩val={overlap_tv}, "
                f"train∩test={overlap_tt}, val∩test={overlap_vt}"
            )


def extract_registrable_domain(url_or_domain: str) -> str:
    """Normalise a URL or hostname to its registrable domain.

    We do not import tldextract; the simple suffix heuristic (last 2 labels,
    or 3 for known multi-part suffixes) is good enough for our AU-first list
    and avoids a heavy dependency. Update MULTI_PART_SUFFIXES as needed.
    """
    if "://" in url_or_domain:
        host = urlparse(url_or_domain).hostname or ""
    else:
        host = url_or_domain
    host = host.lower().strip().lstrip(".")
    if not host:
        return ""

    parts = host.split(".")
    if len(parts) <= 2:
        return host

    # Known compound TLDs — extend as new markets are activated
    last_three = ".".join(parts[-3:])
    last_two = ".".join(parts[-2:])
    if last_two in MULTI_PART_SUFFIXES:
        return ".".join(parts[-3:])
    return last_two


MULTI_PART_SUFFIXES = frozenset({
    "com.au", "net.au", "org.au", "edu.au", "gov.au", "id.au",
    "co.nz", "net.nz", "org.nz",
    "co.uk", "ac.uk", "gov.uk", "org.uk",
    "com.sg", "edu.sg", "gov.sg",
    "com.my", "edu.my", "gov.my",
    "com.hk", "edu.hk", "gov.hk",
})


def _stable_bucket(domain: str, n_buckets: int, seed: int) -> int:
    """Hash a domain to a stable bucket in [0, n_buckets)."""
    h = hashlib.blake2b(f"{seed}:{domain}".encode(), digest_size=8).digest()
    return int.from_bytes(h, "big") % n_buckets


def split_by_domain(
    urls: Sequence[str],
    *,
    train_frac: float = 0.7,
    val_frac: float = 0.15,
    seed: int = 42,
) -> SplitResult:
    """Split a list of URLs into train/val/test by their registrable domain.

    All rows belonging to the same domain end up in the same split. The
    proportions are approximate — the exact share depends on how rows
    cluster per domain.

    Raises ValueError on invalid fractions; never silently leaks domains.
    """
    if not (0 < train_frac < 1) or not (0 <= val_frac < 1) or train_frac + val_frac >= 1:
        raise ValueError(
            f"Invalid fractions: train={train_frac} val={val_frac} "
            "(test = 1 - train - val must be > 0)"
        )

    n_buckets = 1000
    train_cut = int(n_buckets * train_frac)
    val_cut = int(n_buckets * (train_frac + val_frac))

    train_idx, val_idx, test_idx = [], [], []
    train_doms, val_doms, test_doms = set(), set(), set()

    for i, url in enumerate(urls):
        domain = extract_registrable_domain(url)
        if not domain:
            # Empty domain rows go to test (least harmful default).
            test_idx.append(i)
            continue
        bucket = _stable_bucket(domain, n_buckets, seed)
        if bucket < train_cut:
            train_idx.append(i)
            train_doms.add(domain)
        elif bucket < val_cut:
            val_idx.append(i)
            val_doms.add(domain)
        else:
            test_idx.append(i)
            test_doms.add(domain)

    result = SplitResult(
        train_indices=tuple(train_idx),
        val_indices=tuple(val_idx),
        test_indices=tuple(test_idx),
        train_domains=frozenset(train_doms),
        val_domains=frozenset(val_doms),
        test_domains=frozenset(test_doms),
    )
    result.assert_no_leakage()
    return result


def assert_holdout_isolation(training_domains: Iterable[str], holdout_domains: Iterable[str]) -> None:
    """Hard guard: training corpus must not contain any holdout domain.

    Call this at the start of every training run. The GOLD holdout is the
    only independent signal; any leak makes promotion decisions meaningless.
    """
    train_set = {extract_registrable_domain(d) for d in training_domains if d}
    holdout_set = {extract_registrable_domain(d) for d in holdout_domains if d}
    leak = train_set & holdout_set
    if leak:
        raise AssertionError(
            f"GOLD holdout leakage — {len(leak)} domain(s) appear in both training "
            f"and holdout sets: {sorted(leak)[:10]}"
        )
