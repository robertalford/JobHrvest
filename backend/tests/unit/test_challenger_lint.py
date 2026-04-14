from pathlib import Path

from app.ml.champion_challenger.challenger_lint import lint_challenger


def _write(tmp_path: Path, name: str, body: str) -> Path:
    path = tmp_path / name
    path.write_text(body)
    return path


def test_lint_rejects_extract_override_without_enrichment(tmp_path):
    path = _write(
        tmp_path,
        "bad_extract.py",
        """
from app.crawlers.tiered_extractor_v16 import TieredExtractorV16

class TieredExtractorV999(TieredExtractorV16):
    async def extract(self, career_page, company, html):
        return []
""".strip(),
    )

    report = lint_challenger(path, {"banned_approaches": []})

    assert report.ok is False
    assert report.violations[0].rule_id == "R2"


def test_lint_rejects_domain_literal_hacks(tmp_path):
    path = _write(
        tmp_path,
        "domain_hack.py",
        """
from app.crawlers.tiered_extractor_v16 import TieredExtractorV16

class TieredExtractorV999(TieredExtractorV16):
    def helper(self, host):
        return host == "example.com"
""".strip(),
    )

    report = lint_challenger(path, {"banned_approaches": []})

    assert report.ok is False
    assert {violation.rule_id for violation in report.violations} == {"R4"}


def test_lint_accepts_simple_super_extract(tmp_path):
    path = _write(
        tmp_path,
        "good.py",
        """
from app.crawlers.tiered_extractor_v16 import TieredExtractorV16

class TieredExtractorV999(TieredExtractorV16):
    async def extract(self, career_page, company, html):
        return await super().extract(career_page, company, html)
""".strip(),
    )

    report = lint_challenger(path, {"banned_approaches": []})

    assert report.ok is True
    assert report.violations == []
