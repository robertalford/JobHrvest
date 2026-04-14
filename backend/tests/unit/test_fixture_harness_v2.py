from app.ml.champion_challenger.fixture_harness import FixtureHarness


def test_from_storage_prefers_manifest_json(tmp_path):
    fixture_dir = tmp_path / "extractor_smoke"
    fixture_dir.mkdir()
    (fixture_dir / "greenhouse.html").write_text("<html><body>jobs</body></html>")
    (fixture_dir / "manifest.json").write_text(
        """
[
  {
    "domain": "boards.greenhouse.io",
    "url": "https://boards.greenhouse.io/acme",
    "snapshot_path": "greenhouse.html",
    "expected_titles": ["Software Engineer"],
    "expected_job_count": 1,
    "ats_platform": "greenhouse"
  }
]
""".strip()
    )

    harness = FixtureHarness.from_storage(str(fixture_dir))

    assert len(harness.fixtures) == 1
    fixture = harness.fixtures[0]
    assert fixture.domain == "boards.greenhouse.io"
    assert fixture.expected_titles == ["Software Engineer"]
    assert fixture.snapshot_path.name == "greenhouse.html"
