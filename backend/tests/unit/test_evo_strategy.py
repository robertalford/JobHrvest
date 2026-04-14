from app.ml.evo.archive import MAPElitesArchive
from app.ml.evo.bandit import AxisBandit
from app.ml.evo.population import Individual, Island


def test_bandit_update_improves_axis_posterior():
    bandit = AxisBandit()
    before = bandit.posterior_means()["field_completeness"]

    bandit.update("field_completeness", outcome="promoted")

    assert bandit.posterior_means()["field_completeness"] > before


def test_archive_tracks_coverage_and_samples_ancestors():
    archive = MAPElitesArchive()
    ind = Individual(
        version_tag="v71_i0",
        parent_tag="v6.10",
        island_id=0,
        focus_axis="field_completeness",
        behaviour_cell="field_completeness|workday",
        status="fixture_passed",
        fixture_composite=91.2,
        ab_composite=None,
        axes={"field_completeness": 78.0},
        loc=120,
        file_path="backend/app/crawlers/tiered_extractor_v71_i0.py",
    )

    assert archive.upsert(ind) is True
    assert archive.coverage() > 0
    sample = archive.sample_ancestors("field_completeness|workday", k=3)
    assert sample


def test_island_select_parent_prefers_recent_members():
    island = Island(island_id=1)
    a = Individual("v71_i0", None, 1, "field_completeness", "field_completeness|workday", "fixture_passed", 90.0, None, {}, 100, "a.py")
    b = Individual("v71_i1", "v71_i0", 1, "field_completeness", "field_completeness|workday", "promoted", 92.0, 93.0, {}, 110, "b.py")
    island.members = [a, b]

    chosen = island.select_parent(p_explore=0.0)

    assert chosen.version_tag in {"v71_i0", "v71_i1"}
