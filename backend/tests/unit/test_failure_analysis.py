"""Unit tests for failure-analysis helpers (no Ollama required)."""

from app.ml.champion_challenger.failure_analysis import (
    FailureCase,
    _format_cases,
    _safe_parse_json,
    _empty_analysis,
)


class TestFailureCase:
    def test_classifies_false_positive(self):
        fc = FailureCase("u", "t", predicted_label=1, predicted_confidence=0.9,
                         true_label=0, features={})
        assert fc.failure_type == "false_positive"

    def test_classifies_false_negative(self):
        fc = FailureCase("u", "t", predicted_label=0, predicted_confidence=0.1,
                         true_label=1, features={})
        assert fc.failure_type == "false_negative"


class TestFormatCases:
    def test_truncates_to_max(self):
        cases = [
            FailureCase(f"u{i}", f"t{i}", 1, 0.5, 0, {"x": i})
            for i in range(40)
        ]
        out = _format_cases(cases, max_cases=10)
        assert out.count("URL:") == 10

    def test_strips_zero_features(self):
        cases = [
            FailureCase("u", "t", 1, 0.5, 0, {"a": 0, "b": 5, "c": False, "d": "value"}),
        ]
        out = _format_cases(cases)
        assert '"b": 5' in out
        assert '"d": "value"' in out
        # Zero-valued features removed for clarity
        assert '"a": 0' not in out
        assert '"c": false' not in out


class TestSafeParseJson:
    def test_plain_json(self):
        assert _safe_parse_json('{"a": 1}') == {"a": 1}

    def test_fenced_json(self):
        assert _safe_parse_json('```json\n{"a": 2}\n```') == {"a": 2}

    def test_extracts_first_object_from_prose(self):
        out = _safe_parse_json('Sure! Here is the analysis: {"a": 3} let me know if you want more')
        assert out == {"a": 3}

    def test_returns_none_on_garbage(self):
        assert _safe_parse_json("not json at all") is None
        assert _safe_parse_json("") is None


def test_empty_analysis_shape():
    e = _empty_analysis()
    assert set(e.keys()) == {"patterns", "missing_features", "suggested_features", "edge_cases"}
    assert all(v == [] for v in e.values())
