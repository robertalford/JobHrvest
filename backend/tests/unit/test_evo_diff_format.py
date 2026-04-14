import pytest

from app.ml.evo.diff_format import DiffApplyError, apply_search_replace_blocks


def test_apply_search_replace_blocks_updates_source():
    original = "class Example:\n    pass\n"
    diff_text = """<<<<<<< SEARCH
class Example:
    pass
=======
class Example:
    def value(self):
        return 1
>>>>>>> REPLACE
"""

    updated = apply_search_replace_blocks(original, diff_text)

    assert "def value" in updated
    assert "return 1" in updated


def test_apply_search_replace_blocks_rejects_missing_anchor():
    original = "print('hello')\n"
    diff_text = """<<<<<<< SEARCH
print('goodbye')
=======
print('hello world')
>>>>>>> REPLACE
"""

    with pytest.raises(DiffApplyError):
        apply_search_replace_blocks(original, diff_text)
