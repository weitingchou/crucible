"""Tests for control_plane.utils — sanitize_plan_name."""

import pytest

from control_plane.utils import sanitize_plan_name


def test_alphanumeric_unchanged():
    assert sanitize_plan_name("smoke123") == "smoke123"


def test_hyphens_and_underscores_preserved():
    assert sanitize_plan_name("smoke-test_v2") == "smoke-test_v2"


def test_dots_preserved():
    assert sanitize_plan_name("smoke.test") == "smoke.test"


def test_spaces_replaced_with_hyphens():
    assert sanitize_plan_name("foo bar") == "foo-bar"


def test_slashes_replaced_with_hyphens():
    assert sanitize_plan_name("foo/bar") == "foo-bar"


def test_consecutive_hyphens_collapsed():
    assert sanitize_plan_name("foo--bar") == "foo-bar"


def test_leading_hyphens_stripped():
    assert sanitize_plan_name("-foo") == "foo"


def test_trailing_hyphens_stripped():
    assert sanitize_plan_name("foo-") == "foo"


def test_leading_and_trailing_hyphens_stripped():
    assert sanitize_plan_name("-foo-") == "foo"


def test_unsafe_chars_at_boundaries_collapsed_and_stripped():
    assert sanitize_plan_name("!!foo!!") == "foo"


def test_mixed_unsafe_chars():
    assert sanitize_plan_name("doris/smoke test@v1") == "doris-smoke-test-v1"


def test_all_unsafe_chars_returns_empty_string():
    # Edge case: all unsafe chars sanitize to empty — callers must guard against this
    assert sanitize_plan_name("!!!") == ""


def test_empty_string_returns_empty_string():
    assert sanitize_plan_name("") == ""
