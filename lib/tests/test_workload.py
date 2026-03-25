"""Tests for crucible_lib.schemas.workload — parse and validate."""

import pytest

from crucible_lib.schemas.workload import Workload, WorkloadQuery, parse_workload, validate_workload

# ── Fixtures ──────────────────────────────────────────────────────────────────

VALID_SQL = """\
-- @type: sql
-- @name: TopProducts
SELECT product_id, SUM(revenue) FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- @name: DailyUsers
SELECT DATE(created_at), COUNT(DISTINCT user_id) FROM events GROUP BY 1;
"""

VALID_CQL = """\
-- @type: cql
-- @name: GetUser
SELECT * FROM users WHERE user_id = ?;
"""


# ── validate_workload ─────────────────────────────────────────────────────────

def test_valid_sql_returns_no_errors():
    assert validate_workload(VALID_SQL) == []


def test_valid_cql_returns_no_errors():
    assert validate_workload(VALID_CQL) == []


def test_missing_type_header():
    content = "-- @name: Q1\nSELECT 1;"
    errors = validate_workload(content)
    assert any("@type" in e for e in errors)


def test_empty_type_value():
    content = "-- @type:   \n-- @name: Q1\nSELECT 1;"
    errors = validate_workload(content)
    assert any("empty" in e.lower() for e in errors)


def test_no_name_blocks():
    content = "-- @type: sql\nSELECT 1;"
    errors = validate_workload(content)
    assert any("@name" in e for e in errors)


def test_duplicate_query_names():
    content = "-- @type: sql\n-- @name: Q1\nSELECT 1;\n-- @name: Q1\nSELECT 2;"
    errors = validate_workload(content)
    assert any("Duplicate" in e for e in errors)


def test_invalid_query_name_with_spaces():
    content = "-- @type: sql\n-- @name: bad name\nSELECT 1;"
    errors = validate_workload(content)
    assert any("invalid" in e.lower() for e in errors)


def test_invalid_query_name_leading_digit():
    content = "-- @type: sql\n-- @name: 1bad\nSELECT 1;"
    errors = validate_workload(content)
    assert any("invalid" in e.lower() for e in errors)


def test_empty_query_body():
    content = "-- @type: sql\n-- @name: Q1\n   \n-- @name: Q2\nSELECT 1;"
    errors = validate_workload(content)
    assert any("empty body" in e.lower() for e in errors)


def test_first_nonblank_is_not_comment():
    content = "SELECT 1;\n-- @type: sql\n-- @name: Q1\nSELECT 1;"
    errors = validate_workload(content)
    assert any("@type" in e for e in errors)


def test_multiple_errors_collected():
    content = "-- @type: sql\n-- @name: 1bad\n\n-- @name: 1bad\n"
    errors = validate_workload(content)
    # Should have: invalid name + duplicate + empty body (x2)
    assert len(errors) >= 2


# ── parse_workload ────────────────────────────────────────────────────────────

def test_parse_returns_workload_object():
    result = parse_workload(VALID_SQL)
    assert isinstance(result, Workload)
    assert result.type == "sql"


def test_parse_extracts_queries():
    result = parse_workload(VALID_SQL)
    assert len(result.queries) == 2
    assert result.queries[0].name == "TopProducts"
    assert result.queries[1].name == "DailyUsers"


def test_parse_query_bodies_are_stripped():
    result = parse_workload(VALID_SQL)
    assert result.queries[0].body.startswith("SELECT product_id")
    assert not result.queries[0].body.startswith("\n")


def test_parse_cql_type():
    result = parse_workload(VALID_CQL)
    assert result.type == "cql"
    assert len(result.queries) == 1
    assert result.queries[0].name == "GetUser"


def test_parse_raises_on_invalid():
    with pytest.raises(ValueError, match="Invalid workload file"):
        parse_workload("-- @name: Q1\nSELECT 1;")


def test_parse_single_query():
    content = "-- @type: sql\n-- @name: Foo\nSELECT 1 FROM dual;\n"
    result = parse_workload(content)
    assert len(result.queries) == 1
    assert result.queries[0].body == "SELECT 1 FROM dual;"


def test_parse_multiline_query_body():
    content = (
        "-- @type: sql\n"
        "-- @name: Complex\n"
        "SELECT a,\n"
        "       b,\n"
        "       c\n"
        "FROM t\n"
        "WHERE x = 1;\n"
    )
    result = parse_workload(content)
    assert "SELECT a," in result.queries[0].body
    assert "WHERE x = 1;" in result.queries[0].body


def test_valid_name_with_underscores():
    content = "-- @type: sql\n-- @name: my_query_v2\nSELECT 1;"
    assert validate_workload(content) == []


def test_leading_blank_lines_before_type_header():
    content = "\n\n-- @type: sql\n-- @name: Q1\nSELECT 1;"
    assert validate_workload(content) == []
    result = parse_workload(content)
    assert result.type == "sql"


def test_empty_content():
    errors = validate_workload("")
    assert len(errors) > 0
    assert any("empty" in e.lower() for e in errors)


def test_whitespace_only_content():
    errors = validate_workload("   \n\n  \n")
    assert len(errors) > 0
    assert any("empty" in e.lower() for e in errors)


def test_parse_raises_on_empty():
    with pytest.raises(ValueError, match="Invalid workload file"):
        parse_workload("")
