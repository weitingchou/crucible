"""Tests for crucible_mcp.errors — HTTP response to CrucibleError mapping."""

import pytest
from unittest.mock import MagicMock

from crucible_mcp.errors import CrucibleError, raise_for_response


def _make_response(status_code: int, json_body=None, text: str = ""):
    resp = MagicMock()
    resp.status_code = status_code
    resp.is_success = 200 <= status_code < 300
    resp.text = text
    if json_body is not None:
        resp.json.return_value = json_body
    else:
        resp.json.side_effect = ValueError("no json")
    return resp


# ---------------------------------------------------------------------------
# 2xx — no error
# ---------------------------------------------------------------------------

def test_success_does_not_raise():
    resp = _make_response(200, {"ok": True})
    raise_for_response(resp)  # should not raise


# ---------------------------------------------------------------------------
# 422 — Pydantic validation errors
# ---------------------------------------------------------------------------

def test_422_pydantic_errors_formatted():
    detail = [
        {"loc": ["body", "plan_yaml"], "msg": "field required"},
        {"loc": ["body", "label"], "msg": "string too short"},
    ]
    resp = _make_response(422, {"detail": detail})
    with pytest.raises(CrucibleError) as exc_info:
        raise_for_response(resp)
    assert exc_info.value.status_code == 422
    assert "InvalidParams" in exc_info.value.detail
    assert "body -> plan_yaml" in exc_info.value.detail
    assert "body -> label" in exc_info.value.detail


# ---------------------------------------------------------------------------
# 404 — generic error
# ---------------------------------------------------------------------------

def test_404_raises_with_detail():
    resp = _make_response(404, {"detail": "Run 'abc' not found."})
    with pytest.raises(CrucibleError) as exc_info:
        raise_for_response(resp)
    assert exc_info.value.status_code == 404
    assert "not found" in exc_info.value.detail


# ---------------------------------------------------------------------------
# 503 — insufficient capacity
# ---------------------------------------------------------------------------

def test_503_insufficient_capacity():
    resp = _make_response(
        503, {"detail": "failed_insufficient_capacity: only 1 node available"}
    )
    with pytest.raises(CrucibleError) as exc_info:
        raise_for_response(resp)
    assert exc_info.value.status_code == 503
    assert "ResourceExhausted" in exc_info.value.detail


def test_500_with_insufficient_capacity_in_body():
    resp = _make_response(
        500, {"detail": "Insufficient capacity for the requested cluster size"}
    )
    with pytest.raises(CrucibleError) as exc_info:
        raise_for_response(resp)
    assert exc_info.value.status_code == 503
    assert "ResourceExhausted" in exc_info.value.detail


# ---------------------------------------------------------------------------
# 5xx — generic server error
# ---------------------------------------------------------------------------

def test_500_generic_error():
    resp = _make_response(500, {"detail": "Internal Server Error"})
    with pytest.raises(CrucibleError) as exc_info:
        raise_for_response(resp)
    assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# Non-JSON response body
# ---------------------------------------------------------------------------

def test_non_json_body_uses_text():
    resp = _make_response(502, text="Bad Gateway")
    with pytest.raises(CrucibleError) as exc_info:
        raise_for_response(resp)
    assert exc_info.value.status_code == 502
    assert "Bad Gateway" in exc_info.value.detail


def test_empty_response_uses_status_code():
    resp = _make_response(504)
    with pytest.raises(CrucibleError) as exc_info:
        raise_for_response(resp)
    assert "504" in exc_info.value.detail
