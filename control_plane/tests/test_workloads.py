"""Tests for POST /v1/workloads — workload upload endpoint."""

from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from control_plane.main import app

client = TestClient(app)

_VALID_CONTENT = """\
-- @type: sql
-- @name: TopProducts
SELECT product_id, SUM(revenue) FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- @name: DailyUsers
SELECT DATE(created_at), COUNT(DISTINCT user_id) FROM events GROUP BY 1;
"""

S3_PUT_PATH = "control_plane.routers.workloads._s3"


def _mock_s3():
    mock_client = MagicMock()
    mock_client.put_object.return_value = {}
    return mock_client


# ---------------------------------------------------------------------------
# POST /v1/workloads — success
# ---------------------------------------------------------------------------

def test_upload_workload_returns_201():
    with patch(S3_PUT_PATH, return_value=_mock_s3()):
        resp = client.post("/v1/workloads", json={"workload_id": "wl-1", "content": _VALID_CONTENT})
    assert resp.status_code == 201


def test_upload_workload_returns_s3_key():
    with patch(S3_PUT_PATH, return_value=_mock_s3()):
        resp = client.post("/v1/workloads", json={"workload_id": "wl-1", "content": _VALID_CONTENT})
    body = resp.json()
    assert body["workload_id"] == "wl-1"
    assert body["s3_key"] == "workloads/wl-1"


def test_upload_workload_calls_s3_put():
    mock = _mock_s3()
    with patch(S3_PUT_PATH, return_value=mock):
        client.post("/v1/workloads", json={"workload_id": "wl-1", "content": _VALID_CONTENT})
    mock.put_object.assert_called_once()
    call_kwargs = mock.put_object.call_args[1]
    assert call_kwargs["Key"] == "workloads/wl-1"
    assert call_kwargs["Body"] == _VALID_CONTENT.encode()


# ---------------------------------------------------------------------------
# POST /v1/workloads — validation failures
# ---------------------------------------------------------------------------

def test_upload_workload_rejects_invalid_content():
    resp = client.post("/v1/workloads", json={"workload_id": "wl-1", "content": "SELECT 1;"})
    assert resp.status_code == 422


def test_upload_workload_rejects_empty_content():
    resp = client.post("/v1/workloads", json={"workload_id": "wl-1", "content": ""})
    assert resp.status_code == 422


def test_upload_workload_rejects_missing_name_blocks():
    resp = client.post("/v1/workloads", json={"workload_id": "wl-1", "content": "-- @type: sql\nSELECT 1;"})
    assert resp.status_code == 422


def test_upload_workload_does_not_call_s3_on_invalid():
    mock = _mock_s3()
    with patch(S3_PUT_PATH, return_value=mock):
        client.post("/v1/workloads", json={"workload_id": "wl-1", "content": "SELECT 1;"})
    mock.put_object.assert_not_called()


# ---------------------------------------------------------------------------
# POST /v1/workloads — workload_id validation
# ---------------------------------------------------------------------------

def test_upload_workload_rejects_path_traversal_id():
    resp = client.post("/v1/workloads", json={"workload_id": "../../etc/passwd", "content": _VALID_CONTENT})
    assert resp.status_code == 422


def test_upload_workload_rejects_slash_in_id():
    resp = client.post("/v1/workloads", json={"workload_id": "foo/bar", "content": _VALID_CONTENT})
    assert resp.status_code == 422


def test_upload_workload_rejects_empty_id():
    resp = client.post("/v1/workloads", json={"workload_id": "", "content": _VALID_CONTENT})
    assert resp.status_code == 422


def test_upload_workload_allows_hyphens_and_underscores():
    with patch(S3_PUT_PATH, return_value=_mock_s3()):
        resp = client.post("/v1/workloads", json={"workload_id": "my-workload_v2", "content": _VALID_CONTENT})
    assert resp.status_code == 201
