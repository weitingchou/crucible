"""Tests for control_plane.routers.sut — SUT type listing and inventory."""

from unittest.mock import AsyncMock, patch
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from control_plane.main import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# GET /v1/sut/types
# ---------------------------------------------------------------------------

def test_list_sut_types_returns_200():
    response = client.get("/v1/sut/types")
    assert response.status_code == 200


def test_list_sut_types_contains_expected_engines():
    response = client.get("/v1/sut/types")
    body = response.json()
    assert "doris" in body["sut_types"]
    assert "trino" in body["sut_types"]
    assert "cassandra" in body["sut_types"]


# ---------------------------------------------------------------------------
# GET /v1/sut/inventory
# ---------------------------------------------------------------------------

def test_inventory_returns_200_when_empty():
    with patch("control_plane.routers.sut.db.get_active_runs", new_callable=AsyncMock) as mock:
        mock.return_value = []
        response = client.get("/v1/sut/inventory")
    assert response.status_code == 200
    assert response.json()["active"] == []


def test_inventory_returns_active_runs():
    with patch("control_plane.routers.sut.db.get_active_runs", new_callable=AsyncMock) as mock:
        mock.return_value = [
            {"run_id": "r1", "run_label": "test-1", "sut_type": "doris", "status": "EXECUTING"},
            {"run_id": "r2", "run_label": "test-2", "sut_type": "trino", "status": "WAITING_ROOM"},
        ]
        response = client.get("/v1/sut/inventory")
    body = response.json()
    assert len(body["active"]) == 2
    assert body["active"][0]["sut_type"] == "doris"
    assert body["active"][1]["status"] == "WAITING_ROOM"
