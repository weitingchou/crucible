"""Tests for control_plane.routers.telemetry — recent stats endpoint."""

from unittest.mock import AsyncMock, patch
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from control_plane.main import app

client = TestClient(app)


def test_recent_stats_returns_200():
    with patch("control_plane.routers.telemetry.db.list_recent_runs", new_callable=AsyncMock) as mock:
        mock.return_value = []
        response = client.get("/v1/telemetry/recent-stats")
    assert response.status_code == 200
    assert response.json()["runs"] == []


def test_recent_stats_returns_formatted_runs():
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    with patch("control_plane.routers.telemetry.db.list_recent_runs", new_callable=AsyncMock) as mock:
        mock.return_value = [
            {
                "run_id": "r1",
                "plan_name": "smoke-test",
                "run_label": "smoke",
                "sut_type": "doris",
                "status": "COMPLETED",
                "scaling_mode": "intra_node",
                "submitted_at": ts,
                "completed_at": ts,
            },
        ]
        response = client.get("/v1/telemetry/recent-stats")
    body = response.json()
    assert len(body["runs"]) == 1
    assert body["runs"][0]["run_id"] == "r1"
    assert body["runs"][0]["status"] == "COMPLETED"
    assert body["runs"][0]["submitted_at"] == "2025-01-01T00:00:00+00:00"


def test_recent_stats_handles_null_completed_at():
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    with patch("control_plane.routers.telemetry.db.list_recent_runs", new_callable=AsyncMock) as mock:
        mock.return_value = [
            {
                "run_id": "r2",
                "plan_name": "load-test",
                "run_label": "pending",
                "sut_type": "trino",
                "status": "PENDING",
                "scaling_mode": "inter_node",
                "submitted_at": ts,
                "completed_at": None,
            },
        ]
        response = client.get("/v1/telemetry/recent-stats")
    body = response.json()
    assert body["runs"][0]["completed_at"] is None
