from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from control_plane.main import app

client = TestClient(app)

DELETE_FIXTURE_PATH = "control_plane.routers.fixtures.delete_fixture"


# ---------------------------------------------------------------------------
# DELETE /fixtures/{fixture_id}/{file_name}
# ---------------------------------------------------------------------------

def test_delete_fixture_returns_204():
    with patch(DELETE_FIXTURE_PATH, new_callable=AsyncMock) as m:
        m.return_value = True
        response = client.delete("/fixtures/dataset-001/data.parquet")
    assert response.status_code == 204


def test_delete_fixture_returns_no_body():
    with patch(DELETE_FIXTURE_PATH, new_callable=AsyncMock) as m:
        m.return_value = True
        response = client.delete("/fixtures/dataset-001/data.parquet")
    assert response.content == b""


def test_delete_fixture_passes_correct_args_to_broker():
    with patch(DELETE_FIXTURE_PATH, new_callable=AsyncMock) as m:
        m.return_value = True
        client.delete("/fixtures/dataset-001/data.parquet")
    m.assert_called_once_with("dataset-001", "data.parquet")


def test_delete_fixture_returns_404_when_not_found():
    with patch(DELETE_FIXTURE_PATH, new_callable=AsyncMock) as m:
        m.return_value = False
        response = client.delete("/fixtures/dataset-001/nonexistent.parquet")
    assert response.status_code == 404
