from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from control_plane.main import app

client = TestClient(app)

LIST_FIXTURE_IDS_PATH = "control_plane.routers.fixtures.list_fixture_ids"
LIST_FIXTURE_FILES_PATH = "control_plane.routers.fixtures.list_fixture_files"
DELETE_FIXTURE_PATH = "control_plane.routers.fixtures.delete_fixture"

_FIXTURE_IDS = {"fixture_ids": ["dataset-001", "dataset-002"]}

_FIXTURE_FILES = {
    "fixture_id": "dataset-001",
    "files": [
        {"name": "data.parquet", "key": "fixtures/dataset-001/data.parquet", "last_modified": "2026-01-01T00:00:00", "size": 1024},
        {"name": "schema.json", "key": "fixtures/dataset-001/schema.json", "last_modified": "2026-01-01T00:00:00", "size": 256},
    ],
}


# ---------------------------------------------------------------------------
# GET /fixtures  (list fixture IDs)
# ---------------------------------------------------------------------------

def test_list_fixture_ids_returns_200():
    with patch(LIST_FIXTURE_IDS_PATH, new_callable=AsyncMock) as m:
        m.return_value = _FIXTURE_IDS
        response = client.get("/fixtures")
    assert response.status_code == 200


def test_list_fixture_ids_returns_ids():
    with patch(LIST_FIXTURE_IDS_PATH, new_callable=AsyncMock) as m:
        m.return_value = _FIXTURE_IDS
        response = client.get("/fixtures")
    assert response.json() == _FIXTURE_IDS


def test_list_fixture_ids_returns_empty_when_none_exist():
    with patch(LIST_FIXTURE_IDS_PATH, new_callable=AsyncMock) as m:
        m.return_value = {"fixture_ids": []}
        response = client.get("/fixtures")
    assert response.json() == {"fixture_ids": []}


# ---------------------------------------------------------------------------
# GET /fixtures/{fixture_id}  (list files)
# ---------------------------------------------------------------------------

def test_list_fixture_files_returns_200():
    with patch(LIST_FIXTURE_FILES_PATH, new_callable=AsyncMock) as m:
        m.return_value = _FIXTURE_FILES
        response = client.get("/fixtures/dataset-001")
    assert response.status_code == 200


def test_list_fixture_files_returns_file_list():
    with patch(LIST_FIXTURE_FILES_PATH, new_callable=AsyncMock) as m:
        m.return_value = _FIXTURE_FILES
        response = client.get("/fixtures/dataset-001")
    assert response.json() == _FIXTURE_FILES


def test_list_fixture_files_passes_correct_id_to_broker():
    with patch(LIST_FIXTURE_FILES_PATH, new_callable=AsyncMock) as m:
        m.return_value = _FIXTURE_FILES
        client.get("/fixtures/dataset-001")
    m.assert_called_once_with("dataset-001")


def test_list_fixture_files_returns_empty_when_no_files():
    with patch(LIST_FIXTURE_FILES_PATH, new_callable=AsyncMock) as m:
        m.return_value = {"fixture_id": "dataset-001", "files": []}
        response = client.get("/fixtures/dataset-001")
    assert response.json() == {"fixture_id": "dataset-001", "files": []}


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
