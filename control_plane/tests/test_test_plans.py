import io
from unittest.mock import AsyncMock, patch

import pytest
import yaml
from fastapi.testclient import TestClient

from control_plane.main import app

client = TestClient(app)

VALID_PLAN = {
    "name": "smoke-test",
    "test_environment": {
        "component": "doris",
        "scaling_mode": "intra_node",
        "worker_count": 1,
    },
    "execution": [{"concurrency": 10, "hold_for": "1m"}],
    "workload": "workloads/tpch_queries.sql",
}

SAVE_PLAN_PATH = "control_plane.routers.test_plans.s3_broker.save_plan"


@pytest.fixture
def mock_save_plan():
    with patch(SAVE_PLAN_PATH, new_callable=AsyncMock) as m:
        m.return_value = {"key": "plans/smoke-test"}
        yield m


# ---------------------------------------------------------------------------
# POST /test-plans  (JSON body)
# ---------------------------------------------------------------------------

def test_create_plan_returns_201(mock_save_plan):
    response = client.post("/test-plans", json=VALID_PLAN)
    assert response.status_code == 201


def test_create_plan_returns_s3_key(mock_save_plan):
    response = client.post("/test-plans", json=VALID_PLAN)
    assert response.json() == {"key": "plans/smoke-test"}


def test_create_plan_calls_save_plan_with_correct_name(mock_save_plan):
    client.post("/test-plans", json=VALID_PLAN)
    name_arg = mock_save_plan.call_args[0][0]
    assert name_arg == "smoke-test"


def test_create_plan_saves_valid_yaml(mock_save_plan):
    client.post("/test-plans", json=VALID_PLAN)
    content_arg = mock_save_plan.call_args[0][1]
    parsed = yaml.safe_load(content_arg)
    assert parsed["name"] == "smoke-test"
    assert parsed["test_environment"]["component"] == "doris"


def test_create_plan_rejects_missing_required_field():
    bad = {k: v for k, v in VALID_PLAN.items() if k != "execution"}
    response = client.post("/test-plans", json=bad)
    assert response.status_code == 422


def test_create_plan_rejects_invalid_component():
    bad = {**VALID_PLAN, "test_environment": {**VALID_PLAN["test_environment"], "component": "oracle"}}
    response = client.post("/test-plans", json=bad)
    assert response.status_code == 422


def test_create_plan_rejects_zero_concurrency():
    bad = {**VALID_PLAN, "execution": [{"concurrency": 0, "hold_for": "1m"}]}
    response = client.post("/test-plans", json=bad)
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# POST /test-plans/upload  (multipart YAML file)
# ---------------------------------------------------------------------------

def _yaml_file(plan: dict, filename: str = "plan.yaml") -> dict:
    content = yaml.dump(plan).encode()
    return {"file": (filename, io.BytesIO(content), "application/yaml")}


def test_upload_plan_returns_201(mock_save_plan):
    response = client.post("/test-plans/upload", files=_yaml_file(VALID_PLAN))
    assert response.status_code == 201


def test_upload_plan_returns_s3_key(mock_save_plan):
    mock_save_plan.return_value = {"key": "plans/smoke-test"}
    response = client.post("/test-plans/upload", files=_yaml_file(VALID_PLAN))
    assert response.json() == {"key": "plans/smoke-test"}


def test_upload_plan_preserves_original_bytes(mock_save_plan):
    """save_plan should receive the raw file bytes, not re-serialized YAML."""
    original = yaml.dump(VALID_PLAN).encode()
    client.post("/test-plans/upload", files={"file": ("plan.yaml", io.BytesIO(original), "application/yaml")})
    content_arg = mock_save_plan.call_args[0][1]
    assert content_arg == original


def test_upload_plan_uses_filename_as_name(mock_save_plan):
    client.post("/test-plans/upload", files=_yaml_file(VALID_PLAN, filename="my_plan.yaml"))
    name_arg = mock_save_plan.call_args[0][0]
    assert name_arg == "my_plan"


def test_upload_plan_rejects_invalid_yaml():
    bad_bytes = b"name: [\nunclosed bracket"
    response = client.post("/test-plans/upload", files={"file": ("bad.yaml", io.BytesIO(bad_bytes), "application/yaml")})
    assert response.status_code == 422


def test_upload_plan_rejects_yaml_failing_schema_validation():
    bad_plan = {**VALID_PLAN, "test_environment": {**VALID_PLAN["test_environment"], "component": "unknown_db"}}
    response = client.post("/test-plans/upload", files=_yaml_file(bad_plan))
    assert response.status_code == 422
