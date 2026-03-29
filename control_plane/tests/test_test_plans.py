import io
from unittest.mock import AsyncMock, patch

import pytest
import yaml
from fastapi.testclient import TestClient

from control_plane.main import app

client = TestClient(app)

VALID_PLAN = {
    "test_metadata": {"run_label": "smoke-test"},
    "test_environment": {
        "env_type": "long-lived",
        "target_db": "tpch",
        "component_spec": {
            "type": "doris",
            "cluster_info": {"host": "localhost:9030", "username": "root", "password": ""},
        },
        "fixtures": [],
    },
    "execution": {
        "executor": "k6",
        "scaling_mode": "intra_node",
        "concurrency": 10,
        "ramp_up": "30s",
        "hold_for": "1m",
        "workload": [],
    },
}

SAVE_PLAN_PATH = "control_plane.routers.test_plans.s3_broker.save_plan"
PLAN_EXISTS_PATH = "control_plane.routers.test_plans.s3_broker.plan_exists"
LIST_PLANS_PATH = "control_plane.routers.test_plans.s3_broker.list_plans"
GET_PLAN_PATH = "control_plane.routers.test_plans.s3_broker.get_plan"
DELETE_PLAN_PATH = "control_plane.routers.test_plans.s3_broker.delete_plan"


@pytest.fixture
def mock_save_plan():
    with patch(SAVE_PLAN_PATH, new_callable=AsyncMock) as m_save, \
         patch(PLAN_EXISTS_PATH, new_callable=AsyncMock) as m_exists:
        m_save.return_value = {"key": "plans/smoke-test"}
        m_exists.return_value = False
        yield m_save


# ---------------------------------------------------------------------------
# POST /test-plans  (JSON body)
# ---------------------------------------------------------------------------

def test_create_plan_returns_201(mock_save_plan):
    response = client.post("/test-plans?name=smoke-test", json=VALID_PLAN)
    assert response.status_code == 201


def test_create_plan_returns_s3_key(mock_save_plan):
    response = client.post("/test-plans?name=smoke-test", json=VALID_PLAN)
    assert response.json() == {"key": "plans/smoke-test"}


def test_create_plan_calls_save_plan_with_correct_name(mock_save_plan):
    client.post("/test-plans?name=smoke-test", json=VALID_PLAN)
    name_arg = mock_save_plan.call_args[0][0]
    assert name_arg == "smoke-test"


def test_create_plan_name_is_independent_of_run_label(mock_save_plan):
    """The plan name comes from the query param, not test_metadata.run_label."""
    client.post("/test-plans?name=my-custom-name", json=VALID_PLAN)
    name_arg = mock_save_plan.call_args[0][0]
    assert name_arg == "my-custom-name"


def test_create_plan_saves_valid_yaml(mock_save_plan):
    client.post("/test-plans?name=smoke-test", json=VALID_PLAN)
    content_arg = mock_save_plan.call_args[0][1]
    parsed = yaml.safe_load(content_arg)
    assert parsed["test_metadata"]["run_label"] == "smoke-test"
    assert parsed["test_environment"]["component_spec"]["type"] == "doris"


def test_create_plan_rejects_missing_required_field():
    bad = {k: v for k, v in VALID_PLAN.items() if k != "execution"}
    response = client.post("/test-plans?name=bad", json=bad)
    assert response.status_code == 422


def test_create_plan_rejects_invalid_executor():
    bad = {**VALID_PLAN, "execution": {**VALID_PLAN["execution"], "executor": "unknown"}}
    response = client.post("/test-plans?name=bad", json=bad)
    assert response.status_code == 422


def test_create_plan_rejects_zero_concurrency():
    bad = {**VALID_PLAN, "execution": {**VALID_PLAN["execution"], "concurrency": 0}}
    response = client.post("/test-plans?name=bad", json=bad)
    assert response.status_code == 422


def test_create_plan_rejects_missing_name_param():
    response = client.post("/test-plans", json=VALID_PLAN)
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# failure_detection schema validation
# ---------------------------------------------------------------------------

def test_create_plan_without_failure_detection(mock_save_plan):
    """Omitting failure_detection is valid (backward compatible)."""
    response = client.post("/test-plans?name=no-fd", json=VALID_PLAN)
    assert response.status_code == 201


def test_create_plan_with_failure_detection_defaults(mock_save_plan):
    """failure_detection with empty object uses Pydantic defaults."""
    plan = {**VALID_PLAN, "execution": {**VALID_PLAN["execution"], "failure_detection": {}}}
    response = client.post("/test-plans?name=fd-defaults", json=plan)
    assert response.status_code == 201


def test_create_plan_with_custom_failure_detection(mock_save_plan):
    """Custom threshold and delay values are accepted."""
    fd = {"enabled": True, "error_rate_threshold": 0.3, "abort_delay": "30s"}
    plan = {**VALID_PLAN, "execution": {**VALID_PLAN["execution"], "failure_detection": fd}}
    response = client.post("/test-plans?name=fd-custom", json=plan)
    assert response.status_code == 201


def test_create_plan_with_failure_detection_disabled(mock_save_plan):
    """enabled: false disables failure detection."""
    fd = {"enabled": False}
    plan = {**VALID_PLAN, "execution": {**VALID_PLAN["execution"], "failure_detection": fd}}
    response = client.post("/test-plans?name=fd-off", json=plan)
    assert response.status_code == 201


def test_create_plan_rejects_zero_error_rate_threshold():
    fd = {"error_rate_threshold": 0}
    plan = {**VALID_PLAN, "execution": {**VALID_PLAN["execution"], "failure_detection": fd}}
    response = client.post("/test-plans?name=bad", json=plan)
    assert response.status_code == 422


def test_create_plan_rejects_error_rate_threshold_above_one():
    fd = {"error_rate_threshold": 1.5}
    plan = {**VALID_PLAN, "execution": {**VALID_PLAN["execution"], "failure_detection": fd}}
    response = client.post("/test-plans?name=bad", json=plan)
    assert response.status_code == 422


def test_create_plan_accepts_error_rate_threshold_at_one(mock_save_plan):
    """error_rate_threshold=1.0 is valid (le=1.0 boundary)."""
    fd = {"error_rate_threshold": 1.0}
    plan = {**VALID_PLAN, "execution": {**VALID_PLAN["execution"], "failure_detection": fd}}
    response = client.post("/test-plans?name=fd-one", json=plan)
    assert response.status_code == 201


def test_create_disposable_plan_without_cluster_info(mock_save_plan):
    """Disposable plans don't require cluster_info (it comes at run time)."""
    disposable = {
        **VALID_PLAN,
        "test_environment": {
            "env_type": "disposable",
            "target_db": "tpch",
            "component_spec": {"type": "doris"},
            "fixtures": [],
        },
    }
    response = client.post("/test-plans?name=disposable", json=disposable)
    assert response.status_code == 201


def test_create_plan_returns_409_when_plan_exists():
    with patch(SAVE_PLAN_PATH, new_callable=AsyncMock) as m_save, \
         patch(PLAN_EXISTS_PATH, new_callable=AsyncMock) as m_exists:
        m_exists.return_value = True
        response = client.post("/test-plans?name=smoke-test", json=VALID_PLAN)
    assert response.status_code == 409
    m_save.assert_not_awaited()


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
    bad_plan = {**VALID_PLAN, "execution": {**VALID_PLAN["execution"], "concurrency": 0}}
    response = client.post("/test-plans/upload", files=_yaml_file(bad_plan))
    assert response.status_code == 422


def test_upload_plan_returns_409_when_plan_exists():
    with patch(SAVE_PLAN_PATH, new_callable=AsyncMock) as m_save, \
         patch(PLAN_EXISTS_PATH, new_callable=AsyncMock) as m_exists:
        m_exists.return_value = True
        response = client.post("/test-plans/upload", files=_yaml_file(VALID_PLAN))
    assert response.status_code == 409
    m_save.assert_not_awaited()


# ---------------------------------------------------------------------------
# PUT /test-plans/{name}  (JSON body — unconditional upsert)
# ---------------------------------------------------------------------------

def test_put_plan_returns_200():
    with patch(SAVE_PLAN_PATH, new_callable=AsyncMock) as m_save:
        m_save.return_value = {"key": "plans/smoke-test"}
        response = client.put("/test-plans/smoke-test", json=VALID_PLAN)
    assert response.status_code == 200
    assert response.json() == {"key": "plans/smoke-test"}


def test_put_plan_uses_url_name():
    """save_plan should be called with the URL name, not the body label."""
    with patch(SAVE_PLAN_PATH, new_callable=AsyncMock) as m_save:
        m_save.return_value = {"key": "plans/url-name"}
        client.put("/test-plans/url-name", json=VALID_PLAN)
    name_arg = m_save.call_args[0][0]
    assert name_arg == "url-name"


# ---------------------------------------------------------------------------
# PUT /test-plans/{name}/upload  (YAML file — unconditional upsert)
# ---------------------------------------------------------------------------

def test_put_upload_plan_returns_200():
    with patch(SAVE_PLAN_PATH, new_callable=AsyncMock) as m_save:
        m_save.return_value = {"key": "plans/smoke-test"}
        response = client.put("/test-plans/smoke-test/upload", files=_yaml_file(VALID_PLAN))
    assert response.status_code == 200
    assert response.json() == {"key": "plans/smoke-test"}


def test_put_upload_rejects_invalid_yaml():
    bad_bytes = b"name: [\nunclosed bracket"
    response = client.put(
        "/test-plans/smoke-test/upload",
        files={"file": ("bad.yaml", io.BytesIO(bad_bytes), "application/yaml")},
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# GET /test-plans  (list)
# ---------------------------------------------------------------------------

_PLAN_LIST = {
    "plans": [
        {"name": "smoke-test", "key": "plans/smoke-test", "last_modified": "2026-01-01T00:00:00", "size": 512},
        {"name": "load-test", "key": "plans/load-test", "last_modified": "2026-01-02T00:00:00", "size": 768},
    ]
}


def test_list_plans_returns_200():
    with patch(LIST_PLANS_PATH, new_callable=AsyncMock) as m:
        m.return_value = _PLAN_LIST
        response = client.get("/test-plans")
    assert response.status_code == 200


def test_list_plans_returns_plans_list():
    with patch(LIST_PLANS_PATH, new_callable=AsyncMock) as m:
        m.return_value = _PLAN_LIST
        response = client.get("/test-plans")
    assert response.json() == _PLAN_LIST


def test_list_plans_returns_empty_list_when_none_exist():
    with patch(LIST_PLANS_PATH, new_callable=AsyncMock) as m:
        m.return_value = {"plans": []}
        response = client.get("/test-plans")
    assert response.json() == {"plans": []}


# ---------------------------------------------------------------------------
# GET /test-plans/{name}  (detail)
# ---------------------------------------------------------------------------

def test_get_plan_returns_200():
    with patch(GET_PLAN_PATH, new_callable=AsyncMock) as m:
        m.return_value = VALID_PLAN
        response = client.get("/test-plans/smoke-test")
    assert response.status_code == 200


def test_get_plan_returns_plan_content():
    with patch(GET_PLAN_PATH, new_callable=AsyncMock) as m:
        m.return_value = VALID_PLAN
        response = client.get("/test-plans/smoke-test")
    assert response.json() == VALID_PLAN


def test_get_plan_passes_correct_name_to_broker():
    with patch(GET_PLAN_PATH, new_callable=AsyncMock) as m:
        m.return_value = VALID_PLAN
        client.get("/test-plans/smoke-test")
    m.assert_called_once_with("smoke-test")


def test_get_plan_returns_404_when_not_found():
    with patch(GET_PLAN_PATH, new_callable=AsyncMock) as m:
        m.return_value = None
        response = client.get("/test-plans/nonexistent")
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /test-plans/{name}
# ---------------------------------------------------------------------------

def test_delete_plan_returns_204():
    with patch(DELETE_PLAN_PATH, new_callable=AsyncMock) as m:
        m.return_value = True
        response = client.delete("/test-plans/smoke-test")
    assert response.status_code == 204


def test_delete_plan_returns_no_body():
    with patch(DELETE_PLAN_PATH, new_callable=AsyncMock) as m:
        m.return_value = True
        response = client.delete("/test-plans/smoke-test")
    assert response.content == b""


def test_delete_plan_passes_correct_name_to_broker():
    with patch(DELETE_PLAN_PATH, new_callable=AsyncMock) as m:
        m.return_value = True
        client.delete("/test-plans/smoke-test")
    m.assert_called_once_with("smoke-test")


def test_delete_plan_returns_404_when_not_found():
    with patch(DELETE_PLAN_PATH, new_callable=AsyncMock) as m:
        m.return_value = False
        response = client.delete("/test-plans/nonexistent")
    assert response.status_code == 404
