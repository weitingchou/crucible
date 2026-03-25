"""Thin async HTTP client wrapping the Crucible Control Plane REST API."""

from __future__ import annotations

import httpx

from .config import settings
from .errors import raise_for_response


def _headers() -> dict[str, str]:
    if settings.crucible_api_token:
        return {"Authorization": f"Bearer {settings.crucible_api_token}"}
    return {}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=settings.crucible_api_url,
        headers=_headers(),
        timeout=30.0,
    )


async def list_sut_types() -> list[str]:
    async with _client() as c:
        resp = await c.get("/v1/sut/types")
        raise_for_response(resp)
        return resp.json()["sut_types"]


async def get_sut_inventory() -> dict:
    async with _client() as c:
        resp = await c.get("/v1/sut/inventory")
        raise_for_response(resp)
        return resp.json()


async def submit_run(plan_yaml: str, label: str) -> dict:
    async with _client() as c:
        resp = await c.post("/v1/test-runs", json={"plan_yaml": plan_yaml, "label": label})
        raise_for_response(resp)
        return resp.json()


async def get_run_status(run_id: str) -> dict:
    async with _client() as c:
        resp = await c.get(f"/v1/test-runs/{run_id}/status")
        raise_for_response(resp)
        return resp.json()


async def stop_run(run_id: str) -> dict:
    async with _client() as c:
        resp = await c.post(f"/v1/test-runs/{run_id}/stop")
        raise_for_response(resp)
        return resp.json()


async def list_fixtures() -> dict:
    async with _client() as c:
        resp = await c.get("/fixtures")
        raise_for_response(resp)
        return resp.json()


async def get_fixture_files(fixture_id: str) -> dict:
    async with _client() as c:
        resp = await c.get(f"/fixtures/{fixture_id}")
        raise_for_response(resp)
        return resp.json()


async def get_recent_stats() -> dict:
    async with _client() as c:
        resp = await c.get("/v1/telemetry/recent-stats")
        raise_for_response(resp)
        return resp.json()


async def get_run_artifacts(run_id: str) -> dict:
    async with _client() as c:
        resp = await c.get(f"/v1/test-runs/{run_id}/artifacts")
        raise_for_response(resp)
        return resp.json()


async def upload_workload(workload_id: str, content: str) -> dict:
    async with _client() as c:
        resp = await c.post(
            "/v1/workloads",
            json={"workload_id": workload_id, "content": content},
        )
        raise_for_response(resp)
        return resp.json()
