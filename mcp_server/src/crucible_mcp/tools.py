"""MCP tool definitions for Crucible."""

from __future__ import annotations

import yaml
from mcp.server.fastmcp import FastMCP
from pydantic import TypeAdapter, ValidationError

from crucible_lib.schemas import ClusterSpec
from crucible_lib.schemas.test_plan import TestPlan
from crucible_lib.schemas.workload import validate_workload

_cluster_spec_adapter = TypeAdapter(ClusterSpec)

from . import client
from .config import settings
from .errors import CrucibleError


def register_tools(mcp: FastMCP) -> None:

    @mcp.tool()
    async def list_supported_suts() -> list[str]:
        """Returns a list of supported System Under Test engine types (e.g. doris, trino, cassandra)."""
        return await client.list_sut_types()

    @mcp.tool()
    async def get_db_inventory() -> dict:
        """Lists SUT instances currently held by an active test run (WAITING_ROOM or EXECUTING status)."""
        return await client.get_sut_inventory()

    @mcp.tool()
    async def validate_test_plan(plan_yaml: str) -> dict:
        """Validates a YAML string against the Crucible V1 test plan schema.

        Returns {"valid": true} on success, or {"valid": false, "errors": [...]} with
        field paths and messages on failure. Does not make any network calls.

        Test plan YAML structure::

            test_metadata:
              run_label: "my-test"            # display label for the run

            test_environment:
              env_type: long-lived            # "long-lived" or "disposable"
              target_db: my_database          # database name on the SUT
              component_spec:
                type: doris                   # SUT engine type
                cluster_info:                 # required for long-lived env_type
                  host: "doris-fe:9030"
                  username: root
                  password: ""
              fixtures:                       # optional list of fixture datasets
                - fixture_id: my-fixture
                  table: my_table
              observability:                  # optional — target engine monitoring
                prometheus_sources:           # list of named Prometheus sources
                  - name: engine              # user-chosen label for this source
                    url: "http://prometheus:9090"
                    metrics:                  # at least one metric required per source
                      - name: "cluster_qps"
                        query: "sum(rate(doris_be_query_total{job='doris-be'}[1m]))"
                      - name: "avg_memory"
                        query: "avg(doris_be_mem_usage_bytes{job='doris-be'})"
                    resolution: 15            # optional, min step in seconds (default: 15)
                    max_data_points: 500      # optional, max points per metric (default: 500)
                  - name: infra               # second source for infrastructure metrics
                    url: "http://prom-infra:9090"
                    metrics:
                      - name: "cpu_usage"
                        query: "avg(rate(node_cpu_seconds_total{mode!='idle'}[1m]))"

            execution:
              executor: k6                    # "k6" or "locust"
              scaling_mode: intra_node        # "intra_node" or "inter_node"
              concurrency: 10                 # number of virtual users
              ramp_up: 30s                    # ramp-up duration
              hold_for: 5m                    # steady-state duration
              workload:
                - workload_id: my-workload    # references uploaded workload file
              failure_detection:              # optional — SUT failure detection
                enabled: true                 # optional (default: true)
                error_rate_threshold: 0.5     # optional (default: 0.5), range (0.0, 1.0]
                abort_delay: "10s"            # optional (default: "10s")

        The ``failure_detection`` section configures automatic SUT failure
        detection.  When enabled (the default), the k6 driver tracks a
        ``query_errors`` Rate metric.  If the error rate exceeds
        ``error_rate_threshold`` after the ``abort_delay`` grace period, k6
        aborts gracefully and the run is marked COMPLETED with partial results.
        Omit the section entirely to use defaults (enabled, 50% threshold, 10s
        delay).  Set ``enabled: false`` to disable.

        The ``observability.prometheus_sources`` section tells the worker which
        Prometheus instances to query after the test completes.  Each source has
        a user-chosen **name** and a **url** pointing at a Prometheus HTTP API.
        Each metric entry has:

        - **name**: a user-chosen label used in result output
        - **query**: a raw PromQL expression; the worker passes it directly to
          ``/api/v1/query_range`` scoped to the test's time window

        ``resolution`` and ``max_data_points`` control query step size:
        ``step = max(resolution, test_duration_seconds // max_data_points)``.
        """
        try:
            raw = yaml.safe_load(plan_yaml)
        except yaml.YAMLError as exc:
            return {"valid": False, "errors": [{"path": "<yaml>", "message": str(exc)}]}

        try:
            TestPlan.model_validate(raw)
        except ValidationError as exc:
            errors = [
                {
                    "path": " -> ".join(str(p) for p in e["loc"]),
                    "message": e["msg"],
                }
                for e in exc.errors()
            ]
            return {"valid": False, "errors": errors}

        return {"valid": True}

    @mcp.tool()
    async def upload_test_plan(plan_yaml: str, name: str) -> dict:
        """Validates and uploads a test plan to Crucible without starting a run.

        *plan_yaml* is the full YAML test plan string. See ``validate_test_plan``
        for the complete schema, including the optional ``observability.prometheus_sources``
        section for collecting target engine metrics during the test.

        The plan is stored under the given *name* and can be reused for
        multiple test runs via ``submit_test_run`` or ``trigger_run_by_plan``.

        Returns ``{"success": true, "key": "plans/<name>"}`` on success.
        """
        validation = await validate_test_plan(plan_yaml)
        if not validation["valid"]:
            return {"success": False, "errors": validation["errors"]}

        try:
            raw = yaml.safe_load(plan_yaml)
            result = await client.upload_plan(name, raw)
        except CrucibleError as exc:
            return {"success": False, "error": exc.detail}
        return {"success": True, **result}

    @mcp.tool()
    async def submit_test_run(
        plan_yaml: str,
        plan_name: str,
        label: str = "",
        cluster_spec: dict | None = None,
        cluster_settings: str | None = None,
    ) -> dict:
        """Validates and submits a YAML test plan to the Crucible dispatcher.

        Validates the plan locally first. If valid, automatically injects
        K6_PROMETHEUS_RW_SERVER_URL into the plan environment if configured.

        *plan_yaml* is the full YAML test plan string. See ``validate_test_plan``
        for the complete schema, including the optional ``observability.prometheus_sources``
        section for collecting target engine metrics during the test.
        *plan_name* is the stable plan identity (used as S3 key and run_id prefix).
        *label* is an optional free-form display label; defaults to *plan_name* if empty.
        *cluster_spec* is an optional cluster topology dict (e.g.
        ``{"type": "doris", "backend_node": {"replica": 3}}``).  Required for
        disposable environment plans.
        *cluster_settings* is an optional free-form string recording the benchmark
        factor under test (e.g. a concurrency setting).
        Returns the run_id on success.
        """
        # Validate locally first
        validation = await validate_test_plan(plan_yaml)
        if not validation["valid"]:
            return {"success": False, "errors": validation["errors"]}

        if cluster_spec is not None:
            try:
                _cluster_spec_adapter.validate_python(cluster_spec)
            except ValidationError as exc:
                errors = [
                    {"path": " -> ".join(str(p) for p in e["loc"]), "message": e["msg"]}
                    for e in exc.errors()
                ]
                return {"success": False, "errors": errors}

        # Auto-inject K6_PROMETHEUS_RW_SERVER_URL if configured and plan lacks it
        if settings.k6_prometheus_rw_url:
            try:
                raw = yaml.safe_load(plan_yaml)
                if not raw.get("k6_prometheus_rw_server_url"):
                    raw["k6_prometheus_rw_server_url"] = settings.k6_prometheus_rw_url
                    plan_yaml = yaml.dump(raw, default_flow_style=False)
            except Exception:
                pass  # best-effort injection

        try:
            result = await client.submit_run(plan_yaml, plan_name, label, cluster_spec, cluster_settings)
        except CrucibleError as exc:
            return {"success": False, "error": exc.detail}
        return {"success": True, **result}

    @mcp.tool()
    async def trigger_run_by_plan(
        plan_name: str,
        label: str = "",
        cluster_spec: dict | None = None,
        cluster_settings: str | None = None,
    ) -> dict:
        """Triggers a new test run using an existing plan stored in Crucible.

        This is the primary way to re-run a test plan against different cluster
        configurations. Provide *cluster_spec* to specify the cluster topology
        (e.g. ``{"type": "doris", "backend_node": {"replica": 3}}``).
        *cluster_spec* is required for disposable environment plans.

        *plan_name* identifies the previously uploaded plan.
        *label* is an optional free-form display label for the run.
        *cluster_settings* is an optional free-form string recording the benchmark
        factor under test (e.g. a concurrency setting).
        Returns the run_id on success.
        """
        if cluster_spec is not None:
            try:
                _cluster_spec_adapter.validate_python(cluster_spec)
            except ValidationError as exc:
                errors = [
                    {"path": " -> ".join(str(p) for p in e["loc"]), "message": e["msg"]}
                    for e in exc.errors()
                ]
                return {"success": False, "errors": errors}

        try:
            result = await client.trigger_run(plan_name, label, cluster_spec, cluster_settings)
        except CrucibleError as exc:
            return {"success": False, "error": exc.detail}
        return {"success": True, **result}

    @mcp.tool()
    async def list_test_runs(run_label: str = "") -> dict:
        """Lists test runs, optionally filtered by *run_label*.

        Returns all runs ordered by submission time (newest first).
        Each run includes full metadata: status, cluster_spec, cluster_settings,
        timestamps, and error details.

        Provide *run_label* to filter runs matching that label.
        """
        try:
            return await client.list_runs(run_label or None)
        except CrucibleError as exc:
            return {"error": exc.detail}

    @mcp.tool()
    async def monitor_test_progress(run_id: str) -> dict:
        """Returns the real-time status of a test run: PENDING, WAITING_ROOM, EXECUTING, COMPLETED, or FAILED."""
        try:
            return await client.get_run_status(run_id)
        except CrucibleError as exc:
            if exc.status_code == 404:
                return {"error": f"Run '{run_id}' not found."}
            return {"error": exc.detail}

    @mcp.tool()
    async def get_test_results(run_id: str) -> dict:
        """Returns structured test results for a completed run.

        The response merges k6 driver metrics and Prometheus observability
        data collected after the test finished.  Returns 409 if the run is
        still in progress (PENDING, WAITING_ROOM, EXECUTING).

        Response structure::

            {
              "run_id": "my-plan_20250315-1430_a1b2c3d4",
              "status": "COMPLETED",
              "collected_at": "2025-03-15T14:35:00+00:00",
              "collection_error": null,      # null on success, or error string
              "k6": {
                "metrics": [
                  {
                    "name": "sql_duration_TopProducts",
                    "type": "trend",
                    "stats": {
                      "count": 1500,
                      "min": 1.2,
                      "max": 245.6,
                      "avg": 23.4,
                      "med": 18.7,
                      "p90": 45.2,
                      "p95": 67.8,
                      "p99": 123.4,
                      "rate": null
                    }
                  }
                ]
              },
              "observability": {
                "sources": [
                  {
                    "name": "engine",
                    "url": "http://prometheus:9090",
                    "metrics": [
                      {
                        "name": "cluster_qps",
                        "query": "sum(rate(doris_be_query_total[1m]))",
                        "values": [
                          [1710510600, "245.3"],
                          [1710510615, "312.7"]
                        ]
                      }
                    ]
                  }
                ]
              }
            }

        **k6.metrics**: one entry per k6 metric parsed from CSV output.
        Trend metrics include percentile stats; counter metrics include rate.
        Metric names match the ``-- @name`` annotations in the workload SQL
        file (e.g. ``sql_duration_TopProducts``).

        **observability.sources**: one entry per Prometheus source defined in
        the test plan's ``observability.prometheus_sources``.  Each metric's
        ``values`` is a list of ``[unix_timestamp, string_value]`` pairs
        returned by Prometheus ``query_range``.

        **collection_error**: ``null`` when both k6 CSV parsing and all
        Prometheus queries succeeded.  On partial failure (e.g. one
        Prometheus source unreachable), the reachable data is still
        returned and this field describes what failed.
        """
        try:
            return await client.get_run_results(run_id)
        except CrucibleError as exc:
            if exc.status_code == 404:
                return {"error": f"Run '{run_id}' not found."}
            if exc.status_code == 409:
                return {"error": exc.detail}
            return {"error": exc.detail}

    @mcp.tool()
    async def emergency_stop(run_id: str) -> dict:
        """Triggers the SIGTERM → SIGKILL escalation flow to stop a running test.

        Sends SIGTERM to the Celery worker task. The worker's teardown path
        will escalate to SIGKILL if k6 subprocesses don't exit within the grace period.
        """
        try:
            return await client.stop_run(run_id)
        except CrucibleError as exc:
            return {"error": exc.detail}

    @mcp.tool()
    async def upload_workload_sql(workload_id: str, content: str) -> dict:
        """Validates and uploads a workload file to Crucible.

        The workload file must begin with a ``-- @type: <type>`` header (e.g.
        ``-- @type: sql`` or ``-- @type: cql``) followed by one or more
        ``-- @name: <QueryName>`` annotated query blocks.

        Example content::

            -- @type: sql
            -- @name: TopProducts
            SELECT product_id, SUM(revenue) FROM orders GROUP BY 1 LIMIT 10;

            -- @name: DailyUsers
            SELECT DATE(created_at), COUNT(DISTINCT user_id) FROM events GROUP BY 1;

        The ``workload_id`` is the identifier used in the test plan's
        ``execution.workload[].workload_id`` field.

        Returns ``{"success": true, "workload_id": ..., "s3_key": ...}`` on success.
        """
        errors = validate_workload(content)
        if errors:
            return {"success": False, "errors": errors}

        try:
            result = await client.upload_workload(workload_id, content)
        except CrucibleError as exc:
            return {"success": False, "error": exc.detail}
        return {"success": True, **result}
