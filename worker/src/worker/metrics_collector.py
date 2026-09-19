"""Collect test results (k6 CSV stats + Prometheus observability) and persist to S3.

Called by the executor task after k6 completes. Transitions through
COLLECTING → COMPLETED, uploading ``results/{run_id}/results.json`` to S3.
"""

from __future__ import annotations

import csv
import json
import math
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests

from worker.config import settings
from worker.db import update_run_status


def collect_and_store(
    run_id: str,
    plan: dict,
    abort_reason: str | None = None,
) -> None:
    """Download k6 CSVs from S3, parse, query Prometheus, upload results.json.

    CSVs are downloaded from S3 (not read from local disk) so that
    inter-node runs can merge data from all executor segments.  Each
    executor uploads its CSV to a unique S3 key before this function
    is called.

    On any error the run still moves to COMPLETED with ``collection_error``
    recorded in the JSON.  Only k6 execution failures mark a run as FAILED.

    If *abort_reason* is set (e.g. SUT failure detection), it is included in
    the results JSON so consumers can distinguish a normal completion from an
    early abort with partial results.
    """
    update_run_status(run_id, "COLLECTING")

    collection_error: str | None = None
    k6_metrics: list[dict] = []
    obs_sources: list[dict] = []

    # ── 1. Download and parse k6 CSV artifacts from S3 ────────────────────
    try:
        k6_metrics = _download_and_parse_csvs(run_id)
    except Exception as exc:
        collection_error = f"k6 CSV parsing failed: {type(exc).__name__}: {exc}"

    # ── 2. Query Prometheus observability sources ─────────────────────────
    # `or {}` rather than a .get default: a plan stored via the JSON endpoints
    # round-trips through model_dump(), which writes an explicit
    # `observability: null` when the section is absent.
    obs_config = (
        (plan.get("test_environment", {}).get("observability") or {})
        .get("prometheus_sources", [])
    )
    if obs_config:
        try:
            obs_sources, obs_error = _query_prometheus_sources(obs_config, run_id)
        except Exception as exc:
            # Collection is best-effort, like the k6 and S3 stages either side.
            # Letting this escape would strand the run in COLLECTING with no
            # results.json at all, losing the k6 data already parsed above.
            obs_error = f"Prometheus collection failed: {type(exc).__name__}: {exc}"
        if obs_error:
            collection_error = (
                f"{collection_error}; {obs_error}" if collection_error else obs_error
            )

    # ── 3. Assemble and upload ────────────────────────────────────────────
    results = {
        "run_id": run_id,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "collection_error": collection_error,
        "abort_reason": abort_reason,
        "k6": {"metrics": k6_metrics},
        "observability": {"sources": obs_sources},
    }

    try:
        _upload_results_json(run_id, results)
    except Exception as exc:
        err = f"S3 upload failed: {type(exc).__name__}: {exc}"
        collection_error = (
            f"{collection_error}; {err}" if collection_error else err
        )

    update_run_status(run_id, "COMPLETED", set_completed_at=True)


# ── k6 CSV parsing ──────────────────────────────────────────────────────────


def _download_and_parse_csvs(run_id: str) -> list[dict]:
    """List k6 CSV artifacts in S3, download, parse, and compute summary stats.

    This replaces local-disk reading so that inter-node runs can merge
    data from all executor segments.  Each CSV was uploaded by an executor
    to ``results/{run_id}/k6_raw_{segment}_{instance}.csv``.
    """
    s3 = _s3_client()
    prefix = f"results/{run_id}/k6_raw_"
    metric_values: dict[str, list[float]] = {}

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue
            resp = s3.get_object(Bucket=settings.s3_bucket, Key=key)
            body = resp["Body"].read().decode("utf-8")
            reader = csv.DictReader(body.splitlines())
            for row in reader:
                name = row.get("metric_name", "")
                value_str = row.get("metric_value", "")
                if not name or not value_str:
                    continue
                try:
                    value = float(value_str)
                except ValueError:
                    continue
                metric_values.setdefault(name, []).append(value)

    return _summarize_metrics(metric_values)


def _summarize_metrics(metric_values: dict[str, list[float]]) -> list[dict]:
    """Compute summary stats for each metric."""
    results = []
    for name, values in sorted(metric_values.items()):
        if not values:
            continue
        values.sort()
        count = len(values)
        total = sum(values)

        results.append({
            "name": name,
            "type": "trend",
            "stats": {
                "count": count,
                "min": round(values[0], 6),
                "max": round(values[-1], 6),
                "avg": round(total / count, 6),
                "med": round(_percentile(values, 50), 6),
                "p90": round(_percentile(values, 90), 6),
                "p95": round(_percentile(values, 95), 6),
                "p99": round(_percentile(values, 99), 6),
                "rate": None,
            },
        })
    return results


def _percentile(sorted_values: list[float], p: float) -> float:
    """Compute the p-th percentile of a sorted list using linear interpolation."""
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (p / 100) * (len(sorted_values) - 1)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    return sorted_values[f] * (c - k) + sorted_values[c] * (k - f)


# ── Prometheus querying ──────────────────────────────────────────────────────


def _query_prometheus_sources(
    sources_config: list[dict],
    run_id: str,
) -> tuple[list[dict], str | None]:
    """Query each Prometheus source. Returns (results, error_string_or_None)."""
    # Look up the run's time window from the DB
    start_ts, end_ts = _get_run_time_window(run_id)
    if start_ts is None:
        end_ts = time.time()
        start_ts = end_ts - 300  # fallback: last 5 minutes

    duration_seconds = end_ts - start_ts

    results: list[dict] = []
    errors: list[str] = []

    for src_cfg in sources_config:
        src_name = src_cfg.get("name", "unnamed")
        src_url = src_cfg.get("url", "")
        resolution = src_cfg.get("resolution", 15)
        max_data_points = src_cfg.get("max_data_points", 500)
        step = max(resolution, int(duration_seconds // max_data_points)) if max_data_points > 0 else resolution

        source_metrics: list[dict] = []
        source_errors: list[str] = []

        # A per-run endpoint may present a certificate from a private CA that
        # the worker image cannot know about, so verify against a CA supplied
        # with the source.  Written to a temp file because that is what the
        # verify= parameter takes; removed again as soon as the source is done.
        ca_path: str | None = None
        ca_pem = (src_cfg.get("tls") or {}).get("ca_bundle_pem")
        # One session per source: every metric then shares a connection and a
        # single parse of the CA bundle, instead of a fresh TLS handshake each.
        session = requests.Session()
        try:
            if ca_pem:
                with tempfile.NamedTemporaryFile(
                    "w", prefix="prom-ca-", suffix=".pem", delete=False
                ) as fh:
                    ca_path = fh.name
                    fh.write(ca_pem)
                session.verify = ca_path

            for metric_cfg in src_cfg.get("metrics", []):
                metric_name = metric_cfg.get("name", "")
                query = metric_cfg.get("query", "")
                try:
                    values = _prometheus_query_range(
                        src_url, query, start_ts, end_ts, step, session=session
                    )
                    source_metrics.append({
                        "name": metric_name,
                        "query": query,
                        "values": values,
                    })
                except Exception as exc:
                    source_errors.append(
                        f"metric '{metric_name}': {type(exc).__name__}: {exc}"
                    )
        finally:
            session.close()
            if ca_path:
                Path(ca_path).unlink(missing_ok=True)

        errors.extend(
            f"Prometheus source '{src_name}' {e}" for e in source_errors
        )

        # Emit the source even when every metric failed, so a consumer can tell
        # "this source broke" from "this source was never configured".
        if source_metrics or source_errors:
            results.append({
                "name": src_name,
                "url": src_url,
                "metrics": source_metrics,
                "error": "; ".join(source_errors) if source_errors else None,
            })

    error_str = "; ".join(errors) if errors else None
    return results, error_str


def _prometheus_query_range(
    base_url: str,
    query: str,
    start: float,
    end: float,
    step: int,
    session: requests.Session | None = None,
) -> list[list]:
    """Execute a Prometheus query_range and return [[timestamp, value], ...].

    *session* carries the per-source TLS settings (``session.verify`` is either
    a CA bundle path or True).  Hostname verification is on either way.
    """
    params = {
        "query": query,
        "start": start,
        "end": end,
        "step": f"{step}s",
    }
    http = session or requests
    resp = http.get(
        f"{base_url}/api/v1/query_range",
        params=params,
        timeout=30,
    )
    if resp.status_code >= 400:
        # raise_for_status() would discard the body, but a bad PromQL expression
        # is the most common failure here and the reason is only in the body.
        detail = ""
        try:
            detail = (resp.json() or {}).get("error") or ""
        except ValueError:
            detail = resp.text[:200]
        raise RuntimeError(
            f"HTTP {resp.status_code} from {base_url}: {detail or resp.reason}"
        )
    data = resp.json()
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus returned status={data.get('status')}")

    # Flatten all result series into a single values list
    values: list[list] = []
    for series in data.get("data", {}).get("result", []):
        values.extend(series.get("values", []))
    return values


def _get_run_time_window(run_id: str) -> tuple[float | None, float | None]:
    """Read started_at from the test_runs table and return (start_epoch, now_epoch)."""
    import psycopg2
    url = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT started_at FROM test_runs WHERE run_id = %s", (run_id,)
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if row and row[0]:
        started_at = row[0]
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        return started_at.timestamp(), time.time()
    return None, None


# ── S3 upload ────────────────────────────────────────────────────────────────


def _upload_results_json(run_id: str, results: dict) -> None:
    """Upload the results dict as JSON to S3."""
    body = json.dumps(results, indent=2).encode()
    _s3_client().put_object(
        Bucket=settings.s3_bucket,
        Key=f"results/{run_id}/results.json",
        Body=body,
        ContentType="application/json",
    )


def _s3_client():
    kwargs: dict = {
        "region_name": settings.aws_region,
        "endpoint_url": settings.aws_endpoint_url or None,
    }
    if settings.aws_access_key_id and settings.aws_secret_access_key:
        kwargs["aws_access_key_id"] = settings.aws_access_key_id
        kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
    return boto3.client("s3", **kwargs)
