import os
import signal
import subprocess

from crucible_lib.net import parse_host
from worker.config import settings


def spawn_k6(
    run_id: str,
    segment_flag: str,
    instance_index: int,
    plan: dict,
    extra_env: dict | None = None,
) -> subprocess.Popen:
    """Spawn a single k6 process and return the Popen handle.

    Args:
        run_id: Unique identifier for the test run (injected as a Prometheus tag).
        segment_flag: k6 execution segment string, e.g. ``"0%:50%"``.
        instance_index: Per-node index used to name the CSV artifact file.
        plan: Full test plan dict; DB connection details are read from
            ``test_environment.component_spec.cluster_info``.
        extra_env: Additional environment variables merged into the subprocess env
            (e.g. ``DOWNLOADED_SQL_PATH``).
    """
    cluster_info = plan["test_environment"]["component_spec"]["cluster_info"]
    db_host, db_port = parse_host(cluster_info["host"], cluster_info.get("port"))

    env = os.environ.copy()
    env["K6_PROMETHEUS_RW_SERVER_URL"] = settings.prometheus_rw_url
    env["K6_PROMETHEUS_RW_INJECT_TAGS"] = f"run_id={run_id},segment={segment_flag}"
    env["DB_HOST"] = db_host
    env["DB_PORT"] = str(db_port)
    env["DB_USER"] = cluster_info.get("username", "root")
    env["DB_PASS"] = cluster_info.get("password", "")
    env["DB_NAME"] = plan["test_environment"]["target_db"]
    if extra_env:
        env.update(extra_env)

    execution = plan["execution"]
    concurrency: int = int(execution.get("concurrency", 1))
    ramp_up: str = execution.get("ramp_up", "")
    hold_for: str = execution.get("hold_for", "30s")

    cmd = [settings.k6_binary, "run"]

    if ramp_up:
        cmd += ["--stage", f"{ramp_up}:{concurrency},{hold_for}:{concurrency}"]
    else:
        cmd += ["--vus", str(concurrency), "--duration", hold_for]

    cmd += [
        "--execution-segment", segment_flag,
        "--out", "experimental-prometheus-rw",
        "--out", f"csv=/tmp/k6_raw_{run_id}_{instance_index}.csv",
        settings.sql_driver_path,
    ]
    return subprocess.Popen(cmd, env=env)


def wait_and_teardown(processes: list[subprocess.Popen], timeout: int) -> None:
    """Wait for all k6 processes; escalate SIGTERM → SIGKILL on hang.

    Args:
        processes: List of running k6 Popen handles.
        timeout: Seconds to wait before sending SIGTERM.
    """
    try:
        for p in processes:
            p.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        for p in processes:
            p.send_signal(signal.SIGTERM)
        try:
            for p in processes:
                p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            for p in processes:
                p.kill()
