import os
import signal
import subprocess

from worker.config import settings


def spawn_k6(
    run_id: str,
    segment_flag: str,
    instance_index: int,
    extra_env: dict | None = None,
) -> subprocess.Popen:
    """Spawn a single k6 process and return the Popen handle.

    Args:
        run_id: Unique identifier for the test run (injected as a Prometheus tag).
        segment_flag: k6 execution segment string, e.g. ``"0%:50%"``.
        instance_index: Per-node index used to name the CSV artifact file.
        extra_env: Additional environment variables merged into the subprocess env
            (e.g. ``DOWNLOADED_SQL_PATH``).
    """
    env = os.environ.copy()
    env["K6_PROMETHEUS_RW_SERVER_URL"] = settings.prometheus_rw_url
    env["K6_PROMETHEUS_RW_INJECT_TAGS"] = f"run_id={run_id},segment={segment_flag}"
    if extra_env:
        env.update(extra_env)

    cmd = [
        settings.k6_binary, "run",
        settings.sql_driver_path,
        "--execution-segment", segment_flag,
        "--out", "experimental-prometheus-rw",
        "--out", f"csv=/tmp/k6_raw_{run_id}_{instance_index}.csv",
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
