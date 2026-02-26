import os
import subprocess
import tempfile

import yaml

from worker.config import settings

# Taurus lifecycle phases executed in this order.
_LIFECYCLE = ("prepare", "startup", "execution", "shutdown")


def execute_taurus(plan: dict, overrides: dict) -> None:
    """Merge *overrides* into *plan*, write a temp YAML config, and run bzt.

    Taurus (bzt) handles the full driver lifecycle internally:
    prepare → startup → execution → shutdown.

    Raises:
        RuntimeError: if bzt exits with a non-zero return code.
    """
    merged = {**plan, **overrides}

    # Inject the k6 binary path and the generic SQL driver script so that
    # Taurus can find them without requiring user-supplied paths.
    for exec_block in merged.get("execution", []):
        exec_block.setdefault("env", {}).update({
            "K6_BINARY": settings.k6_binary,
            "SQL_DRIVER_PATH": settings.sql_driver_path,
        })

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yml", delete=False, prefix="crucible_bzt_"
    ) as fh:
        yaml.dump(merged, fh)
        config_path = fh.name

    try:
        proc = subprocess.Popen(
            ["bzt", config_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        for line in proc.stdout:
            print(line, end="", flush=True)

        proc.wait()

        if proc.returncode != 0:
            raise RuntimeError(
                f"Taurus exited with code {proc.returncode} "
                f"for plan '{plan.get('name', '<unnamed>')}'"
            )
    finally:
        os.unlink(config_path)
