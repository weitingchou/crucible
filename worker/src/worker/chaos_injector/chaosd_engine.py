"""Chaosd REST API engine — injects/recovers faults on EC2 or bare-metal targets."""

from __future__ import annotations

import logging

import requests

from .base import ChaosEngine

logger = logging.getLogger(__name__)

CHAOSD_PORT = 31767


class ChaosdEngine(ChaosEngine):
    """Injects faults via the chaosd HTTP API on remote hosts."""

    def inject(self, experiment: dict, run_id: str) -> str:
        target = experiment["target"]
        address = target["address"]
        fault_type = experiment["fault_type"]
        params = experiment.get("parameters", {})

        url = f"http://{address}:{CHAOSD_PORT}/api/attack/{fault_type}"
        logger.info("Injecting chaosd fault %s on %s for run %s", fault_type, address, run_id)
        resp = requests.post(url, json=params, timeout=30)
        resp.raise_for_status()

        body = resp.json()
        uid = body.get("uid", body.get("id", ""))
        return uid

    def collect_status(self, experiment: dict, run_id: str, handle: str) -> dict | None:
        """Query chaosd for experiment status by UID.

        Calls GET /api/experiments/ and filters for the matching UID.
        Returns the experiment record (status, created_at, updated_at,
        kind, action) or None if the query fails or the UID is not found.
        """
        target = experiment["target"]
        address = target["address"]
        url = f"http://{address}:{CHAOSD_PORT}/api/experiments/"
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            for record in resp.json():
                if record.get("uid") == handle:
                    return {
                        "uid": record.get("uid"),
                        "status": record.get("status"),
                        "kind": record.get("kind"),
                        "action": record.get("action"),
                        "created_at": record.get("created_at"),
                        "updated_at": record.get("updated_at"),
                        "launch_mode": record.get("launch_mode"),
                    }
            logger.warning("Chaosd experiment %s not found in /api/experiments/", handle)
            return None
        except Exception:
            logger.exception("Failed to query chaosd status for %s on %s", handle, address)
            return None

    @staticmethod
    def normalize_status(raw: dict) -> dict | None:
        """Convert chaosd experiment record into the unified schema."""
        if not raw:
            return None

        chaosd_status = raw.get("status", "")
        if chaosd_status in ("success", "destroyed"):
            status = "recovered"
        elif chaosd_status == "error":
            status = "error"
        else:
            status = "injected"

        created_at = raw.get("created_at")
        updated_at = raw.get("updated_at")
        recover_time = updated_at if status == "recovered" else None

        return {
            "status": status,
            "created_at": created_at,
            "updated_at": updated_at,
            "targets": [{
                "id": raw.get("uid", ""),
                "inject_time": created_at,
                "recover_time": recover_time,
            }],
        }

    def recover(self, experiment: dict, run_id: str, handle: str) -> None:
        target = experiment["target"]
        address = target["address"]

        url = f"http://{address}:{CHAOSD_PORT}/api/attack/{handle}"
        logger.info("Recovering chaosd fault %s on %s for run %s", handle, address, run_id)
        try:
            resp = requests.delete(url, timeout=30)
            resp.raise_for_status()
        except Exception:
            logger.exception("Failed to recover chaosd fault %s — manual cleanup may be needed", handle)
