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
