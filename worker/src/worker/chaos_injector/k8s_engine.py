"""Chaos Mesh CRD engine — creates/deletes fault CRDs via the Kubernetes API."""

from __future__ import annotations

import logging

from kubernetes import client, config

from .base import ChaosEngine

logger = logging.getLogger(__name__)


def _build_crd_manifest(experiment: dict, run_id: str) -> dict:
    """Translate a chaos experiment dict into a Chaos Mesh CRD body."""
    target = experiment["target"]
    params = experiment.get("parameters", {})
    schedule = experiment.get("schedule", {})
    crd_name = f"crucible-{run_id}-{experiment['name']}".lower().replace("_", "-")[:63]

    spec: dict = {
        "selector": {"labelSelectors": target.get("selector", {})},
        "mode": "all",
        "duration": schedule.get("duration", "1m"),
    }
    spec.update(params)

    return {
        "apiVersion": "chaos-mesh.org/v1alpha1",
        "kind": experiment["fault_type"].capitalize() if not experiment["fault_type"][0].isupper()
        else experiment["fault_type"],
        "metadata": {
            "name": crd_name,
            "namespace": target["namespace"],
            "labels": {"crucible.io/run-id": run_id},
        },
        "spec": spec,
    }


class K8sChaosEngine(ChaosEngine):
    """Injects faults by creating Chaos Mesh CRDs in the target K8s cluster."""

    def __init__(self) -> None:
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self._api = client.CustomObjectsApi()

    def inject(self, experiment: dict, run_id: str) -> str:
        manifest = _build_crd_manifest(experiment, run_id)
        kind = manifest["kind"].lower()
        namespace = manifest["metadata"]["namespace"]
        name = manifest["metadata"]["name"]

        logger.info("Injecting chaos CRD %s/%s (kind=%s) for run %s", namespace, name, kind, run_id)
        self._api.create_namespaced_custom_object(
            group="chaos-mesh.org",
            version="v1alpha1",
            namespace=namespace,
            plural=kind,
            body=manifest,
        )
        return f"{kind}/{namespace}/{name}"

    def recover(self, experiment: dict, run_id: str, handle: str) -> None:
        kind, namespace, name = handle.split("/", 2)
        logger.info("Recovering chaos CRD %s/%s (kind=%s) for run %s", namespace, name, kind, run_id)
        try:
            self._api.delete_namespaced_custom_object(
                group="chaos-mesh.org",
                version="v1alpha1",
                namespace=namespace,
                plural=kind,
                name=name,
            )
        except Exception:
            logger.exception("Failed to delete chaos CRD %s — manual cleanup may be needed", handle)
