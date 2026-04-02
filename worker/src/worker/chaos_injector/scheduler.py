"""ChaosScheduler — background thread that orchestrates chaos experiment timing."""

from __future__ import annotations

import logging
import threading

from worker.driver_manager.k6_manager import parse_k6_duration

from .base import ChaosEngine
from .chaosd_engine import ChaosdEngine
from .k8s_engine import K8sChaosEngine

logger = logging.getLogger(__name__)


def _get_engine(env_type: str) -> ChaosEngine:
    if env_type == "k8s":
        return K8sChaosEngine()
    elif env_type == "ec2":
        return ChaosdEngine()
    raise ValueError(f"Unsupported chaos target env_type: {env_type}")


class ChaosScheduler(threading.Thread):
    """Orchestrates chaos experiments in a background thread.

    Lifecycle per experiment:
      sleep(start_after) -> inject -> sleep(duration) -> recover

    Call cancel() to trigger early recovery and stop the thread.
    """

    def __init__(self, chaos_spec: dict, run_id: str) -> None:
        super().__init__(daemon=True, name=f"chaos-{run_id}")
        self._chaos_spec = chaos_spec
        self._run_id = run_id
        self._cancel_event = threading.Event()
        # Track active injections for cleanup: list of (experiment, engine, handle)
        self._active: list[tuple[dict, ChaosEngine, str]] = []
        self._lock = threading.Lock()

    def cancel(self) -> None:
        """Signal the scheduler to stop and recover all active experiments."""
        self._cancel_event.set()

    def _sleep(self, seconds: int) -> bool:
        """Sleep for *seconds*, returning True if cancelled during the wait."""
        return self._cancel_event.wait(timeout=seconds)

    def run(self) -> None:
        experiments = self._chaos_spec.get("experiments", [])
        for exp in experiments:
            if self._cancel_event.is_set():
                break
            self._run_experiment(exp)
        # Final cleanup — recover anything still active
        self._recover_all()

    def _run_experiment(self, experiment: dict) -> None:
        schedule = experiment.get("schedule", {})
        start_after = parse_k6_duration(schedule.get("start_after", "0s"))
        duration = parse_k6_duration(schedule.get("duration", "1m"))
        env_type = experiment["target"]["env_type"]

        logger.info(
            "Chaos experiment '%s': waiting %ds before injection",
            experiment["name"], start_after,
        )
        if self._sleep(start_after):
            return  # cancelled during wait

        try:
            engine = _get_engine(env_type)
            handle = engine.inject(experiment, self._run_id)
            with self._lock:
                self._active.append((experiment, engine, handle))
            logger.info(
                "Chaos experiment '%s': injected (handle=%s), holding for %ds",
                experiment["name"], handle, duration,
            )
        except Exception:
            logger.exception("Failed to inject chaos experiment '%s'", experiment["name"])
            return

        if self._sleep(duration):
            return  # cancelled during hold — _recover_all handles cleanup

        # Normal recovery
        try:
            engine.recover(experiment, self._run_id, handle)
            with self._lock:
                self._active = [(e, eng, h) for e, eng, h in self._active if h != handle]
            logger.info("Chaos experiment '%s': recovered", experiment["name"])
        except Exception:
            logger.exception("Failed to recover chaos experiment '%s'", experiment["name"])

    def _recover_all(self) -> None:
        """Recover all active experiments (called on cancel or thread exit)."""
        with self._lock:
            active = list(self._active)
            self._active.clear()
        for experiment, engine, handle in active:
            try:
                engine.recover(experiment, self._run_id, handle)
                logger.info("Chaos cleanup: recovered '%s'", experiment["name"])
            except Exception:
                logger.exception(
                    "Chaos cleanup: failed to recover '%s' (handle=%s)", experiment["name"], handle,
                )
