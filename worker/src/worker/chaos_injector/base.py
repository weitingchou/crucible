"""Abstract base for chaos engines."""

from __future__ import annotations

import abc
import logging

logger = logging.getLogger(__name__)


class ChaosEngine(abc.ABC):
    """Interface for injecting and recovering chaos experiments."""

    @abc.abstractmethod
    def inject(self, experiment: dict, run_id: str) -> str:
        """Apply the fault and return a handle string used for recovery."""

    @abc.abstractmethod
    def recover(self, experiment: dict, run_id: str, handle: str) -> None:
        """Remove the fault using the handle returned by inject()."""

    def collect_status(self, experiment: dict, run_id: str, handle: str) -> dict | None:
        """Read engine-specific status before recovery. Override in subclasses."""
        return None

    @staticmethod
    def normalize_status(raw: dict) -> dict | None:
        """Convert raw engine status into the unified schema. Override in subclasses."""
        return None
