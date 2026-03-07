from abc import ABC, abstractmethod


class FixtureStrategy(ABC):
    """Base class for all fixture ingestion strategies.

    Subclasses must implement `load`. The optional `init` hook runs once
    before any fixtures are loaded and is the right place for one-time
    setup such as creating the target database/keyspace.
    """

    def init(self, config: dict) -> None:
        """One-time setup before fixture loading begins. No-op by default."""

    @abstractmethod
    def load(self, s3_uris: list[str], config: dict) -> None:
        """Load a single fixture's S3 files into the target database."""
