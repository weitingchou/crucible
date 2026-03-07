from . import FixtureStrategy


class StandardStrategy(FixtureStrategy):
    """Fallback ingestion path for database types not yet covered by a dedicated strategy."""

    def load(self, s3_uris: list[str], config: dict) -> None:
        component = config.get("component", "<unknown>")
        raise NotImplementedError(
            f"No ingestion strategy implemented for component '{component}'. "
            "Add a new strategy class and register it in FixtureLoader._get_strategy()."
        )
