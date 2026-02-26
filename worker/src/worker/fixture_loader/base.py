import boto3

from worker.config import settings

# Maps component names to their loader strategy.
_MPP_COMPONENTS = {"doris", "trino"}
_NOSQL_COMPONENTS = {"cassandra"}


class FixtureLoader:
    """Selects and executes the correct ingestion strategy for a given SUT component.

    The S3 bucket is listed for files matching the fixture_id prefix. Each file
    is then passed to the appropriate strategy:

    * MPP (Doris / Trino)  → ZeroDownloadStrategy  (S3 TVF SQL)
    * NoSQL (Cassandra)    → StreamingStrategy      (async row streaming)
    * Other                → StandardStrategy       (raises NotImplementedError)
    """

    def __init__(self, component: str, config: dict) -> None:
        self.component = component.lower()
        self.config = config

    def load(self) -> None:
        s3 = boto3.client("s3", region_name=settings.aws_region)
        fixture_id: str = self.config["fixture_id"]
        prefix = f"fixtures/{fixture_id}/"

        paginator = s3.get_paginator("list_objects_v2")
        s3_uris: list[str] = [
            f"s3://{settings.s3_bucket}/{obj['Key']}"
            for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=prefix)
            for obj in page.get("Contents", [])
        ]

        if not s3_uris:
            raise FileNotFoundError(f"No fixture files found under prefix: {prefix}")

        strategy = self._get_strategy()
        strategy.load(s3_uris, self.config)

    def _get_strategy(self):
        if self.component in _MPP_COMPONENTS:
            from .strategies.zero_download import ZeroDownloadStrategy
            return ZeroDownloadStrategy()
        if self.component in _NOSQL_COMPONENTS:
            from .strategies.streaming import StreamingStrategy
            return StreamingStrategy()
        from .strategies.standard import StandardStrategy
        return StandardStrategy()
