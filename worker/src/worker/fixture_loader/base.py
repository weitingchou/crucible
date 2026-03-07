import boto3

from worker.config import settings

# Maps component names to their loader strategy.
_MPP_COMPONENTS = {"doris", "trino"}
_NOSQL_COMPONENTS = {"cassandra"}


class FixtureLoader:
    """Selects and executes the correct ingestion strategy for a given SUT component.

    Reads test_environment.fixtures (an array of {fixture_id, table}) and
    component_spec.cluster_info from the test plan. For each fixture, lists S3
    files under the fixture_id prefix and passes them to the appropriate strategy:

    * MPP (Doris / Trino)  → ZeroDownloadStrategy  (S3 TVF SQL)
    * NoSQL (Cassandra)    → StreamingStrategy      (async row streaming)
    * Other                → StandardStrategy       (raises NotImplementedError)

    Each strategy receives a merged config dict of the form:
        {**cluster_info, "target_db": <str>, "fixture_id": <str>, "table": <str>}
    """

    def __init__(self, plan: dict) -> None:
        env = plan["test_environment"]
        component_spec = env["component_spec"]
        self.component: str = component_spec["type"].lower()
        self.cluster_info: dict = component_spec.get("cluster_info") or {}
        self.target_db: str = env["target_db"]
        self.fixtures: list[dict] = env.get("fixtures", [])

    def load(self) -> None:
        s3 = boto3.client("s3", region_name=settings.aws_region)
        strategy = self._get_strategy()
        base_config = {**self.cluster_info, "target_db": self.target_db}
        strategy.init(base_config)

        for fixture in self.fixtures:
            fixture_id: str = fixture["fixture_id"]
            prefix = f"fixtures/{fixture_id}/"

            paginator = s3.get_paginator("list_objects_v2")
            s3_uris: list[str] = [
                f"s3://{settings.s3_bucket}/{obj['Key']}"
                for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=prefix)
                for obj in page.get("Contents", [])
            ]

            if not s3_uris:
                raise FileNotFoundError(f"No fixture files found under prefix: {prefix}")

            config = {
                **self.cluster_info,
                "target_db": self.target_db,
                "s3_access_key": settings.aws_access_key_id,
                "s3_secret_key": settings.aws_secret_access_key,
                **fixture,
            }
            strategy.load(s3_uris, config)

    def _get_strategy(self):
        if self.component in _MPP_COMPONENTS:
            from .strategies.zero_download import ZeroDownloadStrategy
            return ZeroDownloadStrategy()
        if self.component in _NOSQL_COMPONENTS:
            from .strategies.streaming import StreamingStrategy
            return StreamingStrategy()
        from .strategies.standard import StandardStrategy
        return StandardStrategy()
