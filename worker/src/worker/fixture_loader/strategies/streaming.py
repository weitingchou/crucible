import csv

import boto3
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

from worker.config import settings


class StreamingStrategy:
    """NoSQL path: streams CSV rows from S3 directly into Cassandra.

    Data flows: S3 → worker RAM (single line buffer) → Cassandra.
    The local disk is never written to. Connection details come from
    component_spec.cluster_info; host follows the "hostname:port" format.
    config keys required: host, target_db, table, columns (list[str]).
    """

    def load(self, s3_uris: list[str], config: dict) -> None:
        s3 = boto3.client("s3", region_name=settings.aws_region)
        host, _port = _parse_host(config["host"])
        cluster = Cluster([host])
        session = cluster.connect(config["target_db"])

        columns: list[str] = config["columns"]
        placeholders = ", ".join(["?"] * len(columns))
        col_list = ", ".join(columns)
        insert_stmt = session.prepare(
            f"INSERT INTO {config['table']} ({col_list}) VALUES ({placeholders})"
        )

        try:
            for uri in s3_uris:
                bucket, key = _parse_s3_uri(uri)
                obj = s3.get_object(Bucket=bucket, Key=key)
                line_gen = (line.decode("utf-8") for line in obj["Body"].iter_lines())
                reader = csv.reader(line_gen)
                next(reader, None)  # skip header row
                execute_concurrent_with_args(
                    session, insert_stmt, reader, concurrency=150
                )
        finally:
            cluster.shutdown()


def _parse_host(host_str: str) -> tuple[str, int]:
    """Split 'hostname:port' into (hostname, port). Defaults to 9042 if omitted."""
    if ":" in host_str:
        host, _, port_str = host_str.rpartition(":")
        return host, int(port_str)
    return host_str, 9042


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split 's3://bucket/key/path' into (bucket, key)."""
    without_scheme = uri.removeprefix("s3://")
    bucket, _, key = without_scheme.partition("/")
    return bucket, key
