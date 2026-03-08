import pymysql

from . import FixtureStrategy


class ZeroDownloadStrategy(FixtureStrategy):
    """MPP path: issues SQL to the SUT so the cluster pulls Parquet from S3 directly.

    No data passes through the Celery worker. Targets: Doris, Trino.

    Connection details come from component_spec.cluster_info in the test plan.
    The host field follows the format "hostname:port" (e.g. "doris-fe:9030").
    DB credentials are read from cluster_info.username / cluster_info.password.
    S3 credentials (s3_access_key, s3_secret_key) are injected at runtime.
    """

    def init(self, config: dict) -> None:
        host, port = _parse_host(config["host"])
        conn = pymysql.connect(
            host=host, port=port,
            user=config["username"], password=config["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE IF NOT EXISTS `{config['target_db']}`")
            conn.commit()
        finally:
            conn.close()

    def load(self, s3_uris: list[str], config: dict) -> None:
        host, port = _parse_host(config["host"])
        conn = pymysql.connect(
            host=host,
            port=port,
            user=config["username"],
            password=config["password"],
            database=config["target_db"],
        )
        endpoint_prop = f'"s3.endpoint" = "{config["s3_endpoint"]}",' if config.get("s3_endpoint") else ""
        path_style = "true" if config.get("s3_endpoint") else "false"

        def _s3_select(uri: str) -> str:
            return f"""SELECT * FROM S3(
                            "uri"            = "{uri}",
                            "s3.access_key"  = "{config['s3_access_key']}",
                            "s3.secret_key"  = "{config['s3_secret_key']}",
                            "s3.region"      = "{config.get('s3_region', 'ap-southeast-1')}",
                            {endpoint_prop}
                            "use_path_style" = "{path_style}",
                            "format"         = "parquet"
                        )"""

        try:
            with conn.cursor() as cur:
                # Create the table from parquet schema (CTAS), then insert remaining files.
                cur.execute(f"DROP TABLE IF EXISTS `{config['table']}`")
                cur.execute(
                    f"CREATE TABLE `{config['table']}` ENGINE=OLAP "
                    f"DISTRIBUTED BY RANDOM BUCKETS 1 "
                    f'PROPERTIES ("replication_num" = "1") '
                    f"AS {_s3_select(s3_uris[0])}"
                )
                for uri in s3_uris[1:]:
                    cur.execute(f"INSERT INTO `{config['table']}` {_s3_select(uri)}")
            conn.commit()
        finally:
            conn.close()


def _parse_host(host_str: str) -> tuple[str, int]:
    """Split 'hostname:port' into (hostname, port). Defaults to 9030 if omitted."""
    if ":" in host_str:
        host, _, port_str = host_str.rpartition(":")
        return host, int(port_str)
    return host_str, 9030
