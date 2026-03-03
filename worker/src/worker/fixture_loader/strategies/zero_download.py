import pymysql


class ZeroDownloadStrategy:
    """MPP path: issues SQL to the SUT so the cluster pulls Parquet from S3 directly.

    No data passes through the Celery worker. Targets: Doris, Trino.

    Connection details come from component_spec.cluster_info in the test plan.
    The host field follows the format "hostname:port" (e.g. "localhost:9030").
    DB credentials (db_user, db_password) and S3 credentials (s3_access_key,
    s3_secret_key) are injected at runtime via cluster_info extras.
    """

    def load(self, s3_uris: list[str], config: dict) -> None:
        host, port = _parse_host(config["host"])
        conn = pymysql.connect(
            host=host,
            port=port,
            user=config["db_user"],
            password=config["db_password"],
            database=config["target_db"],
        )
        try:
            with conn.cursor() as cur:
                for uri in s3_uris:
                    sql = f"""
                        INSERT INTO {config['table']}
                        SELECT * FROM S3(
                            "uri"        = "{uri}",
                            "ACCESS_KEY" = "{config['s3_access_key']}",
                            "SECRET_KEY" = "{config['s3_secret_key']}",
                            "format"     = "parquet"
                        );
                    """
                    cur.execute(sql)
            conn.commit()
        finally:
            conn.close()


def _parse_host(host_str: str) -> tuple[str, int]:
    """Split 'hostname:port' into (hostname, port). Defaults to 9030 if omitted."""
    if ":" in host_str:
        host, _, port_str = host_str.rpartition(":")
        return host, int(port_str)
    return host_str, 9030
