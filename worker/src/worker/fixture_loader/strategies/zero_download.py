import pymysql


class ZeroDownloadStrategy:
    """MPP path: issues SQL to the SUT so the cluster pulls Parquet from S3 directly.

    No data passes through the Celery worker. Targets: Doris, Trino.
    The database connection details are expected in *config* under the keys
    db_host, db_port, db_user, db_password, db_name, table, s3_access_key,
    s3_secret_key.
    """

    def load(self, s3_uris: list[str], config: dict) -> None:
        conn = pymysql.connect(
            host=config["db_host"],
            port=int(config.get("db_port", 9030)),
            user=config["db_user"],
            password=config["db_password"],
            database=config["db_name"],
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
