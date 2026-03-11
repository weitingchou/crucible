#!/usr/bin/env python3
"""
setup_tpch_fixtures.py

Generates TPC-H data using DuckDB's built-in TPC-H extension and uploads
it as Parquet fixtures to the local MinIO instance, ready for a Crucible
end-to-end test.

Each of the eight TPC-H tables is uploaded under its own fixture_id so the
Crucible fixture loader can target the correct database table:

    s3://project-crucible-storage/fixtures/tpch-sf001-lineitem/lineitem.parquet
    s3://project-crucible-storage/fixtures/tpch-sf001-customer/customer.parquet
    ...
    s3://project-crucible-storage/workloads/tpch_queries.sql

Prerequisites:
    pip install duckdb boto3

    MinIO must already be running:
        docker compose -f infrastructure/docker-compose.yml up minio -d

Usage:
    # Default: SF 0.01 (~10 MB total, generates in seconds)
    python tests/setup_tpch_fixtures.py

    # Larger dataset
    python tests/setup_tpch_fixtures.py --scale 0.1

    # Keep generated Parquet files locally under tests/fixtures/tpch/
    python tests/setup_tpch_fixtures.py --keep-local

    # Custom endpoint / credentials
    python tests/setup_tpch_fixtures.py --endpoint http://localhost:9000 \\
        --access-key minioadmin --secret-key minioadmin
"""

import argparse
import pathlib
import sys
import tempfile

TABLES = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]

TESTS_DIR = pathlib.Path(__file__).parent
WORKLOAD_SRC = TESTS_DIR / "workloads" / "tpch_queries.sql"
WORKLOAD_S3_KEY = "workloads/tpch-queries.sql"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate TPC-H Parquet fixtures and upload to MinIO."
    )
    p.add_argument(
        "--scale",
        type=float,
        default=0.01,
        help="TPC-H scale factor (default: 0.01 ≈ 10 MB total)",
    )
    p.add_argument(
        "--endpoint",
        default="http://localhost:9000",
        help="S3/MinIO endpoint URL (default: http://localhost:9000)",
    )
    p.add_argument("--access-key", default="minioadmin")
    p.add_argument("--secret-key", default="minioadmin")
    p.add_argument(
        "--bucket",
        default="project-crucible-storage",
        help="Target S3 bucket (default: project-crucible-storage)",
    )
    p.add_argument(
        "--fixture-prefix",
        default=None,
        help=(
            "Override fixture ID prefix. "
            "Each table becomes <prefix>-<table>. "
            "Default: tpch-sf<scale> e.g. tpch-sf001"
        ),
    )
    p.add_argument(
        "--keep-local",
        action="store_true",
        help="Keep generated Parquet files in tests/fixtures/tpch/ after upload.",
    )
    return p.parse_args()


def check_prerequisites() -> None:
    missing = []
    try:
        import duckdb  # noqa: F401
    except ImportError:
        missing.append("duckdb")
    try:
        import boto3  # noqa: F401
    except ImportError:
        missing.append("boto3")
    if missing:
        print(f"ERROR: Missing required packages: {', '.join(missing)}")
        print(f"       pip install {' '.join(missing)}")
        sys.exit(1)
    if not WORKLOAD_SRC.exists():
        print(f"ERROR: Workload file not found: {WORKLOAD_SRC}")
        sys.exit(1)


def generate_tpch(scale: float, output_dir: pathlib.Path) -> None:
    import duckdb

    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Generating TPC-H data at scale factor {scale:g} ...")

    con = duckdb.connect()
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"CALL dbgen(sf={scale})")

    for table in TABLES:
        dest = output_dir / f"{table}.parquet"
        con.execute(f"COPY {table} TO '{dest}' (FORMAT PARQUET)")
        size_mb = dest.stat().st_size / 1_048_576
        print(f"  {table:<12}  {size_mb:6.2f} MB  →  {dest.name}")

    con.close()
    print()


def _s3_client(endpoint: str, access_key: str, secret_key: str):
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def ensure_bucket(s3, bucket: str) -> None:
    from botocore.exceptions import ClientError

    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"Created bucket: {bucket}\n")


def upload_fixtures(
    parquet_dir: pathlib.Path,
    fixture_prefix: str,
    bucket: str,
    s3,
) -> dict[str, str]:
    """Upload one Parquet file per table; return {table: fixture_id}."""
    print(f"Uploading fixtures to s3://{bucket}/fixtures/")
    fixture_ids = {}
    for table in TABLES:
        local = parquet_dir / f"{table}.parquet"
        fixture_id = f"{fixture_prefix}-{table}"
        key = f"fixtures/{fixture_id}/{table}.parquet"
        s3.upload_file(str(local), bucket, key)
        fixture_ids[table] = fixture_id
        print(f"  uploaded  {key}")
    return fixture_ids


def upload_workload(bucket: str, s3) -> None:
    print(f"\nUploading workload to s3://{bucket}/{WORKLOAD_S3_KEY}")
    s3.upload_file(str(WORKLOAD_SRC), bucket, WORKLOAD_S3_KEY)
    print(f"  uploaded  {WORKLOAD_S3_KEY}")


def print_summary(fixture_ids: dict[str, str], bucket: str) -> None:
    print("\n" + "─" * 60)
    print("Fixtures ready. Fixture IDs:")
    for table, fid in fixture_ids.items():
        print(f"  {table:<12}  {fid}")
    print(f"\nWorkload key:  {WORKLOAD_S3_KEY}")
    print("\nUpdate tests/plans/tpch_doris.yaml if you used a non-default")
    print("scale factor, then upload the plan and submit the test run:")
    print(f"  aws --endpoint-url http://localhost:9000 s3 cp \\")
    print(f"    tests/plans/tpch_doris.yaml s3://{bucket}/plans/tpch_doris.yaml")
    print(f"  curl -X POST http://localhost:8000/test-runs/plans/tpch_doris.yaml")


def main() -> None:
    args = parse_args()
    check_prerequisites()

    scale_tag = f"{args.scale:g}".replace(".", "")
    fixture_prefix = args.fixture_prefix or f"tpch-sf{scale_tag}"

    s3 = _s3_client(args.endpoint, args.access_key, args.secret_key)
    ensure_bucket(s3, args.bucket)

    if args.keep_local:
        parquet_dir = TESTS_DIR / "fixtures" / "tpch"
        generate_tpch(args.scale, parquet_dir)
        fixture_ids = upload_fixtures(parquet_dir, fixture_prefix, args.bucket, s3)
    else:
        with tempfile.TemporaryDirectory(prefix="crucible_tpch_") as tmp:
            parquet_dir = pathlib.Path(tmp)
            generate_tpch(args.scale, parquet_dir)
            fixture_ids = upload_fixtures(parquet_dir, fixture_prefix, args.bucket, s3)

    upload_workload(args.bucket, s3)
    print_summary(fixture_ids, args.bucket)


if __name__ == "__main__":
    main()
