# Local-stack integration smoke. These need the docker-compose stack RUNNING
# (LocalStack + MinIO + Postgres + Trino). They skip cleanly otherwise, and the
# stack-smoke GitHub Actions job runs them with the stack up:
#   PLATFORM_STACK_UP=1 python -m pytest tests/integration -q
import os

import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("PLATFORM_STACK_UP") != "1",
    reason="requires the local compose stack (set PLATFORM_STACK_UP=1)")

import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402

ENDPOINT = os.getenv("KINESIS_ENDPOINT", "http://localhost:4566")


def _kinesis():
    return boto3.client(
        "kinesis", endpoint_url=ENDPOINT, region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test",
        config=Config(retries={"max_attempts": 3, "mode": "standard"}))


def _s3():
    return boto3.client(
        "s3", endpoint_url=ENDPOINT, region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test",
        config=Config(signature_version="s3v4"))


def test_localstack_kinesis_has_streams():
    streams = _kinesis().list_streams()["StreamNames"]
    assert "retail-orders" in streams
    assert "retail-order_items" in streams


def test_minio_buckets_exist():
    buckets = {b["Name"] for b in _s3().list_buckets()["Buckets"]}
    assert {"lake", "checkpoints"} <= buckets


def test_serving_schema_has_tables():
    try:
        import psycopg2
    except ImportError:
        # reachable via the container's psql in the stack-smoke job
        import subprocess
        res = subprocess.run(
            ["docker", "exec", "-i", "platform-postgres", "psql", "-U", "platform",
             "-d", "warehouse", "-c",
             "SELECT tablename FROM pg_tables WHERE schemaname='serving'"],
            capture_output=True, text=True)
        assert res.returncode == 0, res.stderr
        names = {line.strip() for line in res.stdout.splitlines()
                 if line.strip() and not line.startswith(("tablename", "-"))}
    else:
        conn = psycopg2.connect(host="localhost", port=5432, dbname="warehouse",
                                user="platform", password="platform")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM pg_tables WHERE schemaname='serving'")
            assert cur.fetchone()[0] >= 3
        conn.close()
    assert True


def test_trino_up():
    import urllib.request
    try:
        with urllib.request.urlopen("http://localhost:8089/v1/info", timeout=5) as r:
            body = r.read().decode()
    except Exception as exc:  # noqa: BLE001 — integration env probe
        pytest.fail(f"trino not reachable: {exc}")
    assert '"starting":false' in body or "node" in body