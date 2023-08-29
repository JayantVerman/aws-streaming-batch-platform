# DataFrame-level tests for the bronze transforms. These need a real
# SparkSession (pyspark + Java 17); they skip cleanly when either is missing
# (e.g. this Windows dev box) and run in CI/containers.
import base64
import io
import json
import shutil

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("fastavro")

if shutil.which("java") is None:
    pytest.skip("java not available — SparkSession tests skipped", allow_module_level=True)

from fastavro import parse_schema, schemaless_writer  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from streaming.spark_streaming_jobs import transforms  # noqa: E402

SCHEMA = {
    "type": "record",
    "name": "order",
    "fields": [
        {"name": "order_id", "type": "long"},
        {"name": "order_date", "type": "string"},
        {"name": "order_customer_id", "type": "long"},
        {"name": "order_status", "type": "string"},
    ],
}
SCHEMA_JSON = json.dumps(SCHEMA)


def _avro(record: dict) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, parse_schema(SCHEMA), record)
    return buf.getvalue()


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]")
               .appName("bronze-transform-tests").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _raw_df(spark, rows):
    """rows: list of (data_bytes, partition_key, seq_no)"""
    schema = "data binary, partitionKey string, sequenceNumber string"
    return spark.createDataFrame(rows, schema)


def test_decode_split_and_dedup(spark):
    good1 = _avro({"order_id": 1, "order_date": "2013-07-25T00:00:00",
                   "order_customer_id": 10, "order_status": "CLOSED"})
    good2 = _avro({"order_id": 2, "order_date": "2013-07-25T00:00:00",
                   "order_customer_id": 11, "order_status": "PENDING_PAYMENT"})
    dup1 = _avro({"order_id": 1, "order_date": "2013-07-25T00:00:00",
                  "order_customer_id": 10, "order_status": "CLOSED"})
    raw = _raw_df(spark, [
        (good1, "1", "s1"),
        (good2, "2", "s2"),
        (dup1, "1", "s3"),          # replayed business key within the batch
        (b"not-avro-at-all", "9", "s4"),  # corrupt -> dead letter
    ])

    decoded = transforms.decode_records(raw, SCHEMA_JSON)
    ok, dead = transforms.split_dead_letters(decoded, (
        "order_id", "order_date", "order_customer_id", "order_status"))

    assert ok.count() == 3
    assert dead.count() == 1

    deduped = transforms.dedup_business_key(ok, "order_id")
    assert deduped.count() == 2

    bronze = transforms.select_bronze_columns(deduped, (
        "order_id", "order_date", "order_customer_id", "order_status"))
    cols = bronze.columns
    assert cols[0] == "order_id"
    assert "_kinesis_partition_key" in cols and "_ingested_at" in cols
    # raw payload is not part of bronze
    assert "_raw_data" not in cols

    dead_rows = transforms.build_dead_letter_records(dead, "orders").collect()
    assert len(dead_rows) == 1
    assert dead_rows[0]["entity"] == "orders"
    assert dead_rows[0]["reason"] == "avro_decode_failed"
    assert dead_rows[0]["payload_b64"] == base64.b64encode(
        b"not-avro-at-all").decode()


def test_lineage_columns_are_kept(spark):
    good = _avro({"order_id": 5, "order_date": "2013-07-25T00:00:00",
                  "order_customer_id": 10, "order_status": "CLOSED"})
    decoded = transforms.decode_records(
        _raw_df(spark, [(good, "5", "s9")]), SCHEMA_JSON)
    row = decoded.collect()[0]
    assert row["_kinesis_partition_key"] == "5"
    assert row["_kinesis_sequence_number"] == "s9"
    assert row["_ingested_at"] is not None
