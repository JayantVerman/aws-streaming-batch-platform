# DataFrame-level tests for the batch transforms. Need pyspark + Java 17;
# skip cleanly without them (same pattern as test_bronze_transforms.py).
import datetime
import json
import shutil

import pytest

pytest.importorskip("pyspark")

if shutil.which("java") is None:
    pytest.skip("java not available — SparkSession tests skipped", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

from batch.spark_batch_jobs import transforms  # noqa: E402
from batch.spark_batch_jobs.job_config import load_entity_columns  # noqa: E402

ORDERS_ROW = {
    "order_id": 1, "order_date": "2013-07-25T00:00:00",
    "order_customer_id": 10, "order_status": "CLOSED",
    "_kinesis_partition_key": "1", "_kinesis_sequence_number": "s1",
    "_kinesis_arrival_timestamp": None, "_ingested_at": None,
}


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]")
               .appName("batch-transform-tests").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _orders_df(spark, rows):
    return spark.createDataFrame(rows, list(ORDERS_ROW.keys()))


def test_latest_per_business_key_and_silver_projection(spark):
    df = _orders_df(spark, [ORDERS_ROW, dict(ORDERS_ROW, _kinesis_sequence_number="s0")])
    # same business key twice, different sequence numbers: latest wins
    deduped = transforms.latest_per_business_key(df, "order_id")
    assert deduped.count() == 1

    silver = transforms.silver_orders_df(df)
    assert silver.columns == [
        "order_id", "order_date", "order_customer_id", "order_status",
        "_kinesis_partition_key", "_kinesis_sequence_number",
        "_kinesis_arrival_timestamp", "_ingested_at"]


def test_validate_orders_splits_bad_dates(spark):
    good = dict(ORDERS_ROW)
    bad = dict(ORDERS_ROW, order_id=2, order_date="not-a-date")
    typed_valid, invalid = transforms.validate_orders(
        _orders_df(spark, [good, bad]))
    assert typed_valid.count() == 1
    assert invalid.count() == 1
    # invalid rows keep the original raw value for quarantine
    assert invalid.select("order_date").collect()[0][0] == "not-a-date"


def test_validate_order_items_rejects_negatives(spark):
    items = spark.createDataFrame(
        [(1, 1, 1, 2, 49.98, 24.99), (2, 1, 2, -1, 10.0, 10.0)],
        ["order_item_id", "order_item_order_id", "order_item_product_id",
         "order_item_quantity", "order_item_subtotal", "order_item_product_price"])
    good, bad = transforms.validate_order_items(items)
    assert good.count() == 1 and bad.count() == 1


def test_quarantine_rows_shape(spark):
    bad = _orders_df(spark, [dict(ORDERS_ROW, order_date="not-a-date")])
    q = transforms.quarantine_rows(bad, "orders", "validation_failed")
    row = q.collect()[0]
    assert row["entity"] == "orders" and row["reason"] == "validation_failed"
    payload = json.loads(row["payload"])
    assert payload["order_id"] == 1 and payload["order_status"] == "CLOSED"


def test_silver_dim_df_casts_and_dedups(spark):
    entity = load_entity_columns("departments")
    bronze = spark.createDataFrame(
        [(1, "Fashion", None), (1, "Fashion", None)],
        ["department_id", "department_name", "_ingested_at"])
    silver = transforms.silver_dim_df(bronze, entity)
    assert silver.columns == ["department_id", "department_name", "_ingested_at"]
    assert silver.count() == 1


def test_daily_sales_and_customer_orders(spark):
    orders = spark.createDataFrame(
        [(1, datetime.datetime(2013, 7, 25, 0, 0, 0), 10, "CLOSED")],
        ["order_id", "order_date", "order_customer_id", "order_status"])
    items = spark.createDataFrame(
        [(1, 1, 100, 2, 49.98, 24.99), (2, 1, 100, 1, 24.99, 24.99)],
        ["order_item_id", "order_item_order_id", "order_item_product_id",
         "order_item_quantity", "order_item_subtotal", "order_item_product_price"])
    customers = spark.createDataFrame(
        [(10, "Mary", "Smith")], ["customer_id", "customer_fname", "customer_lname"])

    daily = transforms.daily_sales(orders, items).collect()
    assert daily[0]["total_orders"] == 1
    assert daily[0]["total_items"] == 3
    assert abs(daily[0]["total_revenue"] - 74.97) < 1e-6

    per_customer = transforms.customer_orders(orders, customers).collect()
    assert per_customer[0]["customer_name"] == "Mary Smith"
    assert per_customer[0]["total_orders"] == 1

