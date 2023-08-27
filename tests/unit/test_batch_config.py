# Unit tests for batch job config/SQL builders (no JVM needed).
import pytest

from batch.spark_batch_jobs import job_config as bjc


def test_load_entity_columns_contract():
    e = bjc.load_entity_columns("order_items")
    assert e.columns[0] == ("order_item_id", "long")
    assert e.delivery == "streaming"
    assert e.business_key == "order_item_id"
    assert e.source_file == "order_items/part-00000"


def test_silver_facts_keep_kinesis_lineage():
    e = bjc.load_entity_columns("orders")
    cols = dict(e.silver_columns)
    # typed, not raw-string
    assert cols["order_date"] == "timestamp"
    assert cols["order_id"] == "bigint"
    # streaming lineage columns present
    assert "_kinesis_partition_key" in cols
    assert "_kinesis_sequence_number" in cols
    assert cols["_ingested_at"] == "timestamp"


def test_silver_dims_have_no_kinesis_lineage():
    e = bjc.load_entity_columns("customers")
    cols = dict(e.silver_columns)
    assert "_kinesis_partition_key" not in cols
    assert cols["customer_id"] == "bigint"
    assert cols["customer_zipcode"] == "string"


def test_silver_ddl_is_iceberg_v2_for_merge():
    e = bjc.load_entity_columns("orders")
    sql = bjc.silver_ddl(e)
    assert sql.startswith("CREATE TABLE IF NOT EXISTS lake.retail_silver.orders")
    assert "format-version'='2" in sql  # MERGE requires Iceberg v2


def test_merge_sql_keeps_newest_row():
    e = bjc.load_entity_columns("orders")
    sql = bjc.merge_sql(e)
    assert "MERGE INTO lake.retail_silver.orders t USING updates u" in sql
    assert "ON t.order_id = u.order_id" in sql
    assert "u._ingested_at >= t._ingested_at THEN UPDATE SET *" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql


def test_bronze_dim_spec_source_path():
    spec = bjc.get_bronze_dim_spec("departments")
    assert spec.bronze_table == "lake.retail.departments"
    assert spec.source_path == "data/sample_raw/retail_db/departments/part-00000"
    # bronze dims keep timestamps raw; _ingested_at is added
    assert dict(spec.schema_columns())["department_id"] == "bigint"


def test_bronze_dim_spec_rejects_streaming_entity():
    with pytest.raises(SystemExit, match="streaming entity"):
        bjc.get_bronze_dim_spec("orders")


def test_gold_marts_ddl():
    for name in bjc.GOLD_MARTS:
        sql = bjc.gold_ddl(name)
        assert sql.startswith(f"CREATE TABLE IF NOT EXISTS lake.retail_gold.{name}")
    assert "total_revenue double" in bjc.gold_ddl("daily_sales")
    assert "total_orders bigint" in bjc.gold_ddl("customer_orders")
    with pytest.raises(SystemExit, match="unknown gold mart"):
        bjc.gold_table("does_not_exist")


def test_parse_date_bound():
    assert bjc.parse_date_bound(None) is None
    assert bjc.parse_date_bound("2013-07-25") == "2013-07-25"
    with pytest.raises(SystemExit, match="invalid date"):
        bjc.parse_date_bound("25/07/2013")
