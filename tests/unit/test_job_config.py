# Unit tests for the bronze streaming job configuration (no JVM needed —
# job_config.py is deliberately pyspark-free).
import json

import pytest

from streaming.spark_streaming_jobs import job_config as jc


@pytest.fixture()
def spec_orders():
    cfg = jc.SparkJobConfig(
        env="local", kinesis_endpoint="http://localhost:4566",
        s3_endpoint="http://minio:9000", region="us-east-1",
        catalog_name="lake", namespace="retail", warehouse="s3a://lake/warehouse",
        checkpoints_bucket="checkpoints", trigger_interval="10 seconds",
        starting_position="TRIM_HORIZON", packages=(),
        kinesis_connector_jar="", cluster_master="spark://spark-master:7077",
        local_master="local[2]", s3_access_key="k", s3_secret_key="s")
    return jc.get_entity_spec("orders", cfg), cfg


def test_entity_spec_from_real_contract(spec_orders):
    spec, _ = spec_orders
    # column order comes from the Avro contract, not the raw file
    assert spec.field_names == (
        "order_id", "order_date", "order_customer_id", "order_status")
    assert spec.business_key == "order_id"
    assert spec.stream_name == "retail-orders"
    assert spec.bronze_table == "lake.retail.orders"
    assert spec.dead_letter_path == "s3a://lake/quarantine/streaming/orders"


def test_stream_per_entity(spec_orders):
    spec, cfg = spec_orders
    other = jc.get_entity_spec("order_items", cfg)
    assert other.stream_name == "retail-order_items"
    assert other.bronze_table == "lake.retail.order_items"
    assert other.business_key == "order_item_id"


def test_create_table_sql_shape(spec_orders):
    spec, _ = spec_orders
    sql = spec.create_table_sql()
    assert sql.startswith("CREATE TABLE IF NOT EXISTS lake.retail.orders")
    # avsc types map to spark types in contract order
    assert "order_id bigint" in sql and "order_date string" in sql
    # lineage columns exist and the lake partition is on ingestion day
    assert "_kinesis_partition_key string" in sql
    assert "_kinesis_sequence_number string" in sql
    assert "_ingested_at timestamp" in sql
    assert "PARTITIONED BY (days(_ingested_at))" in sql


def test_batch_entity_rejected():
    with pytest.raises(SystemExit, match="batch entity"):
        jc.get_entity_spec("customers")


def test_unknown_entity_rejected():
    with pytest.raises(SystemExit, match="unknown entity"):
        jc.get_entity_spec("no_such_entity")


def test_hadoop_confs_local_minio(spec_orders):
    spec, cfg = spec_orders
    confs = cfg.hadoop_confs()
    assert confs["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert confs["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert confs["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert confs["spark.hadoop.fs.s3a.access.key"] == "k"
    assert confs["spark.hadoop.fs.s3a.aws.credentials.provider"].endswith(
        "SimpleAWSCredentialsProvider")


def test_hadoop_confs_aws_untouched():
    cfg = jc.SparkJobConfig(
        env="aws", kinesis_endpoint=None, s3_endpoint=None, region="eu-west-1",
        catalog_name="lake", namespace="retail", warehouse="s3a://lake/warehouse",
        checkpoints_bucket="checkpoints", trigger_interval="10 seconds",
        starting_position="TRIM_HORIZON", packages=(),
        kinesis_connector_jar="", cluster_master="", local_master="",
        s3_access_key=None, s3_secret_key=None)
    # on AWS the IAM provider chain + default S3 endpoint apply: no overrides
    assert cfg.hadoop_confs() == {}
    assert cfg.kinesis_endpoint is None


def test_checkpoint_path(spec_orders):
    _, cfg = spec_orders
    assert cfg.checkpoint_path("orders") == "s3a://checkpoints/streaming/orders"


def test_read_avsc_rejects_unions(tmp_path):
    schema = {"type": "record", "name": "bad", "fields": [
        {"name": "id", "type": ["long", "null"]}]}
    (tmp_path / "bad.avsc").write_text(json.dumps(schema), encoding="utf-8")
    with pytest.raises(ValueError, match="non-nullable"):
        jc.read_avsc(tmp_path, "bad")
