"""DataFrame transforms for the batch jobs.

Pure DataFrame-in -> DataFrame-out functions (the "business logic separated
from Spark session boilerplate" standard) so they're unit-testable with a
local SparkSession (tests/unit/test_batch_transforms.py, Java-gated).

Validation model: bronze rows that fail typing/business rules are split out
and written to the silver quarantine path by the job — never silently dropped,
consistent with the producer and the streaming bronze layer.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def latest_per_business_key(df: DataFrame, key: str) -> DataFrame:
    """Keep only the newest row per business key (ingestion stamp, then
    sequence number when the bronze table has kinesis lineage)."""
    order_cols = [F.col("_ingested_at").desc()]
    if "_kinesis_sequence_number" in df.columns:
        order_cols.insert(0, F.col("_kinesis_sequence_number").desc())
    window = Window.partitionBy(key).orderBy(*order_cols)
    return (
        df.withColumn("_rank", F.row_number().over(window))
        .where(F.col("_rank") == 1)
        .drop("_rank")
    )


def validate_orders(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Orders: order_date must parse to a real timestamp; ids must be present.
    Returns (typed_valid, invalid)."""
    typed = bronze_df.withColumn("order_date_ts", F.to_timestamp("order_date"))
    bad = typed.where(
        F.col("order_date_ts").isNull()
        | F.col("order_id").isNull()
        | F.col("order_customer_id").isNull()
    )
    good = typed.where(
        F.col("order_date_ts").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("order_customer_id").isNotNull()
    )
    return good, bad


def validate_order_items(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Order items: ids present, non-negative quantity/subtotal/price."""
    bad = bronze_df.where(
        F.col("order_item_id").isNull()
        | F.col("order_item_order_id").isNull()
        | F.col("order_item_quantity").isNull()
        | (F.col("order_item_quantity") < 0)
        | (F.col("order_item_subtotal") < 0)
        | (F.col("order_item_product_price") < 0)
    )
    good = bronze_df.subtract(bad)
    return good, bad


def silver_orders_df(valid_typed: DataFrame) -> DataFrame:
    """Dedup + project to the silver orders column order (must match the DDL
    exactly — the MERGE uses UPDATE SET * / INSERT *)."""
    deduped = latest_per_business_key(valid_typed, "order_id")
    return deduped.select(
        "order_id",
        F.col("order_date_ts").alias("order_date"),
        "order_customer_id",
        "order_status",
        "_kinesis_partition_key",
        "_kinesis_sequence_number",
        "_kinesis_arrival_timestamp",
        "_ingested_at",
    )


def silver_items_df(valid: DataFrame) -> DataFrame:
    deduped = latest_per_business_key(valid, "order_item_id")
    return deduped.select(
        "order_item_id",
        "order_item_order_id",
        "order_item_product_id",
        "order_item_quantity",
        "order_item_subtotal",
        "order_item_product_price",
        "_kinesis_partition_key",
        "_kinesis_sequence_number",
        "_kinesis_arrival_timestamp",
        "_ingested_at",
    )


def quarantine_rows(df: DataFrame, entity: str, reason: str) -> DataFrame:
    """Shape invalid rows for the quarantine path: the full original row as
    JSON, plus entity/reason/ingestion stamp for triage."""
    return df.select(
        F.lit(entity).alias("entity"),
        F.lit(reason).alias("reason"),
        F.to_json(F.struct(*df.columns)).alias("payload"),
        F.current_timestamp().alias("_quarantined_at"),
    )


def silver_dim_df(bronze_dim: DataFrame, entity) -> DataFrame:
    """Dims: cast timestamp-typed source columns, dedup on business key,
    project to the silver dim column order."""
    deduped = latest_per_business_key(bronze_dim, entity.business_key)
    cols = []
    for name, source_type in entity.columns:
        if source_type == "timestamp":
            cols.append(F.to_timestamp(name).alias(name))
        else:
            cols.append(F.col(name))
    cols.append("_ingested_at")
    return deduped.select(*cols)


# ---------------------------------------------------------------- gold marts


def daily_sales(orders: DataFrame, items: DataFrame) -> DataFrame:
    items_dated = items.join(
        orders.select("order_id", F.col("order_date").alias("_order_date")),
        items["order_item_order_id"] == orders["order_id"],
        "inner",
    )
    return items_dated.groupBy(F.to_date("_order_date").alias("order_date")).agg(
        F.countDistinct("order_item_order_id").alias("total_orders"),
        F.sum("order_item_quantity").cast("bigint").alias("total_items"),
        F.sum("order_item_subtotal").alias("total_revenue"),
    )


def category_sales(
    items: DataFrame, products: DataFrame, categories: DataFrame
) -> DataFrame:
    enriched = items.join(
        products.select("product_id", "product_category_id"),
        items["order_item_product_id"] == products["product_id"],
        "inner",
    ).join(
        categories.select("category_id", "category_name"),
        F.col("product_category_id") == F.col("category_id"),
        "inner",
    )
    return enriched.groupBy("category_id", "category_name").agg(
        F.sum("order_item_quantity").cast("bigint").alias("total_items"),
        F.sum("order_item_subtotal").alias("total_revenue"),
    )


def customer_orders(orders: DataFrame, customers: DataFrame) -> DataFrame:
    enriched = orders.join(
        customers.select(
            "customer_id",
            F.concat_ws(" ", "customer_fname", "customer_lname").alias("customer_name"),
        ),
        orders["order_customer_id"] == customers["customer_id"],
        "inner",
    )
    return enriched.groupBy("customer_id", "customer_name").agg(
        F.to_date(F.min("order_date")).alias("first_order"),
        F.to_date(F.max("order_date")).alias("last_order"),
        F.count("*").cast("bigint").alias("total_orders"),
    )
