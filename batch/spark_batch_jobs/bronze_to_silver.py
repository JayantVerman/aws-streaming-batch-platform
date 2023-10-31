"""Bronze -> silver: typing, validation, authoritative business-key dedup.

The silver layer is the clean, deduped, typed view of the lake. Dedup here is
the authoritative one: an Iceberg MERGE INTO on the business key keeps the
row with the newest ingestion stamp, so replayed/streamed duplicates in bronze
collapse exactly once, whatever their arrival path.

All entities go through this job: facts (orders, order_items) typed and
date-filterable via --start-date/--end-date; dimensions cast and deduped.
Validation failures land in s3a://lake/quarantine/silver/<entity>/ — never
silently dropped.

Usage:
  spark-submit ... batch/spark_batch_jobs/bronze_to_silver.py --entity orders
  # backfill a date range (facts only):
  ... --entity order_items --start-date 2013-07-25 --end-date 2013-07-31
"""

from __future__ import annotations

# Submit-agnostic bootstrap: spark-submit only puts the script's own dir on
# sys.path, so add the repo root before any platform imports.
import sys
from pathlib import Path as _BootstrapPath

sys.path.insert(0, str(_BootstrapPath(__file__).resolve().parents[2]))

import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from batch.spark_batch_jobs import transforms
from batch.spark_batch_jobs.job_config import (
    SILVER_NAMESPACE,
    load_entity_columns,
    merge_sql,
    parse_date_bound,
    silver_ddl,
)
from streaming.common.logging_utils import get_logger
from streaming.common.spark_utils import build_session, ensure_namespace
from streaming.spark_streaming_jobs.job_config import get_job_config

logger = get_logger("bronze-to-silver")


def _apply_order_bounds(df, args):
    """order_date in bronze is an ISO-8601 string — lexicographic compare is
    a correct date filter (same trick as the producer's watermark)."""
    if args.start_date:
        df = df.where(F.col("order_date") >= F.lit(f"{args.start_date}T00:00:00"))
    if args.end_date:
        df = df.where(F.col("order_date") <= F.lit(f"{args.end_date}T23:59:59"))
    return df


def _merge_updates(spark: SparkSession, updates, entity) -> None:
    updates.createOrReplaceTempView("updates")
    spark.sql(merge_sql(entity))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform one bronze entity into silver (typed + deduped).")
    parser.add_argument("--entity", required=True,
                        help="entity from config/entities.yaml")
    parser.add_argument("--start-date", default=None,
                        help="inclusive lower bound YYYY-MM-DD (facts only)")
    parser.add_argument("--end-date", default=None,
                        help="inclusive upper bound YYYY-MM-DD (facts only)")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    args.start_date = parse_date_bound(args.start_date)
    args.end_date = parse_date_bound(args.end_date)
    entity = load_entity_columns(args.entity)
    cfg = get_job_config()

    spark = build_session(cfg, f"bronze-to-silver-{entity.name}")
    ensure_namespace(spark, cfg, SILVER_NAMESPACE)
    spark.sql(silver_ddl(entity))
    bronze = spark.table(entity.bronze_table)

    if entity.delivery == "streaming":
        if entity.name == "orders":
            bronze = _apply_order_bounds(bronze, args)
            good, bad = transforms.validate_orders(bronze)
            updates = transforms.silver_orders_df(good)
        elif entity.name == "order_items":
            if args.start_date or args.end_date:
                # items carry no date; bound them through their orders
                orders = _apply_order_bounds(
                    spark.table("lake.retail.orders"), args)
                bronze = bronze.join(
                    orders.select("order_id"),
                    bronze["order_item_order_id"] == orders["order_id"],
                    "left_semi",
                )
            good, bad = transforms.validate_order_items(bronze)
            updates = transforms.silver_items_df(good)
        else:
            raise SystemExit(f"unexpected streaming entity {entity.name!r}")
    else:
        bad = bronze.where(F.col(entity.business_key).isNull())
        good = bronze.where(F.col(entity.business_key).isNotNull())
        updates = transforms.silver_dim_df(good, entity)

    bad_count = bad.count()
    if bad_count:
        transforms.quarantine_rows(bad, entity.name, "validation_failed").write.mode(
            "append").json(entity.quarantine_path)

    updates_count = updates.count()
    _merge_updates(spark, updates, entity)
    logger.info("bronze->silver complete: %s", json.dumps({
        "entity": entity.name,
        "updates_merged": updates_count,
        "quarantined": bad_count,
        "start_date": args.start_date,
        "end_date": args.end_date,
    }))


def main() -> None:
    run()


if __name__ == "__main__":
    main()

