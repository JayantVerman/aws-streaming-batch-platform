"""Silver -> gold: business marts (full recompute per run, idempotent).

Small data + full recompute is deliberate: gold marts are cheap to rebuild
and overwrite-per-run makes them trivially idempotent. When volumes grow this
becomes incremental — the transform functions won't change.

Marts:
  daily_sales     order_date, total_orders, total_items, total_revenue
  category_sales  category_id/name, total_items, total_revenue
  customer_orders customer_id/name, first/last order, total_orders

Usage:
  spark-submit ... batch/spark_batch_jobs/silver_to_gold.py
  # rebuild for one date range instead of everything:
  ... --start-date 2013-07-25 --end-date 2013-07-31
"""

from __future__ import annotations

# Submit-agnostic bootstrap: spark-submit only puts the script's own dir on
# sys.path, so add the repo root before any platform imports.
import sys
from pathlib import Path as _BootstrapPath

sys.path.insert(0, str(_BootstrapPath(__file__).resolve().parents[2]))

import argparse
import json

from pyspark.sql import functions as F

from batch.spark_batch_jobs import transforms
from batch.spark_batch_jobs.job_config import (
    GOLD_MARTS,
    GOLD_NAMESPACE,
    gold_ddl,
    gold_table,
    parse_date_bound,
)
from streaming.common.logging_utils import get_logger
from streaming.common.spark_utils import build_session, ensure_namespace
from streaming.spark_streaming_jobs.job_config import get_job_config

logger = get_logger("silver-to-gold")

SILVER_ROOT = "lake.retail_silver"


def _require_tables(spark, names: list[str]) -> None:
    missing = [n for n in names if not spark.catalog.tableExists(f"{SILVER_ROOT}.{n}")]
    if missing:
        raise SystemExit(
            f"silver tables missing: {missing} — run load_dimensions.py and "
            "bronze_to_silver.py for those entities first")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild gold marts from silver.")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD, inclusive")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    args.start_date = parse_date_bound(args.start_date)
    args.end_date = parse_date_bound(args.end_date)
    cfg = get_job_config()

    spark = build_session(cfg, "silver-to-gold")
    ensure_namespace(spark, cfg, GOLD_NAMESPACE)
    _require_tables(spark, ["orders", "order_items", "products", "categories",
                            "customers"])

    orders = spark.table(f"{SILVER_ROOT}.orders")
    if args.start_date:
        orders = orders.where(
            F.to_date("order_date") >= F.lit(args.start_date).cast("date"))
    if args.end_date:
        orders = orders.where(
            F.to_date("order_date") <= F.lit(args.end_date).cast("date"))

    items = spark.table(f"{SILVER_ROOT}.order_items")
    products = spark.table(f"{SILVER_ROOT}.products")
    categories = spark.table(f"{SILVER_ROOT}.categories")
    customers = spark.table(f"{SILVER_ROOT}.customers")

    marts = {
        "daily_sales": transforms.daily_sales(orders, items),
        "category_sales": transforms.category_sales(items, products, categories),
        "customer_orders": transforms.customer_orders(orders, customers),
    }
    summary = {}
    for name, mart in marts.items():
        spark.sql(gold_ddl(name))
        count = mart.count()
        mart.writeTo(gold_table(name)).overwritePartitions()
        summary[name] = count
    logger.info("gold rebuild complete: %s", json.dumps({
        "marts": summary, "start_date": args.start_date, "end_date": args.end_date}))


def main() -> None:
    run()


if __name__ == "__main__":
    main()
