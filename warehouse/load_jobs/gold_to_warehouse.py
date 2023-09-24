"""Gold -> warehouse (serving schema) load job.

Truncate-overwrite per mart: the DDL in warehouse/ddl/ is the source of truth
for table shape; this job only moves data, so a run is idempotent and never
drifts the schema. Identical code serves Postgres locally and Redshift Serverless
on AWS — only connection parameters change (config/settings.yaml + env).

Usage:
  spark-submit ... warehouse/load_jobs/gold_to_warehouse.py            # all marts
  spark-submit ... warehouse/load_jobs/gold_to_warehouse.py --mart daily_sales
"""

from __future__ import annotations

# Submit-agnostic bootstrap: repo root on sys.path before platform imports.
import sys
from pathlib import Path as _BootstrapPath

sys.path.insert(0, str(_BootstrapPath(__file__).resolve().parents[2]))

import argparse
import json

from streaming.common.logging_utils import get_logger
from streaming.common.spark_utils import build_session
from streaming.spark_streaming_jobs.job_config import SparkJobConfig, get_job_config
from warehouse.load_jobs.job_config import GOLD_TO_WAREHOUSE, get_warehouse_config

logger = get_logger("gold-to-warehouse")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load gold marts into the serving warehouse schema.")
    parser.add_argument(
        "--mart", default=None,
        help="only this mart (daily_sales|category_sales|customer_orders); "
             "default: all")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    cfg = get_job_config()
    wh = get_warehouse_config()

    # validate mart names against the mapping up front
    marts = [args.mart] if args.mart else sorted(GOLD_TO_WAREHOUSE)
    for mart in marts:
        wh.table(mart)

    # The JDBC driver must be on the classpath FROM THE START of the session —
    # choose jdbc package and build the SparkSession including it.
    spark = build_session(
        SparkJobConfig(
            env=cfg.env,
            kinesis_endpoint=cfg.kinesis_endpoint,
            s3_endpoint=cfg.s3_endpoint,
            region=cfg.region,
            catalog_name=cfg.catalog_name,
            namespace=cfg.namespace,
            warehouse=cfg.warehouse,
            checkpoints_bucket=cfg.checkpoints_bucket,
            trigger_interval=cfg.trigger_interval,
            starting_position=cfg.starting_position,
            packages=cfg.packages + (wh.jdbc_package,),
            kinesis_connector_jar=cfg.kinesis_connector_jar,
            cluster_master=cfg.cluster_master,
            local_master=cfg.local_master,
            s3_access_key=cfg.s3_access_key,
            s3_secret_key=cfg.s3_secret_key,
        ),
        "gold-to-warehouse",
    )

    summary = {}
    for mart in marts:
        source = spark.table(f"lake.retail_gold.{mart}")
        count = source.count()
        source.write.mode("overwrite") \
            .option("truncate", "true") \
            .jdbc(
                url=wh.jdbc_url,
                table=wh.table(mart),
                mode="overwrite",
                properties={
                    "user": wh.user,
                    "password": wh.password,
                    "driver": wh.driver,
                },
            )
        summary[mart] = count
        logger.info("mart loaded: %s", json.dumps(
            {"mart": mart, "rows": count}))

    logger.info("warehouse load complete: %s", json.dumps({
        "marts": summary, "schema": wh.schema, "jdbc_url": wh.jdbc_url}))


def main() -> None:
    run()


if __name__ == "__main__":
    main()

/* wip */
