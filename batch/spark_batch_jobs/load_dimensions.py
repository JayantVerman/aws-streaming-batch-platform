"""Batch path: raw dimension files -> bronze Iceberg (full-snapshot overwrite).

The batch/historical route from the architecture: the same raw files the
producer replays into Kinesis for the facts are bulk-loaded straight into
bronze for the dimensions — how real platforms handle backfills/file drops.
Idempotent: every run replaces the dimension snapshot in place.

EMR-step compatible: the raw source is a plain path (local dir or s3a://),
endpoints/creds come from settings.yaml + env, nothing is hardcoded.

Usage:
  spark-submit ... batch/spark_batch_jobs/load_dimensions.py --entity customers
  # backfill/restore from another location (e.g. a lake raw drop):
  ... --entity products --source-path s3a://lake/raw/retail_db/products
"""

from __future__ import annotations

# Submit-agnostic bootstrap: spark-submit only puts the script's own dir on
# sys.path, so add the repo root before any platform imports (works for
# python -m, Airflow SparkSubmitOperator client mode, and EMR steps alike).
import sys
from pathlib import Path as _BootstrapPath

sys.path.insert(0, str(_BootstrapPath(__file__).resolve().parents[2]))

import argparse
import json
from dataclasses import replace

from pyspark.sql import DataFrame, SparkSession, types as T
from pyspark.sql import functions as F

from streaming.common.logging_utils import get_logger
from streaming.common.spark_utils import build_session, ensure_namespace
from batch.spark_batch_jobs.job_config import (
    BRONZE_NAMESPACE,
    BronzeDimSpec,
    ENTITY_TO_BRONZE,
    get_bronze_dim_spec,
    get_job_config,
)

logger = get_logger("batch-load-dimensions")

# bronze SQL type -> pyspark type (for the CSV reader schema)
_TO_PYSPARK = {
    "bigint": T.LongType(),
    "int": T.IntegerType(),
    "double": T.DoubleType(),
    "string": T.StringType(),
}


def bronze_struct_type(spec: BronzeDimSpec) -> T.StructType:
    return T.StructType(
        [T.StructField(name, _TO_PYSPARK[t], True) for name, t in spec.schema_columns()
         if name != "_ingested_at"]
    )


def quarantine_malformed(df: DataFrame, spec: BronzeDimSpec) -> tuple[DataFrame, DataFrame]:
    """CSV PERMISSIVE mode turns malformed rows into all-null rows; the
    business key being null is the marker. Invalid rows -> quarantine path."""
    bad = df.where(F.col(spec.entity.business_key).isNull())
    good = df.where(F.col(spec.entity.business_key).isNotNull())
    shaped = bad.select(
        F.lit(spec.entity.name).alias("entity"),
        F.lit("raw_parse_error").alias("reason"),
        F.to_json(F.struct(*df.columns)).alias("payload"),
        F.current_timestamp().alias("_quarantined_at"),
    )
    return good, shaped


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk-load a batch dimension entity into bronze Iceberg.")
    parser.add_argument("--entity", required=True,
                        help="batch entity from config/entities.yaml")
    parser.add_argument("--source-path", default=None,
                        help="override the raw source (local dir or s3a://...)")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    cfg = get_job_config()
    spec = get_bronze_dim_spec(args.entity, cfg)
    if args.source_path:
        spec = replace(spec, source_path=args.source_path)
    logger.info("loading dimension: %s", json.dumps({
        "entity": spec.entity.name, "source": spec.source_path,
        "bronze_table": spec.bronze_table,
    }))

    spark = build_session(cfg, f"load-dimensions-{spec.entity.name}")
    ensure_namespace(spark, cfg, BRONZE_NAMESPACE)
    spark.sql(spec.create_table_sql())

    raw = (
        spark.read.option("header", False).option("mode", "PERMISSIVE")
        .schema(bronze_struct_type(spec)).csv(spec.source_path)
    )
    good, quarantined = quarantine_malformed(raw, spec)
    bronze = good.withColumn("_ingested_at", F.current_timestamp())

    quarantined_count = quarantined.count()
    if quarantined_count:
        quarantined.write.mode("append").json(
            f"s3a://lake/quarantine/bronze/{spec.entity.name}")
    bronze_count = bronze.count()
    # full-snapshot semantics: this run's file replaces the previous snapshot
    bronze.writeTo(spec.bronze_table).overwritePartitions()
    logger.info("dimension load complete: %s", json.dumps({
        "entity": spec.entity.name,
        "bronze_rows": bronze_count,
        "quarantined": quarantined_count,
        "mode": "overwrite_snapshot",
    }))


def main() -> None:
    run()


if __name__ == "__main__":
    main()
