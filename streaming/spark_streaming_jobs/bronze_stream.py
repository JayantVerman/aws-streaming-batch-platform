"""Bronze streaming job: Kinesis -> Avro decode -> bronze Iceberg table.

One process per streaming entity (orders, order_items). Runs identically
against LocalStack Kinesis + MinIO (ENV=local) and real Kinesis + S3 (ENV=aws)
— only endpoints/creds differ, injected via settings/env, never hardcoded.

Usage:
  # in the docker stack (scripts/run_bronze_stream.sh wraps spark-submit):
  spark-submit ... streaming/spark_streaming_jobs/bronze_stream.py --entity orders

  # plain pyspark local run (downloads jars per settings.yaml, needs Java 17):
  python -m streaming.spark_streaming_jobs.bronze_stream --entity orders

  # one-shot catch-up run then exit (useful after downtime / for backfills):
  ... --trigger available-now
"""

from __future__ import annotations

# Submit-agnostic bootstrap: repo root on sys.path before platform imports.
import sys
from pathlib import Path as _Path

sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))

import argparse
import json

from pyspark.sql import DataFrame, SparkSession

from streaming.common.logging_utils import get_logger
from streaming.common.spark_utils import build_session, ensure_namespace
from streaming.spark_streaming_jobs import transforms
from streaming.spark_streaming_jobs.job_config import (
    EntitySpec,
    SparkJobConfig,
    get_entity_spec,
    get_job_config,
)

logger = get_logger("bronze-stream")


def ensure_table(spark: SparkSession, cfg: SparkJobConfig, spec: EntitySpec) -> None:
    ensure_namespace(spark, cfg, cfg.namespace)
    spark.sql(spec.create_table_sql())
    logger.info("bronze table ready: %s", spec.bronze_table)


def make_batch_writer(
    spark: SparkSession, cfg: SparkJobConfig, spec: EntitySpec
):
    """foreachBatch callback: dedup -> bronze append, dead letters -> quarantine."""

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        ok_df, dead_df = transforms.split_dead_letters(
            batch_df, spec.field_names)
        bronze = transforms.select_bronze_columns(
            transforms.dedup_business_key(ok_df, spec.business_key),
            spec.field_names,
        )
        bronze_count = bronze.count()
        if bronze_count:
            bronze.writeTo(spec.bronze_table).append()
        dead_count = dead_df.count()
        if dead_count:
            transforms.build_dead_letter_records(dead_df, spec.name).write.mode(
                "append").json(spec.dead_letter_path)
        logger.info("batch %s: %s", json.dumps({
            "entity": spec.name,
            "batch_id": batch_id,
            "bronze_rows": bronze_count,
            "dead_letters": dead_count,
        }))

    return write_batch


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream a Kinesis entity into its bronze Iceberg table.")
    parser.add_argument("--entity", required=True,
                        help="streaming entity from config/entities.yaml")
    parser.add_argument("--trigger", choices=("processing", "available-now"),
                        default="processing",
                        help="processing: continuous micro-batches; "
                             "available-now: catch up and exit")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    cfg = get_job_config()
    spec = get_entity_spec(args.entity, cfg)
    logger.info("starting bronze stream: %s", json.dumps({
        "entity": spec.name,
        "stream": spec.stream_name,
        "bronze_table": spec.bronze_table,
        "checkpoint": cfg.checkpoint_path(spec.name),
        "trigger": args.trigger,
    }))

    spark = build_session(cfg, f"bronze-stream-{spec.name}", with_kinesis_jar=True)
    ensure_table(spark, cfg, spec)

    raw = (
        spark.readStream.format("aws-kinesis")
        .option("kinesis.streamName", spec.stream_name)
        .option("kinesis.consumerType", "GetRecords")
        .option("kinesis.startingPosition", cfg.starting_position)
        .option("kinesis.region", cfg.region)
    )
    if cfg.kinesis_endpoint:
        # LocalStack / VPC endpoint. On AWS this is omitted and the connector
        # talks to the real service.
        raw = raw.option("kinesis.endpointUrl", cfg.kinesis_endpoint)
    raw_df = raw.load()

    writer = (
        raw_df.writeStream
        .foreachBatch(make_batch_writer(spark, cfg, spec))
        .option("checkpointLocation", cfg.checkpoint_path(spec.name))
    )
    if args.trigger == "available-now":
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.trigger(processingTime=cfg.trigger_interval).start()
    query.awaitTermination()


def main() -> None:
    run()


if __name__ == "__main__":
    main()

/* wip */
