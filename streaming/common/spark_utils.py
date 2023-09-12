"""Shared Spark session/confs for streaming and batch jobs.

One place builds the Iceberg catalog + s3a configuration so the streaming and
batch paths can never drift apart. Both read endpoints/creds from
config/settings.yaml + environment overrides via job_config.get_job_config().
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from streaming.spark_streaming_jobs.job_config import SparkJobConfig


def build_session(
    cfg: SparkJobConfig, app_name: str, with_kinesis_jar: bool = False
) -> SparkSession:
    """SparkSession with the Iceberg catalog and (local) MinIO s3a confs.

    with_kinesis_jar: only the bronze streaming jobs need the Kinesis connector
    jar; batch jobs skip it so a run never downloads what it won't use.
    """
    builder = SparkSession.builder.appName(app_name).config(
        "spark.jars.packages", ",".join(cfg.packages)
    ).config(
        # Spark 3.5 native Prometheus UI metrics (driver). Scraped from
        # spark-master/worker (daemons) and driver UIs in client mode.
        "spark.ui.prometheus.enabled", "true"
    )
    if with_kinesis_jar:
        builder = builder.config("spark.jars", cfg.kinesis_connector_jar)
    for key, value in {**cfg.catalog_confs(), **cfg.hadoop_confs()}.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def ensure_namespace(spark: SparkSession, cfg: SparkJobConfig, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.catalog_name}.{namespace}")

/* wip */
