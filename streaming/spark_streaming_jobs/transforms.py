"""DataFrame transforms for the bronze streaming jobs.

The only module besides bronze_stream.py that imports pyspark. Kept as pure
DataFrame-in -> DataFrame-out functions so they can be unit-tested with a
local SparkSession (see tests/unit/test_bronze_transforms.py).

Decode-failure model: from_avro runs in PERMISSIVE mode, so a record that
can't be decoded against the schema comes back with null field values. Every
field in our Avro contracts is non-nullable, therefore "any null field" ==
"corrupt record" and such rows are routed to the dead-letter path instead of
bronze. (If a Spark build ignored the mode option, corrupt records would fail
the batch loudly instead — no silent loss either way.)
"""

from __future__ import annotations

from functools import reduce
from operator import and_

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.avro.functions import from_avro

# Columns the Kinesis connector exposes per record (we keep those that exist).
KINESIS_RAW_META_COLS = (
    "partitionKey",
    "sequenceNumber",
    "approximateArrivalTimestamp",
)


def decode_records(raw_df: DataFrame, avro_schema_json: str) -> DataFrame:
    """Kinesis source df -> one row per record with a parsed 'record' struct,
    kinesis lineage columns (_kinesis_*) and the raw payload kept for
    dead-lettering."""
    parsed = from_avro(F.col("data"), avro_schema_json, {"mode": "PERMISSIVE"}).alias(
        "record")
    cols = [parsed, F.col("data").alias("_raw_data")]
    for col_name in KINESIS_RAW_META_COLS:
        if col_name in raw_df.columns:
            cols.append(F.col(col_name).alias(f"_kinesis_{col_name}"))
    cols.append(F.current_timestamp().alias("_ingested_at"))
    return raw_df.select(*cols)


def split_dead_letters(df: DataFrame, field_names: tuple[str, ...]) -> tuple[DataFrame, DataFrame]:
    """Return (ok_df, dead_df). A record is dead when the decoded struct is
    null or any non-nullable field is null (corrupt/partial Avro payload)."""
    not_null = reduce(
        and_,
        [F.col("record").isNotNull()]
        + [F.col(f"record.{name}").isNotNull() for name in field_names],
    )
    return df.where(not_null), df.where(~not_null)


def dedup_business_key(df: DataFrame, business_key: str) -> DataFrame:
    """Collapse duplicates within one micro-batch (producer replays re-send the
    watermark record). Combined with Iceberg's transactional append and the
    checkpoint offsets this gives idempotent bronze writes; cross-batch replay
    duplicates are resolved authoritatively by the silver MERGE job."""
    return df.dropDuplicates([business_key])


def select_bronze_columns(df: DataFrame, field_names: tuple[str, ...]) -> DataFrame:
    """Flatten the record struct into bronze column order + lineage columns."""
    cols = [F.col(f"record.{name}") for name in field_names]
    cols += [F.col(c) for c in df.columns if c.startswith("_kinesis_")]
    if "_ingested_at" in df.columns:
        cols.append(F.col("_ingested_at"))
    return df.select(*cols)


def build_dead_letter_records(dead_df: DataFrame, entity: str) -> DataFrame:
    """Shape dead rows for the quarantine path: entity, reason, lineage and the
    raw payload (base64) so nothing is lost and the row can be reprocessed."""
    return dead_df.select(
        F.lit(entity).alias("entity"),
        F.lit("avro_decode_failed").alias("reason"),
        F.base64(F.col("_raw_data")).alias("payload_b64"),
        *[
            F.col(c)
            for c in ("_kinesis_partition_key", "_kinesis_sequence_number",
                      "_kinesis_arrival_timestamp", "_ingested_at")
            if c in dead_df.columns
        ],
    )
