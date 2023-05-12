"""Config + SQL builders for the batch (bronze -> silver -> gold) jobs.

Pure Python, no pyspark — unit-testable without a JVM (same discipline as
streaming/spark_streaming_jobs/job_config.py). Column order and types come
from config/entities.yaml; endpoints/creds from config/settings.yaml + env.

Layers:
  bronze  lake.retail.<entity>          dims: full-snapshot overwrite (batch path)
  silver  lake.retail_silver.<entity>   typed, deduped via MERGE on business key
  gold    lake.retail_gold.<mart>       business marts, full recompute per run
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from streaming.spark_streaming_jobs.job_config import (
    METADATA_COLUMNS,
    DEFAULT_ENTITIES_PATH,
    SparkJobConfig,
    get_job_config,
    load_yaml,
)

# entities.yaml column type -> Spark SQL type per layer. Bronze keeps
# timestamps as raw strings (source-faithful); silver casts to real types.
ENTITY_TO_BRONZE = {
    "long": "bigint", "int": "int", "double": "double",
    "string": "string", "timestamp": "string",
}
ENTITY_TO_SILVER = {
    "long": "bigint", "int": "int", "double": "double",
    "string": "string", "timestamp": "timestamp",
}

BRONZE_NAMESPACE = "retail"          # shared with the streaming bronze writer
SILVER_NAMESPACE = "retail_silver"
GOLD_NAMESPACE = "retail_gold"

SILVER_VERSION_2 = "TBLPROPERTIES ('format-version'='2')"  # required for MERGE

# Gold marts: name -> ordered (column, type) pairs
GOLD_MARTS: dict[str, tuple[tuple[str, str], ...]] = {
    "daily_sales": (
        ("order_date", "date"),
        ("total_orders", "bigint"),
        ("total_items", "bigint"),
        ("total_revenue", "double"),
    ),
    "category_sales": (
        ("category_id", "bigint"),
        ("category_name", "string"),
        ("total_items", "bigint"),
        ("total_revenue", "double"),
    ),
    "customer_orders": (
        ("customer_id", "bigint"),
        ("customer_name", "string"),
        ("first_order", "date"),
        ("last_order", "date"),
        ("total_orders", "bigint"),
    ),
}


@dataclass(frozen=True)
class EntityColumns:
    """One entity as contracted in config/entities.yaml (any delivery mode)."""

    name: str
    columns: tuple[tuple[str, str], ...]  # (name, source_type) in contract order
    business_key: str
    incremental_column: str | None
    delivery: str  # "streaming" | "batch"
    source_file: str  # relative raw file from the contract

    @property
    def silver_columns(self) -> tuple[tuple[str, str], ...]:
        """Silver schema: typed business columns + lineage columns actually
        present in this entity's bronze table (facts have kinesis lineage,
        batch-loaded dims don't)."""
        cols = [(n, ENTITY_TO_SILVER[t]) for n, t in self.columns]
        if self.delivery == "streaming":
            cols += [c for c in METADATA_COLUMNS if c[0] != "_ingested_at"]
        cols.append(("_ingested_at", "timestamp"))
        return tuple(cols)

    @property
    def silver_table(self) -> str:
        return f"lake.{SILVER_NAMESPACE}.{self.name}"

    @property
    def bronze_table(self) -> str:
        return f"lake.{BRONZE_NAMESPACE}.{self.name}"

    @property
    def quarantine_path(self) -> str:
        return f"s3a://lake/quarantine/silver/{self.name}"


def load_entity_columns(
    entity_name: str, entities_path: str = DEFAULT_ENTITIES_PATH
) -> EntityColumns:
    entity = load_yaml(entities_path).get("entities", {}).get(entity_name)
    if entity is None:
        raise SystemExit(f"unknown entity {entity_name!r} (see config/entities.yaml)")
    columns = tuple(entity["columns"].items())
    return EntityColumns(
        name=entity_name,
        columns=columns,
        business_key=entity["business_key"],
        incremental_column=entity.get("incremental_column"),
        delivery=entity.get("delivery", "batch"),
        source_file=entity["file"],
    )


def silver_ddl(entity: EntityColumns) -> str:
    col_defs = ", ".join(f"{n} {t}" for n, t in entity.silver_columns)
    return (
        f"CREATE TABLE IF NOT EXISTS {entity.silver_table} "
        f"({col_defs}) USING iceberg {SILVER_VERSION_2}"
    )


def merge_sql(entity: EntityColumns) -> str:
    """Authoritative business-key dedup: keep the silver row with the newest
    ingestion stamp. `updates` is the deduped, typed new data (same column
    order as the silver DDL) exposed as a temp view by the job."""
    return (
        f"MERGE INTO {entity.silver_table} t "
        f"USING updates u "
        f"ON t.{entity.business_key} = u.{entity.business_key} "
        f"WHEN MATCHED AND u._ingested_at >= t._ingested_at THEN UPDATE SET * "
        f"WHEN NOT MATCHED THEN INSERT *"
    )


def parse_date_bound(raw: str | None) -> str | None:
    """Validate a --start-date/--end-date value (YYYY-MM-DD); None passes."""
    if raw is None:
        return None
    try:
        datetime.strptime(raw, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"invalid date {raw!r} — use YYYY-MM-DD") from exc
    return raw


@dataclass(frozen=True)
class BronzeDimSpec:
    """A batch-loaded dimension: raw file -> bronze (full snapshot)."""

    entity: EntityColumns
    bronze_table: str
    source_path: str  # resolved raw file location (local dir or s3a://)

    def schema_columns(self) -> tuple[tuple[str, str], ...]:
        cols = [(n, ENTITY_TO_BRONZE[t]) for n, t in self.entity.columns]
        return tuple(cols) + (("_ingested_at", "timestamp"),)

    def create_table_sql(self) -> str:
        col_defs = ", ".join(f"{n} {t}" for n, t in self.schema_columns())
        return (
            f"CREATE TABLE IF NOT EXISTS {self.bronze_table} "
            f"({col_defs}) USING iceberg"
        )


def get_bronze_dim_spec(
    entity_name: str,
    cfg: SparkJobConfig | None = None,
    entities_path: str = DEFAULT_ENTITIES_PATH,
) -> BronzeDimSpec:
    """Resolve a batch entity. The raw source path: --source-path value /
    settings.yaml sources.local_base_path / data/sample_raw/retail_db, joined
    with the contract's relative file — works identically for a local dir and
    an s3a:// prefix (EMR step compatibility)."""
    cfg = cfg or get_job_config()
    entity = load_entity_columns(entity_name, entities_path)
    if entity.delivery != "batch":
        raise SystemExit(
            f"{entity_name!r} is a {entity.delivery} entity — dimensions load via "
            "the batch path; facts arrive through Kinesis (see config/entities.yaml)")

    settings = (
        load_yaml("config/settings.yaml") if Path("config/settings.yaml").exists() else {}
    )
    base = (
        settings.get("sources", {}).get("local_base_path")
        or "data/sample_raw/retail_db"
    )
    source = f"{base.rstrip('/')}/{entity.source_file}"
    return BronzeDimSpec(
        entity=entity, bronze_table=entity.bronze_table, source_path=source
    )


def gold_table(name: str) -> str:
    if name not in GOLD_MARTS:
        raise SystemExit(f"unknown gold mart {name!r} (have: {sorted(GOLD_MARTS)})")
    return f"lake.{GOLD_NAMESPACE}.{name}"


def gold_ddl(name: str) -> str:
    col_defs = ", ".join(f"{n} {t}" for n, t in GOLD_MARTS[name])
    return (
        f"CREATE TABLE IF NOT EXISTS {gold_table(name)} "
        f"({col_defs}) USING iceberg"
    )
