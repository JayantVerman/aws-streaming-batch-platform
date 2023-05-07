"""Config loading for the Spark streaming (bronze) jobs.

Pure Python on purpose: no pyspark import here, so everything in this module
is unit-testable without a JVM. Only transforms.py and bronze_stream.py touch
Spark.

Sources of truth:
  - config/entities.yaml  -> entity contract (columns, business key, delivery)
  - config/settings.yaml  -> endpoints, catalog, jars, trigger cadence
  - env overrides         -> SPARK_S3A_ENDPOINT, SPARK_KINESIS_ENDPOINT,
                             SPARK_MASTER_URL, AWS_ACCESS_KEY_ID / SECRET
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_SETTINGS_PATH = "config/settings.yaml"
DEFAULT_ENTITIES_PATH = "config/entities.yaml"
DEFAULT_SCHEMAS_DIR = "streaming/schemas"

# Avro primitive type -> Spark SQL type (bronze keeps source-faithful types;
# silver does any casting/refinement).
AVRO_TO_SPARK = {
    "long": "bigint",
    "int": "int",
    "double": "double",
    "float": "float",
    "string": "string",
    "boolean": "boolean",
    "bytes": "binary",
}

# Kinesis lineage columns appended to every bronze row (plus _ingested_at for
# lake partitioning). Bronze is append-only raw, but real platforms still
# stamp provenance on every record.
METADATA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("_kinesis_partition_key", "string"),
    ("_kinesis_sequence_number", "string"),
    ("_kinesis_arrival_timestamp", "timestamp"),
    ("_ingested_at", "timestamp"),
)

ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
ICEBERG_CATALOG_CLASS = "org.apache.iceberg.spark.SparkCatalog"


def load_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


@dataclass(frozen=True)
class EntitySpec:
    """One streaming entity, fully resolved for a bronze writer."""

    name: str
    business_key: str
    incremental_column: str | None
    stream_name: str
    avro_schema_json: str
    fields: tuple[tuple[str, str], ...]  # (name, avro_type) in contract order
    bronze_table: str
    dead_letter_path: str

    @property
    def field_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.fields)

    def bronze_schema_columns(self) -> list[tuple[str, str]]:
        cols = [(name, AVRO_TO_SPARK[avro]) for name, avro in self.fields]
        return cols + list(METADATA_COLUMNS)

    def create_table_sql(self) -> str:
        col_defs = ", ".join(
            f"{name} {spark_type}" for name, spark_type in self.bronze_schema_columns()
        )
        return (
            f"CREATE TABLE IF NOT EXISTS {self.bronze_table} "
            f"({col_defs}) USING iceberg "
            f"PARTITIONED BY (days(_ingested_at))"
        )


@dataclass(frozen=True)
class SparkJobConfig:
    """Everything the bronze job needs besides the entity contract."""

    env: str
    kinesis_endpoint: str | None
    s3_endpoint: str | None
    region: str
    catalog_name: str
    namespace: str
    warehouse: str
    checkpoints_bucket: str
    trigger_interval: str
    starting_position: str
    packages: tuple[str, ...]
    kinesis_connector_jar: str
    cluster_master: str
    local_master: str
    s3_access_key: str | None
    s3_secret_key: str | None

    def qualified(self, name: str) -> str:
        return f"{self.catalog_name}.{self.namespace}.{name}"

    def checkpoint_path(self, entity: str) -> str:
        return f"s3a://{self.checkpoints_bucket}/streaming/{entity}"

    def hadoop_confs(self) -> dict[str, str]:
        """spark.hadoop.* settings. Local: MinIO via explicit keys. AWS: IAM
        provider chain + default S3 endpoint, nothing overridden."""
        confs: dict[str, str] = {}
        if self.env != "local":
            return confs
        if self.s3_endpoint:
            confs["spark.hadoop.fs.s3a.endpoint"] = self.s3_endpoint
            confs["spark.hadoop.fs.s3a.path.style.access"] = "true"
            confs["spark.hadoop.fs.s3a.connection.ssl.enabled"] = (
                "false" if self.s3_endpoint.startswith("http://") else "true"
            )
        confs["spark.hadoop.fs.s3a.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"
        if self.s3_access_key and self.s3_secret_key:
            confs["spark.hadoop.fs.s3a.access.key"] = self.s3_access_key
            confs["spark.hadoop.fs.s3a.secret.key"] = self.s3_secret_key
            confs["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        return confs

    def catalog_confs(self) -> dict[str, str]:
        return {
            "spark.sql.extensions": ICEBERG_EXTENSIONS,
            f"spark.sql.catalog.{self.catalog_name}": ICEBERG_CATALOG_CLASS,
            f"spark.sql.catalog.{self.catalog_name}.type": "hadoop",
            f"spark.sql.catalog.{self.catalog_name}.warehouse": self.warehouse,
        }


def get_job_config(settings_path: str | Path = DEFAULT_SETTINGS_PATH) -> SparkJobConfig:
    settings = load_yaml(settings_path)
    spark_cfg = settings.get("spark", {})
    env = settings.get("env", "local")

    kinesis_cfg = settings.get("streaming", {}).get("kinesis", {})
    return SparkJobConfig(
        env=env,
        # env > yaml: inside docker the endpoints differ from host-side runs
        kinesis_endpoint=os.getenv("SPARK_KINESIS_ENDPOINT")
        or (kinesis_cfg.get("local_endpoint") if env == "local" else None),
        s3_endpoint=os.getenv("SPARK_S3A_ENDPOINT") or spark_cfg.get("s3_endpoint"),
        region=spark_cfg.get("aws_region", "us-east-1"),
        catalog_name=spark_cfg.get("catalog_name", "lake"),
        namespace=spark_cfg.get("namespace", "retail"),
        warehouse=spark_cfg.get("warehouse", "s3a://lake/warehouse"),
        checkpoints_bucket=spark_cfg.get("checkpoints_bucket", "checkpoints"),
        trigger_interval=spark_cfg.get("trigger_interval", "10 seconds"),
        starting_position=spark_cfg.get("starting_position", "TRIM_HORIZON"),
        packages=tuple(spark_cfg.get("packages", [])),
        kinesis_connector_jar=spark_cfg.get("kinesis_connector_jar", ""),
        cluster_master=spark_cfg.get("cluster_master", "spark://spark-master:7077"),
        local_master=spark_cfg.get("local_master", "local[2]"),
        # MinIO creds come from the environment (compose sets them); on AWS the
        # IAM provider chain is used and these stay unset.
        s3_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        s3_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def read_avsc(
    schemas_dir: str | Path, schema_name: str
) -> tuple[str, tuple[tuple[str, str], ...]]:
    """Return (schema JSON text, ordered (field, type) pairs) from a .avsc file."""
    path = Path(schemas_dir) / f"{schema_name}.avsc"
    raw = path.read_text(encoding="utf-8")
    schema = json.loads(raw)
    fields = []
    for field in schema.get("fields", []):
        ftype = field["type"]
        if isinstance(ftype, (list, dict)):
            raise ValueError(
                f"{schema_name}.{field['name']}: bronze requires flat non-nullable "
                "Avro fields (unions/records break the decode-null validation rule)"
            )
        fields.append((field["name"], ftype))
    if not fields:
        raise ValueError(f"{schema_name}: no fields in {path}")
    return raw, tuple(fields)


def get_entity_spec(
    entity_name: str,
    cfg: SparkJobConfig | None = None,
    entities_path: str | Path = DEFAULT_ENTITIES_PATH,
    schemas_dir: str | Path = DEFAULT_SCHEMAS_DIR,
) -> EntitySpec:
    cfg = cfg or get_job_config()
    entity = load_yaml(entities_path).get("entities", {}).get(entity_name)
    if entity is None:
        raise SystemExit(f"unknown entity {entity_name!r} (see config/entities.yaml)")
    if entity.get("delivery") != "streaming":
        raise SystemExit(
            f"{entity_name!r} is a batch entity — the streaming bronze path only "
            "takes entities marked delivery: streaming")

    settings = (
        load_yaml(DEFAULT_SETTINGS_PATH) if Path(DEFAULT_SETTINGS_PATH).exists() else {}
    )
    kinesis_cfg = settings.get("streaming", {}).get("kinesis", {})
    stream_name = kinesis_cfg.get("stream_name_template", "retail-{entity}").format(
        entity=entity_name)

    avro_schema_name = entity.get("avro_schema")
    if not avro_schema_name:
        raise SystemExit(f"{entity_name}: no avro_schema in config/entities.yaml")
    schema_json, fields = read_avsc(schemas_dir, avro_schema_name)

    business_key = entity["business_key"]
    if business_key not in dict(fields):
        raise SystemExit(
            f"{entity_name}: business_key {business_key!r} not in Avro schema fields")

    return EntitySpec(
        name=entity_name,
        business_key=business_key,
        incremental_column=entity.get("incremental_column"),
        stream_name=stream_name,
        avro_schema_json=schema_json,
        fields=fields,
        bronze_table=cfg.qualified(entity_name),
        dead_letter_path=f"s3a://lake/quarantine/streaming/{entity_name}",
    )
