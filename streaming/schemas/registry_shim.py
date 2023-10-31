# Local stand-in for the AWS Glue Schema Registry.
#
# LocalStack Community can't emulate the registry, so local runs use this
# file-backed shim. It exposes the same surface the real path needs:
# register / get / validate. ENV=aws swaps in the boto3 Glue implementation
# via get_registry() — caller code doesn't change.

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import boto3
import fastavro
from fastavro.validation import ValidationError as AvroValidationError

logger = logging.getLogger(__name__)


class SchemaValidationError(ValueError):
    """Record does not conform to the registered schema."""


class BaseSchemaRegistry:
    """Interface shared by the local shim and the Glue-backed registry."""

    def register_schema(self, schema_name: str, schema_definition: str) -> int:
        raise NotImplementedError

    def get_schema(self, schema_name: str, version: int | None = None) -> dict[str, Any]:
        raise NotImplementedError

    def validate_record(self, record: dict[str, Any], schema_name: str) -> None:
        """Raise SchemaValidationError when the record doesn't conform."""
        raise NotImplementedError


class LocalSchemaRegistry(BaseSchemaRegistry):
    """File-backed registry: one .avsc file per schema in schemas_dir."""

    def __init__(self, schemas_dir: str | Path):
        self._dir = Path(schemas_dir)

    def register_schema(self, schema_name: str, schema_definition: str) -> int:
        # "Registering" locally is just writing the file; version is always 1.
        path = self._dir / f"{schema_name}.avsc"
        parsed = json.loads(schema_definition)
        self._dir.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(parsed, indent=2) + "\n", encoding="utf-8")
        logger.info("registered schema %s -> %s", schema_name, path)
        return 1

    def get_schema(self, schema_name: str, version: int | None = None) -> dict[str, Any]:
        if version is not None and version != 1:
            raise KeyError(f"{schema_name}: local registry only has version 1")
        path = self._dir / f"{schema_name}.avsc"
        if not path.exists():
            raise KeyError(f"schema {schema_name!r} not found in {self._dir}")
        return json.loads(path.read_text(encoding="utf-8"))

    def validate_record(self, record: dict[str, Any], schema_name: str) -> None:
        schema = fastavro.parse_schema(self.get_schema(schema_name))
        # fastavro (recent versions) ignores unknown fields even with strict=True,
        # so extra-field rejection is enforced here explicitly.
        extra = set(record) - {f["name"] for f in schema["fields"]}
        if extra:
            raise SchemaValidationError(
                f"{schema_name}: unexpected fields {sorted(extra)}")
        try:
            fastavro.validation.validate(record, schema, raise_errors=True)
        except (AvroValidationError, ValueError, TypeError) as exc:
            raise SchemaValidationError(f"{schema_name}: {exc}") from exc


class GlueSchemaRegistry(BaseSchemaRegistry):
    """Real AWS path backed by Glue Schema Registry. Same interface as the shim."""

    def __init__(self, registry_name: str, region_name: str = "us-east-1"):
        self._registry = registry_name
        self._client = boto3.client("glue", region_name=region_name)

    def register_schema(self, schema_name: str, schema_definition: str) -> int:
        resp = self._client.register_schema_version(
            SchemaId={"RegistryName": self._registry, "SchemaName": schema_name},
            SchemaDefinition=schema_definition,
        )
        return int(resp["SchemaVersionNumber"])

    def get_schema(self, schema_name: str, version: int | None = None) -> dict[str, Any]:
        schema_id = {"RegistryName": self._registry, "SchemaName": schema_name}
        if version is None:
            resp = self._client.get_schema_version(
                SchemaId=schema_id, SchemaVersionNumber={"LatestVersion": True})
        else:
            resp = self._client.get_schema_version(
                SchemaId=schema_id, SchemaVersionNumber={"VersionNumber": version})
        return json.loads(resp["SchemaDefinition"])

    def validate_record(self, record: dict[str, Any], schema_name: str) -> None:
        schema = fastavro.parse_schema(self.get_schema(schema_name))
        extra = set(record) - {f["name"] for f in schema["fields"]}
        if extra:
            raise SchemaValidationError(
                f"{schema_name}: unexpected fields {sorted(extra)}")
        try:
            fastavro.validation.validate(record, schema, raise_errors=True)
        except (AvroValidationError, ValueError, TypeError) as exc:
            raise SchemaValidationError(f"{schema_name}: {exc}") from exc


def get_registry(
    schemas_dir: str | Path | None = None,
    env: str | None = None,
) -> BaseSchemaRegistry:
    """ENV=local (default) -> LocalSchemaRegistry; ENV=aws -> GlueSchemaRegistry."""
    env = env or os.getenv("ENV", "local")
    if env == "aws":
        return GlueSchemaRegistry(
            registry_name=os.getenv("SCHEMA_REGISTRY_NAME", "retail-schemas"),
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )
    return LocalSchemaRegistry(schemas_dir or "streaming/schemas")

