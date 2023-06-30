# AWS Glue Schema Registry module

Publishes the Avro schemas that the Kinesis producers serialize with into a Glue
Schema Registry. The root environment reads the actual `.avsc` files from
`streaming/schemas/` (single source of truth — never a copy).

## Cost
Glue Schema Registry has no per-schema cost — effectively **free** at demo scale.

## Inputs
| Name | Type | Default | Notes |
|---|---|---|---|
| `registry_name` | string | `retail-schemas` | |
| `schemas` | map(string) | — | name → Avro JSON definition |
| `compatibility` | string | `BACKWARD` | recommended for evolving streams |

## Outputs
`registry_name`, `registry_arn`, `schema_arns` (name → arn).

## Switch-over
Local runs use the file-backed shim (`streaming/schemas/registry_shim.py`).
`ENV=aws` swaps to `GlueSchemaRegistry` (boto3) with `SCHEMA_REGISTRY_NAME` =
this registry — same interface, zero caller changes.