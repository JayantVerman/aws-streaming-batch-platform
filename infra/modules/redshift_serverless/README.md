# Redshift Serverless module (warehouse)

Namespace + workgroup for the serving warehouse. Same SQL/DDL as the local
Postgres stand-in — only the connection parameters differ (see THE
`warehouse/ddl` + `warehouse/load_jobs`).

## COST WARNING
Serverless bills per RPU-hour while running. Defaults are cost-friendly:
`base_capacity = 8` (the floor) with **auto-pause after 60s idle**, so it sleeps
between loads. Budget ~$15-50/mo at demo volumes.

## Inputs
| Name | Type | Default |
|---|---|---|
| `project` | string | — |
| `namespace_name` | string | `retail-serverless` |
| `workgroup_name` | string | `retail-workgroup` |
| `base_capacity` | number | 8 |
| `auto_pause_seconds` | number | 60 |
| `publicly_accessible` | bool | false |
| `tags` | map(string) | {} |

`publicly_accessible=false` keeps it private; for real usage add an
`aws_redshiftserverless_endpoint_access` in your VPC (out of scope here).

## Outputs
`workgroup_name`, `workgroup_arn`, `namespace_arn`, `endpoint_address`,
`endpoint_port`.