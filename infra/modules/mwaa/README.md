# MWAA module (Managed Workflows for Apache Airflow)

Hosts the two DAGs (`retail_dimensions`, `retail_facts_pipeline`) which are
written MWAA-compatible (standard providers only). DAGs must be bundled into
the `source_bucket` at `dag_s3_path`.

## COST WARNING — the platform's biggest fixed cost
MWAA bills **~$0.50/hr ≈ $365/mo per environment**, whether or not DAGs run —
no auto-pause. Use the local Docker Airflow for daily dev; stand MWAA up only
for a real demo. Keep `max_workers=1` (more workers = more EC2 cost while a
DAG is running) and `schedulers=2` (Airflow 2.7 minimum).

## Inputs
| Name | Type | Default |
|---|---|---|
| `project` | string | — |
| `execution_role_arn` | string | — (from iam) |
| `source_bucket_arn` | string | — (from s3) |
| `dag_s3_path` | string | `dags/` |
| `requirements_s3_key` | string | `""` (none) |
| `airflow_version` | string | `2.7.3` |
| `environment_class` | string | `mw1.small` |
| `subnet_ids` | list(string) | — (>=2 private subnets) |
| `security_group_ids` | list(string) | — |
| `webserver_access_mode` | string | `PUBLIC_ONLY` |
| `schedulers` / `min_workers` / `max_workers` | number | 2 / 1 / 1 |
| `tags` | map(string) | {} |

## Outputs
`environment_id`, `environment_arn`, `webserver_url`, `airflow_version`.