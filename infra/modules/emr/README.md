# Amazon EMR (on EC2) module

Ephemeral-by-default cluster for the batch jobs. Runs a step then terminates
(`keep_alive = false`), so you only pay while a job runs.

## COST WARNING
Instance-hours + EMR markup (~25%) + EBS. An m5.xlarge master + 1 core left
running is ~$15/day. Prefer `keep_alive=false` and spot core nodes.

## VERSION NOTE (2023 pin)
Local stack runs Spark 3.5.0 (bitnami). EMR releases that existed in 2023 ship
Spark 3.3.x — default `emr-6.15.0` (Spark 3.3.2). All batch jobs are written to
run on both. (Some EMR 7.x ship Spark 3.5 but postdate the 2023 window; changing
`release_label` is the only upgrade cost.)

## Inputs
| Name | Type | Default |
|---|---|---|
| `project` | string | — |
| `release_label` | string | `emr-6.15.0` |
| `subnet_id` | string | — |
| `instance_profile` | string | — (from iam) |
| `service_role_arn` | string | — (from iam) |
| `master_instance_type` / `core_instance_type` | string | `m5.xlarge` |
| `core_instance_count` | number | 1 |
| `enable_spot` | bool | true |
| `keep_alive` | bool | false (cost) |
| `ebs_size_gb` | number | 32 |
| `tags` | map(string) | {} |

## Outputs
`cluster_id`, `cluster_name`.