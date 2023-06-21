# S3 data lake module

Creates the object-store buckets for the medallion lake. The platform standard
is `lake` + `checkpoints` (both created locally by MinIO init); real AWS can add
a `raw` landing bucket here too.

## COST / DESTROY WARNING
Demo volumes are effectively **$0/mo**. But a non-empty bucket cannot be destroyed
without `force_destroy = true` — a deliberate, never-default setting. Treat the
`lake` bucket as state, not scratch space. A lifecycle rule expires noncurrent
versions (default 30d) to keep versioning from accumulating cost.

## Inputs
| Name | Type | Default | Notes |
|---|---|---|---|
| `bucket_names` | list(string) | — | e.g. `["lake", "checkpoints"]` |
| `enable_versioning` | bool | true | recommended for the lake |
| `force_destroy` | bool | false | keep false in shared accounts |
| `lifecycle_expire_noncurrent_days` | number | 30 | 0 disables the rule |
| `tags` | map(string) | {} | |

## Outputs
`bucket_ids`, `bucket_arns` (name → arn).