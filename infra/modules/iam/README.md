# IAM module

Creates the roles/policies the AWS services assume:
- **EMR EC2 instance profile** (`-emr-ec2`) — read/write lake + checkpoints S3,
  CloudWatch/logs, Glue schema registry read. Attached to every node.
- **EMR service role** (`-emr-service`) — assumed by Elastic MapReduce.
- **MWAA execution role** (`-mwaa`) — read/put DAGs bundle in S3 + CloudWatch logs.

## Inputs
| Name | Type | Default |
|---|---|---|
| `project` | string | — (name prefix) |
| `s3_bucket_arns` | list(string) | [] |
| `dags_bucket_arn` | string | "" |
| `glue_registry_arn` | string | "" |
| `tags` | map(string) | {} |

## Outputs
`emr_ec2_instance_profile`, `emr_service_role_arn`, `mwaa_execution_role_arn`.

Wire the outputs into the `emr` and `mwaa` modules.