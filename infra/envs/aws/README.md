# Real AWS deployment — `infra/envs/aws`

Deploys the platform's AWS components from the shared modules in
`infra/modules/`. **Plan-safe by default**: a bare `terraform plan` only touches
the cheap non-network core (Kinesis streams, S3 lake, Glue Schema Registry,
IAM). Each heavy service (EMR, MWAA, Redshift) is gated behind a `deploy_*`
boolean and carries explicit cost warnings.

## Cost at a glance
| Component | Approx monthly | Notes |
|---|---|---|
| Kinesis (1 shard x 2 streams) | ~$22 | keep `shard_count=1` |
| S3 (demo volumes) | ~$0 | versioning + lifecycle tame it |
| Glue Schema Registry | ~$0 | |
| Redshift Serverless (8 RPU, auto-pause) | ~$15-50 | sleeps when idle |
| EMR (ephemeral, `keep_alive=false`) | pay-per-run | ~$15/day if left up |
| **MWAA** | **~$365** | fixed, no auto-pause — biggest cost |

## Usage
```
cd infra/envs/aws
terraform init
cp terraform.tfvars.example terraform.tfvars   # edit as needed
terraform plan
terraform apply
```

## Destroy path (clean, no orphaned state)
```
terraform destroy        # tears down gated services first, then core
```
Because the S3 module defaults `force_destroy=false`, a non-empty `lake` bucket
will deliberately fail destroy — empty/archive the bucket or set
`force_destroy=true` (a conscious choice) before retiring. Same applies to EMR
(terminates the cluster) and MWAA/Redshift (removes the envs).

## Remote state
Local state by default. For a real account, migrate to an S3 backend — see the
commented block in `providers.tf` (create the state bucket + DynamoDB lock table
manually first).

## Wiring to the app
- Set `ENV=aws` in config/settings.yaml; endpoints/creds come from env vars
  (SPARK_S3A_ENDPOINT omitted → IAM provider chain; AWS_REGION; etc.)
- `SCHEMA_REGISTRY_NAME` → the `schema_registry_name` output (Glue registry)
- Warehouse JDBC → the `redshift_endpoint` output, away from the local Postgres
