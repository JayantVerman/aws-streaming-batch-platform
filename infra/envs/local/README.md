# Local IaC parity (`infra/envs/local`)

Exercises the same Terraform modules against **LocalStack** (no AWS account), so
the IaC is validated locally before a real `apply`. Emulation is limited: the
gated heavy services (EMR, MWAA, Redshift Serverless) and some Glue/IAM bits are
not faithfully emulated — this env keeps the **core** modules on for parity.

For the actual local data platform (Spark, MinIO, Airflow, Trino, Postgres...)
use the **docker-compose stack** — this folder is only the IaC parity check.

## Usage
```
# start LocalStack (docker compose up -d localstack)
terraform -chdir=infra/envs/local init
terraform -chdir=infra/envs/local plan
terraform -chdir=infra/envs/local apply
```