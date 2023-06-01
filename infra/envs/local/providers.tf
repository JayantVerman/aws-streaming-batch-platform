terraform {
  required_version = "~> 1.5.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.15.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5.0"
    }
  }
}

# LocalStack emulation via the aws provider endpoints (or `tflocal`). Dummy
# creds are expected — LocalStack ignores them. Keeps IaC parity with envs/aws
# without an AWS account. For the REAL local platform (Spark/MinIO/Airflow...)
# use the docker-compose stack — this env only exercises the IaC modules.
provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_version_check = true
  skip_requesting_account_id  = true

  endpoints {
    kinesis           = "http://localhost:4566"
    s3                = "http://localhost:4566"
    glue              = "http://localhost:4566"
    iam               = "http://localhost:4566"
    emr               = "http://localhost:4566"
    redshiftserverless = "http://localhost:4566"
    mwaa              = "http://localhost:4566"
  }
}