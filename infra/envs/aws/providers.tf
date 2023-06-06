terraform {
  required_version = "~> 1.5.7" # 2023 pin (see PROJECT_STATE conventions)

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.15.0" # 2023 release
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5.0" # 2023 release
    }
  }

  # Default: local state. For real AWS sharing, migrate to an S3 backend:
  #   terraform {
  #     backend "s3" {
  #       bucket = "<project>-tfstate"   # create manually, versioning on
  #       key    = "infra/envs/aws/terraform.tfstate"
  #       region = "us-east-1"
  #       dynamodb_table = "terraform-locks"
  #     }
  #   }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = merge(var.tags, {
      environment = var.environment
      managed_by  = "terraform"
    })
  }
}