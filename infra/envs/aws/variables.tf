variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Resource name prefix (also a bucket prefix for uniqueness)"
  type        = string
  default     = "retail"
}

variable "environment" {
  description = "Environment tag (dev/staging/prod)"
  type        = string
  default     = "dev"
}

variable "tags" {
  description = "Extra tags"
  type        = map(string)
  default     = {}
}

# Deploy gates — keep `terraform plan` safe out of the box: only the cheap,
# non-network core (Kinesis streams, S3 lake, schema registry, IAM) plans by
# default. Flip a gate and provide its VPC inputs to provision the heavy
# services (all carry cost warnings in their module READMEs).
variable "deploy_emr" {
  description = "Provision the EMR cluster (on EC2 — costly)"
  type        = bool
  default     = false
}

variable "emr_subnet_id" {
  description = "Subnet for the EMR cluster (required when deploy_emr=true)"
  type        = string
  default     = ""
}

variable "deploy_mwaa" {
  description = "Provision MWAA (~$365/mo fixed — see module README)"
  type        = bool
  default     = false
}

variable "mwaa_subnet_ids" {
  description = "MWAA subnets (>=2 private; required when deploy_mwaa=true)"
  type        = list(string)
  default     = []
}

variable "mwaa_security_group_ids" {
  description = "MWAA security groups (required when deploy_mwaa=true)"
  type        = list(string)
  default     = []
}

variable "deploy_redshift" {
  description = "Provision Redshift Serverless (auto-pausing; modest cost)"
  type        = bool
  default     = false
}