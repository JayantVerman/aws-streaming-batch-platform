# Managed Workflows for Apache Airflow (MWAA).

# COST WARNING — MWAA bills ~$0.50/hr per environment, ~$365/mo, whether or not
# DAGs run. There is no auto-pause. This is the platform's biggest fixed cost;
# treat it as a deliberate line item (use the local Docker Airflow for daily
# dev; stand MWAA up only for the "real stack" demo).

variable "project" {
  description = "Name prefix"
  type        = string
}

variable "execution_role_arn" {
  description = "MWAA execution role ARN (see iam module)"
  type        = string
}

variable "source_bucket_arn" {
  description = "ARN of the bucket hosting the DAGs bundle (+ optional requirements)"
  type        = string
}

variable "dag_s3_path" {
  description = "S3 key prefix of the DAGs within the source bucket"
  type        = string
  default     = "dags/"
}

variable "requirements_s3_key" {
  description = "S3 key of the requirements.txt (empty = none)"
  type        = string
  default     = ""
}

variable "airflow_version" {
  description = "Airflow version (2023 pin, matches local 2.7.3)"
  type        = string
  default     = "2.7.3"
}

variable "environment_class" {
  description = "mw1.small/medium/large"
  type        = string
  default     = "mw1.small"
}

variable "subnet_ids" {
  description = "Subnets (MWAA needs at least 2 private subnets)"
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security groups for the MWAA workers/webserver"
  type        = list(string)
}

variable "webserver_access_mode" {
  description = "PUBLIC_ONLY or PRIVATE_ONLY"
  type        = string
  default     = "PUBLIC_ONLY"
}

variable "schedulers" {
  description = "Number of schedulers (min 2 for Airflow 2.7)"
  type        = number
  default     = 2
}

variable "min_workers" {
  type = number
  default = 1
}

variable "max_workers" {
  description = "Cap worker count — more workers = more EC2 cost while running"
  type        = number
  default     = 1
}

variable "tags" {
  description = "Tags"
  type        = map(string)
  default     = {}
}