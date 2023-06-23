# IAM roles/policies for the data platform.

variable "project" {
  description = "Name prefix for IAM resources, e.g. 'retail'"
  type        = string
}

variable "s3_bucket_arns" {
  description = "ARNs of buckets EMR/streams need to read/write (lake, checkpoints)"
  type        = list(string)
  default     = []
}

variable "dags_bucket_arn" {
  description = "ARN of the bucket holding the Airflow DAGs bundle (MWAA can read/upload)"
  type        = string
  default     = ""
}

variable "glue_registry_arn" {
  description = "Glue Schema Registry ARN (read/register for producers/consumers)"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Tags applied to IAM resources"
  type        = map(string)
  default     = {}
}