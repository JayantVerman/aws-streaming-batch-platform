# Amazon EMR cluster (on EC2).

# COST WARNING — a running EMR cluster bills per instance-hour PLUS the EMR
# markup (~25%) PLUS EBS. An m5.xlarge core node ≈ $0.25-0.30/hr combined;
# with 1 master + 1 core that's ~$15/day if left running. Defaults here are
# deliberately ephemeral: keep_alive=false terminates the cluster when its
# step finishes, so you only pay while a job is running.

# VERSION NOTE — 2023-era pin. Local stack: Spark 3.5.0 (bitnami). Real AWS
# EMR that existed in 2023 ships Spark 3.3.x (e.g. release emr-6.15.0 has
# Spark 3.3.2). All our batch jobs are written to run on both. Some EMR 7.x
# releases ship Spark 3.5 but postdate the 2023 window — a variable change
# here is the only upgrade needed when you choose to.

variable "project" {
  description = "Name prefix"
  type        = string
}

variable "release_label" {
  description = "EMR release (2023-era pin; see module README)"
  type        = string
  default     = "emr-6.15.0"
}

variable "subnet_id" {
  description = "Subnet to launch the cluster into (private for prod)"
  type        = string
}

variable "instance_profile" {
  description = "EMR EC2 instance profile name (see iam module)"
  type        = string
}

variable "service_role_arn" {
  description = "EMR service role ARN (see iam module)"
  type        = string
}

variable "master_instance_type" {
  description = "Master node EC2 type"
  type        = string
  default     = "m5.xlarge"
}

variable "core_instance_type" {
  description = "Core node EC2 type"
  type        = string
  default     = "m5.xlarge"
}

variable "core_instance_count" {
  description = "Number of core nodes (spot-enabled for cost)"
  type        = number
  default     = 1
}

variable "enable_spot" {
  description = "Use spot core nodes (cheaper; interruptions possible)"
  type        = bool
  default     = true
}

variable "keep_alive" {
  description = "Keep the cluster running after steps finish (COSTLY — prefer false)"
  type        = bool
  default     = false
}

variable "ebs_size_gb" {
  description = "EBS volume size (GB) per node"
  type        = number
  default     = 32
}

variable "tags" {
  description = "Tags"
  type        = map(string)
  default     = {}
}