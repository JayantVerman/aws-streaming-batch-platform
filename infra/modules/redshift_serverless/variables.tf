# Redshift Serverless module (warehouse).

# COST WARNING — Redshift Serverless bills per RPU-hour while it runs. Defaults
# here are cost-friendly: base_capacity 8 RPU (the floor) with auto-pause after
# 60s of idle, so it sleeps between loads. Budget ~$15-50/mo depending on usage;
# far cheaper than a provisioned cluster for this workload.

variable "project" {
  description = "Name prefix"
  type        = string
}

variable "namespace_name" {
  description = "Namespace name"
  type        = string
  default     = "retail-serverless"
}

variable "workgroup_name" {
  description = "Workgroup name"
  type        = string
  default     = "retail-workgroup"
}

variable "base_capacity" {
  description = "RPU capacity (8 = minimum)"
  type        = number
  default     = 8
}

variable "auto_pause_seconds" {
  description = "Auto-pause after N seconds idle (cost control)"
  type        = number
  default     = 60
}

variable "publicly_accessible" {
  description = "Expose a public workgroup endpoint (false recommended; add VPC endpoint access otherwise)"
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags"
  type        = map(string)
  default     = {}
}