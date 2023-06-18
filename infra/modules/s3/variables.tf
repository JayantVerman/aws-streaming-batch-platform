# S3 data lake buckets.

# COST / DESTROY WARNING: a non-empty bucket can't be destroyed without
# force_destroy=true — a single deliberate variable, never a default. The
# demo's data volumes are effectively $0/mo, but the bucket is your lake:
# treat it as state, not scratch space.

variable "bucket_names" {
  description = "Bucket names to create (lake, checkpoints, ...)"
  type        = list(string)
}

variable "enable_versioning" {
  description = "Object versioning (recommended for the lake/raw writes in front of Iceberg)"
  type        = bool
  default     = true
}

variable "force_destroy" {
  description = "Permit deleting a bucket that still contains objects. Keep FALSE in shared accounts!"
  type        = bool
  default     = false
}

variable "lifecycle_expire_noncurrent_days" {
  description = "Expire noncurrent versions after N days (0 = keep forever). Cheap way to control lake sprawl."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Tags applied to every bucket"
  type        = map(string)
  default     = {}
}