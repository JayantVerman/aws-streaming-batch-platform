# Kinesis data streams.

# COST WARNING — shard-hours are the variable cost here:
#   $0.015 / shard-hour ~ $10.8 / shard / month at 1 shard.
# Keep 1 shard for the demo (this platform only needs 1 shard anyway),
# and avoid Enhanced Fan-Out (Secondary consumers) unless really needed.
# Extended retention costs extra per shard-hour as well.

variable "streams" {
  description = "Kinesis stream names to create (ONE per streaming entity — the Spark consumer binds one Avro schema per stream)"
  type        = list(string)
}

variable "shard_count" {
  description = "Shards per stream"
  type        = number
  default     = 1
}

variable "retention_hours" {
  description = "Data retention (24-8760 hours). 72 is cheap and plenty for daily bronze->silver."
  type        = number
  default     = 72
}

variable "enable_enhanced_monitoring" {
  description = "Per-shard enhanced monitoring (spot: leaves default enhanced off to save cost)"
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags applied to every stream"
  type        = map(string)
  default     = {}
}