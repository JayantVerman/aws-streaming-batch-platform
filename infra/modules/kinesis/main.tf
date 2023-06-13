resource "aws_kinesis_stream" "stream" {
  for_each         = toset(var.streams)
  name             = each.value
  shard_count      = var.shard_count
  retention_period = var.retention_hours

  stream_mode_details {
    stream_mode = "PROVISIONED" # ON_DEMAND exists, but 1 provisioned shard is the demo answer
  }

  # Basic 1-MB/s metrics are free. Enhanced per-shard metrics cost extra —
  # off by default (var.enable_enhanced_monitoring).
  shard_level_metrics = var.enable_enhanced_monitoring ? ["ALL"] : null

  tags = merge(var.tags, { Name = each.value })
}