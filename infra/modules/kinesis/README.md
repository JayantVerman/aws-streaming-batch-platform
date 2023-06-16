# Kinesis Data Streams module

Creates Kinesis streams — **one per streaming entity** (the Spark bronze consumer
binds one Avro schema per stream, so streams can't mix entities). The demo
platform uses `retail-orders` and `retail-order_items`.

## Cost warning
$0.015 / shard-hour ≈ **$10.8 / shard / month**. Keep `shard_count = 1`
(demo-sized by design). Avoid Enhanced Fan-Out (secondary consumers) and extra
retention — both add to the bill for no benefit here.

## Inputs
| Name | Type | Default | Notes |
|---|---|---|---|
| `streams` | list(string) | — | stream names, e.g. `["retail-orders", "retail-order_items"]` |
| `shard_count` | number | 1 | cost warning above |
| `retention_hours` | number | 72 | 24-8760 |
| `enable_enhanced_monitoring` | bool | false | per-shard metrics cost extra |
| `tags` | map(string) | {} | |

## Outputs
`stream_names`, `stream_arns` (name → arn).