#!/usr/bin/env bash
# LocalStack init hook (runs inside the localstack container at first boot):
# pre-create the Kinesis stream and lake buckets so the smoke test is meaningful.
# The stream is intentionally 1 shard — that's all the demo needs and it keeps
# real-AWS shard-hour costs near zero.
set -e

awslocal s3 mb s3://lake 2>/dev/null || true
awslocal s3 mb s3://checkpoints 2>/dev/null || true

# One stream per streaming entity (orders, order_items) — the Spark bronze
# consumer binds one Avro schema per stream. The producer also auto-creates
# missing streams on demand, so these are a convenience for the smoke test.
for i in 1 2 3 4 5; do
  if awslocal kinesis create-stream --stream-name retail-orders --shard-count 1 2>/dev/null; then
    break
  fi
  sleep 2
done

for i in 1 2 3 4 5; do
  if awslocal kinesis create-stream --stream-name retail-order_items --shard-count 1 2>/dev/null; then
    break
  fi
  sleep 2
done

echo "[localstack-init] buckets 'lake'/'checkpoints' and streams 'retail-orders'/'retail-order_items' ready"
