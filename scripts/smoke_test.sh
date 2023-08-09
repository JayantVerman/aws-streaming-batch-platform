#!/usr/bin/env bash
# Smoke test: verifies every local service actually answers, not just that
# containers exist. Run after scripts/setup.sh (or standalone anytime).
set -uo pipefail
cd "$(dirname "$0")/.."

failures=0
pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; failures=$((failures + 1)); }

echo "[smoke] localstack kinesis..."
resp=$(curl -s http://localhost:4566/ \
  -H 'X-Amz-Target: Kinesis_20131202.ListStreams' \
  -H 'Content-Type: application/x-amz-json-1.1' -d '{}')
echo "$resp" | grep -q 'retail-orders' && pass "kinesis stream retail-orders exists" || fail "kinesis: $resp"

echo "[smoke] localstack s3..."
resp=$(curl -s http://localhost:4566/ -H 'X-Amz-Target: AmazonS3.ListBuckets' \
  -H 'Content-Type: application/x-amz-json-1.1' -d '{}')
echo "$resp" | grep -q 'lake' && pass "s3 bucket 'lake' exists" || fail "s3: $resp"

echo "[smoke] minio..."
curl -sf http://localhost:9000/minio/health/live >/dev/null && pass "minio healthy" || fail "minio health endpoint"

echo "[smoke] postgres..."
docker compose exec -T postgres psql -U platform -d warehouse -c 'SELECT 1;' >/dev/null 2>&1 \
  && pass "postgres query ok" || fail "postgres query"

echo "[smoke] airflow..."
curl -sf http://localhost:8081/health >/dev/null && pass "airflow webserver healthy" || fail "airflow health"

echo "[smoke] trino..."
docker compose exec -T trino trino --execute 'SELECT 1' >/dev/null 2>&1 \
  && pass "trino query ok" || fail "trino query"

echo "[smoke] prometheus..."
curl -sf http://localhost:9090/-/ready >/dev/null && pass "prometheus ready" || fail "prometheus ready"

echo "[smoke] grafana..."
curl -sf http://localhost:3000/api/health >/dev/null && pass "grafana healthy" || fail "grafana health"

echo
if [ "$failures" -gt 0 ]; then
  echo "[smoke] $failures check(s) failed"
  exit 1
fi
echo "[smoke] all checks passed"
