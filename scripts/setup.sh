#!/usr/bin/env bash
# One-command bring-up of the local stack (LocalStack, MinIO, Postgres, Spark,
# Airflow, Trino, Prometheus, Grafana) followed by a smoke test.
#
# Windows note: run from Git Bash or WSL. Requires Docker Desktop running.
set -euo pipefail
cd "$(dirname "$0")/.."

command -v docker >/dev/null 2>&1 || { echo "ERROR: docker not found — start Docker Desktop first"; exit 1; }
docker info >/dev/null 2>&1 || { echo "ERROR: docker daemon not reachable"; exit 1; }

echo "[setup] starting containers..."
docker compose up -d

echo "[setup] waiting for postgres..."
until docker compose exec -T postgres pg_isready -U platform -d warehouse >/dev/null 2>&1; do sleep 2; done

echo "[setup] waiting for localstack..."
until curl -sf http://localhost:4566/_localstack/health >/dev/null 2>&1; do sleep 2; done

echo "[setup] running one-shot init jobs (minio buckets, airflow db/user)..."
docker compose up --exit-code-from minio-init minio-init
docker compose up --exit-code-from airflow-init airflow-init

echo "[setup] starting long-running services..."
docker compose up -d

echo
echo "Stack is up:"
echo "  LocalStack   http://localhost:4566"
echo "  MinIO console http://localhost:9001  (minioadmin/minioadmin)"
echo "  Spark UI     http://localhost:8080"
echo "  Airflow      http://localhost:8081  (admin/admin)"
echo "  Trino        http://localhost:8089"
echo "  Prometheus   http://localhost:9090"
echo "  Grafana      http://localhost:3000  (admin/admin)"
echo

./scripts/smoke_test.sh
