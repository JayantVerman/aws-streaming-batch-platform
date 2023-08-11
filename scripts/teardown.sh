#!/usr/bin/env bash
# Teardown the whole local stack including volumes (lake data, checkpoints,
# LocalStream state, airflow db). Complete clean slate.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[teardown] stopping and removing containers + volumes..."
docker compose down -v --remove-orphans
echo "[teardown] done — local data (lake buckets, checkpoints, state) is gone"
