#!/usr/bin/env bash
# Submit a bronze streaming job. All jar/endpoint configuration lives in
# config/settings.yaml and is applied inside the job's SparkSession builder —
# the only decision here is the master URL.
#
#   SPARK_MASTER_URL=spark://spark-master:7077 ./scripts/run_bronze_stream.sh --entity orders   # in the stack
#   SPARK_MASTER_URL=local[2] ./scripts/run_bronze_stream.sh --entity orders                    # host-side
#
# Optional:
#   SPARK_IVY_DIR  — where spark-submit caches resolved packages (default ~/.ivy2)
set -euo pipefail
cd "$(dirname "$0")/.."
# spark-submit does NOT put the repo root on sys.path — without this, the
# `streaming.*` imports inside the job fail to resolve.
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}$PWD"

MASTER="${SPARK_MASTER_URL:-local[2]}"
IVY_DIR="${SPARK_IVY_DIR:-$HOME/.ivy2}"

exec spark-submit \
  --master "$MASTER" \
  --conf "spark.jars.ivy=$IVY_DIR" \
  streaming/spark_streaming_jobs/bronze_stream.py "$@"
