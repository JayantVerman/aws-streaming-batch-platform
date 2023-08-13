#!/usr/bin/env bash
# Submit a batch job (load_dimensions / bronze_to_silver / silver_to_gold).
# Same layout as run_bronze_stream.sh: config lives in config/settings.yaml,
# the only decision here is the master URL.
#
#   SPARK_MASTER_URL=spark://spark-master:7077 ./scripts/run_batch_job.sh \
#       batch/spark_batch_jobs/bronze_to_silver.py --entity orders
#
# Optional: SPARK_IVY_DIR (default ~/.ivy2). PYTHONPATH gets the repo root —
# spark-submit does not add it to sys.path by itself.
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}$PWD"

MASTER="${SPARK_MASTER_URL:-local[2]}"
IVY_DIR="${SPARK_IVY_DIR:-$HOME/.ivy2}"
JOB="$1"
shift

exec spark-submit \
  --master "$MASTER" \
  --conf "spark.jars.ivy=$IVY_DIR" \
  "$JOB" "$@"
