#!/usr/bin/env bash
# Seed the local Kinesis stream from the raw sample data via the producer.
# Incremental: a per-entity watermark in data/state/ means reruns only send
# new records. Pass --reset-watermark to replay everything from the start.
#
# Examples:
#   ./scripts/seed_data.sh --entity orders
#   ./scripts/seed_data.sh --entity orders --start-date 2013-07-25 --end-date 2013-07-31
#   ./scripts/seed_data.sh --entity orders --rate 500 --reset-watermark
set -euo pipefail
cd "$(dirname "$0")/.."

python -m streaming.producers.replay_producer "$@"
