# Replay raw source records into Kinesis, Avro-encoded and schema-validated.
#
# Incremental by default: a per-entity watermark in data/state/ tracks the last
# value of the entity's incremental column, so reruns only send new records.
# Replaying an already-sent range is safe anyway — the bronze layer dedups on
# the business key, so duplicate sends are idempotent at the lake.
#
# Validation happens before send: rows failing the Avro check are written to
# quarantine/<entity>.jsonl with the failure reason — never silently dropped.

from __future__ import annotations

# Submit-agnostic bootstrap (python -m, spark-submit client mode, EMR step):
# spark-submit only puts the script's own dir on sys.path, so add the repo
# root before any platform imports.
import sys
from pathlib import Path as _BootstrapPath

sys.path.insert(0, str(_BootstrapPath(__file__).resolve().parents[2]))

import argparse
import io
import json
import time
from pathlib import Path
from typing import Any

import boto3
import yaml
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from fastavro import parse_schema, schemaless_writer

from streaming.common.logging_utils import get_logger
from streaming.producers.raw_reader import load_entities, read_records
from streaming.schemas.registry_shim import SchemaValidationError, get_registry

logger = get_logger("producer")

DEFAULT_SETTINGS_PATH = Path("config/settings.yaml")


class KinesisSink:
    """Kinesis put_record with explicit retry/backoff.

    Works against LocalStack or real AWS purely via endpoint_url â€” same code,
    both runtimes. boto3's built-in retries stay on; the loop here adds
    deliberate backoff for throttling-style errors.
    """

    def __init__(
        self,
        stream_name: str,
        endpoint_url: str | None = None,
        region_name: str = "us-east-1",
        max_retries: int = 5,
        backoff_base_seconds: float = 0.5,
    ):
        self._stream = stream_name
        self._max_retries = max_retries
        self._backoff_base = backoff_base_seconds
        self._client = boto3.client(
            "kinesis",
            endpoint_url=endpoint_url,
            region_name=region_name,
            config=Config(retries={"max_attempts": 3, "mode": "standard"}),
        )
        self._ensure_stream()

    def _ensure_stream(self) -> None:
        try:
            self._client.describe_stream_summary(StreamName=self._stream)
        except self._client.exceptions.ResourceNotFoundException:
            logger.info("stream %s missing, creating with 1 shard", self._stream)
            self._client.create_stream(StreamName=self._stream, ShardCount=1)
        except EndpointConnectionError as exc:
            raise RuntimeError(f"kinesis endpoint unreachable: {exc}") from exc
        self._client.get_waiter("stream_exists").wait(
            StreamName=self._stream,
            WaiterConfig={"Delay": 2, "MaxAttempts": 30},
        )
        logger.info("stream %s ready", self._stream)

    def put(self, payload: bytes, partition_key: str) -> None:
        last_error: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                self._client.put_record(
                    StreamName=self._stream, Data=payload, PartitionKey=partition_key
                )
                return
            except ClientError as exc:
                last_error = exc
                sleep_s = self._backoff_base * (2 ** (attempt - 1))
                logger.warning(
                    "put_record attempt %d/%d failed: %s â€” retrying in %.1fs",
                    attempt, self._max_retries, exc, sleep_s,
                )
                time.sleep(sleep_s)
        raise RuntimeError(
            f"put_record failed after {self._max_retries} attempts"
        ) from last_error


class Watermark:
    """Persisted incremental-load cursor: data/state/<entity>.json.

    Holds the last successfully-sent value of the entity's incremental column.
    """

    def __init__(self, state_dir: Path, entity: str, column: str):
        self._path = state_dir / f"{entity}.json"
        self._column = column
        self.value: Any = self._load()

    def _load(self) -> Any:
        if not self._path.exists():
            return None
        data = json.loads(self._path.read_text(encoding="utf-8"))
        if data.get("column") != self._column:
            logger.warning(
                "watermark column changed (%s -> %s), starting from scratch",
                data.get("column"), self._column,
            )
            return None
        return data.get("value")

    def observe(self, value: Any) -> None:
        if self.value is None or value > self.value:
            self.value = value

    def save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps({"column": self._column, "value": self.value}, default=str),
            encoding="utf-8",
        )

    def reset(self) -> None:
        if self._path.exists():
            self._path.unlink()
        self.value = None


class Quarantine:
    """Append-only JSONL sink for rows that failed parsing or validation."""

    def __init__(self, quarantine_dir: Path, entity: str):
        self._path = quarantine_dir / f"{entity}.jsonl"

    def write(self, reason: str, **details: Any) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        entry = {"reason": reason, **details}
        with open(self._path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, default=str) + "\n")


def _parse_bound(raw: str | None, sample_value: Any) -> Any:
    """Coerce a --start-date/--end-date CLI string to the incremental column's type."""
    if raw is None or sample_value is None:
        return None
    if isinstance(sample_value, str):
        return f"{raw}T00:00:00"
    if isinstance(sample_value, int):
        return int(raw)
    if isinstance(sample_value, float):
        return float(raw)
    return raw


def _in_range(value: Any, low: Any, high: Any, watermark: Any) -> bool:
    if low is not None and value < low:
        return False
    if high is not None and value > high:
        return False
    # watermark is inclusive: records at/after the cursor are re-sent (several
    # source rows can share one cursor value, e.g. same-second order dates).
    # That's deliberate at-least-once delivery — bronze dedups on business key.
    if watermark is not None and value < watermark:
        return False
    return True


def _serialize(record: dict[str, Any], parsed_schema: dict[str, Any]) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay raw source records into Kinesis (Avro + schema validation).")
    parser.add_argument("--entity", required=True,
                        help="entity name from config/entities.yaml")
    parser.add_argument("--entities-config", default="config/entities.yaml")
    parser.add_argument("--start-date", help="inclusive lower bound, YYYY-MM-DD")
    parser.add_argument("--end-date", help="inclusive upper bound, YYYY-MM-DD")
    parser.add_argument("--rate", type=float, default=None,
                        help="target records/sec (default from settings)")
    parser.add_argument("--stream-name", default=None)
    parser.add_argument("--endpoint-url", default=None,
                        help="override Kinesis endpoint (LocalStack etc.)")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--limit", type=int, default=None, help="max records to send")
    parser.add_argument("--state-dir", default=None,
                        help="override watermark state dir (default from settings)")
    parser.add_argument("--quarantine-dir", default=None,
                        help="override quarantine dir (default from settings)")
    parser.add_argument("--reset-watermark", action="store_true",
                        help="ignore saved cursor, replay from the beginning")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate + serialize only, no Kinesis writes")
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> dict[str, Any]:
    settings: dict[str, Any] = {}
    if DEFAULT_SETTINGS_PATH.exists():
        settings = yaml.safe_load(DEFAULT_SETTINGS_PATH.read_text(encoding="utf-8")) or {}

    entity = load_entities(args.entities_config).get(args.entity)
    if entity is None:
        raise SystemExit(f"unknown entity {args.entity!r}")
    if entity.get("delivery") != "streaming":
        raise SystemExit(
            f"{args.entity} is a batch entity â€” the Kinesis replay path is only for "
            "streaming entities (see config/entities.yaml)")

    kinesis_cfg = settings.get("streaming", {}).get("kinesis", {})
    producer_cfg = settings.get("producer", {})
    # One stream per streaming entity (see config/settings.yaml): the Spark
    # consumer binds an Avro schema per stream, so streams can't mix entities.
    stream_name = args.stream_name or kinesis_cfg.get(
        "stream_name_template", "retail-{entity}"
    ).format(entity=args.entity)
    endpoint = args.endpoint_url
    if endpoint is None and settings.get("env", "local") == "local":
        endpoint = kinesis_cfg.get("local_endpoint")
    rate = args.rate or producer_cfg.get("default_records_per_sec", 200)
    state_dir = Path(args.state_dir or producer_cfg.get("state_dir", "data/state"))
    quarantine_dir = Path(
        args.quarantine_dir or producer_cfg.get("quarantine_dir", "data/quarantine"))

    registry = get_registry(env=settings.get("env", "local"))
    schema = registry.get_schema(entity["avro_schema"])
    parsed_schema = parse_schema(schema)

    watermark = Watermark(state_dir, args.entity, entity["incremental_column"])
    if args.reset_watermark:
        watermark.reset()
        logger.info("watermark reset for %s", args.entity)
    quarantine = Quarantine(quarantine_dir, args.entity)

    sink: KinesisSink | None = None
    if not args.dry_run:
        sink = KinesisSink(
            stream_name=stream_name,
            endpoint_url=endpoint,
            region_name=args.region,
            max_retries=producer_cfg.get("max_retries", 5),
            backoff_base_seconds=producer_cfg.get("backoff_base_seconds", 0.5),
        )

    sent = 0
    invalid = 0
    skipped = 0
    rows = read_records(entity)
    low_bound: Any = None
    high_bound: Any = None

    while True:
        try:
            line_no, record, error = next(rows)
        except StopIteration:
            break
        if error is not None:
            quarantine.write("raw_parse_error", line=error.line_no,
                             raw=error.raw, error=error.reason)
            invalid += 1
            continue

        try:
            registry.validate_record(record, entity["avro_schema"])
        except SchemaValidationError as exc:
            quarantine.write("schema_validation_failed", line=line_no,
                             record=record, error=str(exc))
            invalid += 1
            continue

        inc_value = record[entity["incremental_column"]]
        if low_bound is None:
            # coerce CLI bounds once, using the first valid record's type
            low_bound = _parse_bound(args.start_date, inc_value)
            high_bound = _parse_bound(args.end_date, inc_value)
        if not _in_range(inc_value, low_bound, high_bound, watermark.value):
            skipped += 1
            continue

        payload = _serialize(record, parsed_schema)
        partition_key = str(record[entity["business_key"]])
        if sink is not None:
            sink.put(payload, partition_key)
        elif sent == 0:
            logger.info("dry-run: first payload would go to %s with partition_key=%s",
                        stream_name, partition_key)

        watermark.observe(inc_value)
        sent += 1
        if sent % 1000 == 0:
            logger.info("progress: %d sent, %d invalid, %d skipped", sent, invalid, skipped)
        if args.limit is not None and sent >= args.limit:
            break
        time.sleep(1.0 / rate)

    if not args.dry_run and sent > 0:
        watermark.save()

    summary = {
        "entity": args.entity,
        "sent": sent,
        "invalid": invalid,
        "skipped": skipped,
        "dry_run": args.dry_run,
        "watermark": str(watermark.value),
    }
    logger.info("producer run complete: %s", json.dumps(summary, default=str))
    return summary


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()

