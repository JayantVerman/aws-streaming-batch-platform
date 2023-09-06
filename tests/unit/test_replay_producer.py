# Producer internals: watermark persistence, range filter, dry-run end-to-end.
import json

from streaming.producers import replay_producer as rp


def test_watermark_roundtrip(tmp_path):
    wm = rp.Watermark(tmp_path, "orders", "order_date")
    assert wm.value is None
    wm.observe("2013-07-25T00:00:00")
    wm.observe("2013-08-01T10:00:00")
    wm.save()

    reloaded = rp.Watermark(tmp_path, "orders", "order_date")
    assert reloaded.value == "2013-08-01T10:00:00"


def test_watermark_column_change_starts_fresh(tmp_path):
    rp.Watermark(tmp_path, "orders", "order_date").save()

    wm = rp.Watermark(tmp_path, "orders", "other_column")
    assert wm.value is None


def test_in_range_bounds_and_watermark():
    assert rp._in_range("2013-08-01T00:00:00", "2013-07-25T00:00:00", None, None)
    assert not rp._in_range("2013-07-01T00:00:00", "2013-07-25T00:00:00", None, None)
    assert not rp._in_range("2013-07-26T00:00:00", None, "2013-07-25T00:00:00", None)
    # watermark is inclusive (at-least-once delivery): same-cursor records are
    # re-sent and deduped downstream at bronze
    assert rp._in_range("2013-08-01T00:00:00", None, None, "2013-08-01T00:00:00")
    assert rp._in_range("2013-08-02T00:00:00", None, None, "2013-08-01T00:00:00")
    assert not rp._in_range("2013-07-31T23:59:59", None, None, "2013-08-01T00:00:00")


def test_dry_run_run_summary(tmp_path):
    args = rp.parse_args([
        "--entity", "orders",
        "--limit", "10",
        "--dry-run",
        "--start-date", "2013-07-25",
        "--end-date", "2013-07-25",
        "--state-dir", str(tmp_path / "state"),
        "--quarantine-dir", str(tmp_path / "quarantine"),
    ])
    summary = rp.run(args)
    assert summary["sent"] == 10
    assert summary["invalid"] == 0
    assert summary["dry_run"] is True
    # dry-run must not persist a watermark
    assert not (tmp_path / "state" / "orders.json").exists()


def test_dry_run_quarantines_bad_rows(tmp_path, monkeypatch):
    # one malformed raw line appended to a temp copy of the orders file
    src = "data/sample_raw/retail_db/orders/part-00000"
    lines = open(src, encoding="utf-8").read().splitlines()
    lines.insert(3, "not,valid,garbage,with,extra,columns")
    bad_file = tmp_path / "orders_bad"
    bad_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    entities = {"orders": {
        "file": str(bad_file),
        "delimiter": ",",
        "columns": {"order_id": "long", "order_date": "timestamp",
                    "order_customer_id": "long", "order_status": "string"},
        "business_key": "order_id",
        "incremental_column": "order_date",
        "delivery": "streaming",
        "avro_schema": "orders",
    }}
    cfg = tmp_path / "entities.yaml"
    cfg.write_text(json.dumps({"entities": entities}), encoding="utf-8")

    args = rp.parse_args([
        "--entity", "orders",
        "--entities-config", str(cfg),
        "--limit", "20",
        "--dry-run",
        "--state-dir", str(tmp_path / "state"),
        "--quarantine-dir", str(tmp_path / "quarantine"),
    ])
    summary = rp.run(args)
    assert summary["sent"] == 20
    assert summary["invalid"] == 1

    quarantine = tmp_path / "quarantine" / "orders.jsonl"
    assert quarantine.exists()
    entry = json.loads(quarantine.read_text(encoding="utf-8").splitlines()[0])
    assert entry["reason"] == "raw_parse_error"
