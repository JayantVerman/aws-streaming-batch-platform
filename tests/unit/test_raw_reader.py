# Raw reader: type casting + row errors, against the real sample files.
import pytest

from streaming.producers.raw_reader import (
    RowParseError,
    _normalize_timestamp,
    load_entities,
    read_records,
)


def test_entities_config_loads_all_six():
    entities = load_entities()
    assert set(entities) == {
        "orders", "order_items", "customers", "products", "categories", "departments"}


def test_orders_first_record_is_typed():
    entities = load_entities()
    _, record, err = next(read_records(entities["orders"]))
    assert err is None
    assert record == {
        "order_id": 1,
        "order_date": "2013-07-25T00:00:00",
        "order_customer_id": 11599,
        "order_status": "CLOSED",
    }


def test_order_items_casts_numerics():
    entities = load_entities()
    _, record, _ = next(read_records(entities["order_items"]))
    assert record["order_item_id"] == 1
    assert record["order_item_quantity"] == 1
    assert record["order_item_subtotal"] == 299.98
    assert isinstance(record["order_item_product_price"], float)


def test_limit_caps_output():
    entities = load_entities()
    records = [r for _, r, err in read_records(entities["customers"], limit=5)]
    assert len(records) == 5
    assert all(r is not None for r in records)


def test_bad_row_yields_error_not_crash(tmp_path):
    # a malformed line mid-file must surface as a yielded error, not kill the read
    src = tmp_path / "bad_entity.txt"
    src.write_text(
        "1,2013-07-25 00:00:00.0,11599,CLOSED\n"
        "garbage,row\n"
        "2,2013-07-26 00:00:00.0,256,PENDING_PAYMENT\n",
        encoding="utf-8",
    )
    entity = {
        "file": str(src),
        "delimiter": ",",
        "columns": {"order_id": "long", "order_date": "timestamp",
                    "order_customer_id": "long", "order_status": "string"},
    }
    results = list(read_records(entity))
    assert len(results) == 3
    assert results[0][1] is not None and results[0][2] is None
    assert results[1][1] is None and isinstance(results[1][2], RowParseError)
    assert results[2][1]["order_id"] == 2


def test_normalize_timestamp_variants():
    assert _normalize_timestamp("2013-07-25 00:00:00.0") == "2013-07-25T00:00:00"
    assert _normalize_timestamp("2013-07-25 13:45:02") == "2013-07-25T13:45:02"
    assert _normalize_timestamp("2013-07-25") == "2013-07-25T00:00:00"
    with pytest.raises(ValueError):
        _normalize_timestamp("not-a-date")
