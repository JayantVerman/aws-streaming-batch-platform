# Data-quality tests: the GE suites are wired into pytest via the compact
# suites_runner (no JVM/GE required — pure python), so `pytest tests/data_quality`
# runs the REAL suite JSONs against sample rows on every CI run.
import json
from pathlib import Path

import pytest

from data_quality.checkpoints.suites_runner import (
    SUITES_DIR,
    load_suite,
    run_suite,
)

SUITES = ["bronze.orders", "bronze.order_items", "silver.orders",
          "silver.order_items"]


@pytest.fixture(scope="module")
def sample_orders():
    # a few real-looking rows matching the bronze orders schema
    return [
        {"order_id": 1, "order_date": "2013-07-25T00:00:00", "order_customer_id": 10,
         "order_status": "CLOSED"},
        {"order_id": 2, "order_date": "2013-07-25T00:00:00", "order_customer_id": 11,
         "order_status": "PENDING_PAYMENT"},
        {"order_id": 3, "order_date": "2013-07-25T01:00:00", "order_customer_id": 12,
         "order_status": "COMPLETE"},
    ]


@pytest.fixture(scope="module")
def sample_items():
    return [
        {"order_item_id": 1, "order_item_order_id": 1, "order_item_product_id": 100,
         "order_item_quantity": 2, "order_item_subtotal": 49.98,
         "order_item_product_price": 24.99},
        {"order_item_id": 2, "order_item_order_id": 1, "order_item_product_id": 101,
         "order_item_quantity": 1, "order_item_subtotal": 12.50,
         "order_item_product_price": 12.50},
    ]


def test_suites_exist_and_are_valid_json():
    for name in SUITES:
        suite = load_suite(name)
        assert suite["expectation_suite_name"] == name
        assert suite["expectations"]


def test_bronze_orders_suite_passes_on_good_data(sample_orders):
    res = run_suite(sample_orders, "bronze.orders")
    assert res["summary"]["failures"] == 0, res["failures"]


def test_silver_orders_suite_uses_typed_df():
    # silver expects parsed timestamps (date type) — passes on a typed frame
    rows = [
        {"order_id": 1, "order_date": _date(2013, 7, 25), "order_customer_id": 10,
         "order_status": "CLOSED"},
        {"order_id": 2, "order_date": _date(2013, 7, 25), "order_customer_id": 11,
         "order_status": "PROCESSING"},
    ]
    res = run_suite(rows, "silver.orders")
    assert res["summary"]["failures"] == 0, res["failures"]


def _date(*a):
    from datetime import date as d
    return d(*a)


def test_bronze_orders_rejects_bad_rows(sample_orders):
    bad = list(sample_orders)
    bad[0] = {**bad[0], "order_status": None}   # null in non-null column
    bad[1] = {**bad[1], "order_date": "garbage"}
    res = run_suite(bad, "bronze.orders")
    assert res["summary"]["failures"] >= 2
    # failures reference the right expectations
    types = {f["expectation"] for f in res["failures"]}
    assert "expect_column_values_to_not_be_null" in types
    assert "expect_column_values_to_match_strftime_format" in types


def test_silver_order_items_unique_key_enforced():
    rows = [{"order_item_id": 7, "order_item_order_id": 1,
             "order_item_product_id": 100, "order_item_quantity": 1,
             "order_item_subtotal": 9.99, "order_item_product_price": 9.99},
            {"order_item_id": 7, "order_item_order_id": 1,
             "order_item_product_id": 100, "order_item_quantity": 1,
             "order_item_subtotal": 9.99, "order_item_product_price": 9.99}]
    res = run_suite(rows, "silver.order_items")
    assert res["summary"]["failures"] >= 1
    assert res["failures"][0]["expectation"] == "expect_column_values_to_be_unique"


def test_unsupported_expectation_rejected():
    from data_quality.checkpoints.suites_runner import run_expectation
    with pytest.raises(NotImplementedError):
        run_expectation([{}], {"expectation_type": "expect_nonsense"})


def test_every_suite_uses_only_supported_expectations():
    supported = {
        "expect_table_row_count_to_be_between",
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_be_between",
        "expect_column_values_to_be_unique",
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_match_strftime_format",
    }
    for suite_path in SUITES_DIR.glob("*.json"):
        suite = json.loads(suite_path.read_text(encoding="utf-8"))
        for exp in suite["expectations"]:
            assert exp["expectation_type"] in supported, \
                f"{suite_path.name}: unsupported {exp['expectation_type']}"