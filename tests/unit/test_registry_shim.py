# Schema registry shim: validation semantics that mirror the Glue contract.
import pytest

from streaming.schemas.registry_shim import (
    LocalSchemaRegistry,
    SchemaValidationError,
)


@pytest.fixture(scope="module")
def registry():
    return LocalSchemaRegistry("streaming/schemas")


def test_get_schema_returns_fields(registry):
    schema = registry.get_schema("orders")
    names = [f["name"] for f in schema["fields"]]
    assert names == ["order_id", "order_date", "order_customer_id", "order_status"]


def test_valid_order_record_passes(registry):
    registry.validate_record(
        {"order_id": 1, "order_date": "2013-07-25T00:00:00",
         "order_customer_id": 11599, "order_status": "CLOSED"},
        "orders",
    )


def test_missing_field_rejected(registry):
    with pytest.raises(SchemaValidationError):
        registry.validate_record(
            {"order_id": 1, "order_date": "2013-07-25T00:00:00",
             "order_customer_id": 11599},
            "orders",
        )


def test_extra_field_rejected(registry):
    with pytest.raises(SchemaValidationError):
        registry.validate_record(
            {"order_id": 1, "order_date": "2013-07-25T00:00:00",
             "order_customer_id": 11599, "order_status": "CLOSED",
             "surprise": "field"},
            "orders",
        )


def test_wrong_type_rejected(registry):
    with pytest.raises(SchemaValidationError):
        registry.validate_record(
            {"order_id": "not-a-long", "order_date": "2013-07-25T00:00:00",
             "order_customer_id": 11599, "order_status": "CLOSED"},
            "orders",
        )


def test_unknown_schema_raises_keyerror(registry):
    with pytest.raises(KeyError):
        registry.get_schema("does_not_exist")
