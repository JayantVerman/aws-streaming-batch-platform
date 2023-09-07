# Unit tests for the warehouse load config (no pyspark/DB needed).
import os

import pytest

from warehouse.load_jobs import job_config as wc


def test_gold_to_warehouse_mapping():
    assert wc.GOLD_TO_WAREHOUSE == {
        "daily_sales": "serving.daily_sales",
        "category_sales": "serving.category_sales",
        "customer_orders": "serving.customer_orders",
    }


def test_table_validates_mart():
    cfg = wc.get_warehouse_config()
    assert cfg.table("daily_sales") == "serving.daily_sales"
    with pytest.raises(SystemExit, match="unknown mart"):
        cfg.table("nope")


def test_warehouse_config_defaults_from_settings():
    cfg = wc.get_warehouse_config()
    assert cfg.jdbc_url == "jdbc:postgresql://postgres:5432/warehouse"
    assert cfg.driver == "org.postgresql.Driver"
    assert cfg.user == "platform"
    assert cfg.schema == "serving"
    assert cfg.jdbc_package == "org.postgresql:postgresql:42.6.0"


def test_warehouse_config_env_overrides(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_URL", "jdbc:redshift://cluster:5439/dev")
    monkeypatch.setenv("WAREHOUSE_DRIVER", "com.amazon.redshift.jdbc.Driver")
    monkeypatch.setenv("WAREHOUSE_USER", "rs_user")
    monkeypatch.setenv("WAREHOUSE_PASSWORD", "supersecret")
    cfg = wc.get_warehouse_config()
    assert cfg.jdbc_url == "jdbc:redshift://cluster:5439/dev"
    assert cfg.driver == "com.amazon.redshift.jdbc.Driver"
    assert cfg.user == "rs_user"
    assert cfg.password == "supersecret"  # only ever from env
