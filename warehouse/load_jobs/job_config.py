"""Warehouse load config — PURE data/helpers, no pyspark (unit-testable).

Gold marts -> serving schema. The same job serves Postgres (local) and Redshift
Serverless (AWS): everything engine-specific is connection parameters from
config/settings.yaml + environment overrides, never code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from streaming.spark_streaming_jobs.job_config import load_yaml

DEFAULT_SETTINGS_PATH = "config/settings.yaml"

# gold Iceberg table -> serving warehouse table (1:1 by name)
GOLD_TO_WAREHOUSE = {
    "daily_sales": "serving.daily_sales",
    "category_sales": "serving.category_sales",
    "customer_orders": "serving.customer_orders",
}


@dataclass(frozen=True)
class WarehouseConfig:
    jdbc_url: str
    driver: str
    user: str
    password: str
    schema: str
    jdbc_package: str

    def table(self, mart: str) -> str:
        try:
            return GOLD_TO_WAREHOUSE[mart]
        except KeyError as exc:
            raise SystemExit(
                f"unknown mart {mart!r} (have: {sorted(GOLD_TO_WAREHOUSE)})") from exc


def get_warehouse_config(
    settings_path: str = DEFAULT_SETTINGS_PATH,
) -> WarehouseConfig:
    settings = load_yaml(settings_path)
    wh = settings.get("warehouse", {})
    # env > yaml on every connection parameter (EMR/MWAA override without
    # touching the repo); the password is ONLY ever read from the environment.
    password_env = wh.get("password_env", "WAREHOUSE_PASSWORD")
    return WarehouseConfig(
        jdbc_url=os.getenv("WAREHOUSE_URL") or wh.get(
            "jdbc_url", "jdbc:postgresql://postgres:5432/warehouse"),
        driver=os.getenv("WAREHOUSE_DRIVER") or wh.get(
            "driver", "org.postgresql.Driver"),
        user=os.getenv("WAREHOUSE_USER") or wh.get("user", ""),
        password=os.getenv(password_env, ""),
        schema=os.getenv("WAREHOUSE_SCHEMA") or wh.get("schema", "serving"),
        jdbc_package=wh.get("jdbc_package", "org.postgresql:postgresql:42.6.0"),
    )

# wip150
