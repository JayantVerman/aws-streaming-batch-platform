"""Pipeline definitions for the Airflow DAGs — PURE DATA, no airflow imports.

The "no logic inside DAG files" standard starts here: entity lists, job paths
and container paths live in this module so they are testable without Airflow
(see tests/unit/test_pipeline_config.py) and changeable without touching DAG
wiring.
"""

from __future__ import annotations

import os

# Dimensions are small static snapshots: refresh daily, low hour.
DIM_ENTITIES = ("customers", "products", "categories", "departments")
# Facts stream continuously; silver/gold refresh daily after the dims DAG.
FACT_ENTITIES = ("orders", "order_items")

JOB_PATHS = {
    "load_dimensions": "batch/spark_batch_jobs/load_dimensions.py",
    "bronze_to_silver": "batch/spark_batch_jobs/bronze_to_silver.py",
    "silver_to_gold": "batch/spark_batch_jobs/silver_to_gold.py",
    "gold_to_warehouse": "warehouse/load_jobs/gold_to_warehouse.py",
}

def project_root() -> str:
    """Repo/bundle root inside the submitting container. Read lazily (not at
    import time) so MWAA/EMR deployments can set PROJECT_ROOT per environment."""
    return os.getenv("PROJECT_ROOT", "/opt/project")


def application_path(job_name: str) -> str:
    """Absolute path of a batch job for spark-submit's `application` argument."""
    try:
        rel = JOB_PATHS[job_name]
    except KeyError as exc:
        raise SystemExit(f"unknown job {job_name!r} (have: {sorted(JOB_PATHS)})") from exc
    return f"{project_root().rstrip('/')}/{rel}"


# Schedules. Both DAGs are @daily so they share the same logical date — that
# is what lets the facts pipeline's ExternalTaskSensor run with no delta.
DIMENSIONS_SCHEDULE = "0 1 * * *"
FACTS_SCHEDULE = "0 2 * * *"

# Start date only needs to be before any real run; catchup is disabled.
PIPELINE_START_DATE = (2023, 12, 1)

/* wip */

/* wip */
