"""retail_dimensions — daily dimension refresh: raw snapshot -> bronze ->
silver, one independent chain per entity (they fail and retry separately).

Pure wiring per the guide's standard: every task is one existing batch job.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from pipeline_config import (
    DIMENSIONS_SCHEDULE,
    DIM_ENTITIES,
    PIPELINE_START_DATE,
)
from platform_common import DEFAULT_ARGS, on_sla_miss, spark_batch_task

with DAG(
    dag_id="retail_dimensions",
    description="Dimension snapshots: raw -> bronze -> silver (per entity)",
    schedule=DIMENSIONS_SCHEDULE,
    start_date=datetime(*PIPELINE_START_DATE),
    catchup=False,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=on_sla_miss,
    tags=["retail", "dimensions", "batch"],
) as dag:
    # Sensor target for retail_facts_pipeline (same logical date).
    dims_in_silver = EmptyOperator(task_id="dims_in_silver")

    for entity in DIM_ENTITIES:
        load = spark_batch_task(
            dag, f"bronze__{entity}", "load_dimensions", ["--entity", entity])
        silver = spark_batch_task(
            dag, f"silver__{entity}", "bronze_to_silver", ["--entity", entity])
        load >> silver >> dims_in_silver

