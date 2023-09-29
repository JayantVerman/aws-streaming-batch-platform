"""retail_facts_pipeline — daily silver/gold refresh over the streamed facts.

Facts silverize in parallel (orders, order_items), then the gold marts rebuild
— but only after today's dimension refresh completed (ExternalTaskSensor on
retail_dimensions; both DAGs are @daily so they share the logical date).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

from pipeline_config import (
    FACT_ENTITIES,
    FACTS_SCHEDULE,
    PIPELINE_START_DATE,
)
from platform_common import DEFAULT_ARGS, on_sla_miss, spark_batch_task

with DAG(
    dag_id="retail_facts_pipeline",
    description="Facts -> silver, then gold marts (waits for today's dims)",
    schedule=FACTS_SCHEDULE,
    start_date=datetime(*PIPELINE_START_DATE),
    catchup=False,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=on_sla_miss,
    tags=["retail", "facts", "batch"],
) as dag:
    facts_in_silver = EmptyOperator(task_id="facts_in_silver")

    silver_tasks = []
    for entity in FACT_ENTITIES:
        silver_tasks.append(spark_batch_task(
            dag, f"silver__{entity}", "bronze_to_silver", ["--entity", entity]))

    wait_for_dims = ExternalTaskSensor(
        task_id="wait_for_dimensions",
        external_dag_id="retail_dimensions",
        external_task_id="dims_in_silver",
        # both DAGs are @daily -> identical logical dates, no delta needed
        mode="reschedule",
        poke_interval=60,
        timeout=timedelta(hours=2),
    )

    gold = spark_batch_task(
        dag, "gold__marts", "silver_to_gold", [],
        sla=timedelta(minutes=30),
    )

    silver_tasks >> facts_in_silver >> wait_for_dims >> gold

    gold >> spark_batch_task(
        dag, "warehouse__serving", "gold_to_warehouse", [],
        sla=timedelta(minutes=15),
    )

/* wip */
