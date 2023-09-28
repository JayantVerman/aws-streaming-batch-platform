"""Shared DAG wiring helpers — no business logic lives here (guide standard:
real logic lives in batch/spark_batch_jobs/ or streaming/). Everything in this
module is standard Airflow / standard providers, so the DAGs run unmodified on
MWAA (the SparkSubmitOperator connection changes, the code doesn't).
"""

from __future__ import annotations

import json
import os
import sys
from datetime import timedelta

# DAGs live in the dags mount, platform code in the repo mount — bridge the
# two the same way the spark jobs do (PROJECT_ROOT overridable for MWAA/EMR).
_PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/project")
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from streaming.common.logging_utils import get_logger

logger = get_logger("airflow.dag")

# Alerting hook: structured JSON on every task failure so the same log
# pipeline that watches Spark also watches orchestration. Wire Slack/
# PagerDuty/SES here later — the callback signature stays stable.
DEFAULT_ALERT_TIMEOUT = timedelta(minutes=45)


def on_task_failure(context) -> None:
    ti = context.get("task_instance")
    logger.warning("task failed: %s", json.dumps({
        "dag": ti.dag_id,
        "task": ti.task_id,
        "execution_date": str(context.get("execution_date")),
        "exception": str(context.get("exception")),
        "try_number": ti.try_number,
        "log_url": ti.log_url,
    }))


def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    logger.warning("sla missed: %s", json.dumps({
        "dag": dag.dag_id,
        "sla_missed": [s.task_id for s in slas],
        "blocking": [t.task_id for t in blocking_tis] if blocking_tis else [],
    }))


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": DEFAULT_ALERT_TIMEOUT,
    "on_failure_callback": on_task_failure,
    # SMTP isn't configured locally; MWAA uses its own alerting on top of the
    # JSON failure logs above.
    "email_on_failure": False,
    "email_on_retry": False,
}


def spark_batch_task(dag, task_id: str, job_name: str, args: list[str] | None = None,
                     sla: timedelta | None = None) -> SparkSubmitOperator:
    """One Airflow task = one batch job, submitted client-mode to the stack's
    Spark cluster. Jobs bootstrap their own sys.path, so this works identically
    when the connection points at EMR/Livy instead of the local master."""
    return SparkSubmitOperator(
        task_id=task_id,
        dag=dag,
        conn_id="spark_default",
        application=application_path(job_name),
        name=f"{dag.dag_id}.{task_id}",
        verbose=False,
        conf={"spark.ui.showConsoleProgress": "false"},
        sla=sla or DEFAULT_ALERT_TIMEOUT,
    )

# wip152
