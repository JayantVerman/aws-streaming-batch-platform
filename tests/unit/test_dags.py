# DAG structure tests. Need a real Airflow install (CI / the airflow image);
# skip cleanly otherwise (this dev box has no Airflow).
import importlib
import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS_DIR = Path(__file__).resolve().parents[2] / "orchestration" / "airflow" / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from airflow.models import DagBag  # noqa: E402


def _bag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_dags_load_without_errors():
    bag = _bag()
    assert bag.import_errors == {}, bag.import_errors


def test_dimensions_dag_structure():
    dag = _bag().get_dag("retail_dimensions")
    assert dag is not None
    task_ids = set(dag.task_ids)
    # one bronze+silver chain per dim entity, joined on the sensor target
    for entity in ("customers", "products", "categories", "departments"):
        assert f"bronze__{entity}" in task_ids
        assert f"silver__{entity}" in task_ids
        assert dag.get_task(f"bronze__{entity}").downstream_task_ids == \
            {f"silver__{entity}"}
    assert "dims_in_silver" in task_ids
    # every silver task feeds the join node
    for entity in ("customers", "products", "categories", "departments"):
        assert dag.get_task(f"silver__{entity}").downstream_task_ids == \
            {"dims_in_silver"}


def test_facts_dag_structure_and_sensor():
    dag = _bag().get_dag("retail_facts_pipeline")
    assert dag is not None
    task_ids = set(dag.task_ids)
    assert {"silver__orders", "silver__order_items", "facts_in_silver",
            "wait_for_dimensions", "gold__marts"} <= task_ids
    sensor = dag.get_task("wait_for_dimensions")
    assert sensor.external_dag_id == "retail_dimensions"
    assert sensor.external_task_id == "dims_in_silver"
    # gold only runs after both silver tasks AND the dims sensor
    assert dag.get_task("gold__marts").upstream_task_ids == \
        {"wait_for_dimensions"}
    assert dag.get_task("facts_in_silver").downstream_task_ids == \
        {"wait_for_dimensions"}


def test_dag_defaults_have_retries_and_alerts():
    from platform_common import DEFAULT_ARGS
    assert DEFAULT_ARGS["retries"] >= 1
    assert DEFAULT_ARGS["on_failure_callback"] is not None
    assert DEFAULT_ARGS["sla"] is not None

# wip176

/* wip */
