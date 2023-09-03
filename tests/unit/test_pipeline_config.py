# Pure-data pipeline config: testable without Airflow installed.
from pathlib import Path

import pytest
import yaml

from orchestration.airflow.dags import pipeline_config as pc

REPO = Path(__file__).resolve().parents[2]


def _entities():
    return yaml.safe_load(
        (REPO / "config" / "entities.yaml").read_text(encoding="utf-8"))["entities"]


def test_entity_lists_match_the_contract():
    entities = _entities()
    assert set(pc.DIM_ENTITIES) == {
        n for n, e in entities.items() if e["delivery"] == "batch"}
    assert set(pc.FACT_ENTITIES) == {
        n for n, e in entities.items() if e["delivery"] == "streaming"}


def test_job_paths_exist_on_disk():
    for name, rel in pc.JOB_PATHS.items():
        assert (REPO / rel).is_file(), f"{name} -> {rel} missing"


def test_application_path_uses_project_root(monkeypatch):
    monkeypatch.delenv("PROJECT_ROOT", raising=False)
    assert pc.application_path("load_dimensions") == \
        "/opt/project/batch/spark_batch_jobs/load_dimensions.py"
    monkeypatch.setenv("PROJECT_ROOT", "/srv/bundle")
    assert pc.application_path("silver_to_gold") == \
        "/srv/bundle/batch/spark_batch_jobs/silver_to_gold.py"


def test_unknown_job_rejected():
    with pytest.raises(SystemExit, match="unknown job"):
        pc.application_path("nope")


def test_both_dags_daily_same_logical_date():
    # the ExternalTaskSensor relies on identical logical dates: both @daily
    assert pc.DIMENSIONS_SCHEDULE.split() == ["0", "1", "*", "*", "*"]
    assert pc.FACTS_SCHEDULE.split() == ["0", "2", "*", "*", "*"]
