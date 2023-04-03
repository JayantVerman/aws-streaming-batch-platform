# Static validation of the observability configs — YAML/JSON parse + the key
# cross-references that would otherwise only break at container start.
import json
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
MONITORING = REPO / "monitoring"


def _yaml(rel: str) -> dict:
    return yaml.safe_load((MONITORING / rel).read_text(encoding="utf-8"))


def test_prometheus_scrapes_expected_jobs():
    cfg = _yaml("prometheus/prometheus.yml")
    jobs = {j["job_name"]: j for j in cfg["scrape_configs"]}
    assert {"prometheus", "localstack", "spark-master", "spark-worker",
            "statsd-exporter", "minio"} <= set(jobs)
    # spark prometheus endpoints point at the 3.5 UI paths
    assert jobs["spark-master"]["metrics_path"] == "/metrics/prometheus"
    assert jobs["spark-worker"]["metrics_path"] == "/metrics/prometheus"
    assert jobs["minio"]["metrics_path"] == "/minio/v2/metrics/node"


def test_grafana_datasource_uids():
    ds = _yaml("grafana/datasources/datasources.yml")["datasources"]
    uids = {d["uid"] for d in ds}
    assert {"prometheus", "loki", "postgres"} <= uids


def test_dashboards_reference_known_datasources():
    ds = _yaml("grafana/datasources/datasources.yml")["datasources"]
    uids = {d["uid"] for d in ds}
    for dash in ("platform_overview.json", "warehouse_health.json"):
        doc = json.loads((MONITORING / "grafana" / "dashboards" / dash).read_text(
            encoding="utf-8"))
        assert doc["panels"], f"{dash}: no panels"
        for panel in doc["panels"]:
            ref = panel.get("datasource", {})
            assert ref.get("uid") in uids, f"{dash}: unknown ds {ref!r}"


def test_dashboard_provider_present():
    _yaml("grafana/dashboards/provider.yml")


def test_loki_and_promtail_parse():
    loki = _yaml("loki/loki.yml")
    assert loki["server"]["http_listen_port"] == 3100
    promtail = _yaml("promtail/promtail.yml")
    assert promtail["clients"][0]["url"].endswith("/loki/api/v1/push")
    assert promtail["scrape_configs"][0]["job_name"] == "docker-logs"


def test_compose_has_observability_services():
    compose = yaml.safe_load((REPO / "docker-compose.yml").read_text(
        encoding="utf-8"))
    services = compose["services"]
    for name in ("statsd-exporter", "loki", "promtail"):
        assert name in services, f"compose missing {name}"
    # airflow statsd wiring points at the exporter
    env = compose["x-airflow-common"]["environment"]
    assert env["AIRFLOW__METRICS__STATSD_HOST"] == "statsd-exporter"
    assert env["AIRFLOW__METRICS__STATSD_ON"] == "true"
    # spark daemons expose the 3.5 prometheus UI
    assert "-Dspark.ui.prometheus.enabled=true" in \
        services["spark-master"]["environment"]["SPARK_DAEMON_JAVA_OPTS"]