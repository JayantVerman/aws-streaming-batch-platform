# Observability runbook

Everything is observable locally: Prometheus (metrics), Loki (logs), Grafana
(dashboards). All 2023-era versions (see docker-compose.yml).

## Scrape surface
| Source | Endpoint | Notes |
|---|---|---|
| Prometheus | `:9090` | self |
| LocalStack | `:4566/metrics` | CloudWatch emulation → kinesis/s3 metric families |
| Spark master | `:8080/metrics/prometheus` | `spark.ui.prometheus.enabled=true` (compose + every job) |
| Spark worker | `:8081/metrics/prometheus` | same |
| statsd-exporter | `:9102` | Airflow metrics (`airflow_*`) via statsd on `:9125/udp` |
| MinIO | `:9000/minio/v2/metrics/node` | node metrics are unauthenticated |

Trino 433 exposes JMX but no native `/metrics` — add the `jmx_exporter` sidecar
for Trino panels, or query the `jmx` catalog directly.

## Dashboards (provisioned in Grafana on startup)
- **Platform Overview** (`platform-overview`) — service `up`, Airflow DAG
  success/failure (7d), Spark/Airflow log volume + warning rate from Loki.
- **Warehouse Health & DQ** (`warehouse-health`) — gold rows, freshness,
  total orders, revenue-per-day, top categories — all from the warehouse
  Postgres datasource (real, guaranteed queries).

## Metric-name caveat (important)
The Airflow/Prometheus panel queries use the *documented* 2023 metric names
(`airflow_dag_run_success_total`, etc.) and LocalStack's CloudWatch families.
When the stack first runs, check Prometheus **Explore** for the exact series
that actually exist (airflow statsd names follow the Airflow 2.7 docs; LocalStack
exposes `aws_kinesis_*`/`aws_s3_*` by default) and adjust the panel expr. The
Warehouse dashboard and log panels need no adjustment.

## Logs (Loki)
Promtail watches Docker containers (`docker.sock`) and labels them
`service=<container_name>` + `compose_service=<compose service>`.
- Query in Grafana → Explore → Loki: `{compose_service="airflow-scheduler"}`,
  `{service=~"platform-spark.*"} |= "ERROR"` etc.
- Retention: 7 days (`loki.yml`).

## Proving it end-to-end
```
docker compose up -d
docker compose ps                        # prometheus, loki, promtail, grafana healthy
open http://localhost:9090/targets       # every target UP (excluding trino scrape)
open http://localhost:3000               # admin/admin → the two dashboards
```