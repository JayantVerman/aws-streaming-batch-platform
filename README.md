# aws-streaming-batch-platform — Retail Analytics Pipeline

Real-time + batch data platform on AWS built with PySpark, Kinesis, Iceberg, and Airflow.
Local development runs on Docker (LocalStack + MinIO + Postgres + Spark + Trino + Grafana);
AWS runs on EMR, Kinesis Data Streams, S3, Glue Schema Registry, Redshift Serverless, and
MWAA — the **same code and the same SQL/DDL** on both.

**Status: complete.** Phases 0–10 are implemented and committed. See
[PROJECT_STATE.md](./PROJECT_STATE.md) for the full build log and what to run.

---

## What this is

An **event-driven fact pipeline** (`orders`, `order_items`) streaming through Kinesis for
low-latency ingestion, and a **batch dimension pipeline** (`customers`, `products`,
`categories`, `departments`) loading from raw source files. Both converge into a
**Bronze → Silver → Gold Iceberg medallion lake**, materialize into a **Redshift Serverless**
warehouse (Postgres locally), and surface in **Power BI** / SQL clients. There's an optional
**Streamlit** dashboard and a ready **Trino** catalog over the lake.

Key properties baked in from the start:

- **Idempotency** — business-key dedup at Bronze (micro-batch) and Silver (Iceberg MERGE,
  the authoritative dedup), so replays and retries never create duplicates.
- **Safe at-least-once delivery** — Kinesis checkpointing + watermarks on facts; full-snapshot
  overwrite semantics on dimensions.
- **Never-silent failure** — malformed records are quarantined (Avro validation in the
  producer, PERMISSIVE CSV/json dead-lettering in the jobs), each with the raw payload + a
  reason, so bad data is debuggable instead of disappearing.
- **No logic in orchestration** — DAGs are thin triggers over reusable, EMR-step-compatible
  Spark jobs.

---

## Quick start (local Docker)

> Docker is required. If it's not installed yet, install Docker Desktop first.

```bash
git clone <this-repo>
cd 05-aws-streaming-batch-platform
cp .env.example .env
./scripts/setup.sh          # builds the Airflow image, starts the stack, runs init jobs
./scripts/smoke_test.sh     # verifies LocalStack, Kinesis, MinIO, Postgres, Airflow, Trino, Spark
```

Start Airflow UI: <http://localhost:8080> (admin / `admin`).
Spark Master: <http://localhost:8081>; history server `:18080`.
Trino: <http://localhost:8099> (CLI in the `trino` container).
PostgreSQL (warehouse): `psql -h localhost -p 5432 -U warehouse -d warehouse` (pw: `warehouse`).
MinIO: <http://localhost:9000> (console: `minioadmin` / `minioadmin`).
Grafana: <http://localhost:3000> (admin / `admin`).

Trigger a full run end-to-end:

```bash
./scripts/seed_data.sh      # replays facts into Kinesis + loads dimensions into bronze
airflow dags trigger retail_dimensions
airflow dags trigger retail_facts_pipeline
```

Run the batch layer in a Spark container:

```bash
docker compose run --rm --profile batch spark-batch
```

To stop everything and wipe state: `./scripts/teardown.sh`.

---

## Project layout

```
05-aws-streaming-batch-platform/
├── config/                      # Entity contracts + runtime settings (single source of truth)
│   ├── entities.yaml            # column order/types, business key, incremental col, path
│   └── settings.yaml            # endpoints, IAM, catalog roots, table format, job paths
├── data/
│   ├── sample_raw/retail_db/    # source dataset (6 entities)
│   └── state/                   # per-entity watermarks for incremental replay
├── streaming/                   # Kinesis ingest path
│   ├── common/spark_utils.py    # shared SparkSession builder
│   ├── schemas/                 # Avro schemas + registry_shim.py (local/Glue switch)
│   └── producers/replay_producer.py   # replays raw rows into Kinesis
├── streaming/spark_streaming_jobs/     # Kinesis → Bronze (Phase 3)
├── batch/spark_batch_jobs/             # Bronze→Silver→Gold (Phase 4)
├── warehouse/                            # serving layer
│   ├── ddl/             # gold_marts.sql (Postgres/Redshift-identical) + verify.sql
│   └── load_jobs/       # gold_to_warehouse.py (JDBC truncate-overwrite)
├── orchestration/airflow/                # DAGs + image + MWAA bootstrap
├── infra/                                 # Terraform modules + local/aws roots
├── monitoring/                            # Prometheus + Loki + Grafana (provisioned)
├── data_quality/great_expectations/       # GE 0.18.8 suites (bronze + silver)
├── tests/                                 # unit | data_quality | integration
├── scripts/                               # setup, smoke, seed, teardown + spark-submit wrappers
├── docs/runbooks/, viz/streamlit_app/     # serving docs + optional dashboard
├── docker-compose.yml, requirements*.txt, .env.example, ARCHITECTURE.md, PROJECT_STATE.md
```

## Environment switching

Local and AWS run **the same job code**. Set `ENV=local` (Docker) or `ENV=aws` (EMR/MWAA) and
point endpoints via environment variables (all templated in `.env.example`). The
schema-registry shim auto-swaps: local file-based Avro validation vs. Glue Schema Registry on
AWS — zero caller changes. Switching is configuration only; nothing is hardcoded in job code.
See [ARCHITECTURE.md §4](./ARCHITECTURE.md#4-environment-switching).

## 2023 authenticity

This project targets the late-2023 toolchain. All tools and libraries are pinned to 2023
releases and documented as such in `PROJECT_STATE.md` and the dependency files. Notable
pins: Python 3.11, PySpark 3.5.0, Spark 3.5.0 (Kinesis connector 1.0.0), Airflow 2.7.3,
Iceberg 1.4.2, Great Expectations 0.18.8, Terraform 1.5.7 / aws-provider 5.15, LocalStack 2.3,
MinIO (Sept 2023), Postgres 16.1, Trino 433, Grafana 10.2.0 / Prometheus v2.47.2,
Streamlit 1.29.0, Loki 2.9.5 / promtail 2.9.5.

## Docs index

- [ARCHITECTURE.md](./ARCHITECTURE.md) — locked decisions, end-to-end data-flow diagram,
  environment-switching table, and per-phase decision logs.
- [PROJECT_STATE.md](./PROJECT_STATE.md) — current phase, file manifest, open blockers.
- [infra/envs/aws/README.md](./infra/envs/aws/README.md) — Terraform layout, plan-safe
  defaults, cost warnings before provisioning.
- [monitoring/README.md](./monitoring/README.md) — Prometheus jobs, dashboards, cost notes.
- [docs/runbooks/serving.md](./docs/runbooks/serving.md) — Power BI + Trino + Streamlit.

---

*This repository was built up phase-by-phase as a reference implementation of a streaming +
batch retail analytics platform (see [ARCHITECTURE.md](./ARCHITECTURE.md) and
[PROJECT_STATE.md](./PROJECT_STATE.md) for the design rationale and build log). It is a
scaffold, not a production drop: the AWS side (`infra/envs/aws`) is plan-safe by default —
no always-on EMR/MWAA/Redshift unless explicitly enabled — and the live stack has not been
exercised on this dev machine (no Docker locally; see `PROJECT_STATE.md` blockers). Review
the per-phase READMEs before provisioning.*

<!-- MARKDOWN-LINKS -->
[airflow]: https://airflow.apache.org/
[emr-spark]: https://spark.apache.org/docs/3.5.0/
[iceberg]: https://iceberg.apache.org/
[kinesis]: https://docs.aws.amazon.com/streams/latest/dev/
[minio]: https://min.io/
[mwaa]: https://docs.aws.amazon.com/mwaa/
[power-bi]: https://powerbi.microsoft.com/
[trino]: https://trino.io/
[localstack]: https://localstack.cloud/

# wip166

/* wip */
