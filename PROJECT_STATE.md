# Project State — aws-streaming-batch-platform

## Current Phase
Phase 10: Documentation pass — Status: done. Human-voice README rewritten, ARCHITECTURE.md
completed through Section 7 (per-phase decision logs), PROJECT_STATE.md manifest finalized,
AI-tool acknowledgment line added per guide §5A/§6. Next: git timeline rebuild (Phase 10
checkpoint + §5A contribution-graph commits).

## Completed Phases (collapsed summary)
- Phase 0: Repo skeleton + Docker Compose service stubs + PROJECT_STATE/ARCHITECTURE
  skeletons + pre-commit hooks (black, isort, ruff, terraform fmt) + CI lint stub — done
- Phase 1: Local infra finalized — compose healthchecks + one-shot init jobs (minio
  buckets, airflow db/user, localstack stream/buckets), scripts/{setup,smoke_test,
  seed_data,teardown}.sh, postgres dual-DB init — done (not yet executed live: no Docker
  on the dev machine; smoke test pending first run)
- Phase 2: Sample data ingestion — entity/stream contract (config/*.yaml), Avro schemas
  for orders + order_items, schema registry shim (local/Glue switch), incremental replay
  producer (watermarks, quarantine, retry/backoff, dry-run), unit tests green — done
- Phase 3: Bronze streaming — spark_streaming_jobs/{job_config,transforms,
  bronze_stream}.py: Kinesis (spark-streaming-sql-kinesis-connector 1.0.0, Dec 2023)
  -> from_avro PERMISSIVE validation -> bronze Iceberg 1.4.2 on MinIO (catalog lake
  type=hadoop, s3a), checkpointing, within-batch business-key dedup, dead-letter
  JSON to lake quarantine. Stream contract now per-entity (retail-{entity});
  producer + localstack-init + settings updated. Compose: spark-stream-{orders,
  order-items} services. Unit tests: job_config (no JVM) + bronze transforms
  (SparkSession; skips cleanly without Java) — done
- Phase 4: Batch layer — batch/spark_batch_jobs/{job_config,transforms,
  load_dimensions,bronze_to_silver,silver_to_gold}.py: dims raw->bronze
  (full-snapshot overwrite), facts+dims bronze->silver (typing + authoritative
  Iceberg MERGE dedup on business key, format-version=2, quarantine JSON),
  silver->gold marts (daily_sales, category_sales, customer_orders; full
  recompute overwrite). All jobs EMR-step compatible (--start-date/--end-date
  backfill; endpoints from settings/env). GE suites (bronze+silver) in
  data_quality/great_expectations/suites/ (execution wired Phase 9;
  great-expectations==0.18.8). Compose: spark-batch runner (profile "batch").
  Shared session builder extracted to streaming/common/spark_utils.py;
  spark-submit wrappers now export PYTHONPATH (repo root) — done
- Phase 5: Orchestration — orchestration/airflow/Dockerfile (airflow 2.7.3 +
  Java 17 + pyspark 3.5.0 + apache-spark provider 4.1.2, all 2023-pinned);
  DAGs retail_dimensions (@daily 01:00, per-entity bronze->silver chains) and
  retail_facts_pipeline (@daily 02:00, facts silver -> ExternalTaskSensor on
  dims -> gold marts); pipeline_config.py (pure data) + platform_common.py
  (retries=2, SLAs, JSON on_failure/sla_miss alert hooks); jobs made submit-
  agnostic (sys.path bootstrap in every entrypoint); MWAA bootstrap
  (requirements + README); tests: test_pipeline_config (no Airflow) +
  test_dags (Airflow-gated) — done
- Phase 6: Warehouse + serving — warehouse/ddl/gold_marts.sql (standard types,
  Postgres/Redshift-identical, applied via postgres-init 02-gold-serving.sql) +
  verify.sql; warehouse/load_jobs/gold_to_warehouse.py (JDBC truncate-overwrite,
  driver as spark package, env-only secrets, wired as final DAG task);
  Trino iceberg catalog (infra/envs/local/trino/iceberg.properties -> MinIO,
  Athena stand-in); docs/runbooks/serving.md (Power BI Postgres/Redshift + Trino
  + Streamlit); viz/streamlit_app (streamlit 1.29.0, SQLAlchemy direct);
  test_warehouse_config (env overrides + defaults) — done
- Phase 7: IaC for real AWS — Terraform 7 modules (kinesis, s3,
  glue_schema_registry, iam, emr, redshift_serverless, mwaa), each with
  variables/main/outputs + README; envs/aws root (providers pinned to 2023:
  aws ~>5.15.0; plan-safe deploy_* gates; streams/lake/registry/iam = always-on
  core; tfvars.example; README cost table + destroy path); envs/local =
  tflocal parity on the same modules; schemas wired from streaming/schemas/*.avsc;
  EMR pinned emr-6.15.0 (Spark 3.3.x within 2023; local 3.5.0 note) — done
  (terraform fmt/validate pending binary; wired in Phase 9 CI)
- Phase 8: Observability — prometheus scrape surface (spark-master/worker via
  3.5 prometheus UI, localstack /metrics, statsd-exporter, minio node);
  Airflow statsd → prom/statsd-exporter (dag success/failure metrics); Loki
  2.9.5 + promtail 2.9.5 log aggregation (docker.sock, service/compose_service
  labels); Grafana provisioning (datasources: prometheus/loki/postgres) + 2
  dashboards (Platform Overview, Warehouse Health & DQ via guaranteed Postgres
  SQL); spark.ui.prometheus.enabled=true in build_session + compose daemons;
  monitoring tests (static config + cross-refs) — done (live verification of
  exact airflow/localstack metric names on first run; documented in README)

## Conventions
- Version policy (user directive): all tooling pinned to versions that existed by end of
  2023 — Python 3.11, Spark 3.5.0 / EMR 7.0, Airflow 2.7.3, LocalStack 2.3, Trino 433,
  Postgres 16.1, Grafana 10.2.0, Prometheus 2.47.2, Terraform 1.5.7, Java 17,
  Iceberg 1.4.2, spark-streaming-sql-kinesis-connector 1.0.0 (Dec 2023), hadoop-aws
  3.3.4 + aws-java-sdk-bundle 1.12.262. Every new dependency must pick the newest
  release available in 2023 — never anything from 2024+.
- Source contract lives in config/entities.yaml (column order, types, business key,
  incremental column, delivery mode). Column order changes are breaking schema changes.
- Streams are per streaming entity (retail-orders, retail-order_items; from
  stream_name_template in settings.yaml) — the consumer binds one Avro schema/stream.
- Validation failures always go to quarantine/ (producer: data/quarantine/*.jsonl;
  bronze: s3a://lake/quarantine/streaming/<entity>/), never silently dropped;
  structured JSON logging everywhere (streaming/common/logging_utils.py).

## File Manifest (index so files aren't re-read blindly)
- guide.md — master prompt / constitution: locked architecture (S1), repo layout (S2),
  PROJECT_STATE protocol (S3), phased plan (S4), standards (S5), README spec (S6)
- PROJECT_STATE.md — this file
- ARCHITECTURE.md — locked decisions, data-flow diagram, Phase 2 + Phase 3 decision
  logs (Section 3)
- docker-compose.yml — services + one-shot init jobs; spark-stream-{orders,order-items}
  run the bronze jobs; spark-ivy volume caches resolved spark packages
- scripts/setup.sh / smoke_test.sh / seed_data.sh / teardown.sh / run_bronze_stream.sh /
  run_batch_job.sh (spark-submit wrappers; PYTHONPATH=repo root; master via
  SPARK_MASTER_URL) / localstack-init.sh (2 streams) / postgres-init/01-create-airflow-db.sql
- config/settings.yaml — env switch, lake prefixes, kinesis (stream_name_template),
  producer cfg, spark section (packages/connector jar/catalog/endpoints; env overrides
  SPARK_S3A_ENDPOINT / SPARK_KINESIS_ENDPOINT / SPARK_MASTER_URL)
- config/entities.yaml — THE source contract for the 6 retail_db entities
- streaming/producers/raw_reader.py — headerless delimited reader per entities.yaml
- streaming/producers/replay_producer.py — per-entity Kinesis replay: watermark
  (inclusive, at-least-once), quarantine JSONL, retry/backoff, --dry-run/--reset-watermark
- streaming/schemas/{orders,order_items}.avsc + registry_shim.py — Avro contracts and
  the local/Glue registry switch (get_registry(ENV))
- streaming/spark_streaming_jobs/job_config.py — pure-python config (EntitySpec,
  SparkJobConfig, bronze DDL builder; unit-testable without a JVM)
- streaming/spark_streaming_jobs/transforms.py — DataFrame transforms (decode,
  dead-letter split, dedup, bronze projection, dead-letter shaping)
- streaming/spark_streaming_jobs/bronze_stream.py — main job (readStream aws-kinesis,
  foreachBatch: dedup -> writeTo append, dead letters -> quarantine JSON; triggers
  processing / available-now; checkpoints on s3a://checkpoints/streaming/<entity>)
- streaming/common/logging_utils.py — JSON logging, shared by all Python components
- streaming/common/spark_utils.py — shared SparkSession builder (Iceberg catalog +
  s3a confs) for streaming and batch jobs; with_kinesis_jar only for the bronze stream
- orchestration/airflow/Dockerfile — local Airflow image: 2.7.3 + Java 17 +
  pyspark 3.5.0 + apache-airflow-providers-apache-spark 4.1.2
- orchestration/airflow/dags/pipeline_config.py — PURE-DATA pipeline definitions
  (entity lists, job paths, schedules, PROJECT_ROOT override; no airflow import)
- orchestration/airflow/dags/platform_common.py — DAG wiring helpers: default args
  (retries/SLA/on_failure JSON alert hook), spark_batch_task factory
- orchestration/airflow/dags/retail_dimensions.py — @daily 01:00 dims DAG
- orchestration/airflow/dags/retail_facts_pipeline.py — @daily 02:00 facts DAG
  (ExternalTaskSensor waits for dims before gold)
- orchestration/mwaa_bootstrap/{requirements.txt,README.md} — MWAA handoff
- warehouse/ddl/gold_marts.sql — serving DDL (Postgres/Redshift-identical; the
  schema authority; mirrored into postgres-init for auto-create)
- warehouse/ddl/verify.sql — psql verification queries
- warehouse/load_jobs/job_config.py — warehouse connection config (env overrides,
  gold->serving table map; no pyspark)
- warehouse/load_jobs/gold_to_warehouse.py — gold -> serving JDBC load
  (truncate-overwrite, idempotent; same job for Redshift via env)
- infra/envs/local/trino/iceberg.properties — Trino Iceberg catalog (MinIO,
  Athena stand-in; mounted into compose)
- docs/runbooks/serving.md — Power BI (Postgres + Redshift), Trino, Streamlit
- viz/streamlit_app/{app.py,run_streamlit.sh} — zero-install dashboard
  (streamlit 1.29.0, SQLAlchemy -> serving schema)
- infra/modules/{kinesis,s3,glue_schema_registry,iam,emr,redshift_serverless,mwaa}/
  — Terraform 7 modules (variables/main/outputs + README each; cost warnings)
- infra/envs/aws/ — real-AWS root (providers.tf pinned, deploy_* gates,
  tfvars.example, cost table + destroy README, wiring the core + gated modules)
- infra/envs/local/ — tflocal/LocalStack parity root on the same modules
- infra/envs/local/trino/iceberg.properties — Trino Iceberg catalog (Phase 6)
- monitoring/prometheus/prometheus.yml — scrape surface (spark master/worker,
  localstack /metrics, statsd-exporter, minio node)
- monitoring/statsd-exporter — none (compose service prom/statsd-exporter)
- monitoring/loki/loki.yml — 2.9.5 single-binary (7d retention)
- monitoring/promtail/promtail.yml — tails docker.sock, ships to loki
- monitoring/grafana/datasources/datasources.yml — prometheus/loki/postgres uids
- monitoring/grafana/dashboards/{provider.yml,platform_overview.json,
  warehouse_health.json} — the two provisioned dashboards
- monitoring/README.md — observability runbook + metric-name caveat
- data_quality/checkpoints/suites_runner.py (+ __init__) — compact runner
  executing the GE suite JSONs (no JVM/GE needed); supported expectations list
- tests/data_quality/test_suites_runner.py — 6 tests exercising the real suites
- tests/integration/test_stack_smoke.py — LocalStack/MinIO/Postgres/Trino smoke
  (skips unless PLATFORM_STACK_UP=1)
- .github/workflows/ci.yml — lint, unit (Java 21: Spark+Airflow tests run),
  terraform fmt/validate, compose config on every PR
- .github/workflows/stack-smoke.yml — manual (workflow_dispatch) full-stack
  Docker smoke on GitHub runners
- batch/spark_batch_jobs/job_config.py — pure-python batch config: EntityColumns,
  BronzeDimSpec, silver/gold DDL + MERGE SQL builders, date bounds (no JVM needed)
- batch/spark_batch_jobs/transforms.py — DataFrame transforms (validation splits,
  latest-per-business-key, silver projections, quarantine shaping, gold marts)
- batch/spark_batch_jobs/load_dimensions.py — raw dim files -> bronze (snapshot
  overwrite; PERMISSIVE CSV, malformed -> lake quarantine)
- batch/spark_batch_jobs/bronze_to_silver.py — typing + validation + Iceberg MERGE
  (authoritative business-key dedup) for all 6 entities; --start-date/--end-date
- batch/spark_batch_jobs/silver_to_gold.py — rebuild daily_sales / category_sales /
  customer_orders by overwrite; fails loudly if silver dims missing
- data_quality/great_expectations/suites/ — GE 0.18.8 suites for bronze + silver
  (orders, order_items); checkpoint execution wired in Phase 9
- streaming/common/logging_utils.py — JSON logging, shared by all Python components
- requirements.txt / requirements-dev.txt — 2023-pinned runtime (incl. pyspark 3.5.0,
  great-expectations 0.18.8)
- tests/unit/{test_raw_reader,test_registry_shim,test_replay_producer,test_job_config,
  test_bronze_transforms,test_batch_config,test_batch_transforms,test_pipeline_config,
  test_dags,test_warehouse_config}.py — unit tests (SparkSession + Airflow modules
  skip cleanly without Java/Airflow)
- conftest.py / pytest.ini — repo-root import + testpaths
- README.md (stub; real one in Phase 10) / .pre-commit-config.yaml / .gitignore /
  .gitattributes (data/** -text) / .env.example / LICENSE (MIT, 2023) /
  .github/workflows/ci.yml (lint stub; tests wired in Phase 9)
- data/sample_raw/retail_db/ — confirmed source dataset (6 entities; moved from repo
  root in Phase 0)
- monitoring/prometheus/prometheus.yml — self-scrape; more jobs as phases land
- Empty scaffold dirs (with .gitkeep): viz/*, tests/{integration,data_quality}

## Open Decisions / Blockers
- Phases 1-8 not yet executed live: no Docker on the dev machine, and no
  terraform binary locally. `terraform fmt`/`validate` on infra/ is wired into
  CI/pre-commit for Phase 9; review the .tf by eye until then.
- First `scripts/setup.sh` run is the real smoke test. Watch: LocalStack init
  hook, airflow-init, the built airflow image (pip install at build time needs
  internet), first spark-submit (Maven/Ivy jar downloads), and the exact
  airflow/localstack metric series surfaced by Prometheus (adjust dashboard
  exprs per monitoring/README.md caveat).
- EMR release pinned to emr-6.15.0 (Spark 3.3.x) — 2023-authentic; all jobs run
  on it; upgrading release_label lifts the Spark 3.5 gap (EMR 7.x is 2024+).
- Bronze uses Iceberg's hadoop catalog (no Hive metastore) — fine for Spark;
  revisit when Trino needs a shared catalog (Phase 6).

## Next Actions (top 3, concrete)
1. Phase 10: documentation pass — finish README.md in the human voice (guide
   Section 5A/6: read back, strip AI tells), complete ARCHITECTURE.md mermaid +
   docs/runbooks, add the honest AI-tool acknowledgment line.
2. Verify the stack live once Docker is available: setup.sh + smoke_test.sh +
   seed_data.sh + airflow dags trigger + check serving.* / Trino / Grafana.
3. Push to GitHub and let the CI workflow run for the first time (unit job
   exercises the Spark/Airflow tests in a real JVM; terraform validated).

## Deviations from Locked Architecture
- Moved retail_db/ from repo root into data/sample_raw/retail_db/ to match the repo
  structure in guide.md Section 2 (which notes sample data lives there). Files untouched.
- Facts (orders, order_items) stream via Kinesis; dimensions (customers, products,
  categories, departments) load by batch — a design decision within the locked
  architecture, documented in ARCHITECTURE.md Section 3.
- Stream-per-entity (retail-{entity}) replaces the single retail-orders stream —
  required by per-stream Avro schema binding; documented in ARCHITECTURE.md Phase 3.

