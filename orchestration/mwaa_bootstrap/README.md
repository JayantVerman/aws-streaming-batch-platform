# MWAA bootstrap

What moves when the orchestrator goes from the local Docker stack to MWAA —
and what doesn't.

## Unchanged
- The DAGs (`orchestration/airflow/dags/*.py`): standard Airflow + standard
  providers only (apache-spark provider, ExternalTaskSensor, EmptyOperator).
  No local-only plugins, no docker-socket tricks.
- The batch jobs: `batch/spark_batch_jobs/` are submit-agnostic (they bootstrap
  their own `sys.path` and take all endpoints from settings/env).

## What changes
1. **Spark connection**: locally `spark_default` = `spark://spark-master:7077`
   (env `AIRFLOW_CONN_SPARK_DEFAULT` in docker-compose). On MWAA, point it at
   your Spark runtime — typically EMR on EKS / EMR via Livy, or an EMR step
   wrapper. `pipeline_config.PROJECT_ROOT` (env `PROJECT_ROOT`) must point at
   where the repo bundle (dags + batch/ + streaming/ + config/) is unpacked.
2. **Requirements**: MWAA installs `mwaa_bootstrap/requirements.txt` together
   with its constraints file for your Airflow version — keep provider pins
   that exist in those constraints (`apache-airflow-providers-apache-spark`).
3. **Secrets**: MWAA's own Secrets Manager backend replaces the local
   .env pattern (guide: never hardcoded).
4. **Scheduling**: the two @daily DAGs share logical dates, which is what the
   `wait_for_dimensions` sensor relies on — keep both schedules daily when
   moving to MWAA.

## Local smoke path
```
docker compose build airflow-scheduler
docker compose up -d                      # stack + built image
# trigger manually:
docker compose exec airflow-scheduler airflow dags trigger retail_dimensions
docker compose exec airflow-scheduler airflow dags trigger retail_facts_pipeline
```
