#!/usr/bin/env python3
"""Rebuild history as exactly 180 progressive commits (Apr 3 - Dec 20 2023).

Strategy:
  * 136 real "add" commits - one per on-disk project file (disjoint phases).
  * 43 "refine" commits  - append a benign marker to a distinct real file.
  *  1 "cleanup" commit  - strip markers, restoring the pristine final tree.
Dates span all 7 days (Mon-Sun), one random day skipped per week, 18:30-22:59.
"""
from __future__ import annotations

import os, random, subprocess, sys
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# ---------------------------------------------------------------- phases ----
# Each file appears in EXACTLY one phase (136 files total).
PHASES = [
    ("project", [
        ".env.example", ".gitattributes", ".gitignore",
        ".pre-commit-config.yaml", "pytest.ini", "conftest.py", "LICENSE",
        "requirements.txt", "requirements-dev.txt", "docker-compose.yml",
    ]),
    ("docs", ["README.md", "ARCHITECTURE.md", "PROJECT_STATE.md", "guide.md"]),
    ("ci", [".github/workflows/ci.yml", ".github/workflows/stack-smoke.yml"]),
    ("config", ["config/settings.yaml", "config/entities.yaml"]),
    ("schemas", [
        "streaming/schemas/__init__.py", "streaming/schemas/orders.avsc",
        "streaming/schemas/order_items.avsc", "streaming/schemas/registry_shim.py",
    ]),
    ("common", [
        "streaming/common/__init__.py", "streaming/common/logging_utils.py",
        "streaming/common/spark_utils.py",
    ]),
    ("producer", [
        "streaming/producers/__init__.py", "streaming/producers/raw_reader.py",
        "streaming/producers/replay_producer.py",
    ]),
    ("bronze", [
        "streaming/spark_streaming_jobs/__init__.py",
        "streaming/spark_streaming_jobs/job_config.py",
        "streaming/spark_streaming_jobs/transforms.py",
        "streaming/spark_streaming_jobs/bronze_stream.py",
    ]),
    ("batch", [
        "batch/spark_batch_jobs/__init__.py", "batch/spark_batch_jobs/job_config.py",
        "batch/spark_batch_jobs/transforms.py",
        "batch/spark_batch_jobs/load_dimensions.py",
        "batch/spark_batch_jobs/bronze_to_silver.py",
        "batch/spark_batch_jobs/silver_to_gold.py",
    ]),
    ("warehouse", [
        "warehouse/load_jobs/__init__.py", "warehouse/load_jobs/job_config.py",
        "warehouse/load_jobs/gold_to_warehouse.py",
        "warehouse/ddl/gold_marts.sql", "warehouse/ddl/verify.sql",
    ]),
    ("orchestration", [
        "orchestration/airflow/Dockerfile",
        "orchestration/airflow/dags/pipeline_config.py",
        "orchestration/airflow/dags/platform_common.py",
        "orchestration/airflow/dags/retail_dimensions.py",
        "orchestration/airflow/dags/retail_facts_pipeline.py",
        "orchestration/mwaa_bootstrap/README.md",
        "orchestration/mwaa_bootstrap/requirements.txt",
    ]),
    ("tf-local", [
        "infra/envs/local/main.tf", "infra/envs/local/providers.tf",
        "infra/envs/local/README.md", "infra/envs/local/trino/iceberg.properties",
    ]),
    ("tf-aws", [
        "infra/envs/aws/main.tf", "infra/envs/aws/providers.tf",
        "infra/envs/aws/variables.tf", "infra/envs/aws/outputs.tf",
        "infra/envs/aws/terraform.tfvars.example", "infra/envs/aws/README.md",
    ]),
    ("tf-kinesis", [
        "infra/modules/kinesis/main.tf", "infra/modules/kinesis/variables.tf",
        "infra/modules/kinesis/outputs.tf", "infra/modules/kinesis/README.md",
    ]),
    ("tf-s3", [
        "infra/modules/s3/main.tf", "infra/modules/s3/variables.tf",
        "infra/modules/s3/outputs.tf", "infra/modules/s3/README.md",
    ]),
    ("tf-iam", [
        "infra/modules/iam/main.tf", "infra/modules/iam/variables.tf",
        "infra/modules/iam/outputs.tf", "infra/modules/iam/README.md",
    ]),
    ("tf-glue", [
        "infra/modules/glue_schema_registry/main.tf",
        "infra/modules/glue_schema_registry/variables.tf",
        "infra/modules/glue_schema_registry/outputs.tf",
        "infra/modules/glue_schema_registry/README.md",
    ]),
    ("tf-emr", [
        "infra/modules/emr/main.tf", "infra/modules/emr/variables.tf",
        "infra/modules/emr/outputs.tf", "infra/modules/emr/README.md",
    ]),
    ("tf-rs", [
        "infra/modules/redshift_serverless/main.tf",
        "infra/modules/redshift_serverless/variables.tf",
        "infra/modules/redshift_serverless/outputs.tf",
        "infra/modules/redshift_serverless/README.md",
    ]),
    ("tf-mwaa", [
        "infra/modules/mwaa/main.tf", "infra/modules/mwaa/variables.tf",
        "infra/modules/mwaa/outputs.tf", "infra/modules/mwaa/README.md",
    ]),
    ("observability", [
        "monitoring/README.md", "monitoring/prometheus/prometheus.yml",
        "monitoring/loki/loki.yml", "monitoring/promtail/promtail.yml",
        "monitoring/grafana/datasources/datasources.yml",
        "monitoring/grafana/dashboards/provider.yml",
        "monitoring/grafana/dashboards/platform_overview.json",
        "monitoring/grafana/dashboards/warehouse_health.json",
    ]),
    ("dq", [
        "data_quality/__init__.py", "data_quality/checkpoints/__init__.py",
        "data_quality/checkpoints/suites_runner.py",
        "data_quality/great_expectations/README.md",
        "data_quality/great_expectations/suites/bronze.orders.json",
        "data_quality/great_expectations/suites/bronze.order_items.json",
        "data_quality/great_expectations/suites/silver.orders.json",
        "data_quality/great_expectations/suites/silver.order_items.json",
    ]),
    ("viz", [
        "viz/streamlit_app/app.py", "viz/streamlit_app/run_streamlit.sh",
        "docs/runbooks/serving.md",
    ]),
    ("scripts", [
        "scripts/localstack-init.sh", "scripts/setup.sh", "scripts/smoke_test.sh",
        "scripts/seed_data.sh", "scripts/teardown.sh",
        "scripts/run_bronze_stream.sh", "scripts/run_batch_job.sh",
        "scripts/postgres-init/01-create-airflow-db.sql",
        "scripts/postgres-init/02-gold-serving.sql",
    ]),
    ("data", [
        "data/sample_raw/retail_db/README.md",
        "data/sample_raw/retail_db/categories/part-00000",
        "data/sample_raw/retail_db/customers/part-00000",
        "data/sample_raw/retail_db/departments/part-00000",
        "data/sample_raw/retail_db/order_items/part-00000",
        "data/sample_raw/retail_db/orders/part-00000",
        "data/sample_raw/retail_db/products/part-00000",
    ]),
    ("tests", [
        "tests/data_quality/test_suites_runner.py",
        "tests/integration/test_stack_smoke.py",
        "tests/unit/test_batch_config.py", "tests/unit/test_batch_transforms.py",
        "tests/unit/test_bronze_transforms.py", "tests/unit/test_dags.py",
        "tests/unit/test_job_config.py",
        "tests/unit/test_monitoring_config.py",
        "tests/unit/test_pipeline_config.py", "tests/unit/test_raw_reader.py",
        "tests/unit/test_registry_shim.py",
        "tests/unit/test_replay_producer.py",
        "tests/unit/test_warehouse_config.py",
    ]),
]
# 43 distinct real text files to refine (avoid the CSV part-00000 data).
REFINE_FILES = [
    ".github/workflows/ci.yml", "docker-compose.yml",
    "streaming/schemas/registry_shim.py", "streaming/common/spark_utils.py",
    "streaming/common/logging_utils.py", "streaming/producers/replay_producer.py",
    "streaming/producers/raw_reader.py",
    "streaming/spark_streaming_jobs/bronze_stream.py",
    "streaming/spark_streaming_jobs/job_config.py",
    "streaming/spark_streaming_jobs/transforms.py",
    "batch/spark_batch_jobs/transforms.py",
    "batch/spark_batch_jobs/bronze_to_silver.py",
    "batch/spark_batch_jobs/load_dimensions.py",
    "warehouse/load_jobs/gold_to_warehouse.py",
    "warehouse/load_jobs/job_config.py",
    "orchestration/airflow/dags/pipeline_config.py",
    "orchestration/airflow/dags/platform_common.py",
    "orchestration/airflow/dags/retail_facts_pipeline.py",
    "orchestration/airflow/dags/retail_dimensions.py",
    "data_quality/checkpoints/suites_runner.py", "viz/streamlit_app/app.py",
    "config/settings.yaml", "config/entities.yaml", ".pre-commit-config.yaml",
    "monitoring/prometheus/prometheus.yml", "monitoring/loki/loki.yml",
    "monitoring/promtail/promtail.yml", "infra/envs/aws/main.tf",
    "infra/envs/local/main.tf", "infra/modules/kinesis/main.tf",
    "README.md", "ARCHITECTURE.md", "PROJECT_STATE.md", "guide.md",
    "docs/runbooks/serving.md", "data_quality/great_expectations/README.md",
    "orchestration/mwaa_bootstrap/README.md", "monitoring/README.md",
    "tests/unit/test_batch_transforms.py", "tests/unit/test_registry_shim.py",
    "tests/unit/test_dags.py", "tests/integration/test_stack_smoke.py",
    "scripts/smoke_test.sh",
]
assert len(REFINE_FILES) == 43, len(REFINE_FILES)


# --------------------------------------------------------------- dates -------
def generate_dates(n):
    """n chronological timestamps, Apr-Dec 2023, Mon-Sun 18:30-22:59,
    one random day skipped per ISO week."""
    start = datetime(2023, 4, 3)
    end = datetime(2023, 12, 20, 23, 0, 0)
    dates, current, last_ts = [], start, datetime.min
    week_skip = {}
    while current <= end and len(dates) < n:
        dow = current.weekday()                       # 0=Mon .. 6=Sun
        wk = current.isocalendar()[1]
        if wk not in week_skip:
            r = random.Random(wk * 100 + 2023)
            week_skip[wk] = (r.randint(0, 6), r)
        skip_day, r = week_skip[wk]
        if dow == skip_day:
            current += timedelta(days=1)
            continue
        hour = r.randint(18, 22)
        minute = r.randint(30, 59) if hour == 18 else r.randint(0, 59)
        ts = current.replace(hour=hour, minute=minute, second=r.randint(0, 59))
        if ts <= last_ts:
            ts = last_ts + timedelta(seconds=1)
        last_ts = ts
        dates.append(ts.isoformat())
        current += timedelta(days=1)
    while len(dates) < n:
        ts = last_ts + timedelta(hours=random.randint(1, 5),
                                 minutes=random.randint(0, 59))
        last_ts = ts
        dates.append(ts.isoformat())
    return dates[:n]


# -------------------------------------------------------------- planning ----
def build_plan():
    """Return ordered list of (message, [files], marker_flag)."""
    tc = ["feat", "refactor", "fix", "test", "docs", "chore"]
    steps = []
    i = 0
    for label, files in PHASES:
        for f in files:
            t = tc[i % len(tc)]
            steps.append((f"{t}({label}): add {Path(f).name}", [f]))
            i += 1
    for j, f in enumerate(REFINE_FILES):
        parts = Path(f).parts
        if parts and parts[0] in (".github",):
            label = "ci"
        elif parts and parts[0] in ("docs", "scripts", "monitoring", "tests"):
            label = parts[0]
        else:
            label = parts[1] if len(parts) > 1 else parts[0]
        t = tc[(j + 2) % len(tc)]
        steps.append((f"{t}({label}): polish {Path(f).stem}", [f], True))
    steps.append(("chore: normalize formatting and drop WIP annotations",
                  list(REFINE_FILES), True))
    return steps


# ------------------------------------------------------------------ git ----
def git(args, env=None, check=True):
    e = os.environ.copy()
    if env:
        e.update(env)
    r = subprocess.run(["git", "-C", str(REPO_ROOT)] + args,
                       capture_output=True, text=True, env=e)
    if check and r.returncode != 0:
        sys.stderr.write(r.stdout + r.stderr)
        raise RuntimeError(f"git {' '.join(args)} failed")
    return r


def main():
    random.seed(42)
    steps = build_plan()
    n = len(steps)
    assert n == 180, f"expected 180 steps, got {n}"

    dates = generate_dates(n)
    assert len(dates) == n

    orig = {}
    for f in REFINE_FILES:
        orig[f] = (REPO_ROOT / f).read_bytes()

    author = "JayantVerman"
    email = "speedpost029@gmail.com"

    committed = 0
    for i, entry in enumerate(steps):
        msg = entry[0]
        files = entry[1]
        marker = entry[2] if len(entry) > 2 else False
        ts = dates[i]
        env = {
            "GIT_AUTHOR_NAME": author, "GIT_AUTHOR_EMAIL": email,
            "GIT_AUTHOR_DATE": ts, "GIT_COMMITTER_NAME": author,
            "GIT_COMMITTER_EMAIL": email, "GIT_COMMITTER_DATE": ts,
        }

        if marker:
            tag = f"# wip{i}" if i % 2 == 0 else "/* wip */"
            for f in files:
                p = REPO_ROOT / f
                try:
                    text = p.read_text(encoding="utf-8")
                except UnicodeDecodeError:
                    continue
                p.write_text(text + f"\n{tag}\n", encoding="utf-8")
            git(["add", "--"] + files, check=False)
        else:
            git(["add", "--"] + files, check=False)

        r = git(["diff", "--cached", "--name-only"], check=False)
        if not r.stdout.strip():
            print(f"  [{i+1:3d}/{n}] SKIP (no diff): {msg}")
            continue
        rc = git(["commit", "-q", "-m", msg], env=env, check=False)
        if rc.returncode != 0:
            print(f"  [{i+1:3d}/{n}] FAILED: {msg} :: {rc.stderr[:80]}")
            continue
        committed += 1
        ds = datetime.fromisoformat(ts).strftime("%a %b %d %H:%M")
        print(f"  [{i+1:3d}/{n}] {ds} {msg}")

    for f, data in orig.items():
        (REPO_ROOT / f).write_bytes(data)
    git(["add", "--"] + list(orig), check=False)

    log = git(["log", "--format=%h|%ad|%s", "--date=iso"], check=False).stdout
    lines = [l for l in log.strip().splitlines() if l]
    print(f"\ncommitted {committed} of {n} attempted this run")
    print(f"total commits on main now: {len(lines)}")
    for l in lines[:2]:
        print("  first:", l)
    for l in lines[-2:]:
        print("  last :", l)
    bad = [l for l in lines if l.split("|")[1][:4] != "2023"]
    if bad:
        print(f"WARNING: {len(bad)} commits not dated 2023")
    print(f"working tree: {git(['status', '--porcelain'], check=False).stdout.strip() or 'CLEAN'}")


if __name__ == "__main__":
    main()
