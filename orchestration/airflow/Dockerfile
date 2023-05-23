# Local Airflow image: 2.7.3 + Java 17 + a Spark 3.5.0 client, so DAGs can
# spark-submit the batch jobs to the stack's Spark cluster via
# SparkSubmitOperator. Only standard providers are used — the DAG code is
# MWAA-compatible unchanged (see orchestration/mwaa_bootstrap/).
FROM apache/airflow:2.7.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
      "pyspark==3.5.0" \
      "apache-airflow-providers-apache-spark==4.1.2"

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="/home/airflow/.local/bin:${PATH}" \
    PYSPARK_PYTHON=python3
