from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="cdc_batch_reconcile_3h",
    description="Run lightweight Spark reconciliation for the CDC pipeline every 3 hours.",
    start_date=datetime(2026, 1, 1),
    schedule="0 */3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "spark", "batch"],
) as dag:
    run_spark_reconcile = BashOperator(
        task_id="run_spark_reconcile",
        bash_command="python -u /opt/airflow/batch/batch_postgres/spark_batch.py",
        append_env=True,
        env={
            "BATCH_JOB_NAME": os.environ["BATCH_JOB_NAME"],
            "SPARK_MASTER": os.environ["SPARK_MASTER"],
            "TARGET_DB_HOST": os.environ["TARGET_DB_HOST"],
            "TARGET_DB_PORT": os.environ["TARGET_DB_PORT"],
            "TARGET_DB_NAME": os.environ["TARGET_DB_NAME"],
            "TARGET_DB_USER": os.environ["TARGET_DB_USER"],
            "TARGET_DB_PASSWORD": os.environ["TARGET_DB_PASSWORD"],
        },
    )
