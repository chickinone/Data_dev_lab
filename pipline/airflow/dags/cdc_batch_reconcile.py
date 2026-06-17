from __future__ import annotations

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
        bash_command="python -u /opt/airflow/batch/spark_batch.py",
        append_env=True,
        env={
            "BATCH_JOB_NAME": "cdc_spark_reconcile",
            "SPARK_MASTER": "local[*]",
            "TARGET_DB_HOST": "postgres-target",
            "TARGET_DB_PORT": "5432",
            "TARGET_DB_NAME": "targetdb",
            "TARGET_DB_USER": "postgres",
            "TARGET_DB_PASSWORD": "postgres",
        },
    )
