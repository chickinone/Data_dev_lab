from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="object_batch_reconcile_3h",
    description="Run MinIO object metadata reconciliation every 3 hours.",
    start_date=datetime(2026, 1, 1),
    schedule="0 */3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["minio", "object", "batch"],
) as dag:
    run_object_reconcile = BashOperator(
        task_id="run_object_reconcile",
        bash_command="python -u /opt/airflow/batch/batch_object/object_batch.py",
        append_env=True,
        env={
            "OBJECT_BATCH_JOB_NAME": os.environ["OBJECT_BATCH_JOB_NAME"],
            "MINIO_TARGET_ENDPOINT": os.environ["MINIO_TARGET_ENDPOINT"],
            "MINIO_TARGET_ROOT_USER": os.environ["MINIO_TARGET_ROOT_USER"],
            "MINIO_TARGET_ROOT_PASSWORD": os.environ["MINIO_TARGET_ROOT_PASSWORD"],
            "MINIO_TARGET_BUCKETS": os.environ["MINIO_TARGET_BUCKETS"],
            "MONGO_TARGET_HOST": os.environ["MONGO_TARGET_HOST"],
            "MONGO_TARGET_PORT": os.environ["MONGO_TARGET_PORT"],
            "MONGO_TARGET_DATABASE": os.environ["MONGO_TARGET_DATABASE"],
            "MONGO_TARGET_ROOT_USERNAME": os.environ["MONGO_TARGET_ROOT_USERNAME"],
            "MONGO_TARGET_ROOT_PASSWORD": os.environ["MONGO_TARGET_ROOT_PASSWORD"],
            "MONGO_TARGET_AUTH_SOURCE": os.environ["MONGO_TARGET_AUTH_SOURCE"],
        },
    )
