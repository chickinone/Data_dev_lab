from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="mongo_batch_reconcile_3h",
    description="Run MongoDB target mart refresh and reconciliation every 3 hours.",
    start_date=datetime(2026, 1, 1),
    schedule="0 */3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["mongodb", "cdc", "batch"],
) as dag:
    run_mongo_reconcile = BashOperator(
        task_id="run_mongo_reconcile",
        bash_command="python -u /opt/airflow/batch/batch_mongo/mongo_batch.py",
        append_env=True,
        env={
            "MONGO_BATCH_JOB_NAME": os.environ["MONGO_BATCH_JOB_NAME"],
            "MONGO_HOST": os.environ["MONGO_HOST"],
            "MONGO_PORT": os.environ["MONGO_PORT"],
            "MONGO_DATABASE": os.environ["MONGO_DATABASE"],
            "MONGO_ROOT_USERNAME": os.environ["MONGO_ROOT_USERNAME"],
            "MONGO_ROOT_PASSWORD": os.environ["MONGO_ROOT_PASSWORD"],
            "MONGO_AUTH_SOURCE": os.environ["MONGO_AUTH_SOURCE"],
            "MONGO_REPLICA_SET": os.environ["MONGO_REPLICA_SET"],
            "MONGO_TARGET_HOST": os.environ["MONGO_TARGET_HOST"],
            "MONGO_TARGET_PORT": os.environ["MONGO_TARGET_PORT"],
            "MONGO_TARGET_DATABASE": os.environ["MONGO_TARGET_DATABASE"],
            "MONGO_TARGET_ROOT_USERNAME": os.environ["MONGO_TARGET_ROOT_USERNAME"],
            "MONGO_TARGET_ROOT_PASSWORD": os.environ["MONGO_TARGET_ROOT_PASSWORD"],
            "MONGO_TARGET_AUTH_SOURCE": os.environ["MONGO_TARGET_AUTH_SOURCE"],
        },
    )
