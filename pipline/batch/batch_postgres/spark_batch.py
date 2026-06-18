import json
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def required_env(name):
    value = os.getenv(name)
    if value is None or value == "":
        sys.exit(f"[spark-batch] missing required environment variable: {name}")
    return value


JOB_NAME = required_env("BATCH_JOB_NAME")
SPARK_MASTER = required_env("SPARK_MASTER")


DB = dict(
    host=required_env("TARGET_DB_HOST"),
    port=int(required_env("TARGET_DB_PORT")),
    dbname=required_env("TARGET_DB_NAME"),
    user=required_env("TARGET_DB_USER"),
    password=required_env("TARGET_DB_PASSWORD"),
)


def log(message):
    print(f"[spark-batch] {message}", flush=True)


def connect_db():
    return psycopg2.connect(**DB)


def ensure_ops_schema(cur):
    cur.execute("CREATE SCHEMA IF NOT EXISTS ops")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.batch_runs (
            batch_id                   BIGSERIAL PRIMARY KEY,
            job_name                   TEXT NOT NULL,
            started_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
            finished_at                TIMESTAMPTZ,
            status                     TEXT NOT NULL,
            duration_seconds           NUMERIC(12,3),
            raw_events_count           BIGINT,
            clean_customers_count      BIGINT,
            clean_orders_count         BIGINT,
            mart_daily_revenue_rows    BIGINT,
            mart_customer_summary_rows BIGINT,
            dq_issue_count             BIGINT,
            metrics                    JSONB,
            error_message              TEXT
        )
        """
    )


def scalar(cur, sql):
    cur.execute(sql)
    return cur.fetchone()[0]


def collect_metrics(cur):
    metrics = {
        "raw_events_count": scalar(cur, "SELECT COUNT(*) FROM raw.cdc_events"),
        "clean_customers_count": scalar(cur, "SELECT COUNT(*) FROM clean.customers"),
        "clean_orders_count": scalar(cur, "SELECT COUNT(*) FROM clean.orders"),
        "mart_daily_revenue_rows": scalar(cur, "SELECT COUNT(*) FROM mart.daily_revenue"),
        "mart_customer_summary_rows": scalar(cur, "SELECT COUNT(*) FROM mart.customer_summary"),
        "customers_unknown_country_count": scalar(
            cur,
            "SELECT COUNT(*) FROM clean.customers WHERE country = 'UNKNOWN'",
        ),
        "orders_without_customer_count": scalar(
            cur,
            """
            SELECT COUNT(*)
            FROM clean.orders o
            LEFT JOIN clean.customers c ON c.id = o.customer_id
            WHERE c.id IS NULL
            """,
        ),
        "orders_invalid_quantity_count": scalar(
            cur,
            "SELECT COUNT(*) FROM clean.orders WHERE quantity < 0",
        ),
        "orders_negative_price_count": scalar(
            cur,
            "SELECT COUNT(*) FROM clean.orders WHERE unit_price < 0",
        ),
        "mart_unknown_country_rows": scalar(
            cur,
            "SELECT COUNT(*) FROM mart.daily_revenue WHERE country = 'UNKNOWN'",
        ),
    }
    return metrics


def compute_dq_issue_count(spark, metrics):
    issue_names = {
        "customers_unknown_country_count",
        "orders_without_customer_count",
        "orders_invalid_quantity_count",
        "orders_negative_price_count",
        "mart_unknown_country_rows",
    }
    rows = [(name, int(value or 0)) for name, value in metrics.items()]
    df = spark.createDataFrame(rows, ["metric_name", "metric_value"])
    result = (
        df.where(F.col("metric_name").isin(sorted(issue_names)))
        .agg(F.sum("metric_value").alias("dq_issue_count"))
        .first()
    )
    return int(result["dq_issue_count"] or 0)


def insert_batch_run(cur):
    cur.execute(
        """
        INSERT INTO ops.batch_runs (job_name, status)
        VALUES (%s, 'RUNNING')
        RETURNING batch_id, started_at
        """,
        (JOB_NAME,),
    )
    return cur.fetchone()


def finish_batch_run(cur, batch_id, started_at, status, metrics, dq_issue_count, error_message=None):
    finished_at = datetime.now(timezone.utc)
    duration = (finished_at - started_at).total_seconds()
    cur.execute(
        """
        UPDATE ops.batch_runs
        SET finished_at = %s,
            status = %s,
            duration_seconds = %s,
            raw_events_count = %s,
            clean_customers_count = %s,
            clean_orders_count = %s,
            mart_daily_revenue_rows = %s,
            mart_customer_summary_rows = %s,
            dq_issue_count = %s,
            metrics = %s,
            error_message = %s
        WHERE batch_id = %s
        """,
        (
            finished_at,
            status,
            duration,
            metrics.get("raw_events_count"),
            metrics.get("clean_customers_count"),
            metrics.get("clean_orders_count"),
            metrics.get("mart_daily_revenue_rows"),
            metrics.get("mart_customer_summary_rows"),
            dq_issue_count,
            Json(metrics),
            error_message,
            batch_id,
        ),
    )


def main():
    started = time.time()
    spark = (
        SparkSession.builder.appName(JOB_NAME)
        .master(SPARK_MASTER)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    conn = connect_db()
    conn.autocommit = False
    batch_id = None
    started_at = None

    try:
        cur = conn.cursor()
        ensure_ops_schema(cur)
        batch_id, started_at = insert_batch_run(cur)
        conn.commit()
        log(f"started batch_id={batch_id}")

        cur.execute("SELECT mart.refresh_all()")
        metrics = collect_metrics(cur)
        dq_issue_count = compute_dq_issue_count(spark, metrics)
        status = "SUCCESS" if dq_issue_count == 0 else "SUCCESS_WITH_WARNINGS"

        finish_batch_run(cur, batch_id, started_at, status, metrics, dq_issue_count)
        conn.commit()
        log(f"finished batch_id={batch_id} status={status} metrics={json.dumps(metrics, default=str)}")
    except Exception as exc:
        conn.rollback()
        log(f"failed: {exc}")
        if batch_id is not None:
            cur = conn.cursor()
            finish_batch_run(cur, batch_id, started_at, "FAILED", {}, 0, str(exc))
            conn.commit()
        raise
    finally:
        conn.close()
        spark.stop()
        log(f"elapsed_seconds={time.time() - started:.3f}")


if __name__ == "__main__":
    main()
