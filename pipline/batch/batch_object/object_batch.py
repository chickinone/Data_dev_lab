import json
import os
import sys
import time
from datetime import datetime, timezone
from urllib.parse import quote_plus, urlparse

from minio import Minio
from pymongo import MongoClient


def required_env(name):
    value = os.getenv(name)
    if value is None or value == "":
        sys.exit(f"[object-batch] missing required environment variable: {name}")
    return value


JOB_NAME = required_env("OBJECT_BATCH_JOB_NAME")
TARGET_BUCKETS = required_env("MINIO_TARGET_BUCKETS").split()

MINIO_TARGET = {
    "endpoint": required_env("MINIO_TARGET_ENDPOINT"),
    "access_key": required_env("MINIO_TARGET_ROOT_USER"),
    "secret_key": required_env("MINIO_TARGET_ROOT_PASSWORD"),
}
MONGO_TARGET = {
    "host": required_env("MONGO_TARGET_HOST"),
    "port": required_env("MONGO_TARGET_PORT"),
    "database": required_env("MONGO_TARGET_DATABASE"),
    "user": required_env("MONGO_TARGET_ROOT_USERNAME"),
    "password": required_env("MONGO_TARGET_ROOT_PASSWORD"),
    "auth_source": required_env("MONGO_TARGET_AUTH_SOURCE"),
}


def log(message):
    print(f"[object-batch] {message}", flush=True)


def utc_now():
    return datetime.now(timezone.utc)


def minio_client(cfg):
    parsed = urlparse(cfg["endpoint"])
    endpoint = parsed.netloc or parsed.path
    secure = parsed.scheme == "https"
    return Minio(
        endpoint,
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        secure=secure,
    )

def mongo_uri():
    user = quote_plus(MONGO_TARGET["user"])
    password = quote_plus(MONGO_TARGET["password"])
    return (
        f"mongodb://{user}:{password}@{MONGO_TARGET['host']}:{MONGO_TARGET['port']}/"
        f"{MONGO_TARGET['database']}?authSource={MONGO_TARGET['auth_source']}"
    )


def connect_mongo():
    client = MongoClient(mongo_uri(), serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client


def replace_collection(db, name, docs):
    db[name].delete_many({})
    if docs:
        db[name].insert_many(docs)


def scan_target_objects(client):
    objects = {}
    for bucket in TARGET_BUCKETS:
        for obj in client.list_objects(bucket, recursive=True):
            if obj.is_dir:
                continue
            stat = client.stat_object(bucket, obj.object_name)
            objects[(bucket, obj.object_name)] = {
                "bucket": bucket,
                "object_key": obj.object_name,
                "size_bytes": stat.size,
                "etag": stat.etag,
                "last_modified": stat.last_modified,
            }
    return objects


def refresh_object_marts(db):
    now = utc_now()
    by_type = []
    for row in db.clean_object_metadata.aggregate([
        {"$match": {"status": "ACTIVE"}},
        {
            "$group": {
                "_id": "$media_type",
                "total_objects": {"$sum": 1},
                "total_size_bytes": {"$sum": "$size_bytes"},
                "avg_size_bytes": {"$avg": "$size_bytes"},
                "latest_ingested_at": {"$max": "$ingested_at"},
            }
        }
    ]):
        by_type.append({
            "media_type": row["_id"],
            "total_objects": row.get("total_objects", 0),
            "total_size_bytes": row.get("total_size_bytes", 0),
            "avg_size_bytes": round(float(row.get("avg_size_bytes") or 0), 3),
            "latest_ingested_at": row.get("latest_ingested_at"),
            "_refreshed_at": now,
        })
    replace_collection(db, "mart_object_summary_by_type", by_type)

    by_day = []
    for row in db.clean_object_metadata.aggregate([
        {"$match": {"status": "ACTIVE"}},
        {
            "$group": {
                "_id": "$ingested_day",
                "total_objects": {"$sum": 1},
                "total_size_bytes": {"$sum": "$size_bytes"},
            }
        }
    ]):
        by_day.append({
            "ingested_day": row["_id"],
            "total_objects": row.get("total_objects", 0),
            "total_size_bytes": row.get("total_size_bytes", 0),
            "_refreshed_at": now,
        })
    replace_collection(db, "mart_object_summary_by_day", by_day)

    totals = list(db.clean_object_metadata.aggregate([
        {"$match": {"status": "ACTIVE"}},
        {
            "$group": {
                "_id": None,
                "total_objects": {"$sum": 1},
                "total_size_bytes": {"$sum": "$size_bytes"},
            }
        }
    ]))
    total = totals[0] if totals else {}
    db.mart_object_storage_summary.replace_one(
        {"_id": "current"},
        {
            "_id": "current",
            "total_objects": total.get("total_objects", 0),
            "total_size_bytes": total.get("total_size_bytes", 0),
            "_refreshed_at": now,
        },
        upsert=True,
    )
def reconcile_status_and_collect_metrics(db, target_objects):
    now = utc_now()
    metadata_docs = list(db.clean_object_metadata.find({}))
    metadata_keys = {
        (doc.get("target_bucket"), doc.get("target_object_key")): doc
        for doc in metadata_docs
    }
    object_keys = set(target_objects.keys())
    metadata_key_set = set(metadata_keys.keys())

    missing_metadata = object_keys - metadata_key_set
    missing_objects = metadata_key_set - object_keys
    size_mismatch = []

    if missing_objects:
        db.clean_object_metadata.update_many(
            {
                "$or": [
                    {"target_bucket": bucket, "target_object_key": key}
                    for bucket, key in missing_objects
                ]
            },
            {
                "$set": {
                    "status": "MISSING",
                    "missing_detected_at": now,
                    "last_reconciled_at": now,
                }
            },
        )

    active_keys = metadata_key_set & object_keys
    if active_keys:
        db.clean_object_metadata.update_many(
            {
                "$or": [
                    {"target_bucket": bucket, "target_object_key": key}
                    for bucket, key in active_keys
                ]
            },
            {
                "$set": {
                    "status": "ACTIVE",
                    "last_reconciled_at": now,
                },
                "$unset": {"missing_detected_at": ""},
            },
        )

    for key in object_keys & metadata_key_set:
        object_size = target_objects[key]["size_bytes"]
        metadata_size = metadata_keys[key].get("size_bytes")
        if object_size != metadata_size:
            size_mismatch.append(key)

    return {
        "target_object_count": len(object_keys),
        "clean_object_metadata_count": len(metadata_docs),
        "active_metadata_count": db.clean_object_metadata.count_documents({"status": "ACTIVE"}),
        "missing_metadata_status_count": db.clean_object_metadata.count_documents({"status": "MISSING"}),
        "missing_metadata_count": len(missing_metadata),
        "missing_object_count": len(missing_objects),
        "size_mismatch_count": len(size_mismatch),
        "raw_object_events_count": db.raw_object_events.count_documents({}),
        "mart_object_summary_by_type_rows": db.mart_object_summary_by_type.count_documents({}),
        "mart_object_summary_by_day_rows": db.mart_object_summary_by_day.count_documents({}),
        "mart_object_storage_summary_rows": db.mart_object_storage_summary.count_documents({}),
        "missing_metadata_samples": [f"{bucket}/{key}" for bucket, key in sorted(missing_metadata)[:10]],
        "missing_object_samples": [f"{bucket}/{key}" for bucket, key in sorted(missing_objects)[:10]],
        "size_mismatch_samples": [f"{bucket}/{key}" for bucket, key in sorted(size_mismatch)[:10]],
    }


def compute_dq_issue_count(metrics):
    return (
        int(metrics.get("missing_metadata_count") or 0)
        + int(metrics.get("missing_object_count") or 0)
        + int(metrics.get("size_mismatch_count") or 0)
    )


def insert_batch_run(db):
    started_at = utc_now()
    result = db.ops_object_batch_runs.insert_one({
        "job_name": JOB_NAME,
        "started_at": started_at,
        "status": "RUNNING",
    })
    return result.inserted_id, started_at


def finish_batch_run(db, batch_id, started_at, status, metrics, dq_issue_count, error_message=None):
    finished_at = utc_now()
    db.ops_object_batch_runs.update_one(
        {"_id": batch_id},
        {
            "$set": {
                "finished_at": finished_at,
                "status": status,
                "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
                "metrics": metrics,
                "dq_issue_count": dq_issue_count,
                "error_message": error_message,
            }
        },
    )


def main():
    started = time.time()
    minio = minio_client(MINIO_TARGET)
    mongo = connect_mongo()
    db = mongo[MONGO_TARGET["database"]]
    batch_id = None
    started_at = None

    try:
        batch_id, started_at = insert_batch_run(db)
        log(f"started batch_id={batch_id}")
        target_objects = scan_target_objects(minio)
        metrics = reconcile_status_and_collect_metrics(db, target_objects)
        refresh_object_marts(db)
        metrics.update({
            "mart_object_summary_by_type_rows": db.mart_object_summary_by_type.count_documents({}),
            "mart_object_summary_by_day_rows": db.mart_object_summary_by_day.count_documents({}),
            "mart_object_storage_summary_rows": db.mart_object_storage_summary.count_documents({}),
        })
        dq_issue_count = compute_dq_issue_count(metrics)
        status = "SUCCESS" if dq_issue_count == 0 else "SUCCESS_WITH_WARNINGS"
        finish_batch_run(db, batch_id, started_at, status, metrics, dq_issue_count)
        log(f"finished batch_id={batch_id} status={status} metrics={json.dumps(metrics, default=str)}")
    except Exception as exc:
        log(f"failed: {exc}")
        if batch_id is not None:
            finish_batch_run(db, batch_id, started_at, "FAILED", {}, 0, str(exc))
        raise
    finally:
        mongo.close()
        log(f"elapsed_seconds={time.time() - started:.3f}")


if __name__ == "__main__":
    main()
