import json
import os
import sys
import time
from datetime import datetime, timezone
from urllib.parse import quote_plus

from pymongo import MongoClient


def required_env(name):
    value = os.getenv(name)
    if value is None or value == "":
        sys.exit(f"[mongo-batch] missing required environment variable: {name}")
    return value


JOB_NAME = required_env("MONGO_BATCH_JOB_NAME")

SOURCE = {
    "host": required_env("MONGO_HOST"),
    "port": required_env("MONGO_PORT"),
    "database": required_env("MONGO_DATABASE"),
    "user": required_env("MONGO_ROOT_USERNAME"),
    "password": required_env("MONGO_ROOT_PASSWORD"),
    "auth_source": required_env("MONGO_AUTH_SOURCE"),
    "replica_set": required_env("MONGO_REPLICA_SET"),
}

TARGET = {
    "host": required_env("MONGO_TARGET_HOST"),
    "port": required_env("MONGO_TARGET_PORT"),
    "database": required_env("MONGO_TARGET_DATABASE"),
    "user": required_env("MONGO_TARGET_ROOT_USERNAME"),
    "password": required_env("MONGO_TARGET_ROOT_PASSWORD"),
    "auth_source": required_env("MONGO_TARGET_AUTH_SOURCE"),
}


def log(message):
    print(f"[mongo-batch] {message}", flush=True)


def mongo_uri(cfg, include_replica_set=False):
    user = quote_plus(cfg["user"])
    password = quote_plus(cfg["password"])
    uri = (
        f"mongodb://{user}:{password}@{cfg['host']}:{cfg['port']}/"
        f"{cfg['database']}?authSource={cfg['auth_source']}"
    )
    if include_replica_set:
        uri += f"&replicaSet={cfg['replica_set']}"
    return uri


def connect(cfg, include_replica_set=False):
    client = MongoClient(mongo_uri(cfg, include_replica_set=include_replica_set), serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client


def scalar(collection):
    return collection.count_documents({})


def replace_collection(db, name, docs):
    db[name].delete_many({})
    if docs:
        db[name].insert_many(docs)


def refresh_product_review_summary(db):
    now = datetime.now(timezone.utc)
    docs = []
    for row in db.clean_product_reviews.aggregate([
        {
            "$group": {
                "_id": "$product_id",
                "review_count": {"$sum": 1},
                "avg_rating": {"$avg": "$rating"},
                "approved_count": {"$sum": {"$cond": [{"$eq": ["$status", "APPROVED"]}, 1, 0]}},
                "pending_count": {"$sum": {"$cond": [{"$eq": ["$status", "PENDING"]}, 1, 0]}},
                "rejected_count": {"$sum": {"$cond": [{"$eq": ["$status", "REJECTED"]}, 1, 0]}},
                "last_review_at": {"$max": "$updated_at"},
            }
        }
    ]):
        docs.append({
            "product_id": row["_id"],
            "review_count": row.get("review_count", 0),
            "avg_rating": round(float(row.get("avg_rating") or 0), 3),
            "approved_count": row.get("approved_count", 0),
            "pending_count": row.get("pending_count", 0),
            "rejected_count": row.get("rejected_count", 0),
            "last_review_at": row.get("last_review_at"),
            "_refreshed_at": now,
        })
    replace_collection(db, "mart_product_review_summary", docs)


def refresh_customer_engagement_summary(db):
    now = datetime.now(timezone.utc)
    customer_ids = set()
    customer_ids.update(db.clean_customer_profiles.distinct("customer_id"))
    customer_ids.update(db.clean_product_reviews.distinct("customer_id"))
    customer_ids.update(db.clean_click_events.distinct("customer_id"))

    docs = []
    for customer_id in customer_ids:
        profile = db.clean_customer_profiles.find_one({"customer_id": customer_id}) or {}
        clicks = list(db.clean_click_events.aggregate([
            {"$match": {"customer_id": customer_id}},
            {"$group": {"_id": "$customer_id", "total_clicks": {"$sum": 1}, "last_click_at": {"$max": "$event_time"}}},
        ]))
        reviews = list(db.clean_product_reviews.aggregate([
            {"$match": {"customer_id": customer_id}},
            {
                "$group": {
                    "_id": "$customer_id",
                    "total_reviews": {"$sum": 1},
                    "avg_rating_given": {"$avg": "$rating"},
                    "last_review_at": {"$max": "$updated_at"},
                }
            },
        ]))
        click_row = clicks[0] if clicks else {}
        review_row = reviews[0] if reviews else {}
        docs.append({
            "customer_id": customer_id,
            "full_name": profile.get("full_name"),
            "country": profile.get("country", "UNKNOWN"),
            "loyalty_tier": (profile.get("loyalty") or {}).get("tier"),
            "total_clicks": click_row.get("total_clicks", 0),
            "total_reviews": review_row.get("total_reviews", 0),
            "avg_rating_given": round(float(review_row.get("avg_rating_given") or 0), 3),
            "last_click_at": click_row.get("last_click_at"),
            "last_review_at": review_row.get("last_review_at"),
            "_refreshed_at": now,
        })
    replace_collection(db, "mart_customer_engagement_summary", docs)


def refresh_country_profile_summary(db):
    now = datetime.now(timezone.utc)
    docs = []
    for row in db.clean_customer_profiles.aggregate([
        {
            "$group": {
                "_id": "$country",
                "profile_count": {"$sum": 1},
                "gold_customers": {"$sum": {"$cond": [{"$eq": ["$loyalty.tier", "GOLD"]}, 1, 0]}},
                "newsletter_opt_in": {"$sum": {"$cond": [{"$eq": ["$preferences.newsletter", True]}, 1, 0]}},
            }
        }
    ]):
        docs.append({
            "country": row["_id"],
            "profile_count": row.get("profile_count", 0),
            "gold_customers": row.get("gold_customers", 0),
            "newsletter_opt_in": row.get("newsletter_opt_in", 0),
            "_refreshed_at": now,
        })
    replace_collection(db, "mart_country_profile_summary", docs)


def refresh_all_marts(db):
    refresh_product_review_summary(db)
    refresh_customer_engagement_summary(db)
    refresh_country_profile_summary(db)


def collect_metrics(source_db, target_db):
    return {
        "source_customer_profiles_count": scalar(source_db.customer_profiles),
        "target_raw_customer_profiles_count": scalar(target_db.raw_customer_profiles),
        "target_clean_customer_profiles_count": scalar(target_db.clean_customer_profiles),
        "source_product_reviews_count": scalar(source_db.product_reviews),
        "target_raw_product_reviews_count": scalar(target_db.raw_product_reviews),
        "target_clean_product_reviews_count": scalar(target_db.clean_product_reviews),
        "source_click_events_count": scalar(source_db.click_events),
        "target_raw_click_events_count": scalar(target_db.raw_click_events),
        "target_clean_click_events_count": scalar(target_db.clean_click_events),
        "mart_product_review_summary_rows": scalar(target_db.mart_product_review_summary),
        "mart_customer_engagement_summary_rows": scalar(target_db.mart_customer_engagement_summary),
        "mart_country_profile_summary_rows": scalar(target_db.mart_country_profile_summary),
        "unknown_country_profiles_count": target_db.clean_customer_profiles.count_documents({"country": "UNKNOWN"}),
        "invalid_review_rating_count": target_db.clean_product_reviews.count_documents({"$or": [{"rating": {"$lt": 0}}, {"rating": {"$gt": 5}}]}),
        "missing_review_product_id_count": target_db.clean_product_reviews.count_documents({"product_id": 0}),
        "missing_click_event_type_count": target_db.clean_click_events.count_documents({"event_type": "UNKNOWN"}),
    }


def compute_dq_issue_count(metrics):
    issue_keys = [
        "unknown_country_profiles_count",
        "invalid_review_rating_count",
        "missing_review_product_id_count",
        "missing_click_event_type_count",
    ]
    mismatch_pairs = [
        ("source_customer_profiles_count", "target_clean_customer_profiles_count"),
        ("source_product_reviews_count", "target_clean_product_reviews_count"),
        ("source_click_events_count", "target_clean_click_events_count"),
    ]
    count = sum(int(metrics.get(key) or 0) for key in issue_keys)
    for left, right in mismatch_pairs:
        if metrics.get(left) != metrics.get(right):
            count += 1
    return count


def insert_batch_run(db):
    started_at = datetime.now(timezone.utc)
    result = db.ops_batch_runs.insert_one({
        "job_name": JOB_NAME,
        "started_at": started_at,
        "status": "RUNNING",
    })
    return result.inserted_id, started_at


def finish_batch_run(db, batch_id, started_at, status, metrics, dq_issue_count, error_message=None):
    finished_at = datetime.now(timezone.utc)
    db.ops_batch_runs.update_one(
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
    source_client = connect(SOURCE, include_replica_set=True)
    target_client = connect(TARGET)
    source_db = source_client[SOURCE["database"]]
    target_db = target_client[TARGET["database"]]
    batch_id = None
    started_at = None

    try:
        batch_id, started_at = insert_batch_run(target_db)
        log(f"started batch_id={batch_id}")
        refresh_all_marts(target_db)
        metrics = collect_metrics(source_db, target_db)
        dq_issue_count = compute_dq_issue_count(metrics)
        status = "SUCCESS" if dq_issue_count == 0 else "SUCCESS_WITH_WARNINGS"
        finish_batch_run(target_db, batch_id, started_at, status, metrics, dq_issue_count)
        log(f"finished batch_id={batch_id} status={status} metrics={json.dumps(metrics, default=str)}")
    except Exception as exc:
        log(f"failed: {exc}")
        if batch_id is not None:
            finish_batch_run(target_db, batch_id, started_at, "FAILED", {}, 0, str(exc))
        raise
    finally:
        source_client.close()
        target_client.close()
        log(f"elapsed_seconds={time.time() - started:.3f}")


if __name__ == "__main__":
    main()
