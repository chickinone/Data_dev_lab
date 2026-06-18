import json
import os
import sys
import time
from datetime import datetime, timezone
from urllib.parse import quote_plus

from bson import ObjectId, json_util
from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError


def required_env(name):
    value = os.getenv(name)
    if value is None or value == "":
        sys.exit(f"[mongo-sink-consumer] missing required environment variable: {name}")
    return value


KAFKA_BOOTSTRAP = required_env("KAFKA_BOOTSTRAP")
MONGO_TOPIC_PREFIX = required_env("MONGO_TOPIC_PREFIX")
MONGO_SOURCE_DB = required_env("MONGO_DATABASE")
MONGO_DLQ_TOPIC = required_env("MONGO_DLQ_TOPIC")
MONGO_SINK_GROUP_ID = required_env("MONGO_SINK_GROUP_ID")
MAX_RETRIES = int(required_env("MAX_RETRIES"))

COLLECTIONS = ("customer_profiles", "product_reviews", "click_events")
TOPICS = [f"{MONGO_TOPIC_PREFIX}.{MONGO_SOURCE_DB}.{collection}" for collection in COLLECTIONS]


TARGET = {
    "host": required_env("MONGO_TARGET_HOST"),
    "port": required_env("MONGO_TARGET_PORT"),
    "database": required_env("MONGO_TARGET_DATABASE"),
    "user": required_env("MONGO_TARGET_ROOT_USERNAME"),
    "password": required_env("MONGO_TARGET_ROOT_PASSWORD"),
    "auth_source": required_env("MONGO_TARGET_AUTH_SOURCE"),
}

def log(message):
    print(f"[mongo-sink-consumer] {message}", flush=True)


def target_uri():
    user = quote_plus(TARGET["user"])
    password = quote_plus(TARGET["password"])
    return (
        f"mongodb://{user}:{password}@{TARGET['host']}:{TARGET['port']}/"
        f"{TARGET['database']}?authSource={TARGET['auth_source']}"
    )

def connect_target():
    uri = target_uri()
    for attempt in range(1, 31):
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            log("connected to MongoDB target")
            return client
        except ServerSelectionTimeoutError as exc:
            log(f"MongoDB target not ready (attempt {attempt}): {exc}")
            time.sleep(3)
    sys.exit("[mongo-sink-consumer] could not connect to MongoDB target")


def ensure_indexes(db):
    db.raw_customer_profiles.create_index("customer_id", unique=True, sparse=True)
    db.clean_customer_profiles.create_index("customer_id", unique=True, sparse=True)
    db.raw_product_reviews.create_index("review_id", unique=True, sparse=True)
    db.clean_product_reviews.create_index("review_id", unique=True, sparse=True)
    db.raw_click_events.create_index("event_id", unique=True, sparse=True)
    db.clean_click_events.create_index("event_id", unique=True, sparse=True)
    db["_cdc_offsets"].create_index(
        [("source_topic", 1), ("source_partition", 1), ("source_offset", 1)],
        unique=True,
    )
    db.ops_failed_events.create_index(
        [("source_topic", 1), ("source_partition", 1), ("source_offset", 1)],
    )
    db.ops_failed_events.create_index("failed_at")
    db.mart_product_review_summary.create_index("product_id", unique=True)
    db.mart_customer_engagement_summary.create_index("customer_id", unique=True)
    db.mart_country_profile_summary.create_index("country", unique=True)


def decode_bytes(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def loads_json(value):
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, bytes):
        value = decode_bytes(value)
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except Exception:
        try:
            return json_util.loads(value)
        except Exception:
            return {"_raw": value}


def loads_mongo_doc(value):
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, bytes):
        value = decode_bytes(value)
    if isinstance(value, str):
        return json_util.loads(value)
    return value


def clean_str(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def safe_int(value, default=0, minimum=None, maximum=None):
    try:
        result = int(value)
    except (TypeError, ValueError):
        result = default
    if minimum is not None:
        result = max(result, minimum)
    if maximum is not None:
        result = min(result, maximum)
    return result


def clean_bool(value):
    return value if isinstance(value, bool) else bool(value)


def normalize_country(value):
    country = clean_str(value)
    if not country:
        return "UNKNOWN"

    country = country.upper()
    if country in ("VIETNAM", "VIET NAM", "VN"):
        return "VIETNAM"
    if country in ("USA", "UNITED STATES", "US"):
        return "USA"
    if country == "JAPAN":
        return "JAPAN"
    if country == "SINGAPORE":
        return "SINGAPORE"
    return country


def normalize_upper(value, default=None):
    text = clean_str(value)
    if not text:
        return default
    return text.upper()


def clean_list(values):
    if not isinstance(values, list):
        return []
    cleaned = []
    for value in values:
        text = clean_str(value)
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned


def transform_customer_profile(doc):
    preferences = doc.get("preferences") if isinstance(doc.get("preferences"), dict) else {}
    loyalty = doc.get("loyalty") if isinstance(doc.get("loyalty"), dict) else {}
    cleaned = dict(doc)
    cleaned["full_name"] = clean_str(doc.get("full_name"))
    cleaned["country"] = normalize_country(doc.get("country"))
    cleaned["preferences"] = {
        "category": clean_str(preferences.get("category")),
        "newsletter": clean_bool(preferences.get("newsletter")),
        "price_sensitivity": normalize_upper(preferences.get("price_sensitivity"), "UNKNOWN"),
    }
    cleaned["loyalty"] = {
        "tier": normalize_upper(loyalty.get("tier"), "BRONZE"),
        "points": safe_int(loyalty.get("points"), default=0, minimum=0),
    }
    cleaned["devices"] = clean_list(doc.get("devices"))
    return cleaned


def transform_product_review(doc):
    cleaned = dict(doc)
    cleaned["customer_id"] = safe_int(doc.get("customer_id"), default=0, minimum=0)
    cleaned["product_id"] = safe_int(doc.get("product_id"), default=0, minimum=0)
    cleaned["rating"] = safe_int(doc.get("rating"), default=0, minimum=0, maximum=5)
    cleaned["status"] = normalize_upper(doc.get("status"), "PENDING")
    cleaned["title"] = clean_str(doc.get("title"))
    cleaned["body"] = clean_str(doc.get("body"))
    cleaned["tags"] = clean_list(doc.get("tags"))
    return cleaned


def transform_click_event(doc):
    metadata = doc.get("metadata") if isinstance(doc.get("metadata"), dict) else {}
    cleaned = dict(doc)
    cleaned["customer_id"] = safe_int(doc.get("customer_id"), default=0, minimum=0)
    cleaned["product_id"] = safe_int(doc.get("product_id"), default=0, minimum=0)
    cleaned["event_type"] = normalize_upper(doc.get("event_type"), "UNKNOWN")
    cleaned["session_id"] = clean_str(doc.get("session_id"))
    cleaned["metadata"] = {
        "user_agent": clean_str(metadata.get("user_agent")),
        "referrer": normalize_upper(metadata.get("referrer"), "UNKNOWN"),
        "page": normalize_upper(metadata.get("page"), "UNKNOWN"),
    }
    return cleaned


TRANSFORMS = {
    "customer_profiles": transform_customer_profile,
    "product_reviews": transform_product_review,
    "click_events": transform_click_event,
}


def refresh_product_review_summary(db, product_id):
    if product_id is None:
        return

    rows = list(db.clean_product_reviews.aggregate([
        {"$match": {"product_id": product_id}},
        {
            "$group": {
                "_id": "$product_id",
                "review_count": {"$sum": 1},
                "avg_rating": {"$avg": "$rating"},
                "approved_count": {
                    "$sum": {"$cond": [{"$eq": ["$status", "APPROVED"]}, 1, 0]}
                },
                "pending_count": {
                    "$sum": {"$cond": [{"$eq": ["$status", "PENDING"]}, 1, 0]}
                },
                "rejected_count": {
                    "$sum": {"$cond": [{"$eq": ["$status", "REJECTED"]}, 1, 0]}
                },
                "last_review_at": {"$max": "$updated_at"},
            }
        },
    ]))
    if not rows:
        db.mart_product_review_summary.delete_one({"product_id": product_id})
        return

    row = rows[0]
    doc = {
        "product_id": row["_id"],
        "review_count": row.get("review_count", 0),
        "avg_rating": round(float(row.get("avg_rating") or 0), 3),
        "approved_count": row.get("approved_count", 0),
        "pending_count": row.get("pending_count", 0),
        "rejected_count": row.get("rejected_count", 0),
        "last_review_at": row.get("last_review_at"),
        "_refreshed_at": datetime.now(timezone.utc),
    }
    db.mart_product_review_summary.replace_one({"product_id": product_id}, doc, upsert=True)


def refresh_customer_engagement_summary(db, customer_id):
    if customer_id is None:
        return

    profile = db.clean_customer_profiles.find_one({"customer_id": customer_id}) or {}
    clicks = list(db.clean_click_events.aggregate([
        {"$match": {"customer_id": customer_id}},
        {
            "$group": {
                "_id": "$customer_id",
                "total_clicks": {"$sum": 1},
                "last_click_at": {"$max": "$event_time"},
            }
        },
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

    if not profile and not click_row and not review_row:
        db.mart_customer_engagement_summary.delete_one({"customer_id": customer_id})
        return

    doc = {
        "customer_id": customer_id,
        "full_name": profile.get("full_name"),
        "country": profile.get("country", "UNKNOWN"),
        "loyalty_tier": (profile.get("loyalty") or {}).get("tier"),
        "total_clicks": click_row.get("total_clicks", 0),
        "total_reviews": review_row.get("total_reviews", 0),
        "avg_rating_given": round(float(review_row.get("avg_rating_given") or 0), 3),
        "last_click_at": click_row.get("last_click_at"),
        "last_review_at": review_row.get("last_review_at"),
        "_refreshed_at": datetime.now(timezone.utc),
    }
    db.mart_customer_engagement_summary.replace_one({"customer_id": customer_id}, doc, upsert=True)


def refresh_country_profile_summary(db, country):
    if not country:
        return

    rows = list(db.clean_customer_profiles.aggregate([
        {"$match": {"country": country}},
        {
            "$group": {
                "_id": "$country",
                "profile_count": {"$sum": 1},
                "gold_customers": {
                    "$sum": {"$cond": [{"$eq": ["$loyalty.tier", "GOLD"]}, 1, 0]}
                },
                "newsletter_opt_in": {
                    "$sum": {"$cond": [{"$eq": ["$preferences.newsletter", True]}, 1, 0]}
                },
            }
        },
    ]))
    if not rows:
        db.mart_country_profile_summary.delete_one({"country": country})
        return

    row = rows[0]
    doc = {
        "country": row["_id"],
        "profile_count": row.get("profile_count", 0),
        "gold_customers": row.get("gold_customers", 0),
        "newsletter_opt_in": row.get("newsletter_opt_in", 0),
        "_refreshed_at": datetime.now(timezone.utc),
    }
    db.mart_country_profile_summary.replace_one({"country": country}, doc, upsert=True)


def refresh_mart_for_change(db, collection_name, old_doc=None, new_doc=None):
    product_ids = set()
    customer_ids = set()
    countries = set()

    for doc in (old_doc, new_doc):
        if not doc:
            continue
        if collection_name == "product_reviews":
            product_ids.add(doc.get("product_id"))
            customer_ids.add(doc.get("customer_id"))
        elif collection_name == "click_events":
            customer_ids.add(doc.get("customer_id"))
        elif collection_name == "customer_profiles":
            customer_ids.add(doc.get("customer_id"))
            countries.add(doc.get("country"))

    for product_id in product_ids:
        refresh_product_review_summary(db, product_id)
    for customer_id in customer_ids:
        refresh_customer_engagement_summary(db, customer_id)
    for country in countries:
        refresh_country_profile_summary(db, country)


def parse_document_id(raw):
    raw = loads_json(raw)
    if raw is None:
        return None
    value = raw.get("id") if isinstance(raw, dict) else raw
    if isinstance(value, str):
        try:
            value = json_util.loads(value)
        except Exception:
            pass
    if isinstance(value, dict):
        if "$oid" in value:
            return ObjectId(value["$oid"])
        if "$numberLong" in value:
            return int(value["$numberLong"])
        if "$numberInt" in value:
            return int(value["$numberInt"])
    return value


def msg_identity(msg):
    return (msg.topic(), msg.partition(), msg.offset())


def collection_from_payload(msg, payload):
    source = payload.get("source") or {}
    return source.get("collection") or msg.topic().split(".")[-1]


def build_cdc_metadata(msg, payload):
    source = payload.get("source") or {}
    return {
        "source_type": "mongodb",
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "source_database": source.get("db") or MONGO_SOURCE_DB,
        "source_collection": source.get("collection") or msg.topic().split(".")[-1],
        "op": payload.get("op"),
        "source_ts_ms": payload.get("ts_ms"),
        "synced_at": datetime.now(timezone.utc),
    }


def apply_change(db, msg):
    event = json.loads(decode_bytes(msg.value()))
    payload = event.get("payload", event)
    op = payload.get("op")
    collection_name = collection_from_payload(msg, payload)

    if collection_name not in COLLECTIONS:
        log(f"ignored topic={msg.topic()} collection={collection_name}")
        return

    raw_target = db[f"raw_{collection_name}"]
    clean_target = db[f"clean_{collection_name}"]
    document_id = parse_document_id(msg.key())
    cdc = build_cdc_metadata(msg, payload)
    old_clean_doc = clean_target.find_one({"_id": document_id}) if document_id is not None else None
    new_clean_doc = None

    if op in ("c", "r", "u"):
        doc = loads_mongo_doc(payload.get("after"))
        if doc is None:
            raise ValueError(f"missing after document for op={op}")
        if "_id" not in doc and document_id is not None:
            doc["_id"] = document_id

        raw_doc = dict(doc)
        raw_doc["_cdc"] = cdc

        clean_doc = TRANSFORMS[collection_name](doc)
        clean_doc["_cdc"] = cdc
        new_clean_doc = clean_doc

        raw_target.replace_one({"_id": raw_doc["_id"]}, raw_doc, upsert=True)
        clean_target.replace_one({"_id": clean_doc["_id"]}, clean_doc, upsert=True)
    elif op == "d":
        if document_id is None:
            raise ValueError("missing document id for delete event")
        raw_target.delete_one({"_id": document_id})
        clean_target.delete_one({"_id": document_id})
    else:
        log(f"ignored unsupported op={op} topic={msg.topic()}")
        return

    db["_cdc_offsets"].update_one(
        {
            "source_topic": msg.topic(),
            "source_partition": msg.partition(),
            "source_offset": msg.offset(),
        },
        {
            "$setOnInsert": {
                "source_topic": msg.topic(),
                "source_partition": msg.partition(),
                "source_offset": msg.offset(),
                "collection": collection_name,
                "op": op,
                "processed_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )
    refresh_mart_for_change(db, collection_name, old_clean_doc, new_clean_doc)
    log(f"{collection_name} op={op} offset={msg.offset()} raw+clean+mart")


def build_failed_event(msg, exc, retry_count):
    return {
        "source_type": "mongodb",
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "message_key": decode_bytes(msg.key()),
        "message_value": loads_json(msg.value()),
        "error_message": str(exc),
        "retry_count": retry_count,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "consumer_group": MONGO_SINK_GROUP_ID,
    }


def publish_failed_event(producer, failed_event):
    producer.produce(
        MONGO_DLQ_TOPIC,
        key=failed_event.get("message_key"),
        value=json.dumps(failed_event, default=json_util.default).encode("utf-8"),
    )
    producer.flush(10)


def record_failed_event(db, failed_event):
    doc = dict(failed_event)
    doc["dlq_topic"] = MONGO_DLQ_TOPIC
    db.ops_failed_events.insert_one(doc)


def wait_for_topics(consumer, topics, total_timeout=180):
    deadline = time.time() + total_timeout
    pending = set(topics)
    while pending and time.time() < deadline:
        try:
            md = consumer.list_topics(timeout=10)
            pending = set(topics) - set(md.topics.keys())
        except Exception as exc:
            log(f"metadata fetch failed: {exc}")
        if pending:
            log(f"waiting for topics: {sorted(pending)}")
            time.sleep(5)
    if pending:
        log(f"topics still missing after {total_timeout}s, subscribing anyway: {sorted(pending)}")
    else:
        log(f"topics available: {topics}")


def main():
    mongo = connect_target()
    db = mongo[TARGET["database"]]
    ensure_indexes(db)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": MONGO_SINK_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "topic.metadata.refresh.interval.ms": 10_000,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    wait_for_topics(consumer, TOPICS)
    consumer.subscribe(TOPICS)
    log(f"subscribed to {TOPICS}")

    attempts = {}
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log(f"consumer error: {msg.error()}")
                continue

            ident = msg_identity(msg)
            try:
                if msg.value() is not None:
                    apply_change(db, msg)
                attempts.pop(ident, None)
                consumer.commit(msg, asynchronous=False)
            except Exception as exc:
                attempt = attempts.get(ident, 0) + 1
                attempts[ident] = attempt
                if attempt < MAX_RETRIES:
                    log(f"error processing message attempt={attempt}/{MAX_RETRIES} (will retry): {exc}")
                    consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    time.sleep(1)
                    continue

                failed_event = build_failed_event(msg, exc, attempt)
                try:
                    publish_failed_event(producer, failed_event)
                    record_failed_event(db, failed_event)
                    consumer.commit(msg, asynchronous=False)
                    attempts.pop(ident, None)
                    log(f"sent to DLQ topic={MONGO_DLQ_TOPIC} source={msg.topic()} offset={msg.offset()}")
                except Exception as dlq_exc:
                    log(f"failed to write DLQ event (will retry source message): {dlq_exc}")
                    time.sleep(1)
    except KeyboardInterrupt:
        log("shutting down...")
    finally:
        consumer.close()
        producer.flush(5)
        mongo.close()

if __name__ == "__main__":
    main()
