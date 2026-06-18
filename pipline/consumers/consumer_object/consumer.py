import json
import os
import sys
import time
from datetime import datetime, timezone
from urllib.parse import quote_plus, urlparse

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


def required_env(name):
    value = os.getenv(name)
    if value is None or value == "":
        sys.exit(f"[object-sink-consumer] missing required environment variable: {name}")
    return value


KAFKA_BOOTSTRAP = required_env("KAFKA_BOOTSTRAP")
OBJECT_EVENTS_TOPIC = required_env("OBJECT_EVENTS_TOPIC")
OBJECT_DLQ_TOPIC = required_env("OBJECT_DLQ_TOPIC")
OBJECT_SINK_GROUP_ID = required_env("OBJECT_SINK_GROUP_ID")
MAX_RETRIES = int(required_env("MAX_RETRIES"))

SOURCE_MINIO = {
    "endpoint": required_env("MINIO_SOURCE_ENDPOINT"),
    "access_key": required_env("MINIO_SOURCE_ROOT_USER"),
    "secret_key": required_env("MINIO_SOURCE_ROOT_PASSWORD"),
}
TARGET_MINIO = {
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

TARGET_BUCKET_BY_TYPE = {
    "image": "clean-images",
    "video": "clean-videos",
    "audio": "clean-audio",
    "document": "clean-documents",
}


def log(message):
    print(f"[object-sink-consumer] {message}", flush=True)


def utc_now():
    return datetime.now(timezone.utc)


def decode_bytes(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def loads_json(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        value = decode_bytes(value)
    return json.loads(value)


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
    uri = mongo_uri()
    for attempt in range(1, 31):
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            log("connected to MongoDB target")
            return client
        except ServerSelectionTimeoutError as exc:
            log(f"MongoDB target not ready (attempt {attempt}): {exc}")
            time.sleep(3)
    sys.exit("[object-sink-consumer] could not connect to MongoDB target")


def ensure_indexes(db):
    db.raw_object_events.create_index("event_id", unique=True)
    db.clean_object_metadata.create_index("object_id", unique=True)
    db.clean_object_metadata.create_index([("media_type", 1), ("ingested_day", 1)])
    db.ops_object_failed_events.create_index("failed_at")
    db["_object_offsets"].create_index(
        [("source_topic", 1), ("source_partition", 1), ("source_offset", 1)],
        unique=True,
    )
    db.mart_object_summary_by_type.create_index("media_type", unique=True)
    db.mart_object_summary_by_day.create_index("ingested_day", unique=True)


def clean_str(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_media_type(value):
    media_type = (clean_str(value) or "document").lower()
    return media_type if media_type in TARGET_BUCKET_BY_TYPE else "document"


def clean_event(event):
    media_type = normalize_media_type(event.get("media_type"))
    size_bytes = int(event.get("size_bytes") or 0)
    if size_bytes <= 0:
        raise ValueError("object size_bytes must be greater than 0")

    source_bucket = clean_str(event.get("source_bucket"))
    object_key = clean_str(event.get("object_key"))
    object_id = clean_str(event.get("object_id"))
    event_id = clean_str(event.get("event_id"))
    if not source_bucket or not object_key or not object_id or not event_id:
        raise ValueError("missing required object event identifiers")

    ingested_at = utc_now()
    return {
        "object_id": object_id,
        "event_id": event_id,
        "source_bucket": source_bucket,
        "source_object_key": object_key,
        "target_bucket": TARGET_BUCKET_BY_TYPE[media_type],
        "target_object_key": object_key,
        "file_name": clean_str(event.get("file_name")),
        "extension": (clean_str(event.get("extension")) or "").lower(),
        "media_type": media_type,
        "content_type": clean_str(event.get("content_type")) or "application/octet-stream",
        "size_bytes": size_bytes,
        "etag": clean_str(event.get("etag")),
        "last_modified": clean_str(event.get("last_modified")),
        "source_type": clean_str(event.get("source_type")) or "manual_import",
        "status": "ACTIVE",
        "ingested_at": ingested_at,
        "ingested_day": ingested_at.date().isoformat(),
    }


def copy_object(source_client, target_client, clean_doc):
    response = None
    try:
        response = source_client.get_object(
            clean_doc["source_bucket"],
            clean_doc["source_object_key"],
        )
        target_client.put_object(
            clean_doc["target_bucket"],
            clean_doc["target_object_key"],
            response,
            length=clean_doc["size_bytes"],
            content_type=clean_doc["content_type"],
        )
    except S3Error:
        raise
    finally:
        if response is not None:
            response.close()
            response.release_conn()


def refresh_object_marts(db):
    now = utc_now()
    db.mart_object_summary_by_type.delete_many({})
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
        db.mart_object_summary_by_type.replace_one(
            {"media_type": row["_id"]},
            {
                "media_type": row["_id"],
                "total_objects": row.get("total_objects", 0),
                "total_size_bytes": row.get("total_size_bytes", 0),
                "avg_size_bytes": round(float(row.get("avg_size_bytes") or 0), 3),
                "latest_ingested_at": row.get("latest_ingested_at"),
                "_refreshed_at": now,
            },
            upsert=True,
        )

    db.mart_object_summary_by_day.delete_many({})
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
        db.mart_object_summary_by_day.replace_one(
            {"ingested_day": row["_id"]},
            {
                "ingested_day": row["_id"],
                "total_objects": row.get("total_objects", 0),
                "total_size_bytes": row.get("total_size_bytes", 0),
                "_refreshed_at": now,
            },
            upsert=True,
        )

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


def apply_event(db, source_client, target_client, msg):
    event = loads_json(msg.value())
    clean_doc = clean_event(event)
    raw_doc = dict(event)
    raw_doc["_kafka"] = {
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "processed_at": utc_now(),
    }

    copy_object(source_client, target_client, clean_doc)
    db.raw_object_events.replace_one({"event_id": raw_doc["event_id"]}, raw_doc, upsert=True)
    db.clean_object_metadata.replace_one({"object_id": clean_doc["object_id"]}, clean_doc, upsert=True)
    db["_object_offsets"].update_one(
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
                "event_id": raw_doc["event_id"],
                "processed_at": utc_now(),
            }
        },
        upsert=True,
    )
    refresh_object_marts(db)
    log(
        f"object_id={clean_doc['object_id']} copied "
        f"{clean_doc['source_bucket']}/{clean_doc['source_object_key']} -> "
        f"{clean_doc['target_bucket']}/{clean_doc['target_object_key']}"
    )

def build_failed_event(msg, exc, retry_count):
    return {
        "source_type": "object_store",
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "message_key": decode_bytes(msg.key()),
        "message_value": loads_json(msg.value()),
        "error_message": str(exc),
        "retry_count": retry_count,
        "failed_at": utc_now().isoformat(),
        "consumer_group": OBJECT_SINK_GROUP_ID,
    }


def publish_failed_event(producer, failed_event):
    producer.produce(
        OBJECT_DLQ_TOPIC,
        key=failed_event.get("message_key"),
        value=json.dumps(failed_event, default=str).encode("utf-8"),
    )
    producer.flush(10)


def record_failed_event(db, failed_event):
    doc = dict(failed_event)
    doc["dlq_topic"] = OBJECT_DLQ_TOPIC
    db.ops_object_failed_events.insert_one(doc)


def wait_for_topic(consumer, topic, total_timeout=180):
    deadline = time.time() + total_timeout
    while time.time() < deadline:
        try:
            md = consumer.list_topics(timeout=10)
            if topic in md.topics:
                log(f"topic available: {topic}")
                return
        except Exception as exc:
            log(f"metadata fetch failed: {exc}")
        log(f"waiting for topic: {topic}")
        time.sleep(5)
    log(f"topic still missing after {total_timeout}s, subscribing anyway: {topic}")


def main():
    source_client = minio_client(SOURCE_MINIO)
    target_client = minio_client(TARGET_MINIO)
    mongo = connect_mongo()
    db = mongo[MONGO_TARGET["database"]]
    ensure_indexes(db)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": OBJECT_SINK_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "topic.metadata.refresh.interval.ms": 10_000,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    wait_for_topic(consumer, OBJECT_EVENTS_TOPIC)
    consumer.subscribe([OBJECT_EVENTS_TOPIC])
    log(f"subscribed to {OBJECT_EVENTS_TOPIC}")

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

            ident = (msg.topic(), msg.partition(), msg.offset())
            try:
                if msg.value() is not None:
                    apply_event(db, source_client, target_client, msg)
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
                    log(f"sent to DLQ topic={OBJECT_DLQ_TOPIC} source={msg.topic()} offset={msg.offset()}")
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
