import hashlib
import json
import mimetypes
import os
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

from confluent_kafka import Producer
from minio import Minio
from minio.error import S3Error


def required_env(name):
    value = os.getenv(name)
    if value is None or value == "":
        sys.exit(f"[object-event-producer] missing required environment variable: {name}")
    return value


KAFKA_BOOTSTRAP = required_env("KAFKA_BOOTSTRAP")
OBJECT_EVENTS_TOPIC = required_env("OBJECT_EVENTS_TOPIC")
MINIO_ENDPOINT = required_env("MINIO_SOURCE_ENDPOINT")
MINIO_ACCESS_KEY = required_env("MINIO_SOURCE_ROOT_USER")
MINIO_SECRET_KEY = required_env("MINIO_SOURCE_ROOT_PASSWORD")
MINIO_BUCKETS = required_env("MINIO_SOURCE_BUCKETS").split()
SCAN_INTERVAL_SECONDS = float(required_env("OBJECT_EVENT_SCAN_INTERVAL_SECONDS"))
RUN_ONCE = required_env("OBJECT_EVENT_RUN_ONCE").lower() == "true"


def log(message):
    print(f"[object-event-producer] {message}", flush=True)


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def minio_client():
    parsed = urlparse(MINIO_ENDPOINT)
    endpoint = parsed.netloc or parsed.path
    secure = parsed.scheme == "https"
    return Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )


def stable_hash(*parts):
    value = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def media_type_for(bucket, object_name, content_type):
    bucket_name = bucket.lower()
    content = (content_type or "").lower()
    extension = os.path.splitext(object_name.lower())[1].lstrip(".")

    if bucket_name.endswith("images") or content.startswith("image/"):
        return "image"
    if bucket_name.endswith("videos") or content.startswith("video/"):
        return "video"
    if bucket_name.endswith("audio") or content.startswith("audio/"):
        return "audio"
    if extension in {"jpg", "jpeg", "png", "webp", "gif", "bmp", "tiff"}:
        return "image"
    if extension in {"mp4", "mov", "avi", "mkv", "webm"}:
        return "video"
    if extension in {"mp3", "wav", "ogg", "flac", "m4a"}:
        return "audio"
    return "document"


def object_event(bucket, obj, stat):
    guessed_type, _ = mimetypes.guess_type(obj.object_name)
    content_type = stat.content_type or guessed_type or "application/octet-stream"
    last_modified = stat.last_modified or obj.last_modified
    last_modified_iso = last_modified.astimezone(timezone.utc).isoformat() if last_modified else None
    file_name = os.path.basename(obj.object_name)
    extension = os.path.splitext(file_name)[1].lstrip(".").lower()
    object_id = stable_hash(bucket, obj.object_name)

    return {
        "event_id": stable_hash(bucket, obj.object_name, stat.etag, stat.size, last_modified_iso),
        "event_type": "OBJECT_CREATED_OR_UPDATED",
        "event_time": utc_now_iso(),
        "source_type": "manual_import",
        "object_id": object_id,
        "source_bucket": bucket,
        "object_key": obj.object_name,
        "file_name": file_name,
        "extension": extension,
        "media_type": media_type_for(bucket, obj.object_name, content_type),
        "content_type": content_type,
        "size_bytes": stat.size,
        "etag": stat.etag,
        "last_modified": last_modified_iso,
    }


def delivery_report(error, message):
    if error is not None:
        log(f"failed to publish event: {error}")
        return
    log(
        f"published {message.topic()} partition={message.partition()} "
        f"offset={message.offset()}"
    )


def scan_and_publish(client, producer, seen):
    published = 0
    for bucket in MINIO_BUCKETS:
        try:
            objects = client.list_objects(bucket, recursive=True)
            for obj in objects:
                if obj.is_dir:
                    continue
                stat = client.stat_object(bucket, obj.object_name)
                fingerprint = stable_hash(bucket, obj.object_name, stat.etag, stat.size, stat.last_modified)
                if seen.get(f"{bucket}/{obj.object_name}") == fingerprint:
                    continue
                event = object_event(bucket, obj, stat)
                key = event["object_id"].encode("utf-8")
                value = json.dumps(event, sort_keys=True).encode("utf-8")
                producer.produce(
                    OBJECT_EVENTS_TOPIC,
                    key=key,
                    value=value,
                    callback=delivery_report,
                )
                seen[f"{bucket}/{obj.object_name}"] = fingerprint
                published += 1
        except S3Error as exc:
            log(f"scan failed for bucket={bucket}: {exc}")
    producer.flush()
    return published


def main():
    client = minio_client()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    seen = {}

    log(
        f"starting scan buckets={MINIO_BUCKETS} topic={OBJECT_EVENTS_TOPIC} "
        f"interval={SCAN_INTERVAL_SECONDS}s run_once={RUN_ONCE}"
    )
    while True:
        published = scan_and_publish(client, producer, seen)
        log(f"scan complete published={published} tracked_objects={len(seen)}")
        if RUN_ONCE:
            break
        time.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
