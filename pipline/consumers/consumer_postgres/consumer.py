import os
import re
import json
import sys
import time
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json
from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

def required_env(name):
    value = os.getenv(name)
    if value is None or value == "":
        sys.exit(f"missing required environment variable: {name}")
    return value


KAFKA_BOOTSTRAP = required_env("KAFKA_BOOTSTRAP")
TOPIC_PREFIX    = required_env("TOPIC_PREFIX")
DLQ_TOPIC       = required_env("DLQ_TOPIC")
CDC_CONSUMER_GROUP = required_env("CDC_CONSUMER_GROUP")
MAX_RETRIES     = int(required_env("MAX_RETRIES"))


DB = dict(
    host     = required_env("TARGET_DB_HOST"),
    port     = int(required_env("TARGET_DB_PORT")),
    dbname   = required_env("TARGET_DB_NAME"),
    user     = required_env("TARGET_DB_USER"),
    password = required_env("TARGET_DB_PASSWORD"),
)


def log(msg):
    print(f"[consumer] {msg}", flush=True)


def to_timestamp(value):
    """Debezium 'connect' temporal mode uses epoch millis for timestamps.
    Also tolerates ISO strings (e.g. timestamptz) and passes through None."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
    if isinstance(value, str):
        return value          # let psycopg2 cast ISO-8601 strings
    return value


def to_decimal(value):
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def clean_str(value):
    if value is None:
        return None
    s = str(value).strip()
    return s or None



def transform_customer(row):
    """Clean a customers row and derive email_domain."""
    email = clean_str(row.get("email"))
    if email:
        email = email.lower()
    domain = email.split("@", 1)[1] if email and "@" in email else None

    phone = row.get("phone") or ""
    phone = re.sub(r"[^\d+]", "", phone)   
    phone = phone or None

    country = normalize_country(row.get("country"))

    return {
        "id":           row.get("id"),
        "full_name":    clean_str(row.get("full_name")),
        "email":        email,
        "email_domain": domain,
        "phone":        phone,
        "country":      country,
        "created_at":   to_timestamp(row.get("created_at")),
        "updated_at":   to_timestamp(row.get("updated_at")),
    }

def normalize_country(value):
    c = clean_str(value)
    if not c:
        return "UNKNOWN"

    c = c.upper()

    if c in ("VIETNAM", "VIET NAM"):
        return "VIETNAM"
    if c in ("USA", "UNITED STATES", "US"):
        return "USA"
    if c == "JAPAN":
        return "JAPAN"
    if c == "SINGAPORE":
        return "SINGAPORE"

    return c

def transform_order(row):
    """Clean an orders row and derive total_amount."""
    qty = row.get("quantity") or 0
    try:
        qty = max(int(qty), 0)
    except (TypeError, ValueError):
        qty = 0

    unit_price = to_decimal(row.get("unit_price")) or Decimal("0")
    total      = (unit_price * qty).quantize(Decimal("0.01"))

    status = clean_str(row.get("status")) or "NEW"
    status = status.upper()

    return {
        "id":           row.get("id"),
        "customer_id":  row.get("customer_id"),
        "product_name": clean_str(row.get("product_name")),
        "quantity":     qty,
        "unit_price":   unit_price,
        "total_amount": total,
        "status":       status,
        "order_date":   to_timestamp(row.get("order_date")),
        "updated_at":   to_timestamp(row.get("updated_at")),
    }



def _raw_to_date(raw_row, field="order_date"):
    """
    Extract a date from a raw Debezium row field.

    Debezium 'connect' temporal mode encodes TIMESTAMPTZ as epoch-millisecond
    integers.  DATE columns arrive as epoch-day integers, but order_date is
    TIMESTAMPTZ in this schema so epoch-ms is the expected format.
    ISO-8601 strings are also handled for completeness.
    Returns a Python datetime.date or None.
    """
    if raw_row is None:
        return None
    val = raw_row.get(field)
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        return datetime.fromtimestamp(val / 1000.0, tz=timezone.utc).date()
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00")).date()
        except ValueError:
            return val[:10] if len(val) >= 10 else None
    return None


def mart_update_order(cur, op, before, after):
    src      = after if after is not None else before
    order_id = src.get("id") if src else None
    if order_id is None:
        return

    before_day = _raw_to_date(before)
    before_cid = before.get("customer_id") if before else None

    cur.execute(
        "SELECT mart.on_order_change(%s, %s, %s)",
        (order_id, before_day, before_cid),
    )


def mart_update_customer(cur, op, before, after):
    src         = after if after is not None else before
    customer_id = src.get("id") if src else None
    if customer_id is None:
        return

    def norm(row):
        if row is None:
            return None
        return normalize_country(row.get("country"))

    old_country = norm(before)
    new_country = norm(after)

    pass_old = old_country if (op == "d" or old_country != new_country) else None

    cur.execute(
        "SELECT mart.on_customer_change(%s, %s)",
        (customer_id, pass_old),
    )

TABLES = {
    "customers": {
        "clean_table": "clean.customers",
        "pk":          "id",
        "transform":   transform_customer,
        "mart_fn":     mart_update_customer,    # incremental mart handler
    },
    "orders": {
        "clean_table": "clean.orders",
        "pk":          "id",
        "transform":   transform_order,
        "mart_fn":     mart_update_order,
    },
}
TOPICS = [f"{TOPIC_PREFIX}.public.{t}" for t in TABLES]


# DB helpers

def connect_db():
    for attempt in range(1, 31):
        try:
            conn = psycopg2.connect(**DB)
            conn.autocommit = False
            log("connected to target DB")
            return conn
        except Exception as exc:
            log(f"DB not ready (attempt {attempt}): {exc}")
            time.sleep(3)
    sys.exit("could not connect to target DB")


def upsert(cur, table, pk, data):
    cols         = list(data.keys())
    quoted       = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    set_clause   = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c != pk)
    sql = (
        f'INSERT INTO {table} ({quoted}) VALUES ({placeholders}) '
        f'ON CONFLICT ("{pk}") DO UPDATE SET {set_clause}'
    )
    cur.execute(sql, [data[c] for c in cols])


def delete_row(cur, table, pk, pk_value):
    cur.execute(f'DELETE FROM {table} WHERE "{pk}" = %s', (pk_value,))


def pk_from_key(msg, pk_col):
    raw = msg.key()
    if not raw:
        return None
    try:
        return json.loads(raw).get(pk_col)
    except Exception:
        return None


def msg_identity(msg):
    return (msg.topic(), msg.partition(), msg.offset())


def decode_bytes(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def parse_json_or_text(value):
    text = decode_bytes(value)
    if text is None:
        return None
    try:
        return json.loads(text)
    except Exception:
        return {"_raw": text}


def build_failed_event(msg, exc, retry_count):
    return {
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "message_key": decode_bytes(msg.key()),
        "message_value": parse_json_or_text(msg.value()),
        "error_message": str(exc),
        "retry_count": retry_count,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "consumer_group": CDC_CONSUMER_GROUP,
    }


def publish_failed_event(producer, failed_event):
    producer.produce(
        DLQ_TOPIC,
        key=failed_event.get("message_key"),
        value=json.dumps(failed_event, default=str).encode("utf-8"),
    )
    producer.flush(10)


def record_failed_event(conn, failed_event):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO ops.failed_events
            (source_topic, source_partition, source_offset, message_key,
             message_value, error_message, retry_count, dlq_topic, failed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            failed_event["source_topic"],
            failed_event["source_partition"],
            failed_event["source_offset"],
            failed_event["message_key"],
            Json(failed_event["message_value"]),
            failed_event["error_message"],
            failed_event["retry_count"],
            DLQ_TOPIC,
            failed_event["failed_at"],
        ),
    )
    conn.commit()
    cur.close()


# Event processing

def process_message(conn, msg, state):
    table = msg.topic().split(".")[-1]
    cfg   = TABLES.get(table)
    if cfg is None:
        return

    raw_value = msg.value()
    if raw_value is None:
        # tombstone; tombstones.on.delete=false should prevent these, but be safe
        return

    event   = json.loads(raw_value)
    # With schemas disabled the value IS the payload; with schemas enabled it is
    # nested under "payload".  Support both.
    payload = event.get("payload", event)

    op     = payload.get("op")
    before = payload.get("before")
    after  = payload.get("after")
    ts_ms  = payload.get("ts_ms")

    pk_col  = cfg["pk"]
    src_row = after if after is not None else before
    pk_val  = src_row.get(pk_col) if src_row else pk_from_key(msg, pk_col)

    cur = conn.cursor()

    # 1) RAW: append event as-is
    cur.execute(
        "INSERT INTO raw.cdc_events "
        "(source_table, op, ts_ms, pk, before_data, after_data) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (
            table, op, ts_ms,
            None if pk_val is None else str(pk_val),
            Json(before),
            Json(after),
        ),
    )

    # 2) CLEAN: maintain current-state table
    if op in ("c", "r", "u") and after is not None:
        cleaned = cfg["transform"](after)
        cleaned["_op"]        = op
        cleaned["_synced_at"] = datetime.now(timezone.utc)
        upsert(cur, cfg["clean_table"], pk_col, cleaned)
    elif op == "d" and pk_val is not None:
        delete_row(cur, cfg["clean_table"], pk_col, pk_val)

    # 3) MART: targeted UPSERT of only the affected rows
    #   mart_fn calls mart.on_order_change() / mart.on_customer_change() which
    #   recalculate only the specific (order_day, country) buckets and customer
    #   summary rows touched by this event.  No TRUNCATE, no full scan.
    cfg["mart_fn"](cur, op, before, after)

    conn.commit()
    cur.close()

    state["total"] += 1
    log(f"#{state['total']} {table} op={op} pk={pk_val}")


# Topic availability wait

def wait_for_topics(consumer, topics, total_timeout=180):
    """Block until the expected topics exist so we consume from the start."""
    deadline = time.time() + total_timeout
    pending  = set(topics)
    while pending and time.time() < deadline:
        try:
            md      = consumer.list_topics(timeout=10)
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


# Main loop

def main():
    conn = connect_db()

    consumer = Consumer({
        "bootstrap.servers":                  KAFKA_BOOTSTRAP,
        "group.id":                           CDC_CONSUMER_GROUP,
        "auto.offset.reset":                  "earliest",
        "enable.auto.commit":                 False,
        "topic.metadata.refresh.interval.ms": 10_000,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    wait_for_topics(consumer, TOPICS)
    consumer.subscribe(TOPICS)
    log(f"subscribed to {TOPICS}")

    state = {"total": 0, "attempts": {}}

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

            try:
                process_message(conn, msg, state)
                state["attempts"].pop(msg_identity(msg), None)
                consumer.commit(msg, asynchronous=False)
            except Exception as exc:
                conn.rollback()
                ident = msg_identity(msg)
                attempt = state["attempts"].get(ident, 0) + 1
                state["attempts"][ident] = attempt

                if attempt < MAX_RETRIES:
                    log(f"error processing message attempt={attempt}/{MAX_RETRIES} (will retry): {exc}")
                    consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    time.sleep(1)
                    continue

                failed_event = build_failed_event(msg, exc, attempt)
                try:
                    publish_failed_event(producer, failed_event)
                    record_failed_event(conn, failed_event)
                    consumer.commit(msg, asynchronous=False)
                    state["attempts"].pop(ident, None)
                    log(
                        f"sent to DLQ topic={DLQ_TOPIC} "
                        f"source={msg.topic()} offset={msg.offset()} error={exc}"
                    )
                except Exception as dlq_exc:
                    conn.rollback()
                    log(f"failed to write DLQ event (will retry source message): {dlq_exc}")
                    time.sleep(1)
                continue

    except KeyboardInterrupt:
        log("shutting down...")
    finally:
        consumer.close()
        producer.flush(5)
        conn.close()


if __name__ == "__main__":
    main()
