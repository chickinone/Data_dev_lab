import os
import random
import sys
import time
from datetime import datetime, timezone
from urllib.parse import quote_plus

from faker import Faker
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError


fake = Faker()

PRODUCT_CATEGORIES = [
    "laptops",
    "accessories",
    "audio",
    "monitors",
    "office",
]

COUNTRIES = [
    "vietnam",
    " VIETNAM ",
    "Viet Nam",
    "usa",
    "United States",
    "japan",
    " singapore",
]

REVIEW_STATUSES = ["PENDING", "APPROVED", "REJECTED"]
EVENT_TYPES = ["product_view", "search", "add_to_cart", "checkout_start"]


def required_env(name):
    value = os.getenv(name)
    if value is None or value == "":
        sys.exit(f"[fake-mongo-data] missing required environment variable: {name}")
    return value

MONGO_HOST = required_env("MONGO_HOST")
MONGO_PORT = required_env("MONGO_PORT")
MONGO_DATABASE = required_env("MONGO_DATABASE")
MONGO_USER = required_env("MONGO_ROOT_USERNAME")
MONGO_PASSWORD = required_env("MONGO_ROOT_PASSWORD")
MONGO_AUTH_SOURCE = required_env("MONGO_AUTH_SOURCE")
MONGO_REPLICA_SET = required_env("MONGO_REPLICA_SET")

DURATION_SECONDS = int(required_env("MONGO_FAKE_DURATION_SECONDS"))
SLEEP_MIN_SECONDS = float(required_env("MONGO_FAKE_SLEEP_MIN_SECONDS"))
SLEEP_MAX_SECONDS = float(required_env("MONGO_FAKE_SLEEP_MAX_SECONDS"))


def log(message):
    print(f"[fake-mongo-data] {message}", flush=True)


def utcnow():
    return datetime.now(timezone.utc)


def mongo_uri():
    user = quote_plus(MONGO_USER)
    password = quote_plus(MONGO_PASSWORD)
    return (
        f"mongodb://{user}:{password}@{MONGO_HOST}:{MONGO_PORT}/"
        f"{MONGO_DATABASE}?authSource={MONGO_AUTH_SOURCE}&replicaSet={MONGO_REPLICA_SET}"
    )


def connect():
    uri = mongo_uri()
    for attempt in range(1, 31):
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            log("connected to MongoDB")
            return client
        except ServerSelectionTimeoutError as exc:
            log(f"MongoDB not ready (attempt {attempt}): {exc}")
            time.sleep(3)
    sys.exit("[fake-mongo-data] could not connect to MongoDB")


def ensure_indexes(db):
    db.customer_profiles.create_index("customer_id", unique=True)
    db.customer_profiles.create_index("country")
    db.product_reviews.create_index("review_id", unique=True)
    db.product_reviews.create_index([("product_id", 1), ("status", 1)])
    db.click_events.create_index("event_time")


def random_profile(customer_id=None):
    customer_id = customer_id or random.randint(1, 10_000)
    first = fake.first_name()
    last = fake.last_name()
    now = utcnow()
    return {
        "customer_id": customer_id,
        "full_name": f" {first} {last} ",
        "country": random.choice(COUNTRIES),
        "preferences": {
            "category": random.choice(PRODUCT_CATEGORIES),
            "newsletter": random.choice([True, False]),
            "price_sensitivity": random.choice(["low", "medium", "high"]),
        },
        "loyalty": {
            "tier": random.choice(["bronze", "silver", "gold"]),
            "points": random.randint(0, 10_000),
        },
        "devices": random.sample(["web", "ios", "android"], k=random.randint(1, 3)),
        "created_at": now,
        "updated_at": now,
    }


def insert_profile(db):
    doc = random_profile()
    try:
        result = db.customer_profiles.insert_one(doc)
        log(f"+ profile customer_id={doc['customer_id']} _id={result.inserted_id}")
    except PyMongoError as exc:
        log(f"profile insert skipped: {exc}")


def update_profile(db):
    doc = db.customer_profiles.aggregate([{"$sample": {"size": 1}}]).try_next()
    if not doc:
        return insert_profile(db)

    patch = {
        "country": random.choice(COUNTRIES),
        "preferences.newsletter": random.choice([True, False]),
        "preferences.category": random.choice(PRODUCT_CATEGORIES),
        "loyalty.points": max(0, int(doc.get("loyalty", {}).get("points", 0)) + random.randint(-250, 600)),
        "updated_at": utcnow(),
    }
    db.customer_profiles.update_one({"_id": doc["_id"]}, {"$set": patch})
    log(f"~ profile customer_id={doc['customer_id']}")


def delete_profile(db):
    doc = db.customer_profiles.aggregate([{"$sample": {"size": 1}}]).try_next()
    if not doc:
        return
    db.customer_profiles.delete_one({"_id": doc["_id"]})
    log(f"- profile customer_id={doc['customer_id']}")


def get_random_customer_id(db):
    doc = db.customer_profiles.aggregate([{"$sample": {"size": 1}}]).try_next()
    if doc:
        return doc["customer_id"]
    insert_profile(db)
    doc = db.customer_profiles.aggregate([{"$sample": {"size": 1}}]).try_next()
    return doc["customer_id"] if doc else random.randint(1, 10_000)


def insert_review(db):
    now = utcnow()
    review_id = fake.uuid4()
    doc = {
        "review_id": review_id,
        "customer_id": get_random_customer_id(db),
        "product_id": random.randint(1, 500),
        "rating": random.randint(1, 5),
        "status": random.choice(REVIEW_STATUSES),
        "title": fake.sentence(nb_words=5),
        "body": fake.paragraph(nb_sentences=3),
        "tags": random.sample(["shipping", "quality", "price", "support", "packaging"], k=random.randint(1, 3)),
        "created_at": now,
        "updated_at": now,
    }
    db.product_reviews.insert_one(doc)
    log(f"+ review review_id={review_id} rating={doc['rating']}")


def update_review(db):
    doc = db.product_reviews.aggregate([{"$sample": {"size": 1}}]).try_next()
    if not doc:
        return insert_review(db)
    patch = {
        "status": random.choice(REVIEW_STATUSES),
        "rating": random.randint(1, 5),
        "updated_at": utcnow(),
    }
    db.product_reviews.update_one({"_id": doc["_id"]}, {"$set": patch})
    log(f"~ review review_id={doc['review_id']} status={patch['status']}")


def delete_review(db):
    doc = db.product_reviews.aggregate([{"$sample": {"size": 1}}]).try_next()
    if not doc:
        return
    db.product_reviews.delete_one({"_id": doc["_id"]})
    log(f"- review review_id={doc['review_id']}")


def insert_click_event(db):
    doc = {
        "event_id": fake.uuid4(),
        "customer_id": get_random_customer_id(db),
        "event_type": random.choice(EVENT_TYPES),
        "product_id": random.randint(1, 500),
        "session_id": fake.uuid4(),
        "metadata": {
            "user_agent": fake.user_agent(),
            "referrer": random.choice(["direct", "ads", "email", "organic"]),
            "page": random.choice(["home", "search", "product", "cart"]),
        },
        "event_time": utcnow(),
    }
    db.click_events.insert_one(doc)
    log(f"+ click_event event_type={doc['event_type']}")


def main():
    client = connect()
    db = client[MONGO_DATABASE]
    ensure_indexes(db)

    if db.customer_profiles.estimated_document_count() < 10:
        for _ in range(15):
            insert_profile(db)

    actions = [
        (insert_profile, 0.16),
        (update_profile, 0.18),
        (delete_profile, 0.04),
        (insert_review, 0.28),
        (update_review, 0.12),
        (delete_review, 0.04),
        (insert_click_event, 0.18),
    ]
    fns = [fn for fn, _ in actions]
    weights = [weight for _, weight in actions]

    end_time = time.time() + DURATION_SECONDS
    log("generating MongoDB document changes...")
    while time.time() < end_time:
        fn = random.choices(fns, weights=weights, k=1)[0]
        try:
            fn(db)
        except PyMongoError as exc:
            log(f"action error: {exc}")
        time.sleep(random.uniform(SLEEP_MIN_SECONDS, SLEEP_MAX_SECONDS))

    log("done. Stopping.")
    client.close()

if __name__ == "__main__":
    main()
