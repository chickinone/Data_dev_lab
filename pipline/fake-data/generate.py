import os
import sys
import time
import random

import psycopg2
from faker import Faker

fake = Faker()

DB = dict(
    host=os.getenv("SOURCE_DB_HOST", "postgres-source"),
    port=int(os.getenv("SOURCE_DB_PORT", "5432")),
    dbname=os.getenv("SOURCE_DB_NAME", "sourcedb"),
    user=os.getenv("SOURCE_DB_USER", "postgres"),
    password=os.getenv("SOURCE_DB_PASSWORD", "postgres"),
)

PRODUCTS = [
    "Laptop Pro 14", "Wireless Mouse", "Mechanical Keyboard", "27in Monitor",
    "USB-C Hub", "Noise-Cancelling Headphones", "Webcam HD", "Desk Lamp",
    "Office Chair", "Standing Desk",
]

COUNTRY_RAW = [
    "vietnam", " VIETNAM ", "Viet Nam", "usa", "  USA", "United States",
    "japan", "JAPAN ", " singapore", "Singapore",
]

STATUS_FLOW = ["NEW", "PAID", "SHIPPED", "DELIVERED"]


def connect():
    for attempt in range(1, 31):
        try:
            conn = psycopg2.connect(**DB)
            conn.autocommit = True
            print("[fake-data] connected to source DB", flush=True)
            return conn
        except Exception as exc:  # noqa: BLE001
            print(f"[fake-data] DB not ready (attempt {attempt}): {exc}", flush=True)
            time.sleep(3)
    sys.exit("[fake-data] could not connect to source DB")


def messy_email(first, last):
    domain = random.choice(["gmail.com", "yahoo.com", "outlook.com", "company.io"])
    raw = f"{first}.{last}@{domain}"
    if random.random() < 0.5:
        raw = raw.upper()
    if random.random() < 0.5:
        raw = f"  {raw}  "
    return raw


def messy_phone():
    n = "".join(random.choice("0123456789") for _ in range(10))
    fmts = [
        f"({n[:3]}) {n[3:6]}-{n[6:]}",
        f"{n[:3]}.{n[3:6]}.{n[6:]}",
        f"+1 {n[:3]} {n[3:6]} {n[6:]}",
        n,
    ]
    return random.choice(fmts)


def get_random_id(cur, table):
    cur.execute(f"SELECT id FROM {table} ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    return row[0] if row else None


def insert_customer(cur):
    first, last = fake.first_name(), fake.last_name()
    cur.execute(
        "INSERT INTO customers (full_name, email, phone, country) "
        "VALUES (%s, %s, %s, %s) RETURNING id",
        (f"  {first} {last} ", messy_email(first, last), messy_phone(),
         random.choice(COUNTRY_RAW)),
    )
    cid = cur.fetchone()[0]
    print(f"[fake-data] + customer {cid}", flush=True)


def insert_order(cur):
    cid = get_random_id(cur, "customers")
    if cid is None:
        return
    cur.execute(
        "INSERT INTO orders (customer_id, product_name, quantity, unit_price, status) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING id",
        (cid, random.choice(PRODUCTS), random.randint(1, 5),
         round(random.uniform(5, 999), 2), "NEW"),
    )
    oid = cur.fetchone()[0]
    print(f"[fake-data] + order {oid} (customer {cid})", flush=True)


def update_order(cur):
    cur.execute("SELECT id, status FROM orders ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return
    oid, status = row
    if random.random() < 0.15:
        new_status = "CANCELLED"
    else:
        idx = STATUS_FLOW.index(status) if status in STATUS_FLOW else 0
        new_status = STATUS_FLOW[min(idx + 1, len(STATUS_FLOW) - 1)]
    cur.execute(
        "UPDATE orders SET status = %s, updated_at = now() WHERE id = %s",
        (new_status, oid),
    )
    print(f"[fake-data] ~ order {oid}: {status} -> {new_status}", flush=True)


def update_customer(cur):
    cid = get_random_id(cur, "customers")
    if cid is None:
        return
    cur.execute(
        "UPDATE customers SET country = %s, phone = %s, updated_at = now() WHERE id = %s",
        (random.choice(COUNTRY_RAW), messy_phone(), cid),
    )
    print(f"[fake-data] ~ customer {cid}", flush=True)


def delete_order(cur):
    oid = get_random_id(cur, "orders")
    if oid is None:
        return
    cur.execute("DELETE FROM orders WHERE id = %s", (oid,))
    print(f"[fake-data] - order {oid}", flush=True)


def main():
    end_time = time.time() + 60 * 5  
    conn = connect()
    cur = conn.cursor()

    cur.execute("SELECT count(*) FROM customers")
    if cur.fetchone()[0] < 10:
        for _ in range(15):
            insert_customer(cur)

    actions = [
        (insert_customer, 0.20),
        (insert_order,    0.35),
        (update_order,    0.25),
        (update_customer, 0.12),
        (delete_order,    0.08),
    ]
    fns = [a for a, _ in actions]
    weights = [w for _, w in actions]

    print("[fake-data] generating change events... (stop with Ctrl+C / docker stop)", flush=True)

    while time.time() < end_time: 
        fn = random.choices(fns, weights=weights, k=1)[0]
        try:
            fn(cur)
        except Exception as exc:
            print(f"[fake-data] action error: {exc}", flush=True)
        time.sleep(random.uniform(0.5, 2.0))
    
    print("[fake-data] 5 minutes done. Stopping.", flush=True)  


if __name__ == "__main__":
    main()


