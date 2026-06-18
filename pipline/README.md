# Data Pipeline Lab

Demo pipeline xu ly 3 loai du lieu:

- Structured data: PostgreSQL.
- NoSQL document data: MongoDB.
- Unstructured/object data: MinIO cho file, MongoDB cho metadata.

Tat ca cung dung chung backbone:

```text
source
-> event/queue/stream
-> consumer realtime
-> raw
-> clean
-> mart
-> batch reconcile
-> DLQ/ops log
```

## Architecture

**PostgreSQL**

```text
fake-data
-> postgres-source
-> Debezium PostgreSQL connector
-> Kafka topics dbserver1.public.*
-> consumer_postgres
-> postgres-target
   raw.cdc_events
   clean.customers / clean.orders
   mart.daily_revenue / mart.customer_summary
   ops.failed_events / ops.batch_runs
-> Airflow batch_postgres
-> Superset
```

**MongoDB**

```text
fake-mongo-data
-> mongodb
-> Debezium MongoDB connector
-> Kafka topics mongodb.ecommerce_nosql.*
-> consumer_mongo
-> mongodb-target
   raw_*
   clean_*
   mart_*
   ops_failed_events / ops_batch_runs
-> Airflow batch_mongo
```

**Object Store / MinIO**

```text
manual import file
-> minio-source raw-* buckets
-> object-event-producer
-> Kafka topic object.events
-> object-sink-consumer
-> minio-target clean-* buckets
-> mongodb-target object metadata
   raw_object_events
   clean_object_metadata
   mart_object_*
   ops_object_failed_events / ops_object_batch_runs
-> Airflow batch_object
```

Kafka khong luu binary file. Kafka chi luu metadata event, file that nam trong MinIO.

## Folder Layout

```text
pipline/
  airflow/
    dags/
  batch/
    batch_postgres/
    batch_mongo/
    batch_object/
  connectors/
    debezium_postgres/
  consumers/
    consumer_postgres/
    consumer_mongo/
    consumer_object/
  db/
    postgres_source/
    postgres_target/
  fake-data/
    fake_data_structured/
    fake_data_nosql/
  producers/
    object_event_producer/
  docker-compose.yml
  .env.example
```

## Services

| Service | Port | Role |
| --- | ---: | --- |
| `postgres-source` | `5432` | PostgreSQL source |
| `postgres-target` | `5433` | PostgreSQL raw/clean/mart target |
| `mongodb` | `27017` | MongoDB source |
| `mongodb-target` | `27018` | MongoDB target for NoSQL and object metadata |
| `minio-source` | `9000`, `9001` | Object source API/console |
| `minio-target` | `9100`, `9101` | Object target API/console |
| `kafka` | `9092` | Kafka broker |
| `connect` | `8083` | Kafka Connect / Debezium |
| `kafka-ui` | `8080` | Kafka UI |
| `airflow` | `8081` | Batch scheduler |
| `superset` | `8088` | Dashboard for PostgreSQL mart |
| `consumer` | - | PostgreSQL realtime consumer |
| `mongo-sink-consumer` | - | MongoDB realtime consumer |
| `object-event-producer` | - | Scan MinIO source and publish object events |
| `object-sink-consumer` | - | Copy object to target and write metadata |

## Start

Ubuntu/Linux:

```bash
cd ~/Data_dev_lab/pipline
cp .env.example .env
docker compose up -d --build
```

Windows PowerShell:

```powershell
cd d:\ICS\Task\Thang_6\Dev_data_lab\pipline
copy .env.example .env
docker compose up -d --build
```

Check containers:

```bash
docker compose ps
```

Stop without deleting data:

```bash
docker compose down
```

Start again without losing data:

```bash
docker compose up -d
```

Delete everything and start fresh:

```bash
docker compose down -v
docker compose up -d --build
```

Do not use `down -v` if you want to keep PostgreSQL, MongoDB, MinIO, Kafka, Airflow, and Superset data.

## Important URLs

```text
Kafka UI:            http://localhost:8080
Airflow:             http://localhost:8081
Superset:            http://localhost:8088
MinIO source UI:     http://localhost:9001
MinIO target UI:     http://localhost:9101
Kafka Connect REST:  http://localhost:8083
```

On cloud/Ubuntu server, replace `localhost` with the server public IP, or use SSH tunnel.

MinIO accounts from local `.env`:

```text
Source console 9001: minio_source / minio_source
Target console 9101: minio_target / minio_target
```

## Environment

`.env` is local and ignored by Git.

`.env.example` is committed as a safe template with `change-me` values.

If you change `Dockerfile` or `requirements.txt`, rebuild the affected service:

```bash
docker compose up -d --build --force-recreate airflow
docker compose up -d --build --force-recreate object-sink-consumer
```

## PostgreSQL Flow

Start/re-run fake structured data:

```bash
docker compose up -d --build --force-recreate fake-data
docker compose logs -f fake-data
```

Check Debezium PostgreSQL connector:

```bash
curl http://localhost:8083/connectors/ecommerce-source-connector/status
```

Check Kafka topics:

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Check source data:

```bash
docker compose exec postgres-source psql -U postgres -d sourcedb -c "SELECT 'customers' AS table_name, COUNT(*) FROM customers UNION ALL SELECT 'orders', COUNT(*) FROM orders;"
```

Check target raw/clean/mart:

```bash
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT 'raw_events' AS metric, COUNT(*) FROM raw.cdc_events UNION ALL SELECT 'clean_customers', COUNT(*) FROM clean.customers UNION ALL SELECT 'clean_orders', COUNT(*) FROM clean.orders UNION ALL SELECT 'mart_daily_revenue', COUNT(*) FROM mart.daily_revenue UNION ALL SELECT 'mart_customer_summary', COUNT(*) FROM mart.customer_summary UNION ALL SELECT 'failed_events', COUNT(*) FROM ops.failed_events;"
```

Check mart samples:

```bash
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT * FROM mart.daily_revenue ORDER BY order_day DESC, country LIMIT 20;"
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT * FROM mart.customer_summary ORDER BY total_spent DESC LIMIT 20;"
```

Run PostgreSQL batch:

```bash
docker compose exec airflow python -u /opt/airflow/batch/batch_postgres/spark_batch.py
```

Check batch log:

```bash
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT batch_id, started_at, finished_at, status, duration_seconds, dq_issue_count FROM ops.batch_runs ORDER BY batch_id DESC LIMIT 10;"
```

Check PostgreSQL DLQ:

```bash
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT failed_event_id, source_topic, source_offset, retry_count, error_message, failed_at FROM ops.failed_events ORDER BY failed_event_id DESC LIMIT 20;"
```

Notes:

- `fake-data` may generate poison orders to test DLQ.
- Those bad orders intentionally cause numeric overflow and go to `cdc.failed_events`.

## MongoDB Flow

Start/re-run fake NoSQL data:

```bash
docker compose up -d --build --force-recreate fake-mongo-data
docker compose logs -f fake-mongo-data
```

Check Debezium MongoDB connector:

```bash
curl http://localhost:8083/connectors/mongodb-source-connector/status
```

Check MongoDB source:

```bash
docker compose exec mongodb sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin "$MONGO_INITDB_DATABASE" --eval "printjson({customer_profiles: db.customer_profiles.countDocuments(), product_reviews: db.product_reviews.countDocuments(), click_events: db.click_events.countDocuments()})"'
```

Check MongoDB target raw/clean/mart:

```bash
docker compose exec mongodb-target sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin "$MONGO_INITDB_DATABASE" --eval "printjson({raw_profiles: db.raw_customer_profiles.countDocuments(), clean_profiles: db.clean_customer_profiles.countDocuments(), raw_reviews: db.raw_product_reviews.countDocuments(), clean_reviews: db.clean_product_reviews.countDocuments(), raw_clicks: db.raw_click_events.countDocuments(), clean_clicks: db.clean_click_events.countDocuments(), mart_product: db.mart_product_review_summary.countDocuments(), mart_customer: db.mart_customer_engagement_summary.countDocuments(), mart_country: db.mart_country_profile_summary.countDocuments(), failed_events: db.ops_failed_events.countDocuments()})"'
```

Check clean samples:

```bash
docker compose exec mongodb-target sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin "$MONGO_INITDB_DATABASE" --eval "db.clean_customer_profiles.findOne(); db.clean_product_reviews.findOne(); db.clean_click_events.findOne();"'
```

Run MongoDB batch:

```bash
docker compose exec airflow python -u /opt/airflow/batch/batch_mongo/mongo_batch.py
```

Check MongoDB batch log:

```bash
docker compose exec mongodb-target sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin "$MONGO_INITDB_DATABASE" --eval "db.ops_batch_runs.find().sort({started_at:-1}).limit(5).forEach(printjson);"'
```

## Object Store / MinIO Flow

Source buckets:

```text
raw-images
raw-videos
raw-audio
raw-documents
```

Target buckets:

```text
clean-images
clean-videos
clean-audio
clean-documents
quarantine
```

List source and target:

```bash
docker compose run --rm --entrypoint sh minio-init -c 'mc alias set source http://minio-source:9000 "$MINIO_SOURCE_ROOT_USER" "$MINIO_SOURCE_ROOT_PASSWORD" >/dev/null && mc ls --recursive source'
docker compose run --rm --entrypoint sh minio-init -c 'mc alias set target http://minio-target:9000 "$MINIO_TARGET_ROOT_USER" "$MINIO_TARGET_ROOT_PASSWORD" >/dev/null && mc ls --recursive target'
```

Upload test object to source:

```bash
docker compose run --rm --entrypoint sh minio-init -c 'mc alias set source http://minio-source:9000 "$MINIO_SOURCE_ROOT_USER" "$MINIO_SOURCE_ROOT_PASSWORD" >/dev/null && echo demo >/tmp/object-test.txt && mc cp /tmp/object-test.txt source/raw-documents/object-test.txt && mc ls source/raw-documents'
```

Check object Kafka topic:

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list | grep object.events
```

Read object events:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic object.events \
  --from-beginning \
  --max-messages 5
```

Check object producer/consumer logs:

```bash
docker compose logs --tail=100 object-event-producer
docker compose logs --tail=100 object-sink-consumer
```

Check object metadata and mart:

```bash
docker compose exec mongodb-target sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin "$MONGO_INITDB_DATABASE" --eval "printjson({raw_object_events: db.raw_object_events.countDocuments(), clean_object_metadata: db.clean_object_metadata.countDocuments(), active_objects: db.clean_object_metadata.countDocuments({status:\"ACTIVE\"}), missing_objects: db.clean_object_metadata.countDocuments({status:\"MISSING\"}), mart_by_type: db.mart_object_summary_by_type.countDocuments(), mart_by_day: db.mart_object_summary_by_day.countDocuments(), storage_summary: db.mart_object_storage_summary.countDocuments(), failed_events: db.ops_object_failed_events.countDocuments()})"'
```

View object metadata:

```bash
docker compose exec mongodb-target sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin "$MONGO_INITDB_DATABASE" --eval "db.clean_object_metadata.find().sort({ingested_at:-1}).limit(5).forEach(printjson)"'
```

View object mart:

```bash
docker compose exec mongodb-target sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin "$MONGO_INITDB_DATABASE" --eval "db.mart_object_summary_by_type.find().forEach(printjson); db.mart_object_summary_by_day.find().sort({ingested_day:-1}).limit(5).forEach(printjson); db.mart_object_storage_summary.find().forEach(printjson)"'
```

Run object batch:

```bash
docker compose exec airflow python -u /opt/airflow/batch/batch_object/object_batch.py
```

Check object batch log:

```bash
docker compose exec mongodb-target sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin "$MONGO_INITDB_DATABASE" --eval "db.ops_object_batch_runs.find().sort({started_at:-1}).limit(5).forEach(printjson)"'
```

Object batch behavior:

- If metadata exists but the target file is deleted, batch marks metadata as `MISSING`.
- Object mart only counts `status = ACTIVE`.
- Metadata is kept for audit.

## Clean / Transform Rules

**PostgreSQL**

- Customer: trim name, lowercase email, extract email domain, normalize phone, normalize country.
- Order: cast quantity, parse price, compute `total_amount`, uppercase status.

**MongoDB**

- Profiles: normalize country, nested preferences, nested loyalty, devices list.
- Reviews: normalize ids, rating range `0..5`, uppercase status, clean tags.
- Clicks: normalize ids, uppercase event type/referrer/page.

**Object**

- Validate `size_bytes > 0`.
- Normalize `media_type`: `image`, `video`, `audio`, `document`.
- Route to clean bucket by media type.
- Normalize extension/content type/source type.
- Track `ACTIVE` or `MISSING`.
- Mart counts only `ACTIVE` objects.

## Batch DAGs

Airflow DAGs:

```text
cdc_batch_reconcile_3h
mongo_batch_reconcile_3h
object_batch_reconcile_3h
```

Check:

```bash
docker compose exec airflow airflow dags list
```

Each DAG runs every 3 hours:

```text
0 */3 * * *
```

Manual runs:

```bash
docker compose exec airflow python -u /opt/airflow/batch/batch_postgres/spark_batch.py
docker compose exec airflow python -u /opt/airflow/batch/batch_mongo/mongo_batch.py
docker compose exec airflow python -u /opt/airflow/batch/batch_object/object_batch.py
```

## Superset

Superset is currently used for PostgreSQL mart:

```text
http://localhost:8088
```

Connect to PostgreSQL target from inside Superset:

```text
HOST: postgres-target
PORT: 5432
DATABASE: targetdb
USERNAME: TARGET_DB_USER from .env
PASSWORD: TARGET_DB_PASSWORD from .env
```

Recommended datasets:

```text
mart.daily_revenue
mart.customer_summary
ops.batch_runs
```

MongoDB and Object metadata are kept in MongoDB target. Superset does not read MongoDB directly in this lightweight setup.

## Full Health Check

```bash
docker compose ps
curl http://localhost:8083/connectors/ecommerce-source-connector/status
curl http://localhost:8083/connectors/mongodb-source-connector/status
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
docker compose exec airflow airflow dags list
```

PostgreSQL:

```bash
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT 'raw_events' AS metric, COUNT(*) FROM raw.cdc_events UNION ALL SELECT 'clean_customers', COUNT(*) FROM clean.customers UNION ALL SELECT 'clean_orders', COUNT(*) FROM clean.orders UNION ALL SELECT 'mart_daily_revenue', COUNT(*) FROM mart.daily_revenue UNION ALL SELECT 'mart_customer_summary', COUNT(*) FROM mart.customer_summary UNION ALL SELECT 'failed_events', COUNT(*) FROM ops.failed_events;"
```

MongoDB:

```bash
docker compose exec mongodb-target sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin "$MONGO_INITDB_DATABASE" --eval "printjson({clean_profiles: db.clean_customer_profiles.countDocuments(), clean_reviews: db.clean_product_reviews.countDocuments(), clean_clicks: db.clean_click_events.countDocuments(), mart_product: db.mart_product_review_summary.countDocuments(), mart_customer: db.mart_customer_engagement_summary.countDocuments(), mart_country: db.mart_country_profile_summary.countDocuments(), failed_events: db.ops_failed_events.countDocuments()})"'
```

Object:

```bash
docker compose exec mongodb-target sh -c 'mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin "$MONGO_INITDB_DATABASE" --eval "printjson({clean_object_metadata: db.clean_object_metadata.countDocuments(), active_objects: db.clean_object_metadata.countDocuments({status:\"ACTIVE\"}), missing_objects: db.clean_object_metadata.countDocuments({status:\"MISSING\"}), failed_events: db.ops_object_failed_events.countDocuments()})"'
```

## Troubleshooting

Rebuild service after requirements change:

```bash
docker compose up -d --build --force-recreate airflow
docker compose up -d --build --force-recreate object-sink-consumer
```

Connector not running:

```bash
docker compose logs --tail=200 connect
docker compose logs --tail=200 connector-init
docker compose logs --tail=200 mongo-connector-init
```

Consumer not processing:

```bash
docker compose logs --tail=200 consumer
docker compose logs --tail=200 mongo-sink-consumer
docker compose logs --tail=200 object-sink-consumer
```

Object not copied:

```bash
docker compose logs --tail=200 object-event-producer
docker compose logs --tail=200 object-sink-consumer
docker compose run --rm --entrypoint sh minio-init -c 'mc alias set source http://minio-source:9000 "$MINIO_SOURCE_ROOT_USER" "$MINIO_SOURCE_ROOT_PASSWORD" >/dev/null && mc ls --recursive source'
docker compose run --rm --entrypoint sh minio-init -c 'mc alias set target http://minio-target:9000 "$MINIO_TARGET_ROOT_USER" "$MINIO_TARGET_ROOT_PASSWORD" >/dev/null && mc ls --recursive target'
```

Airflow missing dependency after code update:

```bash
docker compose build airflow
docker compose up -d --force-recreate airflow
```

Reset all data:

```bash
docker compose down -v
docker compose up -d --build
```
