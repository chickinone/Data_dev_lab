# CDC Pipeline: PostgreSQL -> Debezium -> Kafka -> Python -> PostgreSQL -> Superset

Pipeline demo Change Data Capture ket hop:

- Realtime ingestion: Debezium + Kafka + Python consumer.
- Batch reconciliation: Airflow chay PySpark job moi 3 tieng.
- BI/Dashboard: Superset doc du lieu tu mart layer.

```text
Fake Data
  -> PostgreSQL source (sourcedb)
  -> Debezium CDC
  -> Kafka topics
  -> Python consumer realtime
       -> raw.cdc_events
       -> clean.customers / clean.orders
       -> mart.daily_revenue / mart.customer_summary

Airflow DAG moi 3 tieng
  -> Spark local job
  -> SELECT mart.refresh_all()
  -> data-quality checks
  -> ops.batch_runs

Superset
  -> doc mart.* va ops.batch_runs
```

## Services

| Service | Port | Vai tro |
| --- | ---: | --- |
| `postgres-source` | `5432` | DB nguon, chua `public.customers`, `public.orders` |
| `postgres-target` | `5433` | DB dich, chua `raw`, `clean`, `mart`, `ops` |
| `kafka` | `9092` | Kafka broker |
| `connect` | `8083` | Kafka Connect / Debezium |
| `kafka-ui` | `8080` | UI xem Kafka topics |
| `consumer` | - | Python realtime CDC consumer |
| `airflow` | `8081` | Airflow UI, chay batch DAG |
| `superset` | `8088` | BI dashboard |

## 1. Start Pipeline

```powershell
cd d:\ICS\Task\Thang_6\Dev_data_lab\pipline
copy .env.example .env
docker compose up -d --build
```

Sua cac gia tri trong `.env` truoc khi chay neu day khong phai moi truong demo local.

Theo doi logs chinh:

```powershell
docker compose logs -f connector-init
docker compose logs -f fake-data consumer
```

Kiem tra container:

```powershell
docker compose ps
```

## 2. Debezium / Kafka

Kiem tra connector:

```powershell
curl -s http://localhost:8083/connectors/ecommerce-source-connector/status
```

Connector va task nen o trang thai `RUNNING`.

Xem topics:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Xem thu event trong topic orders:

```powershell
docker compose exec kafka kafka-console-consumer `
  --bootstrap-server kafka:29092 `
  --topic dbserver1.public.orders `
  --from-beginning `
  --max-messages 5
```

Kafka UI:

```text
http://localhost:8080
```

## 3. Target DB Layers

Target DB co 4 schema chinh:

- `raw`: luu toan bo CDC event dang audit log.
- `clean`: current state da clean/transform.
- `mart`: bang tong hop cho dashboard.
- `ops`: log van hanh batch.

Kiem tra raw:

```powershell
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT source_table, op, count(*) FROM raw.cdc_events GROUP BY source_table, op ORDER BY source_table, op;"
```

Kiem tra clean:

```powershell
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT id, full_name, email, email_domain, phone, country FROM clean.customers ORDER BY id DESC LIMIT 10;"
```

```powershell
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT id, customer_id, product_name, quantity, unit_price, total_amount, status FROM clean.orders ORDER BY id DESC LIMIT 10;"
```

Kiem tra mart:

```powershell
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT * FROM mart.daily_revenue ORDER BY order_day DESC, country;"
```

```powershell
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT * FROM mart.customer_summary ORDER BY total_spent DESC LIMIT 20;"
```

## 4. Realtime Consumer Logic

`consumer/consumer.py` doc CDC event tu cac topic:

- `dbserver1.public.customers`
- `dbserver1.public.orders`

Voi moi event:

1. Append event goc vao `raw.cdc_events`.
2. Upsert/delete dong hien tai vao `clean.customers` hoac `clean.orders`.
3. Goi function mart incremental:
   - `mart.on_customer_change(...)`
   - `mart.on_order_change(...)`
4. Commit DB transaction.
5. Commit Kafka offset.

Transform chinh:

- Customer:
  - trim `full_name`
  - lowercase `email`
  - sinh `email_domain`
  - chuan hoa `phone`
  - chuan hoa `country`, neu thieu thi thanh `UNKNOWN`
- Order:
  - ep `quantity` ve so nguyen khong am
  - parse `unit_price` thanh Decimal
  - tinh `total_amount = quantity * unit_price`
  - uppercase `status`

Routing theo Debezium `op`:

- `c`, `r`, `u`: upsert vao clean.
- `d`: delete khoi clean.
- moi event deu append vao raw.

Neu xu ly event loi, consumer se retry theo cau hinh:

```text
MAX_RETRIES=3
DLQ_TOPIC=cdc.failed_events
```

Sau khi qua so lan retry, event loi duoc:

1. Publish sang Kafka topic `cdc.failed_events`.
2. Ghi metadata loi vao `ops.failed_events`.
3. Commit Kafka offset de consumer khong bi ket mai o mot bad event.

## 5. Mart Logic

`mart.daily_revenue` tong hop theo:

```text
order_day + country
```

Chi tinh order co:

```sql
status <> 'CANCELLED'
```

Metrics:

- `total_orders = COUNT(*)`
- `total_items = SUM(quantity)`
- `total_revenue = SUM(total_amount)`

`mart.customer_summary` tong hop theo customer:

- `total_orders = COUNT(order)`
- `total_spent = SUM(total_amount)` voi order khong `CANCELLED`
- `last_order_date = MAX(order_date)`

Mart realtime dung incremental update de nhanh. Ngoai ra co:

```sql
SELECT mart.refresh_all();
```

Function nay rebuild toan bo `mart.daily_revenue` va `mart.customer_summary` tu `clean`, dung cho batch reconciliation.

## 6. Airflow + Spark Batch

Batch khong thay the realtime. No chay song song de reconcile va kiem tra du lieu dinh ky.

DAG:

```text
cdc_batch_reconcile_3h
```

Lich chay:

```text
0 */3 * * *
```

Tuc la moi 3 tieng.

Batch job lam:

1. Start Spark local session.
2. Goi `SELECT mart.refresh_all();`.
3. Tinh data-quality metrics.
4. Ghi ket qua vao `ops.batch_runs`.

Airflow UI:

```text
http://localhost:8081
Dang nhap bang AIRFLOW_ADMIN_USERNAME / AIRFLOW_ADMIN_PASSWORD trong `.env`.
```

Chay batch thu cong bang terminal:

```powershell
docker compose exec airflow python -u /opt/airflow/batch/spark_batch.py
```

Kiem tra batch log:

```powershell
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT batch_id, started_at, finished_at, status, duration_seconds, dq_issue_count FROM ops.batch_runs ORDER BY batch_id DESC LIMIT 10;"
```

Status co the gap:

- `SUCCESS`: batch chay xong va khong co data-quality issue.
- `SUCCESS_WITH_WARNINGS`: batch chay xong nhung co issue, vi du `UNKNOWN` country hoac order khong join duoc customer.
- `FAILED`: batch loi.

## 7. SQL Queue / Dead Letter Queue

Pipeline SQL hien dung Kafka lam queue chinh cho CDC topics:

```text
dbserver1.public.customers
dbserver1.public.orders
```

Them vao do la dead-letter queue:

```text
cdc.failed_events
```

Topic nay nhan cac CDC event ma `consumer.py` khong xu ly duoc sau `MAX_RETRIES`.

Fake-data mac dinh tao 4 poison orders trong 5 phut de test DLQ:

```text
POISON_EVENTS=4
product_name=[DLQ_TEST] Overflow Order
```

Nhung order nay hop le o source DB, nhung co `quantity * unit_price` qua lon nen khi consumer ghi vao
`clean.orders.total_amount NUMERIC(14,2)` se loi numeric overflow. Consumer retry roi dua event vao DLQ.

Kiem tra topic:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Doc failed events tu Kafka:

```powershell
docker compose exec kafka kafka-console-consumer `
  --bootstrap-server kafka:29092 `
  --topic cdc.failed_events `
  --from-beginning `
  --max-messages 5
```

Kiem tra failed events trong target DB:

```powershell
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT failed_event_id, source_topic, source_offset, retry_count, error_message, failed_at FROM ops.failed_events ORDER BY failed_event_id DESC LIMIT 20;"
```

## 8. Superset Dashboard

Mo Superset:

```text
http://localhost:8088
```

Khoi tao Superset lan dau:

```powershell
docker compose exec superset superset db upgrade
docker compose exec superset sh -c 'superset fab create-admin \
  --username "$SUPERSET_ADMIN_USERNAME" \
  --firstname "$SUPERSET_ADMIN_FIRSTNAME" \
  --lastname "$SUPERSET_ADMIN_LASTNAME" \
  --email "$SUPERSET_ADMIN_EMAIL" \
  --password "$SUPERSET_ADMIN_PASSWORD"'
docker compose exec superset superset init
```

Khi tao database connection trong Superset, neu wizard khong cho dan URL thi nhap tung field:

```text
HOST: postgres-target
PORT: 5432
DATABASE NAME: targetdb
USERNAME: TARGET_DB_USER trong `.env`
PASSWORD: TARGET_DB_PASSWORD trong `.env`
DISPLAY NAME: targetdb
SSL: off
SSH Tunnel: off
```

Dataset nen them:

- `mart.daily_revenue`
- `mart.customer_summary`
- `ops.batch_runs`

Chart goi y cho business dashboard:

- Big Number: `SUM(total_revenue)` tu `mart.daily_revenue`
- Bar Chart: doanh thu theo quoc gia tu `mart.daily_revenue`
- Bar Chart hoac Table: top khach hang tu `mart.customer_summary`
- Time-series Line Chart: doanh thu theo ngay neu du lieu co nhieu `order_day`

Chart goi y cho batch monitoring:

- Latest batch status tu `ops.batch_runs`
- Batch duration trend tu `ops.batch_runs`
- Batch run history table tu `ops.batch_runs`
- Data-quality issue count tu `ops.batch_runs.dq_issue_count`

## 9. Data Quality Queries

Customer thieu country:

```powershell
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT id, full_name, email, country FROM clean.customers WHERE country = 'UNKNOWN' ORDER BY id;"
```

Order khong join duoc customer:

```powershell
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT o.id, o.customer_id, o.total_amount FROM clean.orders o LEFT JOIN clean.customers c ON c.id = o.customer_id WHERE c.id IS NULL;"
```

Recompute revenue truc tiep tu clean de doi chieu mart:

```powershell
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT o.order_date::date AS order_day, COALESCE(NULLIF(c.country, ''), 'UNKNOWN') AS country, COUNT(*) AS total_orders, SUM(o.quantity) AS total_items, SUM(o.total_amount) AS total_revenue FROM clean.orders o LEFT JOIN clean.customers c ON c.id = o.customer_id WHERE o.status <> 'CANCELLED' GROUP BY o.order_date::date, COALESCE(NULLIF(c.country, ''), 'UNKNOWN') ORDER BY order_day DESC, country;"
```

Reconcile mart thu cong:

```powershell
docker compose exec postgres-target psql -U postgres -d targetdb -c "SELECT mart.refresh_all();"
```

## 10. Chay Lai Fake Data

```powershell
docker compose up -d --build --force-recreate fake-data
docker compose logs -f fake-data
```

Mac dinh `fake-data` tao 4 event loi co chu dich trong 5 phut de test `cdc.failed_events`.
Neu khong muon tao poison events, set:

```yaml
POISON_EVENTS: "0"
```

Theo doi consumer:

```powershell
docker compose logs -f consumer
```

## 11. Dung / Don Dep

Dung services, giu du lieu container:

```powershell
docker compose down
```

Dung va xoa volume:

```powershell
docker compose down -v
```

Lenh `down -v` se xoa du lieu DB va replication slot, dung khi muon lam lai tu dau.

## 12. Troubleshooting

Connector khong `RUNNING`:

```powershell
docker compose logs connect
docker compose logs connector-init
```

Consumer khong thay event:

```powershell
docker compose logs consumer
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Airflow khong thay DAG:

```powershell
docker compose logs airflow
```

Superset khong connect duoc target DB:

- Trong Superset container, dung host `postgres-target`, port `5432`.
- Tu may host Windows, Postgres target la `localhost:5433`.

Lam lai toan bo:

```powershell
docker compose down -v
docker compose up -d --build
```
