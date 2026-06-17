# CDC Pipeline: PostgreSQL → Debezium → Kafka → Python → PostgreSQL → Superset

Một pipeline Change Data Capture (CDC) hoàn chỉnh, chạy bằng `docker compose`.

```
Fake Data (Faker)
      │  INSERT / UPDATE / DELETE
      ▼
PostgreSQL  (sourcedb, public.customers / public.orders)
      │  WAL (logical replication)
      ▼
Debezium CDC  (Kafka Connect, pgoutput)
      ▼
Kafka topics  (dbserver1.public.customers, dbserver1.public.orders)
      ▼
Python Consumer
   ├─ parse CDC event
   ├─ lấy before / after
   ├─ clean dữ liệu (trim, lowercase email, chuẩn hoá phone/country...)
   ├─ transform field (email_domain, total_amount...)
   └─ xử lý insert / update / delete
      ▼
PostgreSQL  (targetdb)
   ├─ raw    : raw.cdc_events     (audit log mọi event)
   ├─ clean  : clean.customers / clean.orders   (current state)
   └─ mart   : mart.daily_revenue / mart.customer_summary
      ▼
Superset Dashboard  (http://localhost:8088)
```

## Yêu cầu
- Docker + Docker Compose v2 (`docker compose ...`)
- Trống các cổng: 5432, 5433, 9092, 8083, 8088

## 1. Khởi động toàn bộ pipeline
```bash
cd cdc-pipeline
docker compose up -d --build
```
Thứ tự diễn ra tự động:
1. `postgres-source` / `postgres-target` chạy `init.sql` tạo bảng & schema.
2. `kafka` + `connect` (Debezium) lên.
3. `connector-init` đợi Connect sẵn sàng rồi **đăng ký connector** tự động.
4. `fake-data` bắt đầu sinh dữ liệu, `consumer` bắt đầu xử lý.

Xem log luồng chính:
```bash
docker compose logs -f connector-init     # xác nhận connector đã đăng ký
docker compose logs -f fake-data consumer # thấy event được sinh ra & xử lý
```

## 2. Kiểm tra connector Debezium
```bash
curl -s http://localhost:8083/connectors/ecommerce-source-connector/status | python3 -m json.tool
```
State của connector và task phải là `RUNNING`. Đăng ký lại thủ công nếu cần:
```bash
./debezium/register-connector.sh
```

## 3. Xem dữ liệu chạy qua Kafka
```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic dbserver1.public.orders --from-beginning --max-messages 5
```

## 4. Kiểm tra target DB (raw / clean / mart)
```bash
docker compose exec postgres-target psql -U postgres -d targetdb -c \
  "SELECT op, count(*) FROM raw.cdc_events GROUP BY op;"

docker compose exec postgres-target psql -U postgres -d targetdb -c \
  "SELECT id, email, email_domain, phone, country FROM clean.customers LIMIT 5;"

docker compose exec postgres-target psql -U postgres -d targetdb -c \
  "SELECT * FROM mart.daily_revenue ORDER BY total_revenue DESC LIMIT 10;"

docker compose exec postgres-target psql -U postgres -d targetdb -c \
  "SELECT * FROM mart.customer_summary ORDER BY total_spent DESC LIMIT 10;"
```
So sánh với source:
```bash
docker compose exec postgres-source psql -U postgres -d sourcedb -c \
  "SELECT count(*) FROM customers; SELECT count(*) FROM orders;"
```

## 5. Superset Dashboard
Khởi tạo Superset lần đầu (chạy 1 lần):
```bash
docker compose exec superset superset db upgrade
docker compose exec superset superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@example.com --password admin
docker compose exec superset superset init
```
Mở http://localhost:8088 (admin / admin), rồi:
1. **Settings → Database Connections → + Database → PostgreSQL**, dùng URI:
   ```
   postgresql://postgres:postgres@postgres-target:5432/targetdb
   ```
2. **Datasets → + Dataset**, thêm các bảng trong schema `mart`
   (`daily_revenue`, `customer_summary`) và/hoặc `clean`.
3. Tạo Chart / Dashboard, ví dụ:
   - Doanh thu theo ngày: `mart.daily_revenue` (X = order_day, Y = total_revenue).
   - Doanh thu theo quốc gia: group by `country`.
   - Top khách hàng: `mart.customer_summary` sort `total_spent`.

> Lưu ý: metadata của Superset trong demo này là ephemeral. Nếu cần lưu lâu dài,
> trỏ Superset sang một Postgres metadata riêng và mount volume.

## Cleaning & transform đang làm gì (trong `consumer/consumer.py`)
**customers**
- `full_name`: bỏ khoảng trắng thừa.
- `email`: trim + lowercase; sinh thêm `email_domain`.
- `phone`: chỉ giữ chữ số và dấu `+` đầu.
- `country`: trim + Title Case (`" VIETNAM "` → `Vietnam`).

**orders**
- `quantity`: ép kiểu int, không âm.
- `unit_price`: parse `NUMERIC` (chuỗi) → `Decimal`.
- `total_amount` (derived) = `quantity * unit_price`.
- `status`: trim + UPPERCASE.

**Routing theo `op`**
- `c` (create) / `r` (snapshot) / `u` (update) → UPSERT vào `clean.*`.
- `d` (delete) → DELETE khỏi `clean.*`.
- Mọi event đều được append vào `raw.cdc_events` trước (audit/replay).
- `mart.*` được rebuild bằng `mart.refresh_all()` định kỳ (mỗi 20 event hoặc 15 giây).

## Dừng / dọn dẹp
```bash
docker compose down        # dừng, giữ dữ liệu
docker compose down -v     # dừng và xoá toàn bộ volume + replication slot
```

## Tinh chỉnh nhanh
- Đổi bảng được capture: `table.include.list` trong `debezium/connector-config.json`
  và `TABLES` trong `consumer/consumer.py`.
- Tần suất sinh dữ liệu: `time.sleep(...)` trong `fake-data/generate.py`.
- Nhịp refresh mart: `REFRESH_EVERY_EVENTS` / `REFRESH_EVERY_SECONDS` trong consumer.

## Troubleshooting
- **Connector không RUNNING**: xem `docker compose logs connect`; thường do source DB
  chưa bật `wal_level=logical` (đã set sẵn trong compose) hoặc đăng ký quá sớm —
  chạy lại `./debezium/register-connector.sh`.
- **Consumer không thấy event**: kiểm tra topic đã tồn tại (mục 3) và connector RUNNING.
- **Làm lại từ đầu**: `docker compose down -v && docker compose up -d --build`
  (lệnh `-v` xoá cả replication slot `debezium_slot`).
```
