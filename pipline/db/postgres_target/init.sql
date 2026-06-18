-- TARGET DATABASE (targetdb)
--   raw   : append-only audit log of every CDC event
--   clean : current-state, typed & cleaned tables (upsert/delete applied)
--   mart  : reporting aggregates for Superset

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS ops;

-- RAW : every change event, stored as-is (audit / replay / debug)
CREATE TABLE IF NOT EXISTS raw.cdc_events (
    event_id      BIGSERIAL PRIMARY KEY,
    source_table  TEXT        NOT NULL,
    op            TEXT        NOT NULL,           -- c=create u=update d=delete r=snapshot
    ts_ms         BIGINT,                         -- event timestamp from Debezium
    pk            TEXT,                           -- primary key value as text
    before_data   JSONB,                          -- row image before the change
    after_data    JSONB,                          -- row image after the change
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_events_table ON raw.cdc_events (source_table);
CREATE INDEX IF NOT EXISTS idx_raw_events_op    ON raw.cdc_events (op);

-- CLEAN : latest state per row, cleaned & properly typed
CREATE TABLE IF NOT EXISTS clean.customers (
    id            INT PRIMARY KEY,
    full_name     TEXT,
    email         TEXT,
    email_domain  TEXT,
    phone         TEXT,
    country       TEXT,
    created_at    TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ,
    _op           TEXT,                            -- last operation that touched this row
    _synced_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS clean.orders (
    id            INT PRIMARY KEY,
    customer_id   INT,
    product_name  TEXT,
    quantity      INT,
    unit_price    NUMERIC(12,2),
    total_amount  NUMERIC(14,2),                   -- derived: quantity * unit_price
    status        TEXT,
    order_date    TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ,
    _op           TEXT,
    _synced_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- MART : aggregates the dashboard reads from
CREATE TABLE IF NOT EXISTS mart.daily_revenue (
    order_day     DATE,
    country       TEXT,
    total_orders  INT,
    total_items   INT,
    total_revenue NUMERIC(16,2),
    PRIMARY KEY (order_day, country)
);

CREATE TABLE IF NOT EXISTS mart.customer_summary (
    customer_id     INT PRIMARY KEY,
    full_name       TEXT,
    country         TEXT,
    total_orders    INT,
    total_spent     NUMERIC(16,2),
    last_order_date TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ops.batch_runs (
    batch_id                   BIGSERIAL PRIMARY KEY,
    job_name                   TEXT NOT NULL,
    started_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at                TIMESTAMPTZ,
    status                     TEXT NOT NULL,
    duration_seconds           NUMERIC(12,3),
    raw_events_count           BIGINT,
    clean_customers_count      BIGINT,
    clean_orders_count         BIGINT,
    mart_daily_revenue_rows    BIGINT,
    mart_customer_summary_rows BIGINT,
    dq_issue_count             BIGINT,
    metrics                    JSONB,
    error_message              TEXT
);

CREATE TABLE IF NOT EXISTS ops.failed_events (
    failed_event_id BIGSERIAL PRIMARY KEY,
    source_topic    TEXT NOT NULL,
    source_partition INT,
    source_offset   BIGINT,
    message_key     TEXT,
    message_value   JSONB,
    error_message   TEXT NOT NULL,
    retry_count     INT NOT NULL,
    dlq_topic       TEXT NOT NULL,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_failed_events_topic
    ON ops.failed_events (source_topic);
CREATE INDEX IF NOT EXISTS idx_failed_events_failed_at
    ON ops.failed_events (failed_at);

CREATE OR REPLACE FUNCTION mart.refresh_all()
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE TABLE mart.daily_revenue, mart.customer_summary;

    INSERT INTO mart.daily_revenue
        (order_day, country, total_orders, total_items, total_revenue)
    SELECT
        o.order_date::date,
        COALESCE(NULLIF(c.country, ''), 'UNKNOWN') AS country,
        COUNT(*)                                  AS total_orders,
        COALESCE(SUM(o.quantity), 0)              AS total_items,
        COALESCE(SUM(o.total_amount), 0)          AS total_revenue
    FROM clean.orders o
    LEFT JOIN clean.customers c ON c.id = o.customer_id
    WHERE o.status <> 'CANCELLED'
    GROUP BY
        o.order_date::date,
        COALESCE(NULLIF(c.country, ''), 'UNKNOWN');

    INSERT INTO mart.customer_summary
        (customer_id, full_name, country, total_orders, total_spent, last_order_date)
    SELECT
        c.id,
        c.full_name,
        c.country,
        COUNT(o.id),
        COALESCE(SUM(o.total_amount) FILTER (WHERE o.status <> 'CANCELLED'), 0),
        MAX(o.order_date)
    FROM clean.customers c
    LEFT JOIN clean.orders o ON o.customer_id = c.id
    GROUP BY c.id, c.full_name, c.country;
END;
$$;


CREATE OR REPLACE FUNCTION mart.upsert_revenue_bucket(
    p_order_day DATE,
    p_country   TEXT
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_orders  INT;
    v_items   BIGINT;
    v_revenue NUMERIC(16,2);
BEGIN
    SELECT
        COUNT(*)                         ,
        COALESCE(SUM(o.quantity),     0) ,
        COALESCE(SUM(o.total_amount), 0)
    INTO v_orders, v_items, v_revenue
    FROM  clean.orders    o
    LEFT JOIN clean.customers c ON c.id = o.customer_id
    WHERE o.status                                     <> 'CANCELLED'
      AND o.order_date::date                           =  p_order_day
      AND COALESCE(NULLIF(c.country, ''), 'UNKNOWN')   =  p_country;

    IF v_orders > 0 THEN
        INSERT INTO mart.daily_revenue
            (order_day, country, total_orders, total_items, total_revenue)
        VALUES
            (p_order_day, p_country, v_orders, v_items, v_revenue)
        ON CONFLICT (order_day, country) DO UPDATE SET
            total_orders  = EXCLUDED.total_orders,
            total_items   = EXCLUDED.total_items,
            total_revenue = EXCLUDED.total_revenue;
    ELSE
        DELETE FROM mart.daily_revenue
        WHERE order_day = p_order_day
          AND country   = p_country;
    END IF;
END;
$$;


CREATE OR REPLACE FUNCTION mart.upsert_customer_summary(
    p_customer_id INT
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO mart.customer_summary
        (customer_id, full_name, country, total_orders, total_spent, last_order_date)
    SELECT
        c.id,
        c.full_name,
        c.country,
        COUNT(o.id),
        COALESCE(SUM(o.total_amount) FILTER (WHERE o.status <> 'CANCELLED'), 0),
        MAX(o.order_date)
    FROM  clean.customers c
    LEFT JOIN clean.orders o ON o.customer_id = c.id
    WHERE c.id = p_customer_id
    GROUP BY c.id, c.full_name, c.country
    ON CONFLICT (customer_id) DO UPDATE SET
        full_name       = EXCLUDED.full_name,
        country         = EXCLUDED.country,
        total_orders    = EXCLUDED.total_orders,
        total_spent     = EXCLUDED.total_spent,
        last_order_date = EXCLUDED.last_order_date;

    IF NOT FOUND THEN
        DELETE FROM mart.customer_summary WHERE customer_id = p_customer_id;
    END IF;
END;
$$;


CREATE OR REPLACE FUNCTION mart.on_order_change(
    p_order_id   INT,
    p_before_day DATE DEFAULT NULL,
    p_before_cid INT  DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    r       RECORD;
    old_cty TEXT;
BEGIN
    IF p_before_day IS NOT NULL AND p_before_cid IS NOT NULL THEN
        SELECT COALESCE(NULLIF(country, ''), 'UNKNOWN')
        INTO   old_cty
        FROM   clean.customers
        WHERE  id = p_before_cid;

        PERFORM mart.upsert_revenue_bucket(p_before_day, COALESCE(old_cty, 'UNKNOWN'));
        PERFORM mart.upsert_customer_summary(p_before_cid);
    END IF;

    -- 2) Refresh from current clean state (inserts / updates)
    FOR r IN
        SELECT
            o.order_date::date                         AS d,
            o.customer_id                              AS cid,
            COALESCE(NULLIF(c.country, ''), 'UNKNOWN') AS cty
        FROM  clean.orders    o
        LEFT JOIN clean.customers c ON c.id = o.customer_id
        WHERE o.id = p_order_id
    LOOP
        PERFORM mart.upsert_revenue_bucket(r.d, r.cty);
        PERFORM mart.upsert_customer_summary(r.cid);
    END LOOP;
END;
$$;


CREATE OR REPLACE FUNCTION mart.on_customer_change(
    p_customer_id INT,
    p_old_country TEXT DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    r RECORD;
BEGIN
    -- 1) Rebuild (or delete) the customer summary row
    PERFORM mart.upsert_customer_summary(p_customer_id);

    -- 2) Heal old-country revenue buckets (country changed or customer deleted)
    IF p_old_country IS NOT NULL THEN
        FOR r IN
            SELECT DISTINCT o.order_date::date AS d
            FROM  clean.orders o
            WHERE o.customer_id = p_customer_id
        LOOP
            PERFORM mart.upsert_revenue_bucket(
                r.d,
                COALESCE(NULLIF(p_old_country, ''), 'UNKNOWN')
            );
        END LOOP;
    END IF;


    FOR r IN
        SELECT DISTINCT
            o.order_date::date                         AS d,
            COALESCE(NULLIF(c.country, ''), 'UNKNOWN') AS cty
        FROM  clean.orders    o
        LEFT JOIN clean.customers c ON c.id = o.customer_id
        WHERE o.customer_id = p_customer_id
    LOOP
        PERFORM mart.upsert_revenue_bucket(r.d, r.cty);
    END LOOP;
END;
$$;
