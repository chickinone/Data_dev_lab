
-- SOURCE DATABASE (sourcedb)
-- E-commerce style tables that Debezium will capture changes from.

CREATE TABLE IF NOT EXISTS public.customers (
    id          SERIAL PRIMARY KEY,
    full_name   VARCHAR(200),
    email       VARCHAR(200),
    phone       VARCHAR(50),
    country     VARCHAR(100),
    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.orders (
    id           SERIAL PRIMARY KEY,
    customer_id  INT REFERENCES public.customers(id),
    product_name VARCHAR(200),
    quantity     INT NOT NULL DEFAULT 1,
    unit_price   NUMERIC(10,2) NOT NULL DEFAULT 0,
    status       VARCHAR(50) NOT NULL DEFAULT 'NEW',
    order_date   TIMESTAMP NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL makes UPDATE/DELETE CDC events carry the FULL

ALTER TABLE public.customers REPLICA IDENTITY FULL;
ALTER TABLE public.orders    REPLICA IDENTITY FULL;
