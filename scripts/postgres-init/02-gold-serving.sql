-- Auto-applied on first Postgres boot (docker-entrypoint-initdb.d).
-- GENERATED FROM warehouse/ddl/gold_marts.sql — edit that file, then sync
-- this copy (they must stay identical; serving tables are created here once,
-- the loader only truncates/inserts).

CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.daily_sales (
    order_date    DATE              NOT NULL,
    total_orders  BIGINT,
    total_items   BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (order_date)
);

CREATE TABLE IF NOT EXISTS serving.category_sales (
    category_id   BIGINT            NOT NULL,
    category_name VARCHAR(256),
    total_items   BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (category_id)
);

CREATE TABLE IF NOT EXISTS serving.customer_orders (
    customer_id   BIGINT            NOT NULL,
    customer_name VARCHAR(512),
    first_order   DATE,
    last_order    DATE,
    total_orders  BIGINT,
    PRIMARY KEY (customer_id)
);
