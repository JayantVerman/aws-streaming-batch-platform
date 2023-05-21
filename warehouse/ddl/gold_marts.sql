-- Serving layer DDL — gold marts. Written for Postgres 16 (local) and
-- Redshift Serverless (AWS) simultaneously: standard types only, no
-- engine-specific syntax. Applied automatically on first Postgres boot via
-- scripts/postgres-init/02-gold-serving.sql (mounted at initdb time).
--
-- Schema is authoritative HERE, not in the load job: the loader truncates
-- and inserts (JDBC truncate=true), never creates/alters tables.

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
