-- Post-dload verification queries (psql / DBeaver / Power BI preview):
--   psql "postgresql://platform:<pw>@localhost:5432/warehouse" -f warehouse/ddl/verify.sql

-- row counts per mart
SELECT 'daily_sales' AS mart, COUNT(*) FROM serving.daily_sales
UNION ALL
SELECT 'category_sales', COUNT(*) FROM serving.category_sales
UNION ALL
SELECT 'customer_orders', COUNT(*) FROM serving.customer_orders;

-- busiest days
SELECT order_date, total_orders, total_revenue
FROM serving.daily_sales
ORDER BY total_revenue DESC
LIMIT 10;

-- top categories by revenue
SELECT category_name, total_items, total_revenue
FROM serving.category_sales
ORDER BY total_revenue DESC
LIMIT 10;

-- most frequent customers
SELECT customer_name, first_order, last_order, total_orders
FROM serving.customer_orders
ORDER BY total_orders DESC
LIMIT 10;
