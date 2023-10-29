# Serving layer runbook — querying gold data from BI tools.

## Quick verification (no BI tool)

psql:
```
psql "postgresql://platform:platform@localhost:5432/warehouse" -f warehouse/ddl/verify.sql
```

Trino (Athena stand-in) over the same Iceberg lake:
```
docker compose exec trino trino --catalog iceberg --schema retail_gold \
  --execute 'SELECT * FROM daily_sales ORDER BY total_revenue DESC LIMIT 10'
```

## Power BI — Postgres (local)

1. **Get Data -> PostgreSQL database**.
2. Server `localhost`, Database `warehouse`, user `platform`, password `platform`.
3. In Navigator pick the `serving` schema tables: `daily_sales`,
   `category_sales`, `customer_orders`.
4. Model: the three marts are standalone; set up relationships if you add
   dimensions. Visualize, then **Publish** to your workspace.

Connection strings (Import mode):
```
Driver={PostgreSQL Unicode};Server=localhost;Port=5432;Database=warehouse;Uid=platform;Pwd=platform;
```

## Power BI — Redshift Serverless (AWS)

1. **Get Data -> Amazon Redshift** (connector; also works via ODBC).
2. Server `default.XXXXXXXXXXXX.region.redshift-serverless.amazonaws.com:5439`,
   Database `dev`, IAM or user/password auth.
3. Navigate to the `serving` schema (same DDL, same table names).
4. Row-level security / refresh via the Power BI gateway for on-prem or
   **DirectQuery** if you want live queries against the warehouse.

Redshift ODBC connection string:
```
Driver={Amazon Redshift ODBC Driver (64-bit)};Server=<endpoint>:5439;Database=dev;Uid=<user>;Pwd=<pwd>;
```

## Querying the lake directly (Trino, stand-in for Athena)

Any of the Iceberg layers is visible through Trino once the catalog is up:

| Layer | Catalog.schema.table |
|---|---|
| bronze | `iceberg.retail.orders` (raw, kinesis lineage) |
| silver | `iceberg.retail_silver.orders` (typed, deduped) |
| gold | `iceberg.retail_gold.daily_sales` |

Example:
```
docker compose exec trino trino --catalog iceberg --schema retail_silver \
  --execute "SELECT COUNT(*), COUNT(DISTINCT order_id) FROM orders"
```

## Streamlit fallback

```
bash viz/streamlit_app/run_streamlit.sh   # then open http://localhost:8501
```

Same 4 reports, zero Power BI install — handy for visitors.

# wip170

/* wip */
