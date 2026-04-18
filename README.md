# IENG331-M2-1
# Milestone 2: Python Pipeline

**Team 1**: Kali Munro, Genna Eline, Morgan Ennis

## How to Run

Instructions to run the pipeline from a fresh clone:

```bash
git clone https://github.com/G-Eline/wvu-ieng-331-m2-1.git
cd wvu-ieng-331-m2-1
uv sync
# place olist.duckdb in the data/ directory
uv run wvu-ieng-331-m2-1
uv run wvu-ieng-331-m2-1 --start-date 2024-01-01 --seller-state SP
uv run wvu-ieng-331-m2-1 --start-date 2024-01-01 --end-date 2024-06-01 --seller-state SP
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `--start-date` | string (YYYY-MM-DD) | None (no filter) | Filter orders on or after this date. Applies to both seller scorecard and ABC classification queries. |
| `--end-date` | string (YYYY-MM-DD) | None (no filter) | Filter orders on or before this date. Applies to both seller scorecard and ABC classification queries. |
| `--seller-state` | string (e.g. SP) | None (no filter) | Filter sellers by two-letter Brazilian state code. Applies to seller scorecard query only. |

Running with no arguments produces the full unfiltered analysis across all dates and states.

## Outputs

All output files are written to the `output/` directory, which is created automatically at runtime and is not committed to the repository.

| File | Format | Description |
|------|--------|-------------|
| `summary.csv` | CSV | One row per seller. Contains seller_id, seller_state, total_items_sold, total_revenue, avg_item_price, total_orders, and avg_review_score. Use this for high-level seller performance comparisons. |
| `detail.parquet` | Parquet | One row per product. Contains product_id, product_category_name, total_items_sold, total_revenue, cumulative_revenue_pct, and abc_class (A, B, or C). Use this for product-level inventory prioritization. |
| `chart.html` | HTML | Self-contained interactive Altair bar chart showing total revenue by seller state. Open in any browser. No additional software required. |

## Validation Checks

The pipeline runs the following checks via `validate_database()` in `validation.py` before any queries execute. Validation failures log a WARNING and allow the pipeline to continue with a disclaimer rather than halting entirely. Only a missing database file or a DuckDB query error will halt the pipeline.

| Check | What it verifies | If it fails |
|-------|-----------------|-------------|
| Table existence | All 9 expected tables exist: orders, order_items, order_payments, order_reviews, customers, sellers, products, category_translation, geolocation | WARNING logged, pipeline continues |
| NULL key columns | order_id, customer_id, product_id, seller_id are not entirely NULL | WARNING logged, pipeline continues |
| Date range | orders.order_purchase_timestamp is not empty and contains no future-dated records | WARNING logged, pipeline continues |
| Row count threshold | orders, order_items, and customers each have at least 1,000 rows | WARNING logged, pipeline continues |
| Empty results | Query results are not empty after filters are applied | ERROR raised, pipeline halts |

