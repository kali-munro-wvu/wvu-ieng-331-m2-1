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

## Analysis Summary

This pipline carries forward two analytical views from Milestone 1 and makes them reusable through Python and command-line parameters.
The first analysis is the seller scorecard. This ouput is meant to summarize seller performance using the existing seller scorecard SQL file. The pipeline can filter the analysis by '--start-date' and '--seller-state', which makes it possible to return the same logic on different slices of the Olist dataset without rewriting the query. The summary-level results are written to 'summary.csv' and are also used to generate the visualization in 'chart.html'.
The second analysis is the ABC classification output. This analysis classifies products based on their contribution to overall revenue, which helps identify the highest-impact products. The detailed results are written to 'detail.parquet', which keeps the output efficient while preserving the full dataset structure.
Together, these outputs make the original Milestone 1 analysis more practical because the team can rerun the same work with different filters, save structured outputs, and use the results in later reporting steps.

## Limitations & Caveats

This pipeline depends on the expected Olist DuckDB schema being present. If required tables or columns are missing, validation will log warnings and the pipeline may continue with a disclaimer, but the resulting outputs may be incomplete.
The command-line interface currently accepts '--start-date', '--end-date', and '--seller-state', but the exact effect of each parameter depends on how the SQL files use the passed placeholders. The pipeline structure supports parameterized execution, but future updates may still be needed if additional filtering logic is added to the SQL layer.
The validation layer is designed to catch common structural problems before analysis begins, such as missing tables, empty data ranges, future-dated timestamps, and very small row counts. However, it does not attempt to correct bad data automatically. It only reports issues so the user knows the outputs should be interpreted carefully. 
The generated Altair chart is an exploratory visualization and is meant to support quick inspection of results. It is not intended to be the final presentation layer for a non-technical audience. That part will be expanded in the final deliverable milestone.

## Outputs

The pipeline creates the following files in the 'output/' directory each time it runs:

- 'summary.csv'
  This is the summary-level output. It contains the seller scorecord results returned by the seller performance query. It is intended to be a quick, readable file that can be opened in Excel or another spreadsheet program.
- 'detail.parquet'
  This is the detail-level output. It stores the ABC classification results in Parquet format, which is more efficient for larger datasets and preserves the structure of the full results better than CSV.
- 'chart.html'
  This is an Altair chart exported as a self-contained HTML file. It provides a quick visual of the summary results and can be opened directly in a browser. 

## Parameters

| Parameter | Type | Default | Desfription |
|-----------|------|---------|-------------|
| '--start-date' | date string | 'None' | Optional lower bound for filtering the analysis. Expected format is 'YYYY-MM-DD'. |
| '--end-date' | date string | 'None' | Optional upper bound for filtering the analysis. Expected format is 'YYYY-MM-DD'. |
| '--seller-state' | string | 'None' | Optional seller state filter used to restrict the seller scorecard results to one state. |
