# Design Rationale

---
This doument explains how the Milestone 2 pipeline is structured, how parameters move through the code, how SQL is executed, how validation and error handling work, and how the pipeline could be extended later. 

## Parameter Flow 

The pipeline begins in `pipeline.main()`, which serves as the entry point for the project. Inside `main()`, the first step is calling `parse_args()` from `pipeline.py`. That function uses `argparse` to read command-line options such as `--start-date`, `--end-date`, and `--seller-state`. Each argument is defined with `parser.add_argument()` and defaults to `None` if not provided by the user.

- After the arguments are parsed, the values are stored in the `args` object as `args.start_date`, `args.end_date`, and `args.seller_state`. Before any queries run, `main()` calls `validate_database(DATA_PATH)` from `validation.py` to confirm the database is healthy. The CLI arguments are not used during validation — it always checks the full database regardless of filters. For example, the seller scorecard flow sends the data and state filters into 'get_seller_scorecard()', while the ABC classification flow sends the date filter into 'get_abc_classification()'.
- From there, `main()` passes the relevant parameter values into the query layer. The seller scorecard flow calls `get_seller_scorecard(args.start_date, args.seller_state, args.end_date)`, while the ABC classification flow calls `get_abc_classification(args.start_date, args.end_date)`. Both functions are defined in `queries.py`. - Those functions do not contain SQL directly. Instead, each one calls `run_query()` and passes both the SQL filename and a tuple of Python parameter values. For example, `get_seller_scorecard()` calls `run_query("seller_scorecard.sql", (start_date, state, end_date), db_path)`.

Inside `run_query()`, the code calls `load_sql()` to read the SQL text from the correct file in the `sql/` directory using `pathlib`. Then DuckDB executes that SQL with the parameter tuple bound to the `$1`, `$2`, `$3` placeholders. For example, when `--seller-state SP` is passed, `args.seller_state = "SP"` is passed as the second element of the tuple, binding to `$2` in the WHERE clause:
- This structure keeps the parameter flow organized because the pipline is responsible for orchestration, while the queries module is responsible for SQL execution. 
```sql
AND ($2 IS NULL OR s.seller_state = $2)
```

Because `$2` is `"SP"` and not NULL, DuckDB filters results to only sellers in SP. This structure keeps parameter flow organized because `pipeline.py` is responsible for orchestration while `queries.py` is responsible for SQL execution.

## SQL Parameterization

One example of SQL parameterization in this project is the seller scorecard query. The SQL file lives in `sql/seller_scorecard.sql`, while the Python function that runs it is `get_seller_scorecard()` in `queries.py`.

- The raw SQL file uses positional placeholders `$1`, `$2`, and `$3` instead of directly inserting values into the query string. The WHERE clause looks like this:

```sql
WHERE ($1 IS NULL OR o.order_purchase_timestamp >= $1::TIMESTAMP)
  AND ($3 IS NULL OR o.order_purchase_timestamp <= $3::TIMESTAMP)
  AND ($2 IS NULL OR s.seller_state = $2)
```

Each condition is written so that when the parameter is NULL, the filter is skipped entirely and all rows pass through. This is how running with no arguments produces the full unfiltered analysis.

In Python, `load_sql()` reads the file using pathlib:
```python
path = SQL_DIR / filename
return path.read_text()
```

Then `run_query()` executes it with the parameter tuple:
```python
df = conn.execute(query, params).fetchdf()
```

DuckDB binds the values in the tuple to `$1`, `$2`, `$3` in order before executing.

- This is better than using f-string because parameterized queries are safer and cleaner. It prevents SQL injection — a malicious user could pass a value like `'; DROP TABLE orders; --` as a seller state and destroy the database. Parameterized queries prevent this because DuckDB treats the parameter values as data, never as executable SQL.They reduce the risk of formatting errors and avoid mixing SQL logic with string-building logic in Python. They also make it easier to reuse the same query structure with different input values.
- Keeping SQL in separate '.sql' files instead of writing inline SQL in Python alos improves maintainability. It makes the Python code easier to read, because the query functions focus on loading and running the SQL rather than storing large query strings inside the source code. It also makes it easier to revise or test the SQL separately from the Python pipeline. 


## Validation Logic 

- The validation layer lives in 'validation.py' and is called before the main analysis is allowed to continue. The main database-level validation function is 'validate_database()'.
- The first validation check confirms that all 9 expected tables exist in the DuckDB database: 'orders', 'order_items', 'order_payments', 'order_reviews', 'customers', 'sellers', 'products', 'category_translation', and 'geolocation'. This matters because the SQL analyses depend on joins across these tables. If even one is missing, the analytical outputs may be incomplete or fail entirely.
- The second validation check confirms that key identifier columns are not entirely 'NULL'. In this project, that includes 'order_id', 'customer_id', 'product_id', and 'seller_id'. This matters because these columns are required for joins and for interpreting the data correctly. If a key column is entirely null, the analysis may technically run but the results would not be trustworthy.
- The third validation check looks at the data range in 'orders.order_purchase_timestamp'. It checks that the feild is not empty and that the latest date is not in the future. This matters because the pipeline supports parmeterized date filtering, so the date field has to be valid and realistic for those filters to make sense.
- The fourth validation check verfies that the core tables 'orders', 'order_items', and 'customers' each exceed a minimum row treshold of 1,000 rows. This threshold is not meant to be a perfect rule for every possible dataset. It is used as a practical warning level to catch obviously incomplete or damaged data files.
- If one of these databse-level checks fails, the pipeline logs a 'WARNING' and continues with a disclaimer rather than halting immediately. This design choice makes the pipeline more resilient during grading and testing, especially if the holdout databse contains changes in scale but still follows the same schema.
- In addition to the databse checks, the pipeline also uses 'validate_not_empty()' on the final query outputs. That function raises a 'ValueError' if a result DataFrame comes back empty after filters are applied. This is treated more seriously because an empty analytical result usually means the requested analysis did not actually produce usable output.

## Error Handling 

**Example 1: `duckdb.Error` in `run_query()` in `queries.py`**

```python
except duckdb.Error as exc:
    logger.error(f"DuckDB query error in {sql_file}: {exc}")
    raise
```

This try/except block lives inside `run_query()` in `queries.py`, which is the function responsible for connecting to DuckDB, loading the SQL file, and executing the query. Because this function handles all direct database interaction, it is the most important place to catch database-specific errors.

We catch `duckdb.Error` specifically rather than a general `Exception` because it is the exact error type that DuckDB raises when something goes wrong at the query level — for example if a table referenced in the SQL does not exist, if a column name is misspelled, if the SQL has a syntax error, or if the database connection drops unexpectedly. By catching this specific type, the error message logged by loguru clearly identifies the failure as a database query problem and includes the name of the SQL file that caused it, which makes debugging much faster.

After logging the error we re-raise it with `raise` so the exception continues to propagate up through the call stack. This means the pipeline halts and the user sees the full traceback rather than silently continuing with no output. If we used a bare `except:` instead of `except duckdb.Error`, we would also accidentally catch `KeyboardInterrupt` (triggered when the user presses Ctrl+C to cancel the program) and `SystemExit` (triggered by `sys.exit()`). Catching those would make the program impossible to cancel mid-run and would hide completely unrelated bugs under a generic error message, making the pipeline much harder to maintain and debug.

---

**Example 2: `OSError` in `save_outputs()` in `pipeline.py`**

```python
except OSError as exc:
    logger.error(f"Failed to write output files: {exc}")
    raise
```

This try/except block lives inside `save_outputs()` in `pipeline.py`, which is the function responsible for creating the output directory and writing all three output files — `summary.csv`, `detail.parquet`, and `chart.html`. Because this function performs all file system operations, it is the right place to catch file system errors.

We catch `OSError` specifically because it is the error Python raises for operating system and file system problems. Common examples include not having write permission to the output directory, the disk being full, the file path being too long for the operating system, or the output directory failing to be created. By catching this specific type, the error message logged by loguru clearly identifies the failure as a file system problem rather than a query failure or a logic error in the analysis itself.

Like the first example, we re-raise the exception after logging it so the pipeline halts cleanly and the user sees the full traceback. This is important because if `save_outputs()` fails silently, the user might think the pipeline completed successfully but find no output files, which would be confusing. If we used a bare `except:` instead of `except OSError`, we would mask the real cause of the failure entirely. The user would have no way to tell whether the problem was a permissions issue, a full disk, or something else — and fixing the problem would require guessing rather than reading a clear error message.

## Scaling & Adaptation

- If the Olist dataset grew to 10 million orders, the first part of the pipeline that would likely slow down most is the query execution and DataFrame conversion in 'queries.run_query()'. Right now, the workflow runs the query in DuckDB, fetches the results, and converts them into Polars DataFrame. That works well at the current project scale, but with a much larger dataset the cost of returning large results to Python would become more noticeable.
- To adapt the pipeline, I would keep as much aggregation as possible inside DuckDB before returning results to Python. I would also be more selective about which results are exported to CSV versus Parquet, because Parquet is better for larger datasets. If needed, I would also reduce unnecessary conversions and only materialize the specific fields required for the final outputs.
- If a third output format needed to be added, such as JSON API-style response, the best place to add it would be in 'save_outputs()' inside "pipeline.py'. That function already controls how the existing outputs are written, so it is the most natural place to extend the output layer. The query functions themselves would not need major changes unless the JSON format required a different level of aggregation or different columns. In most cases, I would only modify 'save_outputs()' and possibly add a small helper dunction to keep the output-writing logic organized. 

