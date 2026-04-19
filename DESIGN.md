# Design Rationale

---
This doument explains how the Milestone 2 pipeline is structured, how parameters move through the code, how SQL is executed, how validation and error handling work, and how the pipeline could be extended later. 

## Parameter Flow 

- The pipeline begins in 'pipeline.main()', which serves as the entry point for the project. Inside 'main()', the first step is calling 'parse_args()' from pipeline.py'. That function uses 'argparse' to read command-line options such as '--start-date', '--end-date', and '--seller-state'.
- After the arguments are parsed, the values are stored in the 'args' object. From there, 'main()' passes the relevant parameter values into the query layer. For example, the seller scorecard flow sends the data and state filters into 'get_seller_scorecard()', while the ABC classification flow sends the date filter into 'get_abc_classification()'.
- Those functions are defined in 'queries.py'. They do not contain the SQL directly. Instead, each one calls 'run_query()' and passes both the SQL filename and a tuple of Python parameter values. For example, 'get_seller_scorecard()' passes its values into 'run_query("seller_scorecard.sql", (start_date, state))'.
- Inside 'run_query()', the code first calls 'load_sql()' to read the SQL text from the correct file in the 'sql/' directory. Then DuckDB executes that SQL with the parameter tuple. This means the command-line values travel from 'pipeline.parse_args()' to 'pipeline.main()', then into 'queries.get_seller_scorecard()' or queries.get_abc_classification()', and finally into 'queries.run_query()' where the SQL is executed.
- This structure keeps the parameter flow organized because the pipline is responsible for orchestration, while the queries module is responsible for SQL execution. 

## SQL Parameterization

- One example of SQL parameterization in this project is the seller scorecard query. The SQL file lives in 'sql/seller_scorecard.sql', while the Python function that runs it is 'get_seller_scorecard()' in 'queries.py'.
- The raw SQL file uses positional placeholders such as '$1' and '$2' instead of directly inserting values into the query string. In Pythong, 'get_seller_scorecard()' passes its parameter values into 'run_query()' as a tuple. Then 'run_query()' sends both the SQL text and the parameter tuple into 'conn.execute(query, params)'.
- This is better than using f-string because parameterized queries are safer and cleaner. They reduce the risk of formatting errors and avoid mixing SQL logic with string-building logic in Python. They also make it easier to reuse the same query structure with different input values.
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

- One important 'try/except' block is in 'pipeline.mainI()'. This block catches 'FileNotFoundError', 'duckdb.Error', 'ValueError', and "OSError'. These exceptions were chosen intentionally instead if using a bare 'except:' because each one represents a different kind of failure the user might need to understand.
- 'FileNotFoundError' is used when a required file, such as the DuckDB database, is missing. In that case, the pipeline logs an error message that clearly tells the user what file was not found. 'duckdb.Error' is used for SQL exection failures or database-related problems. This is important because it separates database problems from parameter issues or file-writing issues.
- 'ValueError' is used for invalid input values or failed result-level validation, such as an empty DataFrame after filters are applied. 'OSError' is used when the pipeline cannot write output files like 'summary.csv', 'detail.parquet', or 'chart.html'.
- A second important 'try/except' block is in 'queries.run_query()'. That function catches 'duckdb.Error' when DuckDB cannot execute the requested SQL file correctly. This makes the failure easier to trace because the log can identify which query file caused the issue.
- If the code used a bare 'except:' instead, the user would get less specific feedback. Different errors would all look the same, which would make debugging harder and hide the real cause of the problem. 

## Scaling & Adaptation

- If the Olist dataset grew to 10 million orders, the first part of the pipeline that would likely slow down most is the query execution and DataFrame conversion in 'queries.run_query()'. Right now, the workflow runs the query in DuckDB, fetches the results, and converts them into Polars DataFrame. That works well at the current project scale, but with a much larger dataset the cost of returning large results to Python would become more noticeable.
- To adapt the pipeline, I would keep as much aggregation as possible inside DuckDB before returning results to Python. I would also be more selective about which results are exported to CSV versus Parquet, because Parquet is better for larger datasets. If needed, I would also reduce unnecessary conversions and only materialize the specific fields required for the final outputs.
- If a third output format needed to be added, such as JSON API-style response, the best place to add it would be in 'save_outputs()' inside "pipeline.py'. That function already controls how the existing outputs are written, so it is the most natural place to extend the output layer. The query functions themselves would not need major changes unless the JSON format required a different level of aggregation or different columns. In most cases, I would only modify 'save_outputs()' and possibly add a small helper dunction to keep the output-writing logic organized. 

