# Design Rationale

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
