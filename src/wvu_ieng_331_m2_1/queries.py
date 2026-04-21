from pathlib import Path

import duckdb
import polars as pl
from loguru import logger

# Resolve paths relative to this file's location so the pipeline works
# regardless of where it is called from on the filesystem.
ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "sql"
DATA_PATH = ROOT / "data" / "olist.duckdb"


def load_sql(filename: str) -> str:
    """Read a SQL file from the sql/ directory and return its contents.

    Args:
        filename: The name of the SQL file to load (e.g. 'seller_scorecard.sql').

    Returns:
        The raw SQL query string read from the file.

    Raises:
        FileNotFoundError: If the SQL file does not exist in the sql/ directory.
    """
    path = SQL_DIR / filename

    # Raise a clear error if the SQL file is missing rather than letting
    # Python raise a generic FileNotFoundError with a confusing message
    if not path.exists():
        logger.error(f"SQL file not found: {path}")
        raise FileNotFoundError(f"SQL file not found: {path}")

    logger.info(f"Loading SQL file: {filename}")
    return path.read_text()


def run_query(sql_file: str, params: tuple = (), db_path: Path = DATA_PATH) -> pl.DataFrame:
    """Execute a parameterized SQL query against the DuckDB database.

    Reads the SQL from a file in the sql/ directory, executes it with the
    provided parameters, and returns the result as a Polars DataFrame.
    Using parameterized queries (with $1, $2 placeholders) instead of
    f-strings prevents SQL injection and keeps SQL logic in .sql files
    rather than scattered through Python code.

    Args:
        sql_file: The name of the SQL file to execute (e.g. 'seller_scorecard.sql').
        params: A tuple of parameter values to bind to the query's $1, $2
            placeholders. Defaults to an empty tuple for unparameterized queries.
        db_path: Path to the DuckDB database file. Defaults to DATA_PATH but
            can be overridden so the holdout test can point to a different file.

    Returns:
        A Polars DataFrame containing the query results.

    Raises:
        FileNotFoundError: If the database file does not exist.
        duckdb.Error: If the query fails to execute.
        OSError: If the database file cannot be opened.
    """
    # Check for the database file before attempting to connect so we can
    # raise a clear FileNotFoundError instead of a cryptic DuckDB message
    if not db_path.exists():
        logger.error(f"Database file not found: {db_path}")
        raise FileNotFoundError(f"Database file not found: {db_path}")

    conn: duckdb.DuckDBPyConnection | None = None

    try:
        conn = duckdb.connect(str(db_path))
        query = load_sql(sql_file)
        logger.info(f"Executing query from {sql_file} with params: {params}")

        # Execute with parameterized values — never use f-strings or string
        # concatenation to build SQL, as that opens the door to SQL injection
        df = conn.execute(query, params).fetchdf()
        logger.info(f"Query returned {len(df)} rows.")
        return pl.from_pandas(df)

    except duckdb.Error as exc:
        # Catch DuckDB-specific errors so the message clearly identifies
        # this as a query failure rather than a generic Python exception
        logger.error(f"DuckDB query error in {sql_file}: {exc}")
        raise
    except OSError as exc:
        # Catch file system errors that occur when opening the database
        logger.error(f"OS error when accessing database: {exc}")
        raise
    finally:
        # Always close the connection whether the query succeeded or failed
        if conn is not None:
            conn.close()


def get_seller_scorecard(
    start_date: str | None,
    state: str | None,
    end_date: str | None = None,
    db_path: Path = DATA_PATH,
) -> pl.DataFrame:
    """Retrieve seller scorecard metrics from the database.

    Wraps the seller_scorecard.sql query which calculates per-seller
    performance metrics including order counts, revenue, and average
    review scores. Results can be filtered by date and seller state.

    Args:
        start_date: Optional start date string (e.g. '2024-01-01') to filter
            orders. Passed as $1 in the SQL query. None means no date filter.
        state: Optional two-letter seller state code (e.g. 'SP') to filter
            results. Passed as $2 in the SQL query. None means all states.
        db_path: Path to the DuckDB database file. Defaults to DATA_PATH.

    Returns:
        A Polars DataFrame with one row per seller and columns for
        seller performance metrics.
    """
    logger.info(f"Fetching seller scorecard (start_date={start_date}, state={state})")
    return run_query("seller_scorecard.sql", (start_date, state, end_date), db_path)

def get_abc_classification(
    start_date: str | None,
    end_date: str | None = None,
    db_path: Path = DATA_PATH,
) -> pl.DataFrame:
    """Retrieve ABC classification data for products from the database.

    Wraps the abc_classification.sql query which ranks products by revenue
    and assigns each an ABC class: A (top 80% of revenue), B (next 15%),
    or C (bottom 5%).

    Args:
        start_date: Optional start date string (e.g. '2024-01-01') to filter
            orders. Passed as $1 in the SQL query. None means no date filter.
        db_path: Path to the DuckDB database file. Defaults to DATA_PATH.

    Returns:
        A Polars DataFrame with one row per product containing its ABC
        class and cumulative revenue percentage.
    """
    logger.info(f"Fetching ABC classification (start_date={start_date})")
    return run_query("abc_classification.sql", (start_date, end_date), db_path)

# --- What this module does ---
# load_sql:                Reads a SQL file from the sql/ directory
# run_query:               Connects to DuckDB, executes a parameterized query,
#                          and returns results as a Polars DataFrame
# get_seller_scorecard:    Fetches seller performance metrics, filterable by
#                          date and state
# get_abc_classification:  Fetches product ABC classification data, filterable
#                          by date
