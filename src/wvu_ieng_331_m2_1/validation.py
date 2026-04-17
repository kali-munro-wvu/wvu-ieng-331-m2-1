from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl
from loguru import logger

# List of all 9 tables expected to exist in the Olist DuckDB database.
# Used by validate_database to confirm the schema is complete before analysis.
EXPECTED_TABLES = [
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "customers",
    "sellers",
    "products",
    "category_translation",
    "geolocation",
]


def validate_not_empty(df: pl.DataFrame, name: str) -> None:
    """Ensure a DataFrame is not empty before analysis proceeds.

    Args:
        df: The Polars DataFrame to validate.
        name: A readable name for the DataFrame being checked,
            used in log messages.

    Returns:
        None.

    Raises:
        ValueError: If the DataFrame contains zero rows.
    """
    if df.height == 0:
        logger.error(f"Validation failed: {name} returned no rows.")
        raise ValueError(f"Validation failed: {name} returned no rows")
    logger.info(f"Validation passed: {name} returned {df.height} rows.")


def validate_no_nulls(df: pl.DataFrame, columns: list[str]) -> None:
    """Ensure selected columns in a DataFrame contain no null values.

    Args:
        df: The Polars DataFrame to validate.
        columns: List of column names to check for null values.

    Returns:
        None.

    Raises:
        ValueError: If one or more of the checked columns contain null values.
    """
    # Count nulls in each specified column in a single Polars select pass
    null_counts = df.select([
        pl.col(c).is_null().sum().alias(c)
        for c in columns
    ])

    for col in columns:
        if null_counts[col][0] > 0:
            logger.error(f"Validation failed: {col} contains null values.")
            raise ValueError(f"Validation failed: {col} contains null values")
    logger.info(f"Validation passed: columns {columns} contain no null values.")


def validate_database(db_path: Path) -> bool:
    """Run all required validation checks against the Olist DuckDB database.

    Checks performed:
        - All 9 expected tables exist in the database schema.
        - Key columns (order_id, customer_id, product_id, seller_id)
          are not entirely NULL.
        - The date range in orders.order_purchase_timestamp is not empty
          and does not contain future-dated records.
        - Row counts for orders, order_items, and customers each exceed
          the minimum threshold of 1,000 rows.

    If any check fails, a WARNING is logged and the pipeline continues
    with a disclaimer rather than halting entirely. Only a missing database
    file or a DuckDB query error will raise an exception and halt the pipeline.

    Args:
        db_path: Path to the DuckDB database file to validate.

    Returns:
        True if all validation checks pass, False if one or more fail.

    Raises:
        FileNotFoundError: If the DuckDB file does not exist at db_path.
        duckdb.Error: If a DuckDB query fails during validation.
    """
    # Halt immediately if the database file is missing entirely
    if not db_path.exists():
        logger.error(f"Database file not found: {db_path}")
        raise FileNotFoundError(f"Database file not found: {db_path}")

    logger.info(f"Running database validation checks on {db_path}")
    validation_passed = True
    conn: duckdb.DuckDBPyConnection | None = None

    try:
        conn = duckdb.connect(str(db_path))

        # --- Check 1: All 9 expected tables exist ---
        # Queries the information schema to get actual table names in the
        # database, then compares against our expected list.
        existing_tables_result = conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchall()
        existing_tables = {row[0] for row in existing_tables_result}

        for table_name in EXPECTED_TABLES:
            if table_name not in existing_tables:
                logger.warning(f"Missing expected table: {table_name}")
                validation_passed = False

        # --- Check 2: Key columns are not entirely NULL ---
        # These four columns are foreign/primary keys that must be populated
        # for any joins or analysis to produce meaningful results.
        null_checks = [
            ("orders", "order_id"),
            ("orders", "customer_id"),
            ("order_items", "product_id"),
            ("order_items", "seller_id"),
        ]

        for table_name, column_name in null_checks:
            if table_name in existing_tables:
                # Store fetchone() result before unpacking to safely
                # handle the case where it returns None
                result = conn.execute(
                    f"""
                    SELECT COUNT(*) AS total_rows,
                           COUNT({column_name}) AS non_null_rows
                    FROM {table_name}
                    """
                ).fetchone()

                if result is None:
                    logger.warning(
                        f"Could not retrieve row counts for {table_name}.{column_name}"
                    )
                    validation_passed = False
                    continue

                total_rows, non_null_rows = result

                if total_rows == 0 or non_null_rows == 0:
                    logger.warning(
                        f"Column {table_name}.{column_name} is entirely NULL or the table is empty."
                    )
                    validation_passed = False
                else:
                    logger.info(
                        f"Column {table_name}.{column_name} passed null validation."
                    )

        # --- Check 3: Date range is valid and not future-dated ---
        # An empty date range means no orders exist to analyze.
        # Future dates would indicate data quality issues or corruption.
        if "orders" in existing_tables:
            # Store fetchone() result before unpacking to safely
            # handle the case where it returns None
            date_result = conn.execute(
                """
                SELECT
                    MIN(order_purchase_timestamp) AS min_date,
                    MAX(order_purchase_timestamp) AS max_date
                FROM orders
                """
            ).fetchone()

            if date_result is None:
                logger.warning("Could not retrieve date range from orders table.")
                validation_passed = False
            else:
                min_date, max_date = date_result
                if min_date is None or max_date is None:
                    logger.warning("orders.order_purchase_timestamp is empty.")
                    validation_passed = False
                elif max_date > datetime.now():
                    logger.warning(
                        f"orders.order_purchase_timestamp contains a future date: {max_date}"
                    )
                    validation_passed = False
                else:
                    logger.info(f"Orders date range is {min_date} to {max_date}")

        # --- Check 4: Core tables meet minimum row count threshold ---
        # 1,000 rows was chosen as the threshold because the Olist dataset
        # contains ~100,000 orders. Any count below 1,000 suggests the data
        # is incomplete, truncated, or the wrong file was provided.
        minimum_row_threshold = 1000
        core_tables = ["orders", "order_items", "customers"]

        for table_name in core_tables:
            if table_name in existing_tables:
                # Store fetchone() result before subscripting to safely
                # handle the case where it returns None
                count_result = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()

                row_count = count_result[0] if count_result is not None else 0

                if row_count < minimum_row_threshold:
                    logger.warning(
                        f"Table {table_name} has only {row_count} rows, below {minimum_row_threshold}."
                    )
                    validation_passed = False
                else:
                    logger.info(
                        f"Table {table_name} passed row count validation with {row_count} rows."
                    )

        return validation_passed

    except duckdb.Error as exc:
        # Catch DuckDB-specific errors separately from general exceptions
        # so the error message clearly identifies it as a database query failure
        logger.error(f"DuckDB validation error: {exc}")
        raise
    finally:
        # Always close the connection whether validation passed or failed
        if conn is not None:
            conn.close()


# --- What this module does ---
# validate_not_empty:  Checks that query results are not empty
# validate_no_nulls:   Checks selected columns for null values when needed
# validate_database:   Validates the DuckDB database before analysis starts
#                      Confirms required tables, key columns, dates, and
#                      row counts are present and logs warnings or errors
#                      when validation finds problems
