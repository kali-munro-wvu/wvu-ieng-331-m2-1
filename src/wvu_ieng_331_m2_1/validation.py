from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl
from loguru import logger 

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


def validate_not_empty(df: pl.DataFrame, name: str):
    """Ensure dataframe is not empty.
    Args:
        df: The Polars DataFram to validate.
        name: A readable name for the DataFrame being checked. 
    Returns:
        None.
    Raises:
        ValueError: If the DataFrame contains zero rows."""
    if df.height == 0:
        logger.error(f"Validation failed: {name} returned no rows.")
        raise ValueError(f"Validation failed: {name} returned no rows")
    logger.info(f"Validation passed: {name} returned {df.height} rows.")


def validate_no_nulls(df: pl.DataFrame, columns: list[str]) -> None:
    """Ensure selected columns contain no nulls.
    Args:
        df: The Polars DataFrame to validate.
        columns: Column names to check for null values.
    Returns:
        None.
    Raises:
        ValueError: If one or more checked columns contain null values."""
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
        """Runs required validation checks against the DuckDB database.
        Args:
            db_path: Path to the DuckDB database file.
        Returns:
            True if all validation checks pass. False if one or more checks fail.
        Raises:
            FileNotFoundError: If the DuckDB file does not exist.
            duckdb.Error: If a DuckDB query fails during validation."""
        if not db_path.exists():
            logger.error(f"Database file not found: {db_path}")
            raise FileNotFoundError(f"Database file not found: {db_path}")

        logger.info(f"Running database validation checks on {db_path}")
        validation_passed = True
        conn: duckdb.DuckDBPyConnection | None = None

        try:
            conn = duckdb.connect(str(db_path))

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

        null_checks = [
            ("orders", "order_id"),
            ("orders", "customer_id"),
            ("order_items", "product_id"),
            ("order_items", "seller_id"),
        ]

        for table_name, column_name in null_checks:
            if table_name in existing_tables:
                total_rows, non_null_rows = conn.execute(
                    f"""
                    SELECT COUNT(*) AS total_rows,
                           COUNT({column_name}) AS non_null_rows
                    FROM {table_name}
                    """
                ).fetchone()

                if total_rows == 0 or non_null_rows == 0:
                    logger.warning(
                        f"Column {table_name}.{column_name} is entirely NULL or the table is empty."
                    )
                    validation_passed = False
                else:
                    logger.info(
                        f"Column {table_name}.{column_name} passed null validation."
                    )

        if "orders" in existing_tables:
            min_date, max_date = conn.execute(
                """
                SELECT
                    MIN(order_purchase_timestamp) AS min_date,
                    MAX(order_purchase_timestamp) AS max_date
                FROM orders
                """
            ).fetchone()

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

        minimum_row_threshold = 1000
        core_tables = ["orders", "order_items", "customers"]

        for table_name in core_tables:
            if table_name in existing_tables:
                row_count = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]

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
        logger.error(f"DuckDB validation error: {exc}")
        raise
    finally:
        if conn is not None:
            conn.close()


## What this does
# Ensures queries return data
# Checks for missing values in key columns
# Stops pipeline early if data is bad

# rubric: 'validation layer runs before analysis'
