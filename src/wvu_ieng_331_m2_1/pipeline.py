import argparse
from pathlib import Path

import altair as alt
import polars as pl
from loguru import logger

from .queries import get_abc_classification, get_seller_scorecard
from .validation import validate_database, validate_not_empty

# Resolve the project root and output directory relative to this file
# so the pipeline works regardless of where it is called from.
ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = ROOT / "output"
DATA_PATH = ROOT / "data" / "olist.duckdb"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the pipeline.

    Supports filtering by start date, end date, and seller state.
    Running with no arguments produces the full unfiltered analysis.

    Returns:
        A Namespace object with attributes start_date, end_date,
        and seller_state.

    Raises:
        ValueError: If --start-date is after --end-date.
    """
    parser = argparse.ArgumentParser(description="Olist e-commerce analysis pipeline.")
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Filter orders on or after this date (YYYY-MM-DD). Default: no filter.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Filter orders on or before this date (YYYY-MM-DD). Default: no filter.",
    )
    parser.add_argument(
        "--seller-state",
        type=str,
        default=None,
        help="Filter sellers by two-letter state code (e.g. SP). Default: all states.",
    )

    args = parser.parse_args()

    # Validate that start date is not after end date if both are provided
    if args.start_date and args.end_date:
        if args.start_date > args.end_date:
            logger.error("--start-date cannot be after --end-date")
            raise ValueError("--start-date cannot be after --end-date")

    return args


def save_outputs(seller_df: pl.DataFrame, abc_df: pl.DataFrame) -> None:
    """Save pipeline results to the output/ directory.

    Creates the output/ directory if it does not exist, then writes:
        - summary.csv: seller scorecard metrics, one row per seller
        - detail.parquet: full ABC classification dataset, one row per product
        - chart.html: interactive Altair bar chart of revenue by seller state

    Args:
        seller_df: Polars DataFrame containing seller scorecard results.
        abc_df: Polars DataFrame containing ABC classification results.

    Raises:
        OSError: If the output directory cannot be created or files cannot
            be written to disk.
    """
    try:
        # Create output directory at runtime if it does not already exist
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # summary.csv — aggregated seller metrics, one row per seller
        logger.info("Writing summary.csv...")
        seller_df.write_csv(OUTPUT_DIR / "summary.csv")

        # detail.parquet — full ABC classification dataset, one row per product
        logger.info("Writing detail.parquet...")
        abc_df.write_parquet(OUTPUT_DIR / "detail.parquet")

        # chart.html — interactive Altair bar chart of total revenue by seller state
        logger.info("Writing chart.html...")
        chart = (
            alt.Chart(seller_df.to_pandas())
            .mark_bar()
            .encode(
                x=alt.X("seller_state", title="Seller State"),
                y=alt.Y("total_revenue", title="Total Revenue"),
                tooltip=["seller_state", "total_revenue"],
            )
            .properties(title="Total Revenue by Seller State")
        )
        chart.save(str(OUTPUT_DIR / "chart.html"))
        logger.info(f"All outputs written to {OUTPUT_DIR}")

    except OSError as exc:
        # Catch file system errors that occur when creating the directory
        # or writing output files so the error message is clear
        logger.error(f"Failed to write output files: {exc}")
        raise


def main() -> None:
    """Orchestrate the full pipeline: validate, query, process, and output.

    Workflow:
        1. Parse command-line arguments
        2. Run database validation checks
        3. Query seller scorecard and ABC classification data
        4. Validate query results are not empty
        5. Save outputs to the output/ directory

    Raises:
        FileNotFoundError: If the database file does not exist.
        ValueError: If query results are empty after filtering.
        OSError: If output files cannot be written.
    """
    logger.info("Starting Olist analysis pipeline...")
    args = parse_args()

    # Log which filters are active so the user knows what data is being analyzed
    logger.info(
        f"Parameters — start_date: {args.start_date}, "
        f"end_date: {args.end_date}, "
        f"seller_state: {args.seller_state}"
    )

    # Step 1: Validate the database before running any queries
    # Validation failures log warnings but do not halt the pipeline
    logger.info("Running validation...")
    validation_passed = validate_database(DATA_PATH)
    if not validation_passed:
        logger.warning(
            "One or more validation checks failed. "
            "Pipeline will continue but results may be incomplete."
        )

    # Step 2: Run queries with CLI parameters applied
    logger.info("Querying seller scorecard...")
    seller_df = get_seller_scorecard(args.start_date, args.seller_state)

    logger.info("Querying ABC classification...")
    abc_df = get_abc_classification(args.start_date)

    # Step 3: Validate that query results are not empty
    logger.info("Validating query results...")
    validate_not_empty(seller_df, "seller_scorecard")
    validate_not_empty(abc_df, "abc_classification")

    # Step 4: Save all outputs
    logger.info("Saving outputs...")
    save_outputs(seller_df, abc_df)

    logger.info("Pipeline completed successfully. Outputs written to /output")


# --- What this module does ---
# parse_args:    Parses CLI arguments (--start-date, --end-date, --seller-state)
# save_outputs:  Writes summary.csv, detail.parquet, and chart.html to output/
# main:          Orchestrates the full pipeline: validate → query → process → output
