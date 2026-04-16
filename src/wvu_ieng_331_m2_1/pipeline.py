import argparse
from pathlib import Path

import polars as pl
import altair as alt

from .queries import get_seller_scorecard, get_abc_classification
from .validation import validate_not_empty


ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = ROOT / "output"


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--seller-state", type=str, default=None)

    return parser.parse_args()


def save_outputs(seller_df: pl.DataFrame, abc_df: pl.DataFrame):
    OUTPUT_DIR.mkdir(exist_ok=True)

    # CSV summary
    seller_df.write_csv(OUTPUT_DIR / "summary.csv")

    # Parquet detail
    abc_df.write_parquet(OUTPUT_DIR / "detail.parquet")

    # Simple chart (placeholder example)
    chart = (
        alt.Chart(seller_df.to_pandas())
        .mark_bar()
        .encode(x="seller_state", y="total_revenue")
    )

    chart.save(str(OUTPUT_DIR / "chart.html"))


def main():
    args = parse_args()

    print("Running pipeline...")

    seller_df = get_seller_scorecard(args.start_date, args.seller_state)
    abc_df = get_abc_classification(args.start_date)

    validate_not_empty(seller_df, "seller_scorecard")
    validate_not_empty(abc_df, "abc_classification")

    save_outputs(seller_df, abc_df)

    print("Pipeline completed successfully. Outputs written to /output")


if __name__ == "__main__":
    main()

## What this gives you
# Command-line arguments (--start-date, --seller-state)
# Calls your SQL layer (queries.py)
# Runs validation before continuing
# Serves as the program entry point

# added: summary.csv → main results, detail.parquet → detailed dataset, chart.html → visualization (Altair), Creates /output automatically