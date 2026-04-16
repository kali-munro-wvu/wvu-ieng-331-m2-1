import polars as pl


def validate_not_empty(df: pl.DataFrame, name: str):
    """Ensure dataframe is not empty."""
    if df.height == 0:
        raise ValueError(f"Validation failed: {name} returned no rows")


def validate_no_nulls(df: pl.DataFrame, columns: list[str]):
    """Ensure selected columns contain no nulls."""
    null_counts = df.select([
        pl.col(c).is_null().sum().alias(c)
        for c in columns
    ])

    for col in columns:
        if null_counts[col][0] > 0:
            raise ValueError(f"Validation failed: {col} contains null values")


## What this does
# Ensures queries return data
# Checks for missing values in key columns
# Stops pipeline early if data is bad

# rubric: 'validation layer runs before analysis'