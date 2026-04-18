"""Olist e-commerce analysis pipeline.

This package provides an automated, parameterizable pipeline for analyzing
the Olist Brazilian e-commerce dataset. It reads SQL queries from the sql/
directory, executes them against a DuckDB database, and produces CSV,
Parquet, and HTML outputs.

Modules:
    pipeline:   Entry point and orchestration logic.
    queries:    Data access layer, reads SQL files and returns Polars DataFrames.
    validation: Data validation layer, runs checks before analysis begins.
"""
