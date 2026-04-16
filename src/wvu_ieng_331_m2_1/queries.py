from pathlib import Path
import duckdb
import polars as pl

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "sql"
DATA_PATH = ROOT / "data" / "olist.duckdb"


def load_sql(filename: str) -> str:
    path = SQL_DIR / filename
    return path.read_text()


def run_query(sql_file: str, params: tuple = ()):
    conn = duckdb.connect(str(DATA_PATH))

    query = load_sql(sql_file)

    df = conn.execute(query, params).fetchdf()
    conn.close()

    return pl.from_pandas(df)


def get_seller_scorecard(start_date, state):
    return run_query("seller_scorecard.sql", (start_date, state))


def get_abc_classification(start_date):
    return run_query("abc_classification.sql", (start_date,))


## Understanding the above code aka what this does:
# Loads SQL files from /sql
# Connects to olist.duckdb
# Passes $1, $2 parameters safely
# Returns results as a DataFrame