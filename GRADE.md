# Milestone 2 Grade (Regrade)

**Team 1** — Kali Munro, Genna Eline, Morgan Ennis
**Repo**: kali-munro-wvu/wvu-ieng-331-m2-1

*This grade reflects the resubmitted work. Original score was 18/24.*
*Revision commit: "Update queries.py - FIXED PIPELINE SPACING ERROR"*

| Criterion | Score | Max |
|-----------|------:|----:|
| Pipeline Functionality | 6 | 6 |
| Parameterization & Configuration | 6 | 6 |
| Code Quality | 6 | 6 |
| Project Structure & M1 Integration | 3 | 3 |
| Design Rationale (DESIGN.md) | 3 | 3 |
| **Total** | **24** | **24** |

---

## Pipeline Functionality (6/6)

The single blocker from the original submission — two `return` statements at column 0 in `queries.py` causing a `SyntaxError` on import — has been corrected. Both lines (end of `get_seller_scorecard` and `get_abc_classification`) now have the correct 4-space indentation.

**Default run (olist.duckdb):** Completes successfully. Produces all three outputs:
- `summary.csv` — 3,095 sellers, 233 KB
- `detail.parquet` — 32,951 products, 703 KB
- `chart.html` — interactive Altair bar chart, 647 KB

**Parameterized run (`--start-date 2024-01-01 --end-date 2024-06-01 --seller-state SP`):** Completes successfully. Filters demonstrably change output — seller_scorecard drops from 3,095 to 392 rows; abc_classification drops from 32,951 to 3,068 rows. All three outputs re-written correctly.

**Holdout test (olist_extended.duckdb, 128,028 orders vs 99,441):** Runs without modification, picks up extended data in validation (128,028 orders / 144,672 order_items confirmed in logs). All three outputs produced.

---

## Parameterization & Configuration (6/6)

No changes from original submission; already full marks. Three argparse parameters (`--start-date`, `--end-date`, `--seller-state`), all with `None` defaults for unfiltered analysis. Values flow through to SQL `$1`/`$2`/`$3` placeholders. Cross-parameter validation (start > end raises `ValueError`). Substantive `validate_database()` and `validate_not_empty()` checks. No deductions.

---

## Code Quality (6/6)

With the syntax error resolved, no code quality deductions remain. All quality criteria are met:

- **Type hints**: All function signatures fully annotated, including `str | None` union types and `duckdb.DuckDBPyConnection | None`.
- **Docstrings**: Google-style with Args, Returns, and Raises on every function across all three modules.
- **Logging**: `loguru` used consistently at INFO/WARNING/ERROR levels; zero `print()` statements.
- **Path handling**: `pathlib.Path` throughout for all file and directory operations.
- **Exception handling**: `duckdb.Error`, `OSError`, `FileNotFoundError`, `ValueError` — no bare `except:` clauses; `finally` blocks ensure connections are always closed.
- **SQL separation**: All SQL in `.sql` files; no embedded query strings in Python.

The original deduction was solely for the indentation bug. That bug is fixed; no other quality gaps were present.

---

## Project Structure & M1 Integration (3/3)

Unchanged from original; full marks retained.

| File | Present | Notes |
|------|---------|-------|
| `pyproject.toml` | Yes | Valid; `[project.scripts]` entry correct; `uv_build` backend |
| `uv.lock` | Yes | Committed |
| `.python-version` | Yes | Present |
| `.gitignore` | Yes | Correctly ignores `data/*.duckdb`, `output/`, `.venv/`, `__pycache__/` |
| `src/wvu_ieng_331_m2_1/pipeline.py` | Yes | |
| `src/wvu_ieng_331_m2_1/queries.py` | Yes | |
| `src/wvu_ieng_331_m2_1/validation.py` | Yes | |
| `sql/seller_scorecard.sql` | Yes | Uses `$1`, `$2`, `$3` placeholders |
| `sql/abc_classification.sql` | Yes | Uses `$1`, `$2` placeholders |
| `README.md` | Yes | All sections present |
| `DESIGN.md` | Yes | All 5 sections present |

---

## Design Rationale (3/3)

Unchanged from original; full marks retained. All 5 sections present (Parameter Flow, SQL Parameterization, Validation Logic, Error Handling, Scaling & Adaptation), all cross-referenced to real code that was verified to exist. No fabricated or stale references.

---

## Late Penalty Adjustment

This resubmission was received on or before 11:59 PM Wed Apr 22, 2026, in the **20% off improvement** tier per the resubmission policy. The penalty applies only to points earned beyond the original grade.

| | Points |
|---|---:|
| Original score | 18 / 24 |
| Regraded score | 24 / 24 |
| Improvement | +6 |
| Late penalty (20% × improvement) | -1.2 |
| **Final score** | **22.8 / 24** |
