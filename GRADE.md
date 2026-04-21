# Milestone 2 Grade

**Team 1** — Kali Munro, Genna Eline, Morgan Ennis
**Repo**: kali-munro-wvu/wvu-ieng-331-m2-1

| Criterion | Score | Max |
|-----------|------:|----:|
| Pipeline Functionality | 2 | 6 |
| Parameterization & Configuration | 6 | 6 |
| Code Quality | 4 | 6 |
| Project Structure & M1 Integration | 3 | 3 |
| Design Rationale (DESIGN.md) | 3 | 3 |
| **Total** | **18** | **24** |

---

## Pipeline Functionality (2/6)

**Critical bug — pipeline does not run as submitted.**

`src/wvu_ieng_331_m2_1/queries.py` has two `return` statements at column 0 (outside their enclosing functions), causing a `SyntaxError` on import:

```
File ".../queries.py", line 120
    return run_query("seller_scorecard.sql", ...)
    ^
SyntaxError: 'return' outside function
```

This occurs on line 120 (end of `get_seller_scorecard`) and line 143 (end of `get_abc_classification`). Both lines are missing 4 spaces of indentation.

**After a 2-character-per-line indentation fix:**
- `uv sync`: clean, all dependencies resolved.
- Default run: completes successfully; produces all 3 outputs (summary.csv 228 KB, detail.parquet 690 KB, chart.html 633 KB).
- Param run (`--start-date 2024-01-01 --end-date 2024-06-01 --seller-state SP`): produces filtered results (392 sellers, 3068 products) — params demonstrably change SQL output.
- Holdout test (olist_extended.duckdb): runs without modification; picks up extended data (128 028 orders vs 99 441); all 3 outputs produced. Validation correctly flags future-dated record (2026-03-17 max date).

Score is **2** (partially works; the syntax error is the sole blocker; outputs are otherwise complete and correct).

---

## Parameterization & Configuration (6/6)

- **3 argparse params**: `--start-date`, `--end-date`, `--seller-state`, all documented with types and defaults in `parse_args()`.
- **Defaults = full analysis**: all three default to `None`, producing unfiltered results.
- **Params flow to SQL**: values are passed as a positional tuple to `conn.execute(query, params)`, binding to `$1`/`$2`/`$3` in both SQL files. Changing params demonstrably changes row counts (verified above).
- **Validation layer is substantive** (`validate_database()` in `validation.py`):
  1. Checks all 9 expected tables exist.
  2. Checks 4 key columns (order_id, customer_id, product_id, seller_id) are not entirely NULL.
  3. Checks date range is non-empty and not future-dated.
  4. Checks orders, order_items, and customers each exceed 1,000 rows.
  5. `validate_not_empty()` checks query results post-filter, raising `ValueError` on empty result.
- All validation findings logged via loguru at appropriate WARNING/INFO/ERROR levels.

---

## Code Quality (4/6)

**Strengths:**
- All functions have full Google-style docstrings (Args, Returns, Raises).
- All function signatures are fully type-hinted, including `str | None` union types and `duckdb.DuckDBPyConnection | None`.
- No `print()` statements; loguru used consistently for all log levels.
- `pathlib.Path` used throughout for all file and directory operations.
- Specific exception handling: `duckdb.Error`, `OSError`, `FileNotFoundError`, `ValueError` — no bare `except:` clauses.
- `finally` block in `run_query()` and `validate_database()` ensures connections are always closed.
- Clean module separation; SQL kept in `.sql` files, not embedded in Python strings.

**Deductions:**
- The indentation bug on lines 120 and 143 of `queries.py` is a straightforward Python error that should have been caught by any linter or test run before submission. It renders the package completely unimportable as shipped. This is a code-quality failure that prevents execution.

Score is **4** (reasonable quality, well-annotated, but the unguarded syntax error represents a significant quality gap).

---

## Project Structure & M1 Integration (3/3)

All required files present and correct:

| File | Present | Notes |
|------|---------|-------|
| `pyproject.toml` | Yes | Valid; `[project.scripts]` entry `wvu-ieng-331-m2-1 = "wvu_ieng_331_m2_1.pipeline:main"`; uv_build backend |
| `uv.lock` | Yes | Committed |
| `.python-version` | Yes | Present |
| `.gitignore` | Yes | Comprehensive; correctly ignores `data/*.duckdb`, `output/`, `.venv/`, `__pycache__/` |
| `src/wvu_ieng_331_m2_1/pipeline.py` | Yes | |
| `src/wvu_ieng_331_m2_1/queries.py` | Yes | |
| `src/wvu_ieng_331_m2_1/validation.py` | Yes | |
| `sql/seller_scorecard.sql` | Yes | Uses `$1`, `$2`, `$3` placeholders |
| `sql/abc_classification.sql` | Yes | Uses `$1`, `$2` placeholders |
| `README.md` | Yes | All sections: How to Run, Parameters, Outputs, Validation Checks, Analysis Summary, Limitations |
| `DESIGN.md` | Yes | All 5 sections |

---

## Design Rationale (3/3)

All 5 sections present, specific, and accurately cross-referenced to real code:

| Section | Completeness | Code Reference Accuracy |
|---------|-------------|------------------------|
| Parameter Flow | Complete | Correctly traces `main()` → `parse_args()` → `get_seller_scorecard(args.start_date, args.seller_state, args.end_date)` → `run_query()` → SQL `$2` placeholder |
| SQL Parameterization | Complete | Exact WHERE clause shown; explains NULL-passthrough pattern; explains injection safety; matches actual `seller_scorecard.sql` |
| Validation Logic | Complete | Describes all 4 `validate_database()` checks by name and rationale; references `validate_not_empty()`; matches `validation.py` exactly |
| Error Handling | Complete | Shows actual `except duckdb.Error` and `except OSError` blocks from `run_query()` and `save_outputs()`; explains why bare `except:` is avoided |
| Scaling & Adaptation | Complete | References `queries.run_query()` and `save_outputs()` by name; discusses DuckDB-side aggregation and Parquet preference |

Function names cited in DESIGN.md all exist in the code as described. No fabricated or stale references found.
