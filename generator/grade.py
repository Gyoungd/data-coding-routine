#!/usr/bin/env python3
"""
grade.py — run a solution against a generated problem and compare to the
reference output, in the Cowork sandbox.

Supports two solution kinds (auto-detected by file extension):
  .sql  -> executed in DuckDB (PostgreSQL-ish dialect) over the problem's tables
  .py   -> executed as pandas; each table is preloaded as a DataFrame by name,
           and your code must assign the final answer to a variable `result`
           (a pandas DataFrame).

Comparison mirrors StrataScratch's checker behaviour:
  - row values are compared, not column names
  - floats rounded to 6 dp, dates normalised to YYYY-MM-DD
  - row order ignored UNLESS the problem is marked order_sensitive

Usage:
    python grade.py problems/gen-window-medium-0007/problem.json mysol.sql
    python grade.py problems/gen-window-medium-0007/problem.json mysol.py

Requires: duckdb, pandas
"""
from __future__ import annotations

import json
import os
import sys

import pandas as pd

try:
    import duckdb
except ImportError:
    sys.exit("duckdb not installed. Run: pip install duckdb pandas")

import datetime as dt


def _normalize_value(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, float):
        return round(v, 6)
    if isinstance(v, int):
        return v
    if isinstance(v, (pd.Timestamp, dt.date, dt.datetime)):
        return pd.Timestamp(v).strftime("%Y-%m-%d")
    s = str(v).strip()
    # numeric strings -> number for tolerant compare
    try:
        f = float(s)
        return round(f, 6) if ("." in s or "e" in s.lower()) else int(f)
    except ValueError:
        # date-like strings
        try:
            return pd.Timestamp(s).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return s


def _norm_rows(rows):
    return [[_normalize_value(v) for v in r] for r in rows]


def _load_tables(problem_dir: str, problem: dict) -> dict[str, pd.DataFrame]:
    tables = {}
    for name in problem["tables"]:
        df = pd.read_csv(os.path.join(problem_dir, f"{name}.csv"))
        # restore declared date columns
        for col in problem["schema_sql"].get(name, []):
            if col["type"] == "date" and col["name"] in df.columns:
                df[col["name"]] = pd.to_datetime(df[col["name"]])
        tables[name] = df
    return tables


def run_sql(sql: str, tables: dict[str, pd.DataFrame]):
    con = duckdb.connect()
    try:
        for name, df in tables.items():
            con.register(f"_src_{name}", df)
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM _src_{name}")
        res = con.execute(sql)
        cols = [c[0] for c in res.description]
        return cols, res.fetchall()
    finally:
        con.close()


def run_pandas(code: str, tables: dict[str, pd.DataFrame]):
    ns = {"pd": pd}
    for name, df in tables.items():
        ns[name] = df.copy()
    exec(code, ns)
    if "result" not in ns:
        raise ValueError("Your pandas solution must assign the final answer to "
                         "a variable named `result` (a DataFrame).")
    result = ns["result"]
    if isinstance(result, pd.Series):
        result = result.to_frame()
    if not isinstance(result, pd.DataFrame):
        raise ValueError("`result` must be a pandas DataFrame or Series.")
    return list(result.columns), result.values.tolist()


def compare(expected_rows, actual_rows, order_sensitive: bool):
    exp = _norm_rows(expected_rows)
    act = _norm_rows(actual_rows)
    if len(exp) and len(act) and len(exp[0]) != len(act[0]):
        return False, (f"Column count mismatch: expected {len(exp[0])} columns, "
                       f"got {len(act[0])}.")
    if not order_sensitive:
        exp_s = sorted(exp, key=lambda r: [(str(type(x)), str(x)) for x in r])
        act_s = sorted(act, key=lambda r: [(str(type(x)), str(x)) for x in r])
        ok = exp_s == act_s
    else:
        ok = exp == act
    if ok:
        return True, "All rows match."
    # build a helpful diff
    exp_set = [tuple(r) for r in exp]
    act_set = [tuple(r) for r in act]
    missing = [list(r) for r in exp_set if r not in act_set][:5]
    extra = [list(r) for r in act_set if r not in exp_set][:5]
    msg = [f"Mismatch. expected {len(exp)} rows, got {len(act)} rows."]
    if missing:
        msg.append(f"  rows in reference but missing from yours (up to 5): {missing}")
    if extra:
        msg.append(f"  rows in yours but not in reference (up to 5): {extra}")
    if order_sensitive and len(exp) == len(act):
        msg.append("  (this problem is order-sensitive — check your ORDER BY)")
    return False, "\n".join(msg)


def main():
    if len(sys.argv) != 3:
        sys.exit("Usage: python grade.py <problem.json> <solution.sql|.py>")
    problem_path, sol_path = sys.argv[1], sys.argv[2]
    with open(problem_path) as f:
        problem = json.load(f)
    problem_dir = os.path.dirname(os.path.abspath(problem_path))
    tables = _load_tables(problem_dir, problem)

    with open(sol_path) as f:
        code = f.read()

    try:
        if sol_path.endswith(".py"):
            _, actual = run_pandas(code, tables)
        else:
            _, actual = run_sql(code, tables)
    except Exception as e:  # noqa: BLE001 - surface engine errors to the user
        print("✗ ERROR while running your solution:")
        print(f"  {type(e).__name__}: {e}")
        sys.exit(1)

    ok, msg = compare(problem["expected_rows"], actual, problem["order_sensitive"])
    print(("✓ PASS — " if ok else "✗ FAIL — ") + problem["problem_id"])
    print(msg)
    if not ok:
        print("\nReference SQL (one approach):")
        print("  " + problem["reference_sql"])
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
