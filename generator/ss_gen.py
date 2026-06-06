#!/usr/bin/env python3
"""
ss_gen — StrataScratch-style practice problem generator.

Generates ORIGINAL problems with synthetic, seeded data so practice never runs
out (the public StrataScratch MCP only exposes a small freemium pool). Each
generated problem ships with:
  - synthetic table(s) as CSV (reproducible from a seed)
  - a problem statement + schema
  - a reference SQL solution
  - the expected output, computed by running the reference SQL in DuckDB

Solve in PostgreSQL-style SQL or pandas, then check with grade.py.

Usage:
    python ss_gen.py --category window --difficulty medium --seed 7
    python ss_gen.py --category aggregation            # random seed
    python ss_gen.py --list                            # show categories

Output goes to generator/problems/<problem_id>/
    problem.json, <table>.csv ...

Requires: duckdb, pandas   (pip install duckdb pandas)
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import sys

import pandas as pd

try:
    import duckdb
except ImportError:
    sys.exit("duckdb not installed. Run: pip install duckdb pandas")

HERE = os.path.dirname(os.path.abspath(__file__))
PROBLEMS_DIR = os.path.join(HERE, "problems")

FIRST_NAMES = ["Daniel", "Sarah", "Joseph", "Hannah", "Eric", "Olivia", "Marcus",
               "Priya", "Wei", "Sofia", "Ahmed", "Grace", "Liam", "Yuki", "Noah",
               "Amara", "Tomasz", "Elena", "Kenji", "Maria"]
DEPARTMENTS = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Support"]
ITEMS = ["keyboard", "monitor", "cable", "mouse", "webcam", "dock", "stand"]
CITIES = ["Seoul", "Austin", "Berlin", "Lagos", "Tokyo", "Lima"]


# ---------------------------------------------------------------------------
# Type inference for schema_sql
# ---------------------------------------------------------------------------
def _sql_type(series: pd.Series) -> str:
    if pd.api.types.is_datetime64_any_dtype(series):
        return "date"
    if pd.api.types.is_integer_dtype(series):
        return "bigint"
    if pd.api.types.is_float_dtype(series):
        return "double precision"
    return "text"


def _schema(tables: dict[str, pd.DataFrame]) -> dict:
    return {
        name: [{"name": c, "type": _sql_type(df[c])} for c in df.columns]
        for name, df in tables.items()
    }


# ---------------------------------------------------------------------------
# Category generators. Each returns a dict with keys:
#   title, question, order_sensitive, tables {name: DataFrame}, reference_sql
# ---------------------------------------------------------------------------
def gen_aggregation(rng: random.Random, difficulty: str) -> dict:
    n = {"easy": 12, "medium": 24, "hard": 40}[difficulty]
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "id": i,
            "first_name": rng.choice(FIRST_NAMES),
            "department": rng.choice(DEPARTMENTS),
            "salary": rng.randrange(45_000, 160_000, 1000),
        })
    df = pd.DataFrame(rows)
    min_count = 3 if difficulty == "easy" else 4
    q = (f"From the `employee` table, find departments with {min_count} or more "
         f"employees. For each such department output the department name, the "
         f"head count, and the average salary (rounded to 2 decimals). "
         f"Order by average salary descending.")
    ref = f"""
        SELECT department,
               COUNT(*)                AS headcount,
               ROUND(AVG(salary), 2)   AS avg_salary
        FROM employee
        GROUP BY department
        HAVING COUNT(*) >= {min_count}
        ORDER BY avg_salary DESC
    """
    return dict(title="Department Head Count and Average Salary",
                question=q, order_sensitive=True,
                tables={"employee": df}, reference_sql=ref)


def gen_window(rng: random.Random, difficulty: str) -> dict:
    n = {"easy": 12, "medium": 20, "hard": 36}[difficulty]
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "id": i,
            "first_name": rng.choice(FIRST_NAMES),
            "department": rng.choice(DEPARTMENTS[:4]),
            "salary": rng.randrange(45_000, 160_000, 500),
        })
    df = pd.DataFrame(rows)
    q = ("From the `employee` table, find the highest-paid employee in each "
         "department. Output department, first_name and salary. If several "
         "employees tie for the top salary in a department, return all of them.")
    ref = """
        WITH ranked AS (
            SELECT department, first_name, salary,
                   RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk
            FROM employee
        )
        SELECT department, first_name, salary
        FROM ranked
        WHERE rnk = 1
    """
    return dict(title="Highest Salary in Each Department",
                question=q, order_sensitive=False,
                tables={"employee": df}, reference_sql=ref)


def gen_join(rng: random.Random, difficulty: str) -> dict:
    n_cust = {"easy": 5, "medium": 8, "hard": 12}[difficulty]
    customers = pd.DataFrame([
        {"id": i, "name": rng.choice(FIRST_NAMES), "city": rng.choice(CITIES)}
        for i in range(1, n_cust + 1)
    ])
    n_ord = n_cust * 3
    orders = pd.DataFrame([
        {"order_id": j,
         "cust_id": rng.randint(1, n_cust),
         "item": rng.choice(ITEMS),
         "amount": rng.randrange(20, 500, 5)}
        for j in range(1, n_ord + 1)
    ])
    q = ("There are two tables, `customers` and `orders` (orders.cust_id -> "
         "customers.id). For every customer who has placed at least one order, "
         "output the customer name and their total order amount. "
         "Order by total amount descending.")
    ref = """
        SELECT c.name AS name,
               SUM(o.amount) AS total_amount
        FROM customers c
        JOIN orders o ON o.cust_id = c.id
        GROUP BY c.name
        ORDER BY total_amount DESC
    """
    return dict(title="Total Order Amount per Customer",
                question=q, order_sensitive=True,
                tables={"customers": customers, "orders": orders},
                reference_sql=ref)


def gen_date(rng: random.Random, difficulty: str) -> dict:
    n = {"easy": 20, "medium": 40, "hard": 70}[difficulty]
    base = dt.date(2024, 1, 1)
    rows = []
    for j in range(1, n + 1):
        d = base + dt.timedelta(days=rng.randint(0, 364))
        rows.append({
            "id": j,
            "user_id": rng.randint(1, 8),
            "created_at": d,
            "amount": rng.randrange(10, 300, 5),
        })
    df = pd.DataFrame(rows)
    df["created_at"] = pd.to_datetime(df["created_at"])
    q = ("From the `transactions` table, output the total transaction `amount` "
         "for each calendar month. Output the month as the first day of the "
         "month (a date) and the total amount. Order by month ascending.")
    ref = """
        SELECT DATE_TRUNC('month', created_at)::DATE AS month,
               SUM(amount) AS total_amount
        FROM transactions
        GROUP BY DATE_TRUNC('month', created_at)
        ORDER BY month
    """
    return dict(title="Monthly Transaction Totals",
                question=q, order_sensitive=True,
                tables={"transactions": df}, reference_sql=ref)


def gen_string(rng: random.Random, difficulty: str) -> dict:
    names = ["Hannah", "Joseph", "Sarah", "Daniel", "Deborah", "Joshua",
             "Marcus", "Elijah", "Hyejin", "Yousseh", "Aaliyah", "Kaleah",
             "Noah", "Liam", "Olivia", "Faith", "Micah", "Sherah"]
    n = {"easy": 12, "medium": 18, "hard": 26}[difficulty]
    rows = [{"worker_id": i,
             "first_name": rng.choice(names),
             "department": rng.choice(DEPARTMENTS)} for i in range(1, n + 1)]
    df = pd.DataFrame(rows)
    q = ("From the `worker` table, find all workers whose `first_name` is exactly "
         "6 characters long and ends with the letter 'h'. Output all columns.")
    ref = """
        SELECT *
        FROM worker
        WHERE LENGTH(first_name) = 6
          AND RIGHT(first_name, 1) = 'h'
    """
    return dict(title="Six-Letter First Names Ending in 'h'",
                question=q, order_sensitive=False,
                tables={"worker": df}, reference_sql=ref)


def gen_filter(rng: random.Random, difficulty: str) -> dict:
    n = {"easy": 14, "medium": 24, "hard": 40}[difficulty]
    rows = [{"id": i,
             "first_name": rng.choice(FIRST_NAMES),
             "department": rng.choice(DEPARTMENTS),
             "salary": rng.randrange(40_000, 150_000, 1000),
             "age": rng.randint(22, 60)} for i in range(1, n + 1)]
    df = pd.DataFrame(rows)
    thr = 90_000
    q = (f"From the `employee` table, list employees in the 'Engineering' or "
         f"'Finance' department who earn more than {thr}. "
         f"Output first_name, department and salary.")
    ref = f"""
        SELECT first_name, department, salary
        FROM employee
        WHERE department IN ('Engineering', 'Finance')
          AND salary > {thr}
    """
    return dict(title="High Earners in Engineering or Finance",
                question=q, order_sensitive=False,
                tables={"employee": df}, reference_sql=ref)


GENERATORS = {
    "aggregation": gen_aggregation,
    "window": gen_window,
    "join": gen_join,
    "date": gen_date,
    "string": gen_string,
    "filter": gen_filter,
}
CATEGORY_LABEL = {
    "aggregation": "Aggregation",
    "window": "Window Functions",
    "join": "Join (multi-table)",
    "date": "Date/Time",
    "string": "String/Text",
    "filter": "Filter/Basic",
}


# ---------------------------------------------------------------------------
# Harness: build problem, compute expected output via DuckDB, persist.
# ---------------------------------------------------------------------------
def _normalize_value(v):
    if isinstance(v, float):
        return round(v, 6)
    if isinstance(v, (pd.Timestamp, dt.date, dt.datetime)):
        return pd.Timestamp(v).strftime("%Y-%m-%d")
    return v


def run_sql(sql: str, tables: dict[str, pd.DataFrame]):
    """Run SQL against the given tables in DuckDB. Returns (columns, rows)."""
    con = duckdb.connect()
    try:
        for name, df in tables.items():
            con.register(f"_src_{name}", df)
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM _src_{name}")
        res = con.execute(sql)
        cols = [c[0] for c in res.description]
        rows = res.fetchall()
        return cols, rows
    finally:
        con.close()


def build(category: str, difficulty: str, seed: int) -> dict:
    rng = random.Random(seed)
    spec = GENERATORS[category](rng, difficulty)
    cols, rows = run_sql(spec["reference_sql"], spec["tables"])
    norm_rows = [[_normalize_value(v) for v in r] for r in rows]
    pid = f"gen-{category}-{difficulty}-{seed:04d}"
    return {
        "problem_id": pid,
        "category": CATEGORY_LABEL[category],
        "category_key": category,
        "difficulty": difficulty,
        "seed": seed,
        "title": spec["title"],
        "question": spec["question"],
        "tables": list(spec["tables"].keys()),
        "schema_sql": _schema(spec["tables"]),
        "order_sensitive": spec["order_sensitive"],
        "reference_sql": " ".join(spec["reference_sql"].split()),
        "expected_columns": cols,
        "expected_rows": norm_rows,
        "_dataframes": spec["tables"],  # stripped before JSON dump
    }


def persist(problem: dict) -> str:
    pid = problem["problem_id"]
    out = os.path.join(PROBLEMS_DIR, pid)
    os.makedirs(out, exist_ok=True)
    dfs = problem.pop("_dataframes")
    for name, df in dfs.items():
        save = df.copy()
        for c in save.columns:
            if pd.api.types.is_datetime64_any_dtype(save[c]):
                save[c] = save[c].dt.strftime("%Y-%m-%d")
        save.to_csv(os.path.join(out, f"{name}.csv"), index=False)
    with open(os.path.join(out, "problem.json"), "w") as f:
        json.dump(problem, f, indent=2, default=str)
    return out


def main():
    ap = argparse.ArgumentParser(description="Generate a StrataScratch-style problem.")
    ap.add_argument("--category", choices=list(GENERATORS))
    ap.add_argument("--difficulty", choices=["easy", "medium", "hard"], default="medium")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--list", action="store_true", help="list categories and exit")
    args = ap.parse_args()

    if args.list or not args.category:
        print("Categories:", ", ".join(GENERATORS))
        print("Example: python ss_gen.py --category window --difficulty medium --seed 7")
        return

    seed = args.seed if args.seed is not None else random.randint(0, 9999)
    problem = build(args.category, args.difficulty, seed)
    out = persist(problem)
    print(f"Generated {problem['problem_id']}  ({problem['category']}, {problem['difficulty']})")
    print(f"  -> {out}/problem.json")
    print(f"  tables: {problem['tables']}  expected rows: {len(problem['expected_rows'])}")
    print(f"\n{problem['question']}\n")
    print("Solve, then:  python grade.py", os.path.join(out, "problem.json"), "your_solution.sql")


if __name__ == "__main__":
    main()
