# 10090 — Find the percentage of shipable orders

> ⚠️ **Partial note (SQL only).** As of 2026-06-03 only the PostgreSQL solution was
> completed. The pandas attempt stopped at `orders.head()` and there is no PySpark
> attempt yet. Sections 4.2 / 4.3 are placeholders to fill after solving those.

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 10090 |
| Category | Join (multi-table) |
| Difficulty | Medium |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/10090-find-the-percentage-of-shipable-orders?code_type=1) · [Python](https://platform.stratascratch.com/coding/10090-find-the-percentage-of-shipable-orders?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/10090-find-the-percentage-of-shipable-orders?code_type=6) |
| Solved on | 2026-06-03 (SQL only) |
| Tables | `orders(cust_id, id, order_date, order_details, total_order_cost)` · `customers(address, city, first_name, id, last_name, phone_number)` |

## 2. Problem Restatement (in my words)

Of all orders, what percentage are shipable — where "shipable" means the ordering
customer has a known address?

## 3. My Approach

- **First instinct**: join orders to customers on `cust_id = id`, then count rows
  where address is present and divide by the total.
- **Where I got stuck**: 34 attempts, ~27 minutes. The big themes were:
  1. **`GroupingError`** repeatedly — I kept mixing a bare column (`s.shipable_orders`)
     with `COUNT()` in the same `SELECT` without a `GROUP BY`.
  2. **`count(order_id is not NULL)`** — this counts *every* row, because the boolean
     expression is non-null for all rows. Doesn't filter anything.
  3. **Integer division** — `count/count` returns an integer (0) without a cast.
  4. **Cast syntax thrash** — `decimal(...,2)*100`, `int(count(...))`,
     `::float as ratio` *inside* parentheses → several `SyntaxError`s.
- **Final strategy**: split into a joined CTE, a `shipable` count CTE (filtered on
  `address IS NOT NULL`), and a `total` count CTE, cast both counts to `::FLOAT`, then
  cross-join the two single-row CTEs to divide.

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-06-03 11:01)

```sql
WITH cust_order AS (
    SELECT
        o.id AS order_id,
        c.id AS cust_id,
        c.address
    FROM orders AS o, customers AS c
    WHERE o.cust_id = c.id
),
shipable AS (
    SELECT COUNT(order_id)::FLOAT AS shipable_orders
    FROM cust_order
    WHERE address IS NOT NULL
),
total AS (
    SELECT COUNT(*)::FLOAT AS total_order FROM cust_order
)
SELECT (s.shipable_orders / t.total_order) * 100 AS pct_shipable
FROM total AS t, shipable AS s;
```

### 4.2 Python (pandas) ⬜ NOT SOLVED YET

```python
# TODO: only ran orders.head() on 2026-06-03. Solve and paste accepted code here.
```

### 4.3 PySpark ⬜ NOT SOLVED YET

```python
# TODO: no attempt yet on 2026-06-03. Solve and paste accepted code here.
```

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)`.

### 5.1 PostgreSQL

```sql
SELECT
    100.0 * SUM(CASE WHEN NULLIF(TRIM(c.address), '') IS NOT NULL THEN 1 ELSE 0 END)
    / COUNT(DISTINCT o.id) AS percent_shipable
FROM orders o
LEFT JOIN customers c
    ON o.cust_id = c.id;
```

### 5.2 Python

```python
import pandas as pd

merged_df = pd.merge(orders, customers, left_on='cust_id', right_on='id', how='left')
merged_df['is_shipable'] = merged_df['address'].apply(
    lambda x: 1 if pd.notna(x) and str(x).strip() != '' else 0
)
total_orders = len(orders)
shipable_count = merged_df['is_shipable'].sum()
percent_shipable = 100.0 * shipable_count / total_orders
result = pd.DataFrame({'percent_shipable': [percent_shipable]})
```

### 5.3 PySpark

```python
import pyspark.sql.functions as F
import pandas as pd

merged_df = orders.join(customers, orders.cust_id == customers.id, how='left')
merged_df = merged_df.withColumn('trimmed_address', F.trim(F.col('address')))
merged_df = merged_df.withColumn(
    'is_shipable',
    F.when(F.col('trimmed_address').isNull() | (F.col('trimmed_address') == ''), 0).otherwise(1)
)
total_orders = orders.count()
shipable_count = merged_df.filter(F.col('is_shipable') == 1).count()
percent_shipable = 100.0 * shipable_count / total_orders
result = pd.DataFrame({'percent_shipable': [percent_shipable]})
```

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| Join type | Implicit **INNER** (`FROM o, c WHERE o.cust_id=c.id`) | `LEFT JOIN` + `COUNT(DISTINCT o.id)` | **Correctness bug**: orphaned orders (cust_id with no customer) vanish from BOTH numerator and denominator. Author keeps every order in the denominator |
| "Address known" test | `address IS NOT NULL` | `NULLIF(TRIM(address), '') IS NOT NULL` | I count `''` / whitespace as shipable; author treats blank as unknown |
| Shape | 3 CTEs + cross-join two scalars | single pass, conditional `SUM` | Author scans once; I scan `cust_order` twice |
| Int division guard | `::FLOAT` on both counts | `100.0 *` forces float | Both work; conditional-`SUM` pattern is the idiom to learn |

**Key insight in one line:** the canonical "percentage that satisfy a condition" idiom
is a **single-pass conditional aggregate** over a **LEFT JOIN** —
`100.0 * SUM(CASE WHEN cond THEN 1 ELSE 0 END) / COUNT(*)` — not three CTEs and a
cross join.

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=10090)` — 34 attempts. Highlights:

| # | Time (UTC) | Language | Status | What happened |
|---|---|---|---|---|
| 1 | 10:34 | SQL | run | `select * from orders / customers` — inspect |
| 2 | 10:37 | SQL | run | Built the join `o, c WHERE o.cust_id=c.id` |
| 3–6 | 10:42–10:46 | SQL | ❌ | `GroupingError` (bare column + COUNT, no GROUP BY); `count(order_id is not NULL)` counts all rows |
| 7 | 10:45:18 | SQL | ❌ submit | `count(shipable_orders)/count(order_id)` — both = 1, wrong answer |
| 8–14 | 10:48–11:00 | SQL | ❌ | cast thrash: `int(count(...))`, `decimal(...,2)*100`, `::float as ratio` inside parens → `SyntaxError` |
| 15 | 11:01:24 | SQL | ✅ submit | Split shipable/total CTEs, both `::FLOAT`, then divide |
| 16 | 11:26 | pandas | run | `orders.head()` — **stopped here, not completed** |

What the failed attempts taught me:
- **Don't mix a bare column with an aggregate** in one `SELECT` without `GROUP BY`.
  If you need a single ratio, compute both numbers as scalars first (CTE or
  conditional `SUM`).
- **`COUNT(boolean_expr)` counts every non-null row** — `count(x IS NOT NULL)` does
  *not* filter. Use `SUM(CASE WHEN cond THEN 1 ELSE 0 END)` or
  `COUNT(*) FILTER (WHERE cond)`.
- **Integer / integer = integer** in Postgres. Cast to `::FLOAT` (or multiply by
  `100.0`) before dividing.
- **Cast placement**: `expr::float` goes on the value, not after an alias inside
  parentheses. `(a::float / b::float)` ✅, `(a / b :: float as r)` ❌.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context.
> Note: only the SQL was completed; 8.5 reflects that 2 of 3 languages are missing.

### 8.1 Correctness
- **Score**: 3/5
- Comment: The query passed the grader, so against this dataset it returns the right number. But it is only accidentally correct. Your implicit inner join (`FROM orders o, customers c WHERE o.cust_id = c.id`) silently drops any order whose `cust_id` has no matching customer row from *both* numerator and denominator. The author's `LEFT JOIN ... COUNT(DISTINCT o.id)` keeps every order in the denominator, which is the correct definition of "percentage of orders." On a dataset with orphaned orders your number would be inflated. Fix: `LEFT JOIN customers c ON o.cust_id = c.id` and count orders, not the joined rows.

### 8.2 Performance & Efficiency
- **Score**: 3/5
- Comment: Three CTEs plus two cross joins of single-row results to do one division is more machinery than the problem needs. It scans `cust_order` twice (once for shipable, once for total). The author does it in a single pass with `SUM(CASE WHEN ... THEN 1 ELSE 0 END) / COUNT(...)`. At interview scale the planner will absorb this, but a senior is expected to reach for the one-pass conditional aggregate by reflex. Fix: collapse to a single `SELECT` with conditional `SUM` over one scan.

### 8.3 Readability
- **Score**: 3/5
- Comment: Naming is decent (`cust_order`, `shipable`, `pct_shipable`), and the CTE split is at least self-documenting. But the comma-join syntax is a readability and correctness smell in 2026; explicit `JOIN ... ON` signals intent. The final cross join `FROM total t, shipable s` to multiply two scalars reads as a trick rather than intent. A single conditional-aggregate statement would be both shorter and clearer.

### 8.4 Edge Cases
- **Score**: 2/5
- Comment: Two misses. First, orphaned orders (no matching customer) — your inner join discards them silently, which is the headline correctness bug. Second, `address IS NOT NULL` treats `''` and `'   '` as shipable; a blank string is not a known address. The author guards this with `NULLIF(TRIM(c.address), '') IS NOT NULL`. You also did not consider whether a customer could have multiple orders affecting the `DISTINCT` decision. Fix: use a left join and `NULLIF(TRIM(address), '')` so empty and whitespace addresses are excluded.

### 8.5 Interview-Readiness
- **Score**: 2/5
- Comment: This is where the gap shows. You completed only one of three required languages — pandas stopped at `orders.head()` and there was no PySpark attempt at all, which on its own caps this axis. The SQL took 34 attempts over ~27 minutes, and the transcript shows fundamentals not yet automatic: the repeated `GroupingError` from mixing a bare column with `COUNT()`, `count(order_id is not NULL)` (which counts every row, since the boolean is non-null), and multiple cast `SyntaxError`s on `::float` / `decimal(...)`. Fix: drill the conditional-aggregate ratio pattern (`AVG`/`SUM(CASE...)`) until it is muscle memory so you stop fighting GROUP BY and casts, then time-box SQL to leave room for the other two languages.

### 8.6 Verdict
A working answer that clears the screen but not the bar. The core idea — join, filter shipable, divide — is sound, and your final SQL is clean enough to read, but it rests on an implicit inner join that would misreport the metric the moment orphaned orders exist, and it ignores blank-address handling. More concerning for a FAANG loop is the process signal: 34 attempts wrestling GROUP BY and casts, and only one of three languages delivered. The shortest path to a stronger bar is to internalize the single-pass conditional-aggregate idiom and to get fluent enough in pandas and PySpark that you can produce all three within the time box. This is a no-hire at the senior level today, but a very coachable one.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Left join | `LEFT JOIN c ON o.cust_id=c.id` | `pd.merge(o, c, left_on='cust_id', right_on='id', how='left')` | `o.join(c, o.cust_id==c.id, how='left')` |
| "Known address" test | `NULLIF(TRIM(address),'') IS NOT NULL` | `pd.notna(x) and str(x).strip() != ''` | `F.trim(col).isNotNull() & (F.trim(col) != '')` |
| Conditional count | `SUM(CASE WHEN cond THEN 1 ELSE 0 END)` or `COUNT(*) FILTER (WHERE cond)` | `df['is_shipable'].sum()` | `df.filter(cond).count()` |
| Percentage (float-safe) | `100.0 * num / COUNT(*)` | `100.0 * count / len(orders)` | `100.0 * count / orders.count()` |
| Avoid int division | cast `::FLOAT` or multiply `100.0` | naturally float | naturally float |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: "percentage of rows meeting a condition" =
  `100.0 * SUM(CASE WHEN cond THEN 1 ELSE 0 END) / COUNT(*)` in one pass over a
  **LEFT** join. Reach for this before building three CTEs.
- **Reusable snippet (PostgreSQL)**:
  ```sql
  SELECT 100.0 * SUM(CASE WHEN cond THEN 1 ELSE 0 END) / COUNT(*) AS pct
  FROM base_table;   -- or AVG((cond)::int) * 100
  ```
- **Weaknesses I noticed**:
  1. INNER vs LEFT join instinct — default to LEFT when the denominator is "all of
     the left table".
  2. `GROUP BY` / aggregate mixing rules still cost me time.
  3. Integer-division and cast-placement syntax not yet automatic.
  4. Did not finish pandas/PySpark — time management + cross-language fluency gap.
- **Drill for next session**: redo 10090 in pandas and PySpark, and rewrite the SQL
  as a single conditional-aggregate over a LEFT JOIN. Time-box 12 minutes.

## 11. Related Notes

- Same category (Join / ratio): 9781 "Processed Ticket Rate By Type", 10285
  "Acceptance Rate By Date".
- Same conditional-aggregate idiom: any "rate / percentage / share" problem.
- Reference reading:
  - Postgres `FILTER` clause: https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-AGGREGATES
  - `CASE`: https://www.postgresql.org/docs/current/functions-conditional.html

---

*Generated 2026-06-03 by Ina (PostgreSQL solving) + Claude (author solution fetch via MCP, submission history analysis, FAANG reviewer subagent). pandas/PySpark sections pending.*
