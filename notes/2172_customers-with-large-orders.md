# 2172 — Customers with Large Orders

> Standard note template (v2, 2026-05-27)

---

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 2172 |
| Category | Subquery/CTE (semi-join) |
| Difficulty | Medium |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/2172-customers-with-large-orders?code_type=1) · [Python](https://platform.stratascratch.com/coding/2172-customers-with-large-orders?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/2172-customers-with-large-orders?code_type=6) |
| Solved on | 2026-06-06 |
| Tables | `online_store_customers(customer_id, customer_name)` · `online_store_orders(order_id, customer_id, amount, status)` |

## 2. Problem Restatement (in my words)

> Return the id and name of every customer who has at least one order over $100, counting orders of any status. Each customer appears once.

## 3. My Approach

- First instinct: inner-join customers to orders on `customer_id`, filter `amount > 100`, then `DISTINCT` to collapse customers with several big orders.
- Where I got stuck: nothing conceptual — only small syntax slips (a self-referencing `cust_order_filter` typo in pandas; an indentation error before `.distinct()` in PySpark).
- Final strategy: join → filter `> 100` → keep `customer_id, customer_name` → dedupe (`DISTINCT` / `drop_duplicates()` / `.distinct()`).

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-06-06)

```sql
WITH high_val_cust AS (
    SELECT
        o.amount,
        o.order_id,
        c.customer_name,
        o.customer_id
    FROM online_store_orders AS o
    INNER JOIN online_store_customers AS c ON o.customer_id = c.customer_id
    WHERE amount > 100
    ORDER BY customer_id, customer_name
)
SELECT DISTINCT
    customer_id,
    customer_name
FROM high_val_cust;
```

### 4.2 Python (pandas) ✅ (passed 2026-06-06)

```python
import pandas as pd

cust_order = online_store_customers.merge(online_store_orders, on='customer_id')

# Apply filter
cust_order_filter = cust_order.loc[cust_order['amount'] > 100]

# Get unique customer id & name
result = cust_order_filter[['customer_id', 'customer_name']].drop_duplicates()
```

### 4.3 PySpark ✅ (passed 2026-06-06)

```python
import pyspark
from pyspark.sql import functions as f

cust_order = (online_store_customers
              .join(online_store_orders, on='customer_id')
              .filter(f.col('amount') > 100)
              .select('customer_id', 'customer_name')
              .distinct())

cust_order.toPandas()
```

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)` (Premium).

### 5.1 PostgreSQL

```sql
SELECT
    c.customer_id,
    c.customer_name
FROM online_store_customers c
WHERE EXISTS (
    SELECT 1
    FROM online_store_orders o
    WHERE o.customer_id = c.customer_id
      AND o.amount > 100
);
```

### 5.2 Python

```python
import pandas as pd

large_order_customers = online_store_orders[
    online_store_orders['amount'] > 100
]['customer_id'].unique()

result = online_store_customers[
    online_store_customers['customer_id'].isin(large_order_customers)
][['customer_id', 'customer_name']]
```

### 5.3 PySpark

```python
from pyspark.sql import functions as F

large_order_customers = online_store_orders.filter(
    F.col('amount') > 100
).select('customer_id').distinct()

result = online_store_customers.join(
    large_order_customers,
    on='customer_id',
    how='left_semi'
).select('customer_id', 'customer_name')

result.toPandas()
```

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| Strategy | Join → filter → **DISTINCT** | **EXISTS / semi-join** (no dedup) | Same result. Author never fans out, so no dedup step is needed — cleaner and avoids a wide intermediate. |
| Duplicate handling | Required (`DISTINCT` / `drop_duplicates`) | Not required (existence test) | A customer with 3 orders > $100 produces 3 join rows for me, 1 for the author. |
| Columns pulled | Carried `amount`, `order_id` then dropped | Only the two output columns | Author pulls the minimum; mine is fine but wider. |
| Intent signaled | "match then dedupe" | "filter customers by existence of a matching order" | Semi-join/EXISTS reads as exactly the question being asked. |

Key insight in one line: **"customers who have at least one matching order" is an existence test — `EXISTS` (SQL), `.isin()` (pandas), or a `left_semi` join (Spark) answers it without a fan-out, so no `DISTINCT` is needed.**

## 7. Submission History (auto-captured)

> From MCP `get_my_attempts(question_id=2172)`. All times UTC, 2026-06-06.

| # | Time | Language | Status | Error / Note |
|---|---|---|---|---|
| 1 | 05:47–05:56 | SQL | run | `select *` on both tables — explore |
| 2 | 05:58–05:59 | SQL | run | Built join + `WHERE amount > 100`, then added `ORDER BY` |
| 3 | 06:00:26 | SQL | ✅ submit | CTE + `SELECT DISTINCT` — first submission passed |
| 4 | 06:08:06 | Python | ❌ err | `cust_order.loc[cust_order_filter[...]]` — referenced the var being defined → NameError |
| 5 | 06:09:29 | Python | ✅ submit | Fixed reference, `drop_duplicates()` |
| 6 | 06:38:50 | PySpark | ❌ err | unexpected indent before `.distinct()` |
| 7 | 06:38:58 | PySpark | ✅ submit | Join + filter + select + `.distinct()` |

What the failed attempts taught me:
- Lesson 1: When filtering a freshly built frame, reference the **source** frame in the mask (`df.loc[df['amount'] > 100]`), not the target name you're assigning to.
- Lesson 2: In a chained PySpark expression, every `.method()` continuation must align — a stray indent breaks the chain.
- Lesson 3: Clean run overall — recognize the "at least one" phrasing as an **existence** pattern next time and reach for `EXISTS`/semi-join to skip the dedup.

## 8. Senior DA / DS Review — Structured Rubric

### 8.1 Correctness
- **Score**: 5/5
- Comment: All three engines correct on first valid submission. "Regardless of status" handled correctly by simply not filtering on `status`.

### 8.2 Performance & Efficiency
- **Score**: 4/5
- Comment: Join + `DISTINCT` materializes one row per qualifying order before collapsing — fine at this size, but on a large `orders` table it builds a wide intermediate. `EXISTS` / `left_semi` short-circuits on the first match per customer and never duplicates. The `ORDER BY` inside the CTE does nothing for the final result and can be dropped.

### 8.3 Readability
- **Score**: 4/5
- Comment: Well-commented, clear intermediate names (`cust_order`, `cust_order_filter`, `high_val_cust`). The CTE carries `amount`/`order_id` that the final query discards — trim to just what you output.

### 8.4 Edge Cases
- **Score**: 4/5
- Comment: Dedup correctly handles multi-order customers. Implicit assumption: `amount` has no NULLs (a NULL `> 100` is false, which is the desired behavior anyway). Boundary is correct — `> 100` excludes exactly $100, matching "over $100".

### 8.5 Interview-Readiness
- **Score**: 4/5
- Comment: Fast, correct, verbalizable. To level up, name the pattern out loud: "this is an existence filter, so I'll use `EXISTS` and avoid a join fan-out + `DISTINCT`." That framing signals seniority.

### 8.6 Verdict
Clean, confident solve across all three engines with only cosmetic syntax slips. The one habit that elevates this: **recognize "has at least one ..." as an existence test and default to `EXISTS` / `.isin()` / `left_semi`** rather than join-then-`DISTINCT`. Same answer, but it avoids the fan-out, drops the dedup, and reads as exactly the question asked. Also trim CTEs to only the columns you return.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Existence filter | `WHERE EXISTS (SELECT 1 ...)` | `df['id'].isin(matches)` | `.join(matches, on, how='left_semi')` |
| Join then dedupe (my way) | `JOIN ... DISTINCT` | `.merge(...).drop_duplicates()` | `.join(...).distinct()` |
| Build the match set | `SELECT customer_id ... amount>100` | `orders[orders.amount>100]['customer_id'].unique()` | `orders.filter(...).select('customer_id').distinct()` |
| Strict greater-than | `amount > 100` | `df['amount'] > 100` | `f.col('amount') > 100` |

## 10. Patterns to Remember

- Core pattern this problem teaches: **existence / semi-join filtering** — keep rows of A that have ≥1 match in B, without duplicating A.
- Reusable snippet: `WHERE EXISTS (SELECT 1 FROM b WHERE b.key = a.key AND <cond>)`; pandas `a[a.key.isin(b.loc[cond, 'key'])]`; Spark `a.join(b_filtered, key, 'left_semi')`.
- Weakness I noticed in myself: reached for join + `DISTINCT` by reflex; the existence framing is cleaner.

## 11. Related Notes

- Similar problems in this category: `2110-salary-less-than-twice-the-average` (subquery comparison), `10553-finding-purchases` (filter by membership in another set).
- Reference material: `EXISTS` vs `IN` vs `JOIN`+`DISTINCT`; Spark `left_semi` / `left_anti` joins.

---

*Generated 2026-06-06 by Ina (problem solving) + Claude (author solution fetch, diff, rubric review).*
