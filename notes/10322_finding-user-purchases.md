# 10322 — Finding User Purchases

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 10322 |
| Category | Window Functions |
| Difficulty | Medium |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/10322-finding-user-purchases?code_type=1) · [Python](https://platform.stratascratch.com/coding/10322-finding-user-purchases?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/10322-finding-user-purchases?code_type=6) |
| Solved on | 2025-07-07 (pandas) · 2026-05-28 (PostgreSQL) |
| Tables | `amazon_transactions(created_at date, id bigint, item text, revenue bigint, user_id bigint)` |

## 2. Problem Restatement (in my words)

We have a flat transactions log where each row is one purchase by a `user_id` on some `created_at` date. A user counts as a "returning active user" if their **second** purchase happened **1 to 7 days after** their **first** purchase. Same-day repeat purchases don't count (the gap must be at least 1 day), and a gap of more than a week doesn't count either. Output just the `user_id`s of those users. The two traps: (1) "first" and "second" are per-user ordered events, so you have to rank each user's purchase dates and isolate the 1st and 2nd; (2) multiple purchases can land on the **same calendar day**, so before ranking you must collapse a user's dates to distinct days — otherwise the "second purchase" could just be another transaction from day one, and the gap would be 0.

## 3. My Approach

- **First instinct**: my oldest attempt (2025-06-25) literally opens with `select * from amazon_transactions;` to see the shape, then I started reaching straight for window functions to label each user's purchases in order. The mental model was right from the start — "rank purchases per user, grab #1 and #2, diff the dates" — but the **syntax** of the window function fought me for a long time.
- **Where I got stuck** (this is the honest part — this problem was a window-function syntax gauntlet, see Section 7):
  1. **`ORDER BY` placement inside `OVER()`.** I wrote things like `rank() over (partition by user_id order created_at) by purchase_order` and `... order by created_at) by purchase_order` — mangling where `ORDER BY` goes and gluing a stray `by purchase_order` after the window. Repeated `syntax error at or near "created_at" / "purchase_order"`.
  2. **`rank()` vs `row_number()` vs `first_value()`.** I tried `rank(purchase_date) over (...)` → Postgres said `WITHIN GROUP is required for ordered-set aggregate rank` (because `rank(arg)` is the ordered-set form). I tried `first_value(user_id) over (order by created_at dec)` with `dec` instead of `desc`. I typo'd `row_number(() over (...)` with a double paren.
  3. **Putting a window function in `WHERE`.** `where rank() over (...) = 1` → `window functions are not allowed in WHERE`. You can't filter on a window function directly; it has to be computed in a subquery/CTE first.
  4. **A bare expression in `WHERE`.** In a cross-join sketch (`from t1, t2`) I wrote `where t2.created_at - t1.created_at;` with no comparison → `argument of WHERE must be type boolean, not type integer` (date minus date is an integer count of days, not a true/false).
  5. **`GROUP BY` mismatch.** `(t2.purchase_date - t1.purchase_date) as diff_day` while grouping → the column must appear in `GROUP BY` or be aggregated.
- **Final strategy (the one that passed)**: stop fighting and split it into clean CTEs. CTE `cte` = `SELECT DISTINCT user_id, created_at::date` to collapse same-day purchases. CTE `cte2` = add `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY purchase_date)` as `purchase_order`. Then two tiny CTEs `t1` (where `purchase_order = 1`) and `t2` (where `purchase_order = 2`), `INNER JOIN` them on `user_id` (the inner join itself drops users who never made a 2nd purchase), and filter `WHERE (second_date - first_date) BETWEEN 1 AND 7`. `BETWEEN 1 AND 7` cleanly encodes both rules at once — the `1` lower bound throws out same-day (gap 0), the `7` upper bound throws out anything past a week.
- **In pandas (earlier, 2025-07-07)** I solved it a different way: `groupby('user_id')['created_at'].min()` to get each user's first purchase date, merge it back onto every transaction, then keep rows strictly after the first date and within `Timedelta(days=7)`, and `drop_duplicates()` the surviving `user_id`s. (This is the "anchor on the minimum date" idiom, the same shape as the author's Polars solution — see Section 5.)

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-05-28 13:06)

```sql
WITH cte AS (
    SELECT DISTINCT
        user_id,
        created_at::DATE AS purchase_date
    FROM amazon_transactions
),

cte2 AS (
    SELECT
        user_id,
        purchase_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY purchase_date) AS purchase_order
    FROM cte
),

t1 AS (
    SELECT
        user_id,
        purchase_date AS first_date
    FROM cte2
    WHERE purchase_order = 1
),

t2 AS (
    SELECT
        user_id,
        purchase_date AS second_date
    FROM cte2
    WHERE purchase_order = 2
)

SELECT t1.user_id
FROM t1 INNER JOIN t2 ON t1.user_id = t2.user_id
WHERE (second_date - first_date) BETWEEN 1 AND 7;
```

> Note: `created_at` is already a `date` column in the schema, so `::DATE` is technically redundant here — but `SELECT DISTINCT user_id, created_at::date` is still doing real work: it **collapses multiple same-day purchases to one row per (user, day)** before ranking, which is the whole point of "ignore same-day purchases." If I had ranked the raw transactions, a user's "2nd purchase" could be a second transaction on day 1 and the diff would be 0.

### 4.2 Python (pandas) ✅ (passed 2025-07-07 07:49)

```python
# Import your libraries
import pandas as pd

# Start writing code
first_pc = amazon_transactions.groupby('user_id')['created_at'].min().reset_index().rename(columns = {'created_at': 'first_purchase_date'})
df = amazon_transactions.merge(first_pc, on = 'user_id')
df = df[(df['created_at'] > df['first_purchase_date']) &
    (df['created_at'] <=df['first_purchase_date'] + pd.Timedelta(days=7))]
result = df[['user_id']].drop_duplicates()
```

> This passed, but note it is **not** the same logic as my SQL. SQL isolates the literal 2nd-ranked distinct day; pandas here anchors on the **minimum** date and keeps *any* purchase in the `(first, first+7days]` window. On this dataset both return the same users, but they differ if a user's first *two* purchases are >7 days apart while a *later* one falls inside 7 days of the first — see the diff discussion in Section 6.

### 4.3 PySpark

⚠️ Not attempted / no accepted submission in my history.

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)`.
> Premium-only feature.

### 5.1 PostgreSQL

```sql
WITH daily AS (
  SELECT DISTINCT user_id, created_at::date AS purchase_date
  FROM amazon_transactions
),
ranked AS (
  SELECT
    user_id,
    purchase_date,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY purchase_date) AS rn
  FROM daily
),
first_two AS (
  SELECT
    user_id,
    MAX(CASE WHEN rn = 1 THEN purchase_date END) AS first_date,
    MAX(CASE WHEN rn = 2 THEN purchase_date END) AS second_date
  FROM ranked
  WHERE rn <= 2
  GROUP BY user_id
)
SELECT user_id
FROM first_two
WHERE second_date IS NOT NULL
  AND (second_date - first_date) BETWEEN 1 AND 7
ORDER BY user_id;
```

### 5.2 Python (pandas)

```python
import pandas as pd

amazon_transactions["purchase_date"] = pd.to_datetime(amazon_transactions["created_at"]).dt.date
daily = amazon_transactions[["user_id", "purchase_date"]].drop_duplicates()
ranked = daily.sort_values(["user_id", "purchase_date"])
ranked["rn"] = ranked.groupby("user_id").cumcount() + 1
first_two = (ranked[ranked["rn"] <= 2]
             .pivot(index="user_id", columns="rn", values="purchase_date")
             .reset_index()
             .rename(columns={1: "first_date", 2: "second_date"}))
first_two = first_two.dropna(subset=["second_date"])
first_two["diff"] = (pd.to_datetime(first_two["second_date"]) - pd.to_datetime(first_two["first_date"])).dt.days
result = first_two[(first_two["diff"] >= 1) & (first_two["diff"] <= 7)][["user_id"]]
```

### 5.3 PySpark

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

daily = amazon_transactions.select(
    "user_id",
    F.to_date("created_at").alias("purchase_date")
).distinct()

w = Window.partitionBy("user_id").orderBy("purchase_date")

ranked = daily.withColumn(
    "rn", F.row_number().over(w)
)

first_two = ranked.where(F.col("rn") <= 2).groupBy("user_id").agg(
    F.max(F.when(F.col("rn") == 1, F.col("purchase_date"))).alias("first_date"),
    F.max(F.when(F.col("rn") == 2, F.col("purchase_date"))).alias("second_date")
)

result = first_two.where(
    F.col("second_date").isNotNull() &
    F.datediff("second_date", "first_date").between(1, 7)
).select("user_id")
result.toPandas()
```

> StrataScratch also ships MySQL, MSSQL, Oracle, R, and Polars solutions for this problem. The three engines above are the canonical ones I track. Worth noting: the official **Polars** solution does *not* rank to the 2nd day — it anchors on `min().over("user_id")` and keeps transactions in `(first, first+7days]`, then `unique()`. That min-anchor approach is exactly the shape of **my pandas** solution (4.2), not the author's pandas (5.2).

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine (SQL, 4.1) | Author (SQL, 5.1) | Impact |
|---|---|---|---|
| Isolating 1st & 2nd day | Two separate CTEs `t1` (`purchase_order = 1`) and `t2` (`purchase_order = 2`), then `INNER JOIN` on `user_id` | One `GROUP BY user_id` with conditional `MAX(CASE WHEN rn = 1 ...)` / `MAX(CASE WHEN rn = 2 ...)` pivoted onto one row | Same result. Author folds both dates into a single grouped row (no join); mine splits and re-joins. Author is one fewer table reference |
| Dropping users with no 2nd purchase | The `INNER JOIN t1 ↔ t2` silently drops them (no `t2` row → no match) | Explicit `WHERE second_date IS NOT NULL` after the pivot | Both correct. Mine relies on join semantics; author's is explicit and self-documenting |
| Same-day collapse | `SELECT DISTINCT user_id, created_at::date` (identical idea) | `SELECT DISTINCT user_id, created_at::date` (identical) | Identical — this is the non-negotiable step both of us do |
| Gap filter | `WHERE (second_date - first_date) BETWEEN 1 AND 7` | `AND (second_date - first_date) BETWEEN 1 AND 7` | Identical idiom (date − date = integer days, `BETWEEN 1 AND 7`) |
| `ORDER BY` on output | None (problem only asks for the list; grader accepted unordered) | `ORDER BY user_id` | Author sorts defensively; mine passed without it, but sorting is the safer habit |
| Number of CTEs | 4 (`cte`, `cte2`, `t1`, `t2`) | 3 (`daily`, `ranked`, `first_two`) | Author is slightly tighter via the conditional-aggregate pivot |
| **My pandas vs. author** | Min-anchor: keep any txn in `(min_date, min_date+7d]` | Rank to 2nd distinct day, diff the two | **Semantically different.** Mine flags a user if *any* purchase is within 7 days of the first; author flags only if the **2nd** purchase is. They diverge when the 2nd purchase is >7 days out but a 3rd lands within 7 days of the first |

**Key insight in one line:** the canonical pattern is "collapse to distinct days → `ROW_NUMBER()` per user → keep ranks 1 and 2 → `BETWEEN 1 AND 7` on the date diff"; the author collapses the 1st/2nd dates with a **conditional-aggregate pivot** (`MAX(CASE WHEN rn=1/2 ...)`) so no self-join is needed, and crucially ranks to the *literal 2nd purchase* — which my pandas min-anchor version does not.

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=10322)` — **76 attempts total** across three sessions: **2025-06-25** (8, PostgreSQL, exploratory, no accepted submission), **2025-07-07** (8, pandas, ended in an accepted submission), and **2026-05-28** (60, PostgreSQL, ended in an accepted submission). Most attempts are `run`; rows marked **submitted** are graded. Highlights (times UTC):

| # | Time (UTC) | Lang | Status | What happened |
|---|---|---|---|---|
| 1 | 2025-06-25 06:23 | SQL | run | `select * from amazon_transactions;` — first look at the table |
| 2–8 | 2025-06-25 06:40–07:04 | SQL | ❌ run | Window-function syntax flailing: `rank()/first_value` with bad `ORDER BY` placement, `dec` for `desc`, `order created_at) by ...` — multiple `syntax error at or near` |
| 9–14 | 2025-07-07 07:42–07:48 | pandas | ❌ run | pandas detours: `amazon_transactinos` typo (`NameError`), and `ufunc 'bitwise_and' not supported` (boolean mask combined wrongly / used `and` semantics on Series) |
| 15 | 2025-07-07 07:49 | pandas | run | Clean `groupby min` + merge + `Timedelta(days=7)` window + `drop_duplicates` |
| 16 | 2025-07-07 07:49:28 | pandas | ✅ **submitted** | Same pandas code → **accepted** (first time I solved this problem) |
| 17–18 | 2026-05-28 11:28–11:30 | SQL | run | Re-attacking in SQL ~10 months later; re-inspecting / re-building the CTE skeleton |
| ~ | 2026-05-28 11:33–11:52 | SQL | ❌ run | `rank(purchase_date) over(...)` → `WITHIN GROUP is required for ordered-set aggregate rank`; `row_number(() over(...)` double-paren; `where rank() over(...) = 1` → `window functions are not allowed in WHERE` |
| ~ | 2026-05-28 11:48–11:54 | SQL | ❌ **submitted** | Several graded-incorrect submissions while the window/diff logic was still half-formed (e.g. cross-join `from t1, t2` with `where t2.created_at - t1.created_at` → `argument of WHERE must be type boolean, not type integer`) |
| ~ | 2026-05-28 12:03–12:22 | SQL | ❌ **submitted** | More incorrect submissions; `GroupingError` (`t2.purchase_date - t1.purchase_date` not in `GROUP BY`) and `InvalidSchemaName` (`schema "t1" does not exist` — referenced `t1.` before/without defining the CTE correctly) |
| ~ | 2026-05-28 12:53–12:54 | SQL | ❌ **submitted** | Final couple of incorrect submissions while converging on the `t1`/`t2` split-CTE shape |
| ~ | 2026-05-28 12:54–13:06 | SQL | run | Lowercased draft of the working query: `cte` → `cte2` (`ROW_NUMBER`) → `t1`/`t2` → `INNER JOIN` → `BETWEEN 1 AND 7` |
| 76 | 2026-05-28 13:06:11 | SQL | ✅ **submitted** | Same query, formatted/uppercased → **accepted** |

> Across all 76 attempts there were **2 accepted submissions** (pandas 2025-07-07, PostgreSQL 2026-05-28) and roughly **a dozen graded-incorrect submissions**, almost all in the 2026-05-28 SQL session while the window-function syntax and the diff filter were being worked out. The bulk of the rest are `run` iterations.

What the failed attempts taught me:
- **Window-function syntax is my real weak spot here, not the logic.** I knew "rank per user, take 1st and 2nd, diff the dates" on day one — but I burned ~20 attempts on *where* `ORDER BY` goes inside `OVER()`, `rank()` vs `row_number()` (`rank(col)` is the ordered-set aggregate and wants `WITHIN GROUP`), `desc` vs `dec`, and a stray double paren. Drill: write the skeleton `ROW_NUMBER() OVER (PARTITION BY ___ ORDER BY ___)` from muscle memory before anything else.
- **You cannot filter on a window function in `WHERE`.** `where rank() over(...) = 1` throws `window functions are not allowed in WHERE`. The window value has to be materialised in a subquery/CTE first, then filtered in the outer query. This bit me directly.
- **A `WHERE` clause needs a boolean, and `date − date` is an integer.** `where t2.created_at - t1.created_at` (no comparison) → `argument of WHERE must be type boolean, not type integer`. The fix is the comparison itself: `... BETWEEN 1 AND 7`. (Different root cause from the 2110 `^` trap, but the same family of lesson: read what the operator actually returns.)
- **pandas boolean masks combine with `&`, not `and`,** and each condition must be parenthesised — the `ufunc 'bitwise_and' not supported` error is the tell that I mixed Python `and`/types into a vectorised mask.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context.

### 8.1 Correctness
- **Score**: 3/5
- Comment: Your PostgreSQL (4.1) is correct: the `SELECT DISTINCT user_id, created_at::DATE` collapse, the `ROW_NUMBER()` rank, and `BETWEEN 1 AND 7` are all sound, and the `INNER JOIN t1 ↔ t2` correctly drops users with no 2nd purchase. But your pandas (4.2) answers a *different question*: `groupby('user_id')['created_at'].min()` anchors on the minimum and keeps *any* purchase in `(first, first+7d]`, so a user whose 2nd purchase is on day 10 but 3rd is on day 5 gets flagged — the prompt asks specifically about the **2nd** purchase. It passed only because the seed data doesn't separate the two. And PySpark is absent. Across three required engines you have one faithful solution, one that passes by coincidence, and one missing — that is a 3.

### 8.2 Performance & Efficiency
- **Score**: 4/5
- Comment: The shape is efficient: one `DISTINCT`, one `ROW_NUMBER()` partition scan, then a tiny join on two single-row-per-user CTEs. Nothing here scans the base table twice unnecessarily. The only cost is that splitting into `t1` and `t2` (4.1, lines 50–64) and re-joining materializes two intermediate relations where the author's `MAX(CASE WHEN rn=1/2 ...)` (5.1, lines 115–116) collapses both dates in a single `GROUP BY` pass — one fewer table reference, no join. Concrete fix: fold `t1`/`t2` into one conditional-aggregate CTE so the planner has a single grouped scan instead of a join.

### 8.3 Readability
- **Score**: 4/5
- Comment: This reads cleanly — uppercased keywords, one CTE per step, `first_date`/`second_date` aliases that say exactly what they hold. The split-CTE structure is arguably *easier* to trace than the author's pivot. The dock is the names `cte` and `cte2` (lines 35, 42): they describe nothing. Rename them to `daily` and `ranked` (as the author does) so a reader sees intent without reading the body.

### 8.4 Edge Cases
- **Score**: 4/5
- Comment: You nailed the two real traps — same-day collapse via `DISTINCT`, and the `1` lower bound in `BETWEEN 1 AND 7` killing the gap-0 case. The "no 2nd purchase" case is handled implicitly by the `INNER JOIN`. One gap: that reliance is silent. The author's explicit `WHERE second_date IS NOT NULL` (5.1, line 123) self-documents the same guard. Concrete fix: add a one-line comment on the join, or adopt the explicit NULL check, so a reviewer doesn't have to infer that join semantics are doing the filtering.

### 8.5 Interview-Readiness
- **Score**: 3/5
- Comment: Strong moment: in your 4.2 note you *caught yourself* — "this is **not** the same logic as my SQL" — naming the min-anchor-vs-2nd-day divergence unprompted. That self-audit is exactly the signal a bar-raiser rewards. But two things cap you at a screen-pass, not a bar-raiser. First, you shipped two engines solving different definitions and only noticed after the fact; in a live loop, port the *definition*, not the green checkmark. Second, PySpark is unattempted (4.3) — in a FAANG DA/DS loop the interviewer can pick the language, and "I haven't written this in Spark" is a hard signal. Concrete fix: write the PySpark version cold (`Window.partitionBy('user_id').orderBy('purchase_date')` + `row_number` + `datediff(...).between(1,7)`) before you call this problem closed.

### 8.6 Verdict
The logic in your head was right from day one — collapse to distinct days, rank, diff — and your PostgreSQL proves you can execute it cleanly. The one thing that would move you to the next level on problems like this: **treat all three engines as one problem with one definition, not three separate grader-pleasing exercises.** Your pandas passed while answering a subtly different question, and that only surfaced because you happened to reflect afterward — in an interview, no one hands you forgiving seed data. The 76-attempt, ~20-on-syntax history (Section 7) says the *mechanics* (`ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`, window-in-`WHERE`) are your friction point, not the reasoning; drill the skeleton to muscle memory so the thinking, which is already sound, gets to do the talking.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Collapse to distinct days | `SELECT DISTINCT user_id, created_at::date` | `df[['user_id','purchase_date']].drop_duplicates()` after `pd.to_datetime(...).dt.date` | `.select('user_id', F.to_date('created_at')).distinct()` |
| Rank events per user | `ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY purchase_date)` | `df.groupby('user_id').cumcount() + 1` (after `sort_values`) | `F.row_number().over(Window.partitionBy('user_id').orderBy('purchase_date'))` |
| Pivot 1st/2nd onto one row | `MAX(CASE WHEN rn=1 THEN d END)`, `MAX(CASE WHEN rn=2 THEN d END)` + `GROUP BY` | `.pivot(index='user_id', columns='rn', values='purchase_date')` | `F.max(F.when(F.col('rn')==1, col))`, `F.max(F.when(F.col('rn')==2, col))` |
| Date difference in days | `second_date - first_date` (date − date → int) | `(pd.to_datetime(b) - pd.to_datetime(a)).dt.days` | `F.datediff('second_date','first_date')` |
| "Between 1 and 7 days" | `BETWEEN 1 AND 7` | `(diff >= 1) & (diff <= 7)` | `.between(1, 7)` |
| Window value can't go in filter | compute in CTE, then `WHERE rn = ...` (NOT `WHERE row_number() over(...)`) | n/a (assign column, then mask) | compute with `.withColumn`, then `.where('rn = ...')` |
| Combine boolean conditions | `AND` | `&` with each side parenthesised (NOT `and`) | `&` (NOT `and`) |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: "**n-th event per group**." Whenever a question says *first / second / latest / k-th* purchase-login-order-etc. *per user*, the reflex is: (1) collapse to the right grain (here: distinct **days**, because same-day must not count), (2) `ROW_NUMBER() OVER (PARTITION BY <entity> ORDER BY <time>)`, (3) keep the rank(s) you need, (4) compare across them. The 1st-vs-2nd date gap then becomes a plain `BETWEEN 1 AND 7` on `date − date`.

- **Reusable snippet (PostgreSQL, parameterized)**:
  ```sql
  WITH days AS (                                   -- collapse to the right grain
      SELECT DISTINCT entity_id, event_at::date AS d
      FROM events
  ),
  ranked AS (
      SELECT entity_id, d,
             ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY d) AS rn
      FROM days
  ),
  pivoted AS (                                     -- 1st/2nd onto one row, no self-join
      SELECT entity_id,
             MAX(CASE WHEN rn = 1 THEN d END) AS first_d,
             MAX(CASE WHEN rn = 2 THEN d END) AS second_d
      FROM ranked WHERE rn <= 2
      GROUP BY entity_id
  )
  SELECT entity_id
  FROM pivoted
  WHERE second_d IS NOT NULL
    AND (second_d - first_d) BETWEEN :lo AND :hi;
  ```

- **Weakness I noticed in myself**:
  1. **Window-function syntax is not yet automatic.** The logic was right on day one, but ~20 attempts went to mechanical errors: `ORDER BY` placement inside `OVER()`, `rank(col)` (ordered-set, wants `WITHIN GROUP`) vs plain `row_number()`, `dec`/`desc`, double parens, and trying to filter a window function in `WHERE`. This is muscle-memory I can drill cheaply.
  2. **My pandas and my SQL solved *different problems* and I didn't notice.** SQL ranks to the literal 2nd day; pandas anchors on the min and accepts any later purchase within 7 days. Both passed the grader (the seed data doesn't distinguish them), but they are not equivalent. Lesson: when I port across engines, port the **definition**, not just "make the grader green."
  3. **Cross-engine syntax bleed** (pandas `&` vs `and`, the `bitwise_and` error) and **reading what an operator returns** (`date − date` is an int, illegal alone in `WHERE`).

- **Drill for tomorrow morning** (before any new problems):
  1. Re-solve 10322 cold in PostgreSQL using the **author's conditional-aggregate pivot** (`MAX(CASE WHEN rn=1/2 ...)` + `GROUP BY`) — no `t1`/`t2` self-join. Time-box 8 minutes, and write the `ROW_NUMBER() OVER (...)` from memory.
  2. Rewrite my **pandas** so it matches the SQL definition (rank to the 2nd distinct day via `cumcount`), not the min-anchor — then convince myself which one the question actually asks for.
  3. Port to **PySpark** (zero attempts on this one): `Window.partitionBy('user_id').orderBy('purchase_date')` + `row_number` + `datediff(...).between(1,7)`.

## 11. Related Notes

- **Same category (Window Functions — "n-th event / ordered events per group")**: pair this with any problem that asks for *first / latest / k-th* occurrence per entity, or "consecutive" / "next event" problems. They all reduce to `ROW_NUMBER()/RANK() OVER (PARTITION BY entity ORDER BY time)` then a filter or self-compare across ranks. This is the prototypical "rank-then-pick-the-2nd" exercise.
- **Adjacent skills exercised**: gap-between-events analysis (date − date with a `BETWEEN` window), and the conditional-aggregate **pivot** (`MAX(CASE WHEN ...)`) as a join-free way to put two ranked rows side by side — reusable in any "compare event #1 to event #2" question.
- **Reference reading**:
  - PostgreSQL window functions (why `ORDER BY` lives inside `OVER()`, why they can't go in `WHERE`): https://www.postgresql.org/docs/current/tutorial-window.html
  - `ROW_NUMBER` / `RANK` / ordered-set aggregates (`rank(arg) WITHIN GROUP`): https://www.postgresql.org/docs/current/functions-window.html
  - pandas `groupby().cumcount()` and boolean masking with `&`: https://pandas.pydata.org/docs/reference/api/pandas.core.groupby.DataFrameGroupBy.cumcount.html

---

*Generated 2026-06-30 by Ina (problem solving) + Claude (author solution fetch via MCP, submission history analysis, FAANG reviewer subagent).*
