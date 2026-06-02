# 2024 — Unique Users Per Client Per Month

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 2024 |
| Category | Date/Time (with distinct-count aggregation) |
| Difficulty | Easy |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/2024-unique-users-per-client-per-month?code_type=1) · [Python](https://platform.stratascratch.com/coding/2024-unique-users-per-client-per-month?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/2024-unique-users-per-client-per-month?code_type=6) |
| Solved on | 2026-06-02 |
| Table | `fact_events(client_id text, customer_id text, event_id bigint, event_type text, id bigint, time_id date, user_id text)` |

## 2. Problem Restatement (in my words)

For each client, count how many distinct users it had in each month. All events
fall within a single year, so the month can be output as a plain integer 1–12
(no year component). Output: `client_id`, `month`, distinct user count.

The two traps are grain and metric: the grain is **client × month** (two keys,
not one), and the metric is **distinct users** (`COUNT(DISTINCT)`, not a plain
row count). `customer_id` is a decoy column — it is never needed.

## 3. My Approach

- **First instinct (SQL)**: pull `EXTRACT(MONTH FROM time_id)` into a CTE, then
  group and count. Correct shape, but my first submission grouped by `month`
  **only** and dropped `client_id` — so it answered "unique users per month"
  instead of "per client per month."
- **Where I got stuck (pandas)**: I built a separate `month` frame and merged it
  back onto `id`. Two failed submissions used `.count()` (counts rows, not unique
  users) and grouped by the full `time_id` date instead of the month integer. Also
  hit `KeyError: 'month'` from `rename({...})` without the `columns=` keyword.
- **Where I got stuck (PySpark)**: the longest grind. Import typo
  (`from pyspark.sql import function`), an un-aliased derived column so `month`
  couldn't be resolved, `.count('customer_id')` (wrong API call), a `f.countd`
  typo, and — most importantly — two failed submissions that counted
  `customer_id` (the decoy) instead of `user_id`, one of them non-distinct.
- **Final strategy**: in all three engines — derive month, group by
  `client_id` + `month`, count **distinct** `user_id`.

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-06-02 05:33)

```sql
WITH cte AS (
    SELECT
        user_id,
        customer_id,
        client_id,
        EXTRACT(MONTH FROM time_id) AS month
    FROM fact_events
)
SELECT
    client_id,
    month,
    COUNT(DISTINCT user_id) AS n_user
FROM cte
GROUP BY client_id, month
ORDER BY month;
```

### 4.2 Python (pandas) ✅ (passed 2026-06-02 05:52)

```python
import pandas as pd

# Build a month column, then merge back on id
month = fact_events[['id', 'time_id']]
month['time_id'] = month['time_id'].dt.month
month = month.rename(columns={'time_id': 'month'})

df = pd.merge(
    fact_events[['id', 'user_id', 'customer_id', 'client_id']],
    month, on='id', how='inner'
)

result = df.groupby(['client_id', 'month'])['user_id'].nunique()
result   # NOTE: returns a MultiIndex Series; see Section 6 — add .reset_index()
```

### 4.3 PySpark ✅ (passed 2026-06-02 10:23)

```python
import pyspark
from pyspark.sql import functions as f

df = fact_events.select(
    'id',
    f.month('time_id').alias('month'),   # alias the derived column
    'client_id',
    'user_id'
)

df = df.groupBy(['client_id', 'month']) \
       .agg(f.count_distinct('user_id').alias('users_num'))

df.toPandas()
```

## 5. Author Solutions

Pulled via MCP `get_question(include_solution=True)`.

### 5.1 PostgreSQL

```sql
SELECT client_id,
       EXTRACT(month from time_id) AS month,
       COUNT(DISTINCT user_id) AS users_num
FROM fact_events
GROUP BY client_id, EXTRACT(month from time_id);
```

### 5.2 Python (pandas)

```python
import pandas as pd

result = (
    fact_events.groupby(
        [fact_events["client_id"], fact_events["time_id"].dt.month]
    )["user_id"]
    .nunique()
    .reset_index()
)
result = result.rename(columns={"time_id": "month", "user_id": "users_num"})
```

### 5.3 PySpark

```python
import pyspark.sql.functions as F

result = (
    fact_events.groupby(
        fact_events["client_id"], F.month(fact_events["time_id"]).alias("month")
    )
    .agg(F.countDistinct("user_id").alias("users_num"))
    .toPandas()
)
```

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| SQL structure | CTE that also carries `customer_id` (unused), then group | One-step `GROUP BY` with inline `EXTRACT` | Same result; my CTE is gratuitous for an Easy aggregation and drags a decoy column |
| pandas month derivation | Separate frame + `pd.merge` on `id` | `time_id.dt.month` inline as a groupby key | Author avoids a needless self-join (an extra full scan); mine is over-engineered |
| pandas output shape | MultiIndex **Series**, no `reset_index()` | `.reset_index()` → flat 3-column frame | Mine isn't the clean tabular contract; a stricter grader would reject it |
| Distinct count | `nunique` / `count_distinct` (final) | same | Correct — but I reached it only after submitting plain `.count()` first |
| Output column name | `n_user` (SQL) vs `users_num` (PySpark) — inconsistent | `users_num` everywhere | Anchor to one requested name across all three dialects |
| `count_distinct` vs `countDistinct` | `f.count_distinct` | `F.countDistinct` | Both are valid aliases in modern PySpark; good to know they're interchangeable |

**Key insight in one line:** lock the **grain** (client × month) and the **metric**
(distinct `user_id`) before typing — every one of my failed submissions broke one
of those two, never the syntax.

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=2024)` — 46 attempts. Highlights:

| # | Time (UTC) | Language | Status | What happened |
|---|---|---|---|---|
| 1 | 05:27 | SQL | run | `SELECT * FROM fact_events` — inspect |
| 2 | 05:31 | SQL | run | `EXTRACT(MONTH ...)` + columns — inspect |
| 3 | 05:32 | SQL | ❌ submitted | Grouped by `month` **only** — dropped `client_id` |
| 4 | 05:33 | SQL | ✅ submitted | Added `client_id` to CTE and `GROUP BY` |
| 5 | 05:45–05:46 | Python | run | Building separate `month` frame, `.info()`/`.head()` |
| 6 | 05:47 | Python | ❌ submitted | Grouped by full date `time_id`, used `.count()` |
| 7 | 05:49 | Python | ❌ runs | `rename({...})` without `columns=` → `KeyError: 'month'` |
| 8 | 05:49 | Python | ❌ submitted | `.count()` — counts rows, not distinct users |
| 9 | 05:52 | Python | ✅ submitted | Switched to `.nunique()`, grouped by `client_id`+`month` |
| 10 | 09:51–10:13 | PySpark | ❌ runs | `from pyspark.sql import function` (singular) ImportError |
| 11 | 10:15 | PySpark | ❌ runs | Grouped by un-aliased derived col → `month` UNRESOLVED_COLUMN |
| 12 | 10:17 | PySpark | ❌ runs | `.count('customer_id')` — API takes no positional arg |
| 13 | 10:19 | PySpark | ❌ runs | `count` not defined; `df.f` typo; un-aliased `time_id` |
| 14 | 10:20–10:21 | PySpark | ❌ submitted | Grouped by `user_id`, counted `customer_id` (wrong key + decoy column) |
| 15 | 10:22 | PySpark | ❌ runs | `f.countd` typo |
| 16 | 10:23:16 | PySpark | ❌ submitted | `count_distinct(customer_id)` — distinct, but the **wrong** column |
| 17 | 10:23:52 | PySpark | ✅ submitted | `count_distinct(user_id)`, groupBy `client_id`+`month` |

What the failed attempts taught me:
- **Grain first**: "per client per month" = two grouping keys. I dropped `client_id`
  in SQL and grouped by `user_id` in PySpark. Read the "per X per Y" phrasing as a
  literal `GROUP BY X, Y`.
- **Distinct vs count**: "unique users" → `COUNT(DISTINCT)` / `nunique` /
  `count_distinct`. I shipped plain `.count()` (row count) twice before fixing it.
- **Right column**: `customer_id` is a decoy. Counting it in PySpark gave a wrong —
  but green-looking-to-me — answer until the grader rejected it.
- **pandas idioms**: `rename(columns={...})` needs the keyword; deriving the month
  inline beats building a frame and `merge`-ing it back.
- **PySpark idioms**: `from pyspark.sql import functions` (plural); always
  `.alias()` a derived column or you can't reference it downstream;
  `count_distinct` and `countDistinct` are the same function.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context.

### 8.1 Correctness
- **Score**: 4/5
- Comment: All three accepted solutions produce the correct result set, but the pandas submission ends on a MultiIndex Series without `.reset_index()`, so it does not emit the three flat columns (`client_id`, `month`, `users_num`) the contract specifies and would fail a stricter grader.

### 8.2 Performance & Efficiency
- **Score**: 3/5
- Comment: SQL and PySpark are clean single-pass aggregations, but the pandas path builds a separate `month` frame and `pd.merge`s it back on `id`, an unnecessary self-join that doubles the scan; derive the month column inline with `fact_events["time_id"].dt.month` inside the `groupby` key as the reference does.

### 8.3 Readability
- **Score**: 3/5
- Comment: The SQL CTE is tidy but gratuitous for an Easy aggregation, and it carries `customer_id` into the CTE only to drop it; the pandas detour through `month`/`rename`/`merge` obscures a one-liner, and inconsistent output aliases (`n_user`, `users_num`) across languages signal you are not anchoring to the requested column name.

### 8.4 Edge Cases
- **Score**: 3/5
- Comment: `COUNT(DISTINCT user_id)` and `nunique`/`count_distinct` correctly ignore NULL users, which is fine, but you never stated the single-year assumption that makes a bare month integer safe; if events spanned years you would collapse Jan 2023 and Jan 2024, and flagging that out loud is what separates a 3 from a 5.

### 8.5 Interview-Readiness
- **Score**: 2/5
- Comment: The history shows silent trial-and-error rather than reasoning: in all three languages your first submission used the wrong grain or wrong aggregate (month-only grouping in SQL, `.count()` instead of `.nunique()` in pandas, counting `customer_id` non-distinctly in PySpark), which means you shipped a non-distinct row count as a "unique users" answer before correcting; state the grain (client_id x month) and the distinct-count requirement up front, then write once.

### 8.6 Verdict
Your final code passes, but the path there reveals the core gap: you are pattern-matching syntax instead of locking the problem's grain and metric before typing. The single highest-leverage fix is to verbalize the contract first ("group by client_id and month, count DISTINCT user_id, ignore customer_id entirely") so you stop conflating row counts with unique-user counts and stop dragging unused columns like `customer_id` through your pipelines. Collapse the pandas solution to the reference one-liner with `.dt.month` in the groupby key and finish with `.reset_index()` plus a rename to `users_num`, and standardize that output name across all three dialects. Tighten these and an Easy like this becomes a clean, no-coaching submission rather than a three-try recovery.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Extract month from date | `EXTRACT(MONTH FROM time_id)` / `DATE_PART('month', ...)` | `s.dt.month` | `F.month('time_id')` |
| Count distinct | `COUNT(DISTINCT user_id)` | `.nunique()` (on a grouped Series) | `F.count_distinct('user_id')` / `F.countDistinct(...)` |
| Group by two keys | `GROUP BY client_id, month` | `df.groupby(['client_id', 'month'])` | `df.groupBy(['client_id', 'month'])` |
| Flatten group result | (already tabular) | `.reset_index()` | (already tabular after `.agg`) |
| Rename output column | `... AS users_num` | `.rename(columns={'user_id': 'users_num'})` | `.alias('users_num')` |
| Derive col inline in groupby | n/a (use `EXTRACT` in `GROUP BY`) | `df.groupby([df['client_id'], df['time_id'].dt.month])` | `df.groupBy(col, F.month(c).alias('month'))` |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: "unique X per A per B" =
  `GROUP BY A, B` + `COUNT(DISTINCT X)`. Parse "per A per B" as a literal
  two-key grouping, and "unique/distinct" as a distinct count — never a row count.

- **Reusable snippet (PostgreSQL)**:
  ```sql
  SELECT a, EXTRACT(MONTH FROM ts) AS month, COUNT(DISTINCT x) AS n
  FROM tbl
  GROUP BY a, EXTRACT(MONTH FROM ts);
  ```

- **Weaknesses I noticed in myself**:
  1. I start typing before fixing the grain and the metric. Every failed submission
     here broke one of those two (wrong keys or `.count()` vs distinct), never the
     syntax. Fix: say the contract out loud first.
  2. PySpark fluency gap — import path (`functions` plural), aliasing derived
     columns, and the `count_distinct`/`countDistinct` name cost me ~10 attempts.
  3. pandas: I reach for `merge` when a direct column assignment / inline groupby key
     would do, and I forget `.reset_index()` to get a flat table.
  4. I ignored the decoy column late — counting `customer_id` instead of `user_id`.
     Identify the decoy column during restatement, not during debugging.

- **Drill for tomorrow morning** (10-min time-box, before new problems):
  1. Redo 2024 in pandas as the reference one-liner: inline `.dt.month` in the
     groupby key, `.nunique()`, `.reset_index()`, rename to `users_num`. No `merge`.
  2. Redo 2024 in PySpark cold — no inspection runs — getting the import, alias, and
     `count_distinct(user_id)` right on the first try.

## 11. Related Notes

- Same category (Date/Time) in our queue: 2056 "Number of Shipments Per Month"
  (Easy), 10322 "Finding User Purchases" / 10285 "Acceptance Rate By Date" (Medium).
- Same "distinct count per group" pattern: 9728 "Number of violations",
  9911 "Departments With 5 Employees".
- Reference reading:
  - PostgreSQL date/time functions: https://www.postgresql.org/docs/current/functions-datetime.html
  - pandas `groupby` + `nunique`: https://pandas.pydata.org/docs/reference/api/pandas.core.groupby.DataFrameGroupBy.nunique.html
  - PySpark `month` / `count_distinct`: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html

---

*Generated 2026-06-02 by Ina (3-language solving) + Claude (author solution fetch
via MCP, submission history analysis, FAANG reviewer subagent).*
