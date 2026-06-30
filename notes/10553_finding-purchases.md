# 10553 — Finding Purchases

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 10553 |
| Category | Window Functions |
| Difficulty | Medium |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/10553-finding-purchases?code_type=1) · [Python](https://platform.stratascratch.com/coding/10553-finding-purchases?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/10553-finding-purchases?code_type=6) |
| Solved on | 2026-06-02 |
| Tables | `amazon_transactions(created_at date, id bigint, item text, revenue bigint, user_id bigint)` |

## 2. Problem Restatement (in my words)

For each user, look at their transactions in time order and measure the gap between each purchase and the one immediately before it. A user counts as a "returning active user" if at least one of those gaps is **1 to 7 days inclusive** — i.e. a repeat purchase within a week, but *not* on the same day (a 0-day gap doesn't count). Output the distinct `user_id` of every user who has at least one such gap. The whole problem is per-user "time since previous purchase," so the natural tool is `LAG(created_at)` partitioned by user, ordered by date, then a filter `1 <= diff <= 7`. Two traps: (a) the same-day exclusion means the lower bound is `>= 1`, not `>= 0`; (b) one user can have many qualifying gaps, so the output must be `DISTINCT` or you emit the same `user_id` repeatedly.

## 3. My Approach

- **First instinct**: count purchases per user. My very first real query was `GROUP BY user_id, created_at, COUNT(*)` and then `GROUP BY user_id COUNT(*)` — I was thinking "find users who bought more than once" rather than "measure the gap between consecutive buys." That framing sent me down a dead end.
- **Dead-end branch (13:46–13:52)**: I built a CTE to keep only users with `COUNT(*) > 1`, joined that back to the transactions, then tried to number each user's purchases with `DENSE_RANK() OVER (... ORDER BY created_at)` and filter `WHERE purchase_order = 1`. That doesn't even answer the question (the first purchase tells me nothing about a *repeat* within 7 days), and it crashed twice with `column "purchase_order" does not exist` because I referenced the window-function alias in the `WHERE` of the same SELECT that defines it.
- **The pivot (13:58)**: I abandoned counting/ranking and switched to `LAG(created_at)` to pull each row's previous purchase date onto the same row. That is the right idea and everything after it was just cleanup.
- **Where I got stuck (three small things)**:
  1. `LAG(created_at, 1, 0)` — I tried to give a default of `0` for the first row, but `0` is an integer and the column is a date, so `function lag(date, integer, integer) does not exist`. Dropped the default; the first row's `prev_purchase` is `NULL`, which I want anyway.
  2. **Sign of the subtraction.** I first wrote `prev_purchase - created_at` (negative gaps) before flipping to `created_at - prev_purchase`.
  3. **Output shape.** My first two *submissions* (`diff != 0`, then `diff >= 1`) both came back **incorrect** — and the real reason was that I was selecting `user_id` without `DISTINCT`, so a user with several qualifying gaps appeared multiple times. The instant I changed `SELECT user_id` to `SELECT DISTINCT user_id`, it passed.
- **Final strategy**: CTE 1 = `LAG(created_at) OVER (PARTITION BY user_id ORDER BY created_at)` to get `prev_purchase`. CTE 2 = compute `created_at - prev_purchase AS diff` (Postgres date minus date returns an integer day count). Outer query = `SELECT DISTINCT user_id WHERE diff <= 7 AND diff >= 1`.

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-06-02 14:08)

```sql
WITH cte AS (
    SELECT
        user_id,
        created_at,
        LAG(created_at) OVER (PARTITION BY user_id ORDER BY created_at) AS prev_purchase
    FROM amazon_transactions
),

cte2 AS (
    SELECT
        user_id,
        (created_at - prev_purchase) AS diff
    FROM cte
)

SELECT DISTINCT user_id
FROM cte2
WHERE diff <= 7 AND diff >= 1;
```

> Note: in PostgreSQL, `date - date` returns an `integer` number of days, so `diff` is already a plain integer I can compare with `<= 7` and `>= 1` — no `EXTRACT` or `DATEDIFF` needed. The first purchase per user has `prev_purchase = NULL`, so `diff` is `NULL` there; `NULL >= 1` is unknown, so those rows are silently dropped by the `WHERE` without an explicit `IS NOT NULL` guard (the author adds one anyway — see §6).

### 4.2 Python (pandas)

⚠️ Not attempted / no accepted submission in my history.

### 4.3 PySpark

⚠️ Not attempted / no accepted submission in my history.

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)`.
> Premium-only feature. StrataScratch ships solutions for all eight supported
> engines on this problem; the three canonical engines I track are below, with the
> other five quoted underneath for completeness.

### 5.1 PostgreSQL

```sql
WITH ordered_tx AS
    (SELECT user_id,
            created_at::date AS tx_date,
            LAG(created_at::date) OVER (PARTITION BY user_id
                                        ORDER BY created_at) AS prev_tx_date
     FROM amazon_transactions)
SELECT DISTINCT user_id
FROM ordered_tx
WHERE prev_tx_date IS NOT NULL
    AND tx_date - prev_tx_date > 0
    AND tx_date - prev_tx_date <= 7;
```

### 5.2 Python (pandas)

```python
import pandas as pd

df = amazon_transactions.sort_values(by=['user_id', 'created_at'])
df['prev_date'] = df.groupby('user_id')['created_at'].shift(1)
df['days_diff'] = (df['created_at'] - df['prev_date']).dt.days

result = df[
    (df['days_diff'].notna()) & (df['days_diff'] > 0) & (df['days_diff'] <= 7)
]
returning_users = (
    result['user_id'].drop_duplicates().sort_values().reset_index(drop=True)
)
```

### 5.3 PySpark

```python
from pyspark.sql.functions import col, lag, datediff
from pyspark.sql.window import Window

window = Window.partitionBy('user_id').orderBy('created_at')

df = amazon_transactions.withColumn(
    'prev_date', lag('created_at').over(window)
)
df = df.withColumn('days_diff', datediff(col('created_at'), col('prev_date')))
df = df.filter(
    (col('days_diff').isNotNull())
    & (col('days_diff') > 0)
    & (col('days_diff') <= 7)
)
df = df.select('user_id').distinct().orderBy('user_id')

df.toPandas()
```

<details>
<summary>Other engines shipped by StrataScratch (MySQL, MSSQL, Oracle, R, Polars)</summary>

**MySQL**

```sql
WITH ordered_tx AS
    (SELECT user_id,
            DATE(created_at) AS tx_date,
            LAG(DATE(created_at)) OVER (PARTITION BY user_id
                                        ORDER BY created_at) AS prev_tx_date
     FROM amazon_transactions)
SELECT DISTINCT user_id
FROM ordered_tx
WHERE prev_tx_date IS NOT NULL
    AND DATEDIFF(tx_date, prev_tx_date) > 0
    AND DATEDIFF(tx_date, prev_tx_date) <= 7;
```

**MSSQL**

```sql
WITH ordered_tx AS
    (SELECT user_id,
            CAST(created_at AS DATE) AS tx_date,
            LAG(CAST(created_at AS DATE)) OVER (PARTITION BY user_id
                                                ORDER BY created_at) AS prev_tx_date
     FROM amazon_transactions)
SELECT DISTINCT user_id
FROM ordered_tx
WHERE prev_tx_date IS NOT NULL
    AND DATEDIFF(DAY, prev_tx_date, tx_date) > 0
    AND DATEDIFF(DAY, prev_tx_date, tx_date) <= 7;
```

**Oracle**

```sql
WITH ordered_tx AS
    (SELECT user_id,
            CAST(created_at AS DATE) AS tx_date,
            LAG(CAST(created_at AS DATE)) OVER (PARTITION BY user_id
                                                ORDER BY created_at) AS prev_tx_date
     FROM amazon_transactions)
SELECT DISTINCT user_id
FROM ordered_tx
WHERE prev_tx_date IS NOT NULL
    AND tx_date - prev_tx_date > 0
    AND tx_date - prev_tx_date <= 7;
```

**R**

```r
library(dplyr)

df <- amazon_transactions %>%
    arrange(user_id, created_at) %>%
    group_by(user_id) %>%
    mutate(prev_date = lag(created_at),
           days_diff = as.numeric(difftime(created_at, 
                                         prev_date, 
                                         units = 'days'))) %>%
    filter(!is.na(days_diff) & days_diff > 0 & days_diff <= 7) %>%
    distinct(user_id) %>%
    arrange(user_id)
```

**Polars**

```python
import polars as pl

returning_users = (
    amazon_transactions.lazy()
    .sort(["user_id", "created_at"])
    .with_columns(
        [
            (
                pl.col("created_at")
                - pl.col("created_at").shift(1).over("user_id")
            )
            .dt.total_days()
            .alias("days_diff")
        ]
    )
    .filter(pl.col("days_diff").is_between(1, 7, closed="both"))
    .select("user_id")
    .unique()
    .sort("user_id")
    .collect()
)
```

</details>

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| Window function | `LAG(created_at) OVER (PARTITION BY user_id ORDER BY created_at)` | `LAG(created_at::date) OVER (PARTITION BY user_id ORDER BY created_at)` | Identical idea. `created_at` is already a `date`, so the author's `::date` cast is a harmless no-op here; it would matter only if the column were a timestamp |
| Same-day exclusion | `diff >= 1` | `tx_date - prev_tx_date > 0` | Same boundary, written two ways. `> 0` and `>= 1` are identical for an integer day count — both exclude same-day (0-day) gaps |
| Upper bound | `diff <= 7` | `tx_date - prev_tx_date <= 7` | Identical |
| NULL handling | Relies on `NULL >= 1` evaluating to unknown → first-purchase rows dropped implicitly | Explicit `prev_tx_date IS NOT NULL` | Same result. Author is defensive and self-documenting; mine leans on three-valued logic doing the right thing silently |
| Structure | Two CTEs: one for `LAG`, one to compute `diff`, then filter outside | One CTE for `LAG`, compute the date difference inline in the `WHERE` | Author folds my second CTE into the `WHERE`; mine is one more step but arguably easier to eyeball the `diff` |
| `DISTINCT` | `SELECT DISTINCT user_id` | `SELECT DISTINCT user_id` | Same — and this was the line that actually decided pass/fail for me |

**Key insight in one line:** the canonical idiom is `LAG(date) OVER (PARTITION BY user ORDER BY date)` to get the previous event, then filter `1 <= (this_date - prev_date) <= 7` and **`SELECT DISTINCT`** — the author's explicit `prev_tx_date IS NOT NULL` is the only real robustness difference, and the `DISTINCT` is non-negotiable because a user can have many qualifying gaps.

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=10553)` — **23 attempts total**, all PostgreSQL (code_type 1), all on 2026-06-02. The early `SELECT *` was a separate sitting at 10:24; the real solve happened in one ~28-minute block from 13:40 to 14:08.

| # | Time (UTC) | Language | Status | What happened |
|---|---|---|---|---|
| 1 | 10:24 | SQL | run | `SELECT * FROM amazon_transactions` — inspect the table (separate early sitting) |
| 2 | 13:40 | SQL | ❌ run | `select user_id, created_at count(*)` — missing comma, `syntax error at or near "("` |
| 3 | 13:40 | SQL | ❌ run | Typo `created_atm count(*)` — same syntax error |
| 4 | 13:40 | SQL | run | Fixed: `GROUP BY user_id, created_at` with `COUNT(*)` — counting purchases per user/day |
| 5 | 13:41 | SQL | run | `GROUP BY user_id` with `COUNT(*)` — count purchases per user |
| 6 | 13:46 | SQL | run | New branch: CTE keeping users with `COUNT(*) > 1`, joined back to transactions |
| 7 | 13:50 | SQL | ❌ run | Added `DENSE_RANK() OVER (ORDER BY user_id, created_at)` and `WHERE purchase_order = 1` → `column "purchase_order" does not exist` (window alias used in same-SELECT WHERE) |
| 8 | 13:51 | SQL | run | Dropped the `WHERE purchase_order = 1` to inspect the ranking |
| 9 | 13:51 | SQL | run | Changed rank to `DENSE_RANK() OVER (PARTITION BY user_id ORDER BY created_at)` |
| 10 | 13:52 | SQL | ❌ run | Re-added `WHERE purchase_order = 1` → same `column "purchase_order" does not exist` error |
| 11 | 13:58 | SQL | run | **Pivot to `LAG`**: `LAG(created_at) OVER (PARTITION BY user_id)` (no ORDER BY yet) |
| 12 | 13:59 | SQL | run | Added `ORDER BY created_at` to the window |
| 13 | 14:00 | SQL | run | Selected `user_id, created_at, prev_purchase` to eyeball the lag |
| 14 | 14:02 | SQL | ❌ run | `LAG(created_at, 1, 0)` → `function lag(date, integer, integer) does not exist` (default `0` is int, column is date) |
| 15 | 14:03 | SQL | run | Dropped the default; `prev_purchase - created_at AS diff` (wrong sign) |
| 16 | 14:04 | SQL | run | Flipped to `created_at - prev_purchase AS diff` |
| 17 | 14:05 | SQL | run | Added `WHERE (created_at - prev_purchase) <= 7` — but no same-day exclusion yet |
| 18 | 14:06 | SQL | ❌ **submitted** | Two CTEs, `WHERE diff <= 7 AND diff != 0`, `SELECT user_id` (no DISTINCT) → **incorrect** |
| 19 | 14:08 | SQL | ❌ **submitted** | Changed `!= 0` to `>= 1`, still `SELECT user_id` (no DISTINCT) → **incorrect** |
| 20 | 14:08 | SQL | ✅ **submitted** | Added `DISTINCT`: `SELECT DISTINCT user_id ... WHERE diff <= 7 AND diff >= 1` → **accepted** |

(Run-only intermediate inspections between the milestones above are collapsed; the 23 raw rows include several no-op re-runs of the same query while iterating.)

What the failed attempts taught me:
- **The missing `DISTINCT` was the actual bug, not the comparison operator.** I changed `diff != 0` → `diff >= 1` between my first two submissions thinking the filter was wrong — but for an integer day count those two are identical, so that change did nothing. Both failed for the *same* reason: a user with multiple qualifying gaps was listed multiple times. Lesson: when the question says "output a list of `user_id`," assume it wants **distinct** ids, and when a submission is rejected check the **output shape** (duplicates / column count / order) before assuming the logic is wrong.
- **You can't reference a window-function alias in the `WHERE` of the same SELECT.** `WHERE purchase_order = 1` blew up twice with `column "purchase_order" does not exist`. Window functions are computed after `WHERE`; to filter on one, wrap it in a CTE/subquery and filter outside. (Same root cause cost me two attempts.)
- **`LAG`'s third argument (default) must type-match the column.** `LAG(created_at, 1, 0)` failed because `0` is an integer against a `date` column. I didn't need a default at all — letting the first row be `NULL` is exactly what I want, since a user's first purchase has no "previous" to compare against.
- **I over-engineered the start.** My first instinct (`COUNT(*) > 1` + `DENSE_RANK`) was a whole abandoned branch (~12 minutes) that never even addressed "within 7 days." The problem is "gap to previous event," and the moment I reached for `LAG` it fell out in a few lines.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context.

### 8.1 Correctness
- **Score**: 3/5
- Comment: Your PostgreSQL is correct — `created_at - prev_purchase` between 1 and 7, `SELECT DISTINCT user_id`, all sound. But this axis grades the deliverable, and two of the three engines I track (4.2 pandas, 4.3 PySpark) are blank. One verified language out of three is "passes the screen, not the bar." Fix: port the accepted SQL to pandas (`groupby('user_id')['created_at'].shift(1)`) and PySpark (`lag().over(window)`) and confirm both pass before calling this solved.

### 8.2 Performance & Efficiency
- **Score**: 4/5
- Comment: The shape is optimal — a single `LAG` partition-and-sort over `amazon_transactions`, no self-join, no correlated subquery. Your second CTE (`cte2` computing `diff`) is a no-op materialization step the planner inlines, so it costs nothing at runtime; the author folds the same expression into the `WHERE` for one less logical pass. Fix: collapse `cte2` and write `WHERE (created_at - prev_purchase) BETWEEN 1 AND 7` directly so the query is one CTE deep.

### 8.3 Readability
- **Score**: 3/5
- Comment: The two-CTE split reads fine and the `diff` alias is honest, but `cte` and `cte2` are non-names — in a review I cannot tell their roles without reading the bodies. The author's `ordered_tx` / `prev_tx_date` tell the story at a glance. Fix: rename to `ordered_tx` (or `lagged`) and `gaps`, and name the column `gap_days` not `diff`.

### 8.4 Edge Cases
- **Score**: 3/5
- Comment: The same-day exclusion (`diff >= 1`) and the upper bound are right, and you reason correctly in §5's note that `NULL >= 1` is unknown so first-purchase rows drop silently. But relying on three-valued logic to do unstated work is exactly what bites you under pressure; the author's explicit `prev_tx_date IS NOT NULL` documents intent and survives a refactor. You also never address ties — two purchases on the *same* timestamp give a 0-day gap, correctly excluded, but you never say so. Fix: add `AND prev_purchase IS NOT NULL` so the NULL handling is visible, not incidental.

### 8.5 Interview-Readiness
- **Score**: 2/5
- Comment: Your written narrative in §3 and §7 is genuinely strong — you name the two traps (same-day lower bound, DISTINCT) and diagnose your own `!= 0` → `>= 1` no-op honestly. That reflection is the redeeming signal. But interview-readiness is measured on the live solve, and the history is concerning: ~12 minutes on a `COUNT(*) > 1` + `DENSE_RANK` branch that never modeled "within 7 days," then two rejected submissions because you debugged the operator instead of inspecting output shape. A bar-raiser watching attempts #18–20 sees someone who shipped before checking for duplicate rows. Fix: before the first submit, state the assumption out loud — "the prompt wants a distinct list of user_ids, so I'll `SELECT DISTINCT`" — which would have skipped both failed submissions.

### 8.6 Verdict
The one thing to fix: recognize the pattern before you start typing. This is a textbook "gap to the previous event per entity" problem, and the instant you see "within N days of their previous purchase" the answer is `LAG` plus a range filter — yet you spent twelve minutes counting and ranking first. Senior candidates spend the first 60 seconds naming the pattern and the output contract (distinct ids, exclude same-day) *before* the first keystroke; that single habit erases the dead-end branch and both rejected submissions at once. Your post-hoc analysis is already senior-grade — pull that thinking to the front of the solve. And close the gap honestly: two of three engines here are unwritten, so this problem is one-third done, not done.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Previous row per group | `LAG(created_at) OVER (PARTITION BY user_id ORDER BY created_at)` | `df.sort_values(['user_id','created_at']).groupby('user_id')['created_at'].shift(1)` | `lag('created_at').over(Window.partitionBy('user_id').orderBy('created_at'))` |
| Days between two dates | `created_at - prev` (date minus date → int days) | `(df['created_at'] - df['prev_date']).dt.days` | `datediff(col('created_at'), col('prev_date'))` |
| Gap in `[1, 7]` (exclude same day) | `diff >= 1 AND diff <= 7` | `(d > 0) & (d <= 7)` | `(d > 0) & (d <= 7)` |
| Drop the first-row NULL | `WHERE prev IS NOT NULL` (or rely on `NULL`-fails-filter) | `df['days_diff'].notna()` | `col('days_diff').isNotNull()` |
| Distinct ids out | `SELECT DISTINCT user_id` | `['user_id'].drop_duplicates()` | `.select('user_id').distinct()` |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: "time since the previous event, per entity" → `LAG(time) OVER (PARTITION BY entity ORDER BY time)`, then a difference and a range filter. This is the canonical *gap-between-consecutive-events* shape; recognise it whenever the prompt says "within N days of their previous / last / prior X." The mirror trick (`LEAD`) gives the *next* event instead.

- **Reusable snippet (PostgreSQL, parameterized)**:
  ```sql
  WITH gaps AS (
    SELECT entity_id,
           event_date - LAG(event_date) OVER (PARTITION BY entity_id
                                              ORDER BY event_date) AS gap_days
    FROM events
  )
  SELECT DISTINCT entity_id
  FROM gaps
  WHERE gap_days BETWEEN 1 AND 7;   -- BETWEEN is inclusive; >=1 excludes same-day
  ```

- **Weakness I noticed in myself**:
  1. **I reach for counting/ranking before the right window function.** My instinct was `COUNT(*) > 1` + `DENSE_RANK`, which doesn't model "gap to previous event" at all. Drill: when the question is about *the interval between consecutive rows*, the tool is `LAG`/`LEAD`, not `RANK`/`COUNT`.
  2. **I debug the filter when the real bug is the output shape.** I flipped `!= 0` to `>= 1` (a no-op) when the actual problem was the missing `DISTINCT`. Reflex to build: on a rejected "list of X" submission, first check for **duplicate rows / wrong column set / ordering**, then check the logic.
  3. **Window-alias-in-WHERE keeps biting me.** Two attempts lost to `WHERE purchase_order = 1` on a freshly-defined window column. Memorise: window functions run *after* `WHERE`; filter them in an outer query.

- **Drill for tomorrow morning**:
  1. Re-solve 10553 cold in PostgreSQL using `BETWEEN 1 AND 7` and an explicit `prev IS NOT NULL` guard, in a single CTE (fold the `diff` into the `WHERE` like the author). Time-box 5 minutes.
  2. Port it to pandas (`groupby().shift(1)` + `.dt.days` + boolean mask + `drop_duplicates`) and PySpark (`lag().over(window)` + `datediff` + `.distinct()`), since I have zero non-SQL attempts here.

## 11. Related Notes

- **Same category (Window Functions — "gap between consecutive events")**: any problem that asks for "consecutive days," "time since last purchase/login," "days between orders," or "repeat within N days." This is the prototypical `LAG`-difference exercise; pair it with `LEAD`-based "time until next event" problems and with consecutive-streak problems (where you'd combine `LAG` with a running comparison).
- **Adjacent skills exercised**: filtering on a value derived from a window function — which *forces* the CTE/subquery wrap (you cannot put `LAG(...)` in the same `WHERE`), the same structural lesson as any "rank then filter top-N" problem.
- **Reference reading**:
  - PostgreSQL window functions (`LAG`/`LEAD`, frames): https://www.postgresql.org/docs/current/functions-window.html
  - PostgreSQL date/time arithmetic (why `date - date` yields an integer): https://www.postgresql.org/docs/current/functions-datetime.html
  - pandas `groupby().shift()`: https://pandas.pydata.org/docs/reference/api/pandas.core.groupby.DataFrameGroupBy.shift.html
  - PySpark `lag` + `datediff`: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html

---

*Generated 2026-06-30 by Ina (problem solving) + Claude (author solution fetch via MCP, submission history analysis, FAANG reviewer subagent).*
