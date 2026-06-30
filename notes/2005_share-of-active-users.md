# 2005 — Share of Active Users

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 2005 |
| Category | Aggregation |
| Difficulty | Medium |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/2005-share-of-active-users?code_type=1) · [Python](https://platform.stratascratch.com/coding/2005-share-of-active-users?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/2005-share-of-active-users?code_type=6) |
| Solved on | 2025-06-19 |
| Tables | `fb_active_users(country text, name text, status text, user_id bigint)` |

## 2. Problem Restatement (in my words)

`fb_active_users` is one flat row-per-user table. I have to find what fraction of *all* users satisfy **two conditions at once** — `country = 'USA'` **and** `status = 'open'` — and return that fraction as a percentage of the whole table. So the numerator is "rows where both conditions hold" and the denominator is "every row," and the answer is a single number (one row, one column). The catch is purely arithmetic: it's a ratio of a conditional count to a total count, and if I do integer division or forget to multiply by 100, the percentage comes out wrong (0, or a fraction, instead of a real percent).

## 3. My Approach

- **First instinct (SQL)**: treat it as two separate counts. I started by just eyeballing the table (`SELECT *`), then wrote a standalone `SELECT COUNT(*) ... WHERE country = 'USA' AND status = 'open'` to get the numerator on its own. My mental model was "count the qualifying users, count the total, divide" — two queries, then somehow glue them together.
- **Where I got stuck**: stitching the two counts into one query. I made two distinct mistakes trying to do that:
  1. **`GROUP BY` + `HAVING` on a row-level condition.** I wrote `... GROUP BY user_id HAVING country = 'USA' AND status = 'open'`, expecting `HAVING` to filter rows. Postgres rejected it: `column "fb_active_users.country" must appear in the GROUP BY clause or be used in an aggregate function`. `HAVING` filters *groups* by aggregates, not individual rows by their raw column values — that's what `WHERE` is for.
  2. **A trailing alias after the CTE parenthesis.** I tried to give my CTE an alias by writing `with cond_user as (... ) c` — putting a `c` right after the closing paren — then referencing `c.cond_users`. Postgres threw `syntax error at or near "c"` twice. You name a CTE in the `WITH cond_user AS (...)` part; you don't tack a table alias on after the closing parenthesis like you would in a `FROM (subquery) c`. I also typo'd the column once (`c.cond_user` vs `c.cond_users`).
- **Final strategy**: abandon the two-CTE / two-count idea entirely and do it in **one pass with `SUM(CASE WHEN ... THEN 1 ELSE 0 END)`**. `SUM(CASE WHEN country = 'USA' AND status = 'open' THEN 1 ELSE 0 END)` is the conditional numerator, `COUNT(*)` is the denominator, and multiplying by `100.0` (a float literal) both scales to a percent and forces floating-point division so I don't get integer truncation. That collapsed the whole problem into a single `SELECT` with no `WHERE`, no `GROUP BY`, no CTE — and it passed on the first submission.

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2025-06-19 08:20)

```sql
select sum(case
    when country = 'USA' and status = 'open' then 1
    else 0
    end) * 100.0 / count(*) as us_active_share
from fb_active_users;
```

> This is verbatim my accepted submission (attempt id 72410217, `is_correct = true`, `is_solution_submission = true`). It is character-for-character identical to the StrataScratch author SQL solution apart from lowercase keywords and indentation — see Section 6.

### 4.2 Python (pandas)

⚠️ Not attempted / no accepted submission in my history.

### 4.3 PySpark

⚠️ Not attempted / no accepted submission in my history.

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)`.
> Premium-only feature.

### 5.1 PostgreSQL

```sql
SELECT SUM(CASE
               WHEN country = 'USA'
                    AND status = 'open' THEN 1
               ELSE 0
           END) * 100.0 / COUNT(*) AS us_active_share
FROM fb_active_users;
```

### 5.2 Python

```python
import pandas as pd

us_active_count = fb_active_users[
    (fb_active_users['country'] == 'USA') & 
    (fb_active_users['status'] == 'open')
].shape[0]

total_count = fb_active_users.shape[0]

result = pd.DataFrame({
    'us_active_share': [us_active_count * 100.0 / total_count]
})
```

### 5.3 PySpark

```python
from pyspark.sql import functions as F

result = fb_active_users.select(
    (F.sum(F.when((F.col('country') == 'USA') & (F.col('status') == 'open'), 1).otherwise(0)) * 100.0 / F.count('*'))
    .alias('us_active_share')
).toPandas()
```

> For reference, StrataScratch also ships MySQL, MSSQL, Oracle, R, and Polars
> solutions for this problem (the MySQL/MSSQL/Oracle ones are byte-identical to the
> PostgreSQL `SUM(CASE...)` above); the three engines above are the ones I track.

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| Core idiom | `SUM(CASE WHEN ... THEN 1 ELSE 0 END) * 100.0 / COUNT(*)` | Identical `SUM(CASE WHEN ... THEN 1 ELSE 0 END) * 100.0 / COUNT(*)` | None — same conditional-aggregation pattern |
| Float coercion | `* 100.0` (decimal literal) forces float division | `* 100.0` (decimal literal) forces float division | Same; both avoid integer-division truncation |
| Keyword casing | lowercase (`select`, `sum`, `case`) | UPPERCASE (`SELECT`, `SUM`, `CASE`) | Cosmetic only; Postgres is case-insensitive on keywords |
| Column alias | `us_active_share` | `us_active_share` | Identical output column name |
| Output shape | one row, one column | one row, one column | Identical |

**Key insight in one line:** for "what percentage of rows satisfy condition C," the canonical one-pass idiom is `SUM(CASE WHEN C THEN 1 ELSE 0 END) * 100.0 / COUNT(*)` — I landed on exactly the author's answer, so the only gap is that I burned six failed attempts wandering through `GROUP BY`/`HAVING`/CTE before reaching for it.

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=2005)` — **9 attempts total**, all PostgreSQL (code_type 1), all on 2025-06-19. Two are submissions (a run + the graded submit at the end); the rest are runs.

| # | Time (UTC) | Language | Status | What happened |
|---|---|---|---|---|
| 1 | 08:13 | SQL | run | `SELECT * FROM fb_active_users` — inspect the table, see `country` / `status` / `user_id` |
| 2 | 08:14 | SQL | run | `SELECT count(*) ... WHERE country = 'USA' AND status = 'open'` — get the numerator alone |
| 3 | 08:15 | SQL | ❌ run | `... GROUP BY user_id HAVING country = 'USA' AND status = 'open'` → `column "country" must appear in GROUP BY` (used `HAVING` to filter rows instead of `WHERE`) |
| 4 | 08:16 | SQL | run | Back to a clean `count(user_id) AS cond_users ... WHERE ...` — numerator only |
| 5 | 08:18 | SQL | ❌ run | First CTE try: `with cond_user as (... ) c select count(distinct *)/c.cond_user ...` → `syntax error at or near "c"` (trailing alias after CTE paren; also `count(distinct *)` is invalid) |
| 6 | 08:18 | SQL | run | Re-ran the standalone `count(user_id) AS cond_users ... WHERE ...` to confirm the numerator |
| 7 | 08:18 | SQL | ❌ run | Same CTE shape, fixed column name to `c.cond_users`, **but the `) c` trailing alias is still there** → `syntax error at or near "c"` again |
| 8 | 08:20 | SQL | run | **Switched to `SUM(CASE WHEN ... THEN 1 ELSE 0 END) * 100.0 / COUNT(*)`** — first clean run of the conditional-aggregation query |
| 9 | 08:20 | SQL | ✅ **submitted** | Same `SUM(CASE...)` query → **accepted** (`is_correct = true`) |

What the failed attempts taught me:
- **`HAVING` filters groups, not rows.** My `GROUP BY user_id HAVING country = 'USA'` failed because `HAVING` only sees aggregates or grouped columns. Row-level predicates on raw columns belong in `WHERE`. (Attempt 3.)
- **You name a CTE inside `WITH name AS (...)`; you don't put a table alias after the closing `)`.** I wrote `with cond_user as (...) c` twice and got `syntax error at or near "c"` both times. The `(subquery) alias` habit is for inline `FROM` subqueries, not for CTE definitions. (Attempts 5 and 7.)
- **`COUNT(DISTINCT *)` is not valid SQL** — `DISTINCT` needs explicit column(s). It slipped into my CTE attempts and would have failed even if the alias syntax were fixed.
- **The right move was to stop splitting numerator and denominator into separate queries.** The whole "two counts then divide" framing is what dragged me into CTEs. One `SUM(CASE...) / COUNT(*)` pass does it with no `WHERE`, no `GROUP BY`, no CTE — and the moment I switched, it passed on the next submit.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context.

### 8.1 Correctness
- **Score**: 5/5
- Comment: Your accepted PostgreSQL is exactly right — `SUM(CASE WHEN country = 'USA' AND status = 'open' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)` returns the correct single-row percentage, byte-identical to the author idiom. The `100.0` decimal literal correctly forces float division so you avoid integer truncation to 0. Note this 5 covers PostgreSQL only; pandas and PySpark were not attempted, so there is no cross-engine correctness signal to score.

### 8.2 Performance & Efficiency
- **Score**: 5/5
- Comment: One pass, one aggregate scan, no join, no subquery, no `GROUP BY`. `SUM(CASE...)` over `COUNT(*)` is the minimum work the engine can do for a whole-table ratio — a single sequential scan computing both the conditional sum and the total in the same aggregation. Nothing to tune here; a sort or CTE would only have added cost.

### 8.3 Readability
- **Score**: 5/5
- Comment: The `CASE` is cleanly indented across the `when`/`else`/`end` lines, the alias `us_active_share` is descriptive, and lowercase keywords are fine and consistent. The only cosmetic delta from the author is casing, which Postgres ignores. No CTE is warranted at this size, so its absence is correct, not a gap.

### 8.4 Edge Cases
- **Score**: 4/5
- Comment: Empty-table behavior is the live edge here: if `fb_active_users` had zero rows, `* 100.0 / COUNT(*)` is division by zero and errors out. The grader's data hides this, and you never flagged it. Concrete fix: in an interview, say "if the table can be empty I'd guard the denominator with `NULLIF(COUNT(*), 0)`." You also rely on exact-match equality (`'USA'`, `'open'`) — fine per the prompt's enum-like columns, but worth naming that you're assuming no `'usa'`/`'Open'` casing variants.

### 8.5 Interview-Readiness
- **Score**: 2/5
- Comment: The final artifact is clean, but readiness is judged on the path, and Section 7 shows six failed attempts over seven minutes — `GROUP BY user_id HAVING country = 'USA'` (HAVING is for aggregates, not row predicates — that is `WHERE`), then the `WITH cond_user AS (...) c` trailing-alias syntax error twice, plus an invalid `COUNT(DISTINCT *)`. In a live loop that thrash reads as not recognizing "share of rows" as the canonical conditional-aggregation pattern. Concrete fix: when a prompt asks "what percent of rows satisfy C," reach straight for `SUM(CASE WHEN C THEN 1 ELSE 0 END) / COUNT(*)` and say so out loud — no `SELECT *` warm-up, no two-query numerator/denominator detour. You also solved silently in one engine; verbalizing one assumption (empty table, case sensitivity) and naming the pandas/PySpark equivalents would lift this materially.

### 8.6 Verdict
Your destination is senior-grade — the accepted SQL is the exact answer a strong analyst submits, with correct float coercion and minimal scans. The gap is entirely navigation: you wandered through `GROUP BY`, `HAVING`, and two broken CTE attempts before landing on the one-pass idiom that the problem was always asking for. The one thing that moves you up on similar problems: build a reflex that maps "percentage / share of rows meeting a condition, no per-group breakdown" directly to `SUM(CASE WHEN ... THEN 1 ELSE 0 END) * 100.0 / COUNT(*)` in a single `SELECT`, and narrate that recognition as you write it. Close the loop by porting this to pandas and PySpark, since right now you have a single-engine answer to a problem the routine wants in three.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Conditional count (numerator) | `SUM(CASE WHEN c1 AND c2 THEN 1 ELSE 0 END)` | boolean-mask then `.shape[0]`: `df[(df['c1']==x) & (df['c2']==y)].shape[0]` | `F.sum(F.when((F.col('c1')==x) & (F.col('c2')==y), 1).otherwise(0))` |
| Total count (denominator) | `COUNT(*)` | `df.shape[0]` | `F.count('*')` |
| Boolean AND of conditions | `AND` | `&` (not `and`), each side parenthesized | `&` (not `and`), each side parenthesized |
| Force float division / percent | `* 100.0` (decimal literal) | Python `/` is already float; `* 100.0` to scale | `* 100.0`; division on float column stays float |
| Return a single scalar as a 1-row frame | `SELECT <expr> AS alias` | `pd.DataFrame({'alias': [value]})` | `.select((<expr>).alias('alias')).toPandas()` |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: *"share / percentage of rows meeting a condition"* → `SUM(CASE WHEN condition THEN 1 ELSE 0 END) * 100.0 / COUNT(*)` in one pass. Don't split numerator and denominator into two queries and try to divide them — the conditional `SUM` and the unconditional `COUNT(*)` live happily in the same `SELECT`. The `100.0` (a decimal literal) does double duty: it scales the ratio to a percent *and* makes the division floating-point so you don't get integer truncation to 0.

- **Reusable snippet (PostgreSQL, parameterized)**:
  ```sql
  SELECT SUM(CASE WHEN <condition> THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct
  FROM <table>;
  -- equivalently: AVG(CASE WHEN <condition> THEN 1.0 ELSE 0 END) * 100
  ```

- **Weakness I noticed in myself**:
  1. **I reach for `GROUP BY` / CTEs reflexively, even when no grouping is needed.** This problem has no groups at all — it's a whole-table ratio — yet I tried `GROUP BY user_id` and built two CTE attempts before the obvious single-pass `SUM(CASE...)`. Reflex to build: *"percentage of rows" with no per-group breakdown = one `SELECT`, conditional `SUM` over `COUNT(*)`, no `GROUP BY`.*
  2. **CTE syntax slip.** `WITH name AS (...) alias` is not a thing; the alias goes nowhere after the paren. I made the same `syntax error at or near "c"` twice. Drill the shape: `WITH cte AS (...) SELECT ... FROM cte`.
  3. **`HAVING` vs `WHERE` confusion.** Row predicates → `WHERE`; aggregate predicates → `HAVING`. I used `HAVING` on a raw column and got bitten.

- **Drill for tomorrow morning** (before any new problems):
  1. Re-solve 2005 cold in PostgreSQL straight to the `SUM(CASE...)/COUNT(*)` form — no `SELECT *` exploration, no CTE detour. Time-box 3 minutes.
  2. Port it to pandas (`boolean mask → .shape[0]` over `df.shape[0]`, wrap in `pd.DataFrame`) since I have zero pandas/PySpark attempts on this one. That's the real gap to close.

## 11. Related Notes

- **Same category (Aggregation — "ratio / share of rows meeting a condition")**: other Aggregation notes in our queue that compute a percentage or rate as `conditional count / total count`. This is the prototypical `SUM(CASE WHEN ...)` percentage problem; pair it with any "what fraction / what percent of X" exercise.
- **Adjacent skills exercised**: conditional aggregation (`CASE` inside an aggregate) and float-coercion for percentages — the same `SUM(CASE...)` trick generalizes to "count of rows matching a flag" and to pivot-style conditional counts.
- **Reference reading**:
  - PostgreSQL `CASE` expressions: https://www.postgresql.org/docs/current/functions-conditional.html
  - `GROUP BY` / `HAVING` vs `WHERE` (why row predicates go in `WHERE`): https://www.postgresql.org/docs/current/sql-select.html#SQL-HAVING
  - pandas boolean indexing: https://pandas.pydata.org/docs/user_guide/indexing.html#boolean-indexing

---

*Generated 2026-06-30 by Ina (problem solving) + Claude (author solution fetch via MCP, submission history analysis, FAANG reviewer subagent).*
