# 10130 — Number of Inspections per Risk Category by Inspection Type

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 10130 |
| Category | Aggregation |
| Difficulty | Medium |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/10130-find-the-number-of-inspections-for-each-risk-category-by-inspection-type?code_type=1) · [Python](https://platform.stratascratch.com/coding/10130-find-the-number-of-inspections-for-each-risk-category-by-inspection-type?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/10130-find-the-number-of-inspections-for-each-risk-category-by-inspection-type?code_type=6) |
| Solved on | 2026-06-13 |
| Tables | `sf_restaurant_health_violations(business_address text, business_city text, business_id bigint, business_latitude double precision, business_location text, business_longitude double precision, business_name text, business_phone_number double precision, business_postal_code double precision, business_state text, inspection_date date, inspection_id text, inspection_score double precision, inspection_type text, risk_category text, violation_description text, violation_id text)` |

## 2. Problem Restatement (in my words)

> Each row in `sf_restaurant_health_violations` is one health-inspection/violation record carrying an `inspection_type` (e.g. "Routine - Unscheduled") and a `risk_category` (`Low Risk`, `Moderate Risk`, `High Risk`, or NULL when no risk was recorded). For every `inspection_type`, count how many records fall into each risk bucket — and treat the NULL/no-risk rows as their own fourth bucket, not as something to drop. The output must be **pivoted**: one row per inspection type, with a separate column for each risk bucket plus a `total_inspections` column (the row count for that type). Order by the total number of inspections per type, descending. The two traps are (1) the result is *wide*, not long — you can't just `GROUP BY inspection_type, risk_category` and stop, you have to turn each risk value into its own column; and (2) NULL is a real bucket, so `WHERE risk_category IS NOT NULL` silently throws away rows you were asked to count.

## 3. My Approach

- **First instinct (SQL)**: I read this as a window-function problem. My opening attempts were `SELECT DISTINCT risk_category, inspection_type, COUNT(inspection_id) OVER (PARTITION BY risk_category, inspection_type) AS n_inspection ... WHERE risk_category IS NOT NULL ORDER BY n_inspection DESC`. That produces a **long** table — one row per (type, risk) pair — and it filters the NULLs out entirely. I even **submitted this shape twice** (10:50 and 10:52) and the grader rejected both, because the prompt wanted a pivoted output with NULL as its own column.
- **Where I got stuck**:
  1. **Output shape.** It took two rejected submissions before I re-read the prompt and registered "the output should be pivoted, meaning each risk category + total number should be a separate column." Once I saw that, I dropped the window function and switched to **conditional aggregation** (`SUM(CASE WHEN risk_category = '...' THEN 1 ELSE 0 END)`), one CASE per bucket.
  2. **One stubborn typo.** I transposed `risk_category` → `risk_cateogry` in the *Low Risk* branch and then could not see it. Postgres threw `column "risk_cateogry" does not exist` (with a HINT pointing straight at `risk_category`) on roughly **fifteen** runs. I fixed the other branches, re-ran, and kept missing this one specific letter-swap — even submitting the typo'd version once (11:11) as a solution, which failed.
  3. **`COUNT(CASE)` vs `SUM(CASE)`.** My earliest pivot attempt used `COUNT(CASE WHEN ... THEN 1 ELSE 0 END)`. That's wrong: `COUNT` counts every non-NULL value, and `ELSE 0` makes the expression non-NULL on *every* row, so each bucket would have returned the same number (the group size). I switched to `SUM(CASE ... THEN 1 ELSE 0 END)` so only the matching rows contribute 1.
  4. **SQL syntax slips:** `count() OVER (...)` (needs `count(*)`), `CASE ... THEN 1 ELSE 0` with no `END` (`syntax error at or near ")"`), `==` instead of `=`, `"Low Risk"` double-quoted (read as an identifier), and a trailing-space `'Low Risk '`.
- **Final strategy**: One single-pass `GROUP BY inspection_type`. Four `SUM(CASE...)` columns — `no_risk_results` for `IS NULL`, then `Low Risk` / `Moderate Risk` / `High Risk` — plus `COUNT(*) AS total_inspections`, ordered by `total_inspections`. The instant the `risk_cateogry` typo was fixed, the query passed.

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-06-13 11:14)

```sql
SELECT
    inspection_type,
    SUM(
        CASE
            WHEN risk_category IS NULL THEN 1 ELSE 0
        END
    ) AS no_risk_results,
    SUM(
        CASE
            WHEN risk_category = 'Low Risk' THEN 1 ELSE 0
        END
    ) AS low_risk_results,
    SUM(
        CASE
            WHEN risk_category = 'Moderate Risk' THEN 1 ELSE 0
        END
    ) AS medium_risk_results,
    SUM(
        CASE
            WHEN risk_category = 'High Risk' THEN 1 ELSE 0
        END
    ) AS high_risk_results,
    COUNT(*) AS total_inspections
FROM sf_restaurant_health_violations
GROUP BY
    inspection_type
ORDER BY total_inspections
```

> Note: my accepted query's final line is `ORDER BY total_inspections` with **no `DESC`** — the prompt asks for descending order, and the grader still accepted it (it apparently doesn't enforce sort direction here). The author's version explicitly writes `ORDER BY total_inspections DESC`, which is what I *should* have submitted. See Sections 6 and 8.

### 4.2 Python (pandas)

⚠️ Not attempted / no accepted submission in my history.

### 4.3 PySpark

⚠️ Not attempted / no accepted submission in my history.

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)`.
> Premium-only feature.

### 5.1 PostgreSQL

```sql
SELECT 
    inspection_type,
    
    sum(CASE 
        WHEN risk_category IS NULL THEN 1 ELSE 0
        END
    ) AS no_risk_results,
    
    sum(CASE 
        WHEN risk_category = 'Low Risk' THEN 1 ELSE 0
        END
    ) AS low_risk_results,
    
    
    sum(CASE 
        WHEN risk_category = 'Moderate Risk' THEN 1 ELSE 0
        END
    ) AS medium_risk_results,
    
    sum(CASE 
        WHEN risk_category = 'High Risk' THEN 1 ELSE 0
        END
    ) AS high_risk_results,
    
    count(*) AS total_inspections
FROM sf_restaurant_health_violations
GROUP BY 
    inspection_type
ORDER BY
    total_inspections DESC
```

### 5.2 Python (pandas)

```python
import pandas as pd
import numpy as np

sf_restaurant_health_violations['no_risk_results'] = (sf_restaurant_health_violations.risk_category.isnull()).astype(int)
sf_restaurant_health_violations['low_risk_results'] = (sf_restaurant_health_violations.risk_category == 'Low Risk').astype(int)
sf_restaurant_health_violations['medium_risk_results'] = (sf_restaurant_health_violations.risk_category == 'Moderate Risk').astype(int)
sf_restaurant_health_violations['high_risk_results'] = (sf_restaurant_health_violations.risk_category == 'High Risk').astype(int)
sf_restaurant_health_violations = sf_restaurant_health_violations.groupby('inspection_type').agg({'no_risk_results':'sum', 'low_risk_results':'sum','medium_risk_results':'sum','high_risk_results':'sum'}).reset_index()
sf_restaurant_health_violations['total_inspections'] = sf_restaurant_health_violations.loc[:, 'no_risk_results':'high_risk_results'].sum(axis = 1)
result = sf_restaurant_health_violations.sort_values('total_inspections', ascending = False)
```

### 5.3 PySpark

```python
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

sf_restaurant_health_violations = sf_restaurant_health_violations.withColumn('no_risk_results', F.when(F.col('risk_category').isNull(), 1).otherwise(0))
sf_restaurant_health_violations = sf_restaurant_health_violations.withColumn('low_risk_results', F.when(F.col('risk_category') == 'Low Risk', 1).otherwise(0))
sf_restaurant_health_violations = sf_restaurant_health_violations.withColumn('medium_risk_results', F.when(F.col('risk_category') == 'Moderate Risk', 1).otherwise(0))
sf_restaurant_health_violations = sf_restaurant_health_violations.withColumn('high_risk_results', F.when(F.col('risk_category') == 'High Risk', 1).otherwise(0))

sf_restaurant_health_violations = sf_restaurant_health_violations.groupby('inspection_type').agg(F.sum('no_risk_results').alias('no_risk_results'), F.sum('low_risk_results').alias('low_risk_results'), F.sum('medium_risk_results').alias('medium_risk_results'), F.sum('high_risk_results').alias('high_risk_results'))
sf_restaurant_health_violations = sf_restaurant_health_violations.withColumn('total_inspections', F.col('no_risk_results') + F.col('low_risk_results') + F.col('medium_risk_results') + F.col('high_risk_results'))

result = sf_restaurant_health_violations.orderBy(F.desc('total_inspections')).toPandas()
result
```

> For reference, StrataScratch also ships MySQL, MSSQL, Oracle, R, and Polars solutions for this problem; the three above are the canonical engines I track. (The R and Polars author solutions group-then-`pivot_wider` / `sum_horizontal` instead of conditional aggregation, but land on the same wide shape.)

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| Pivot mechanism | `SUM(CASE WHEN risk_category = '...' THEN 1 ELSE 0 END)` per bucket | Identical — `sum(CASE WHEN ... THEN 1 ELSE 0 END)` per bucket | Same idiom; this is the canonical "pivot in SQL without a PIVOT operator" pattern |
| NULL bucket | `WHEN risk_category IS NULL THEN 1` | `WHEN risk_category IS NULL THEN 1` | Same — both correctly treat NULL as its own column instead of dropping it |
| Total | `COUNT(*)` | `count(*)` | Same. (pandas/PySpark authors instead *add the four bucket columns*; `COUNT(*)` is simpler and identical here) |
| Sort direction | `ORDER BY total_inspections` (**no `DESC`**) | `ORDER BY total_inspections DESC` | **Real miss on my side.** Prompt says descending; grader didn't enforce it, but the author's `DESC` is the correct answer to the spec |
| Column alias for Moderate | `medium_risk_results` ← `'Moderate Risk'` | `medium_risk_results` ← `'Moderate Risk'` | Same (mild naming drift — the data says "Moderate", the column says "medium" — but it matches the author/grader expectation) |
| Output column order | `inspection_type, no_risk, low, medium, high, total` | Identical | Same shape, accepted |

**Key insight in one line:** the canonical idiom for "pivot a categorical column into one count-column each" is **conditional aggregation** — `SUM(CASE WHEN cat = 'x' THEN 1 ELSE 0 END)` per category, all inside a single `GROUP BY inspection_type` — and NULL is just another `WHEN ... IS NULL` branch, not a row to filter out. My SQL matched the author almost token-for-token; the only substantive gap was dropping `DESC`.

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=10130)` — **38 attempts total**, all PostgreSQL (code_type 1), all on 2026-06-13. Highlights:

| # | Time (UTC) | Language | Status | What happened |
|---|---|---|---|---|
| 1 | 10:29 | SQL | run | `SELECT * FROM sf_restaurant_health_violations` — inspect the table |
| 2 | 10:46 | SQL | run | `SELECT DISTINCT risk_category` — discover the bucket strings |
| 3 | 10:48 | SQL | ❌ run | `count() OVER (...)` — `count(*) must be used to call a parameterless aggregate function` |
| 4 | 10:49 | SQL | run | Window approach: `count(inspection_id) OVER (PARTITION BY risk_category, inspection_type)` |
| 5 | 10:50:13 | SQL | run | Same window, added `WHERE risk_category IS NOT NULL` (drops the NULL bucket) |
| 6 | 10:50:41 | SQL | ❌ **submitted** | Window/long shape, `ORDER BY n_inspection DESC` → **incorrect** (output not pivoted; NULL dropped) |
| 7 | 10:52:21 | SQL | run | Re-ran the same long-shape query (added `inspection_type` to SELECT) |
| 8 | 10:52:29 | SQL | ❌ **submitted** | Window/long shape again → **incorrect** (still not pivoted) |
| 9 | 10:57–11:03 | SQL | run | More `SELECT DISTINCT risk_category` probes; pivoted toward `SUM(CASE...)` |
| 10 | 11:01:18 | SQL | ❌ run | `SUM(CASE WHEN ... THEN 1 ELSE 0)` with **no `END`** → `syntax error at or near ")"` |
| 11 | 11:05 | SQL | ❌ run | `risk_cateogry == 'Low Risk'` and `= "Low Risk"` — `==`/double-quote slips, plus the `risk_cateogry` typo |
| 12–18 | 11:05–11:13 | SQL | ❌ run | ~7 runs all failing on `column "risk_cateogry" does not exist` (Low Risk branch typo); also `COUNT(CASE...)` before switching to `SUM`, and a `'Low Risk '` trailing-space variant |
| 19 | 11:11:53 | SQL | ❌ **submitted** | Conditional-aggregation version but **still typo'd** `risk_cateogry` → **incorrect** |
| 20 | 11:14:44 | SQL | run | Typo finally fixed — first clean run of the correct query, `order by total_inspections` |
| 21 | 11:14:48 | SQL | ✅ **submitted** | Same correct query → **accepted** (`ORDER BY total_inspections`, no `DESC`) |

What the failed attempts taught me:
- **Read the output-shape requirement before writing a line.** I burned the first ~22 minutes and **two rejected submissions** building a window-function "long" table when the prompt explicitly said *pivoted, one column per risk category*. "Pivot a category into columns" is a `SUM(CASE...)` job, not `COUNT() OVER (PARTITION BY ...)`. When a prompt describes the *columns* of the output, that's a pivot signal.
- **NULL was a required bucket, and my reflex `WHERE risk_category IS NOT NULL` deleted it.** When a prompt says "records with no value belong to a separate category," do **not** filter NULLs — give them their own `WHEN ... IS NULL THEN 1` branch.
- **`SUM(CASE…)`, not `COUNT(CASE…)`, for conditional counts.** `COUNT` counts every non-NULL, and `ELSE 0` is non-NULL on every row, so `COUNT(CASE…)` returns the group size for *every* bucket. `SUM` adds the 1s only.
- **A one-letter transposition (`risk_cateogry`) cost ~15 runs and one bad submission.** Postgres even printed the fix in its HINT. When the same `column "..." does not exist` error survives several edits, stop re-running and *diff the failing token against the schema character-by-character* — don't trust the eye.
- **I dropped `DESC` and got lucky.** The grader didn't enforce sort direction, but the spec asked for descending. Don't rely on a lenient grader — encode every clause of the prompt.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context.

### 8.1 Correctness
- **Score**: 3/5
- Comment: The PostgreSQL is functionally right — correct NULL branch, `SUM(CASE...)` over `COUNT`, clean `COUNT(*)` total — but two facts pull it off a 4. First, your final `ORDER BY total_inspections` drops the `DESC` the spec explicitly required (Section 4.1 line 59); the grader's leniency masked it, but against the spec it is wrong. Second, only one of three required engines was delivered — pandas and PySpark are both "⚠️ Not attempted" (4.2, 4.3), so two-thirds of the deliverable is unverified. Fix: append `DESC`, and at minimum write the pandas/PySpark pivots even untested, since correctness across the full deliverable set is what's being scored.

### 8.2 Performance & Efficiency
- **Score**: 5/5
- Comment: Optimal shape. The four `SUM(CASE...)` branches plus `COUNT(*)` fold into one single-pass `GROUP BY inspection_type` — no self-joins, no correlated subqueries, no per-row `COUNT() OVER (PARTITION BY ...)` window overhead like your rejected attempts #6/#8. Plan is identical to the author reference and is exactly right for this row-per-violation table.

### 8.3 Readability
- **Score**: 4/5
- Comment: Vertical `CASE` blocks and consistent indentation scan cleanly. The one blemish: `medium_risk_results` (line 49) aliases a `'Moderate Risk'` branch, so the column name silently drifts from the literal it counts. You flagged this yourself in Section 6, which is good, but a reviewer reading the column list cold can't tell `medium` maps to `Moderate`. Fix: rename to `moderate_risk_results` so the alias mirrors the source token.

### 8.4 Edge Cases
- **Score**: 4/5
- Comment: Strong handling of the two traps this problem sets: `WHEN risk_category IS NULL` (line 37) keeps the NULL bucket instead of a reflexive `WHERE ... IS NOT NULL` drop, and `SUM` instead of `COUNT(CASE...)` avoids counting the `ELSE 0` rows. What's missing is exhaustiveness proof: you ran `SELECT DISTINCT risk_category` (attempts #2, #9), but the four hard-coded literals assume that probe is the complete domain — a `'Low risk'` casing variant or trailing-space value would fall into no bucket and silently make the four columns under-sum the `COUNT(*)` total. Fix: add a reconciliation check that the four bucket columns sum to `total_inspections`, or a fifth `ELSE`-catch bucket to surface stragglers.

### 8.5 Interview-Readiness
- **Score**: 2/5
- Comment: The destination logic would whiteboard well, but three signals a FAANG loop reads negatively are all present. (1) The path was noisy and un-narrated — 38 attempts and three rejected submissions (#6, #8 long/un-pivoted; #19 with `risk_cateogry` still typo'd, per Section 7), with no verbalized assumption ("I'll treat NULL as its own bucket", "I'll confirm the risk domain is exhaustive") anywhere in Section 3. (2) You submitted #19 a full minute before the clean run, i.e. before proofreading the column reference — pressing "submit" against an unsatisfied spec is the exact behavior that loses a loop. (3) Only PostgreSQL was attempted; a panel that asks "now do it in pandas" gets nothing. Fix: before any submission, state your assumptions aloud and run the prompt's clauses (pivot shape, NULL bucket, sort direction) as an explicit checklist against your output.

### 8.6 Verdict
Your final SQL is senior-correct — single-pass conditional aggregation with NULL as its own `WHEN` branch and `SUM` over `COUNT` is precisely the answer, and you got subtleties many candidates miss. The gap holding this back is not SQL skill, it is verification discipline plus deliverable breadth: you shipped three submissions against a spec you hadn't fully met (long shape, the `risk_cateogry` transposition, the missing `DESC`), and you stopped at one of three required engines. The single highest-leverage change is to treat "submit" as an irreversible commit: before pressing it, narrate your assumptions, diff every column token against the schema, and tick off each prompt clause — output shape, NULL bucket, and sort direction — against what your query actually returns. Pair that with always porting the solved logic to pandas and PySpark while it is fresh, and your readiness on Medium aggregation problems rises sharply with no change to the SQL itself.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Conditional count / pivot a category into a column | `SUM(CASE WHEN cat = 'x' THEN 1 ELSE 0 END)` | `(df.cat == 'x').astype(int)` then `groupby(...).agg('sum')` | `F.sum(F.when(F.col('cat') == 'x', 1).otherwise(0))` |
| Count the NULL bucket | `SUM(CASE WHEN cat IS NULL THEN 1 ELSE 0 END)` | `df.cat.isnull().astype(int)` | `F.when(F.col('cat').isNull(), 1).otherwise(0)` |
| Conditional count operator | `SUM(CASE…)` — **not** `COUNT(CASE…)` | `.astype(int).sum()` | `F.sum(F.when(...))` |
| Row count per group | `COUNT(*)` | `df.loc[:, 'first':'last'].sum(axis=1)` (sum the buckets) | add the bucket columns: `F.col('a') + F.col('b') + ...` |
| Group + aggregate | `GROUP BY inspection_type` | `.groupby('inspection_type').agg({...:'sum'})` | `.groupby('inspection_type').agg(F.sum(...))` |
| Order descending | `ORDER BY total_inspections DESC` | `.sort_values('total_inspections', ascending=False)` | `.orderBy(F.desc('total_inspections'))` |
| String literal / equality | single quotes `'Low Risk'`, `=` (not `==`, not `"Low Risk"`) | `'Low Risk'` / `"Low Risk"` both fine, `==` | `'Low Risk'`, `==` on a Column |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: **conditional aggregation = SQL pivot.** When the output needs one column per value of a categorical field, you don't `GROUP BY` that field — you `GROUP BY` the *other* dimension and turn each category into its own `SUM(CASE WHEN cat = 'value' THEN 1 ELSE 0 END)` column. NULL is just one more branch (`WHEN cat IS NULL THEN 1`), never a row to filter. The total is `COUNT(*)`.

- **Reusable snippet (PostgreSQL, parameterized)**:
  ```sql
  SELECT group_dim,
         SUM(CASE WHEN cat IS NULL          THEN 1 ELSE 0 END) AS cat_null,
         SUM(CASE WHEN cat = 'A'            THEN 1 ELSE 0 END) AS cat_a,
         SUM(CASE WHEN cat = 'B'            THEN 1 ELSE 0 END) AS cat_b,
         COUNT(*)                                              AS total
  FROM   t
  GROUP  BY group_dim
  ORDER  BY total DESC;   -- don't forget DESC if the prompt says descending
  ```

- **Weakness I noticed in myself**:
  1. **I reach for window functions when a plain `GROUP BY` aggregate is the answer.** My first two submissions were `COUNT() OVER (PARTITION BY ...)` producing a long table — the prompt wanted a *pivot*. Reflex to build: when the prompt names the **columns** of the output (one per category), that's conditional aggregation, not a window function.
  2. **I don't read the output-shape requirement carefully enough before coding.** "Pivoted, each category a separate column" and "no-risk is a separate category" were both stated outright; I missed both on the first pass and paid with rejected submissions.
  3. **I re-run through typos instead of diffing the token.** `risk_cateogry` survived ~15 runs even though Postgres printed the correct name in its HINT. When the same "column does not exist" error persists after edits, compare the token to the schema letter-by-letter.
  4. **I trusted a lenient grader on sort order.** Dropping `DESC` passed by luck; encode every clause of the spec regardless.

- **Drill for tomorrow morning** (before any new problems):
  1. Re-solve 10130 cold in PostgreSQL from the conditional-aggregation pattern — no window function, NULL as its own branch, and `ORDER BY total_inspections DESC`. Time-box 6 minutes.
  2. Then port it to pandas (`(df.cat == ...).astype(int)` → `groupby().agg('sum')` → `sort_values(ascending=False)`) since I have zero pandas/PySpark attempts on this one. If I can't reproduce the pivot, that's the real gap.

## 11. Related Notes

- **Same category (Aggregation — "conditional aggregation / pivot")**: other Aggregation notes in our queue that hinge on `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` to turn a categorical column into per-value count columns, or that ask for "number of X per Y" broken out by a third dimension. This problem is the prototypical SQL-pivot-without-a-PIVOT-operator exercise; pair it with any "count by category, one column each" problem.
- **Adjacent skills exercised**: `GROUP BY` with a `COUNT(*)` total alongside conditional sums; NULL-as-its-own-bucket handling (`WHEN ... IS NULL`) — see other notes where NULL is a value to count rather than filter.
- **Reference reading**:
  - PostgreSQL `CASE` expression (the engine of conditional aggregation): https://www.postgresql.org/docs/current/functions-conditional.html
  - `GROUP BY` aggregate functions (`SUM`, `COUNT`): https://www.postgresql.org/docs/current/functions-aggregate.html
  - pandas pivot via indicator columns + `groupby`: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.groupby.html

---

*Generated 2026-06-30 by Ina (problem solving) + Claude (author solution fetch via MCP, submission history analysis, FAANG reviewer subagent).*
