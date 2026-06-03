# 10004 — Find all Lyft rides which happened on rainy days before noon

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 10004 |
| Category | Date/Time |
| Difficulty | Easy |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/10004-find-all-lyft-rides-which-happened-on-rainy-days-before-noon?code_type=1) · [Python](https://platform.stratascratch.com/coding/10004-find-all-lyft-rides-which-happened-on-rainy-days-before-noon?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/10004-find-all-lyft-rides-which-happened-on-rainy-days-before-noon?code_type=6) |
| Solved on | 2026-06-03 |
| Tables | `lyft_rides(gasoline_cost double, hour bigint, index bigint, travel_distance double, weather text)` |

## 2. Problem Restatement (in my words)

Return every Lyft ride row where the weather was rainy and the ride happened
before noon (i.e. `hour` is 0–11).

## 3. My Approach

- **First instinct**: a single `WHERE` filter on two predicates — `weather = 'rainy'`
  and `hour < 12`. SQL was right on the first real attempt.
- **Where I got stuck (pandas)**: ~4 minutes fighting boolean-mask syntax. I wrote
  the two conditions with Python `and` between Series → `ValueError: truth value of
  a Series is ambiguous`, and mangled the `.loc` brackets (`lyft_rides.[[...]]`,
  `.loc[,...]`) → `SyntaxError`.
- **Where I got stuck (PySpark)**: one typo — single `=` instead of `==` inside
  `filter(...)` → "cannot assign to function call here. Maybe you meant '=='".
- **Final strategy**: parenthesized element-wise predicates joined with `&` in both
  pandas and PySpark; plain two-condition `WHERE` in SQL.

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-06-03 10:24)

```sql
SELECT * FROM lyft_rides
WHERE weather = 'rainy' AND hour < 12;
```

### 4.2 Python (pandas) ✅ (passed 2026-06-03 10:31)

```python
import pandas as pd

result = lyft_rides[(lyft_rides['weather'] == 'rainy') & (lyft_rides['hour'] < 12)]
result
```

### 4.3 PySpark ✅ (passed 2026-06-03 10:34)

```python
import pyspark
from pyspark.sql import functions as f

df = lyft_rides.filter((f.col('weather') == 'rainy') & (f.col('hour') < 12))
df.toPandas()
```

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)`.

### 5.1 PostgreSQL

```sql
SELECT *
FROM lyft_rides
WHERE weather = 'rainy' AND hour BETWEEN 0 AND 11;
```

### 5.2 Python

```python
import pandas as pd
import numpy as np

result = lyft_rides[(lyft_rides['weather'] == 'rainy') & (lyft_rides['hour'].between(0, 11))]
```

### 5.3 PySpark

```python
import pyspark.sql.functions as F

result = lyft_rides.filter(
    (lyft_rides['weather'] == 'rainy') & (F.col('hour').between(0, 11))
).toPandas()
```

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| Hour predicate | `hour < 12` | `hour BETWEEN 0 AND 11` | Equivalent for clock hours 0–23. Mine is arguably more robust: a stray `hour = -1` still reads as "before noon" for me, while the author's hard floor of 0 would drop it |
| Range idiom | comparison | `BETWEEN` / `.between()` | Author shows the canonical inclusive-range idiom; worth knowing for both engines |
| Output | `SELECT *` | `SELECT *` | Same — prompt asks for all rows, so `*` is justified here |

**Key insight in one line:** the logic was never the problem — the only thing that
cost me time was pandas/PySpark boolean-mask syntax (`&` + parentheses, `==` not `=`).

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=10004)` — 18 attempts. Highlights:

| # | Time (UTC) | Language | Status | What happened |
|---|---|---|---|---|
| 1 | 10:23 | SQL | run | `select * from lyft_rides` — inspect |
| 2 | 10:24:06 | SQL | run | Added `where weather='rainy' and hour<12` |
| 3 | 10:24:11 | SQL | ✅ submit | Same query accepted |
| 4 | 10:24 | pandas | run | `lyft_rides.head()` |
| 5–10 | 10:27–10:30 | pandas | ❌ runs | Python `and` between Series → ambiguous-truth `ValueError`; malformed `.loc` / `.[[...]]` → `SyntaxError` |
| 11 | 10:31:28 | pandas | run | Correct `&` mask |
| 12 | 10:31:31 | pandas | ✅ submit | Accepted |
| 13 | 10:31 | pyspark | run | inspect |
| 14 | 10:33:56 | pyspark | ❌ run | `f.col('weather') = 'rainy'` (single `=`) → "Maybe you meant '=='" |
| 15 | 10:34:03 | pyspark | run | Fixed to `==` |
| 16 | 10:34:06 | pyspark | ✅ submit | Accepted |

What the failed attempts taught me:
- **pandas**: combine conditions with `&` (not `and`) and wrap each in parentheses —
  `df[(c1) & (c2)]`. Python `and` on a Series always throws ambiguous-truth.
- **pandas**: `.loc` takes `[row_selector]` or `[row_selector, col_selector]`; a
  leading comma or `df.[[...]]` is a syntax error.
- **PySpark**: comparisons inside `filter()` use `==`. Same `&`-with-parentheses rule
  as pandas.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context.

### 8.1 Correctness
- **Score**: 5/5
- Comment: All three solutions return the correct result set. `weather = 'rainy'` plus `hour < 12` is logically identical to the author's `BETWEEN 0 AND 11` given `hour` is a non-negative `bigint` clock hour (0–23). Nothing was missed; all three passed.

### 8.2 Performance & Efficiency
- **Score**: 5/5
- Comment: Single-pass predicate filters, no needless sorts, joins, or projections. `hour < 12` is sargable in Postgres and would use an index on `(weather, hour)` if one existed. The pandas boolean mask and the PySpark `f.col` predicate both push down cleanly. For an Easy filter there is no efficiency lever left on the table.

### 8.3 Readability
- **Score**: 5/5
- Comment: Consistent and clean across dialects. Importing `functions as f` and using `f.col('hour') < 12` is idiomatic PySpark and reads more directly than the author's `df['hour'].between(...)`. `hour < 12` arguably reads better than `BETWEEN 0 AND 11` because it states the actual business rule ("before noon") rather than encoding the column's domain bounds. `SELECT *` is fine here since the prompt asks to return all rows.

### 8.4 Edge Cases
- **Score**: 4/5
- Comment: `hour < 12` is more robust than `BETWEEN 0 AND 11`: if a malformed row ever carried `hour = -1` or a fractional value, your predicate still behaves sanely as "before noon," whereas the author's hard floor of 0 would silently drop it. One gap: you didn't address case sensitivity or whitespace on `weather` (`'Rainy'`, `' rainy'` would be missed). On a clean StrataScratch fixture this never bites, but in a real loop you'd want to verbalize the assumption that `weather` is a normalized enum. Fix: state the assumption aloud, or defensively use `lower(trim(weather)) = 'rainy'`.

### 8.5 Interview-Readiness
- **Score**: 3/5
- Comment: The final code is submittable, but the submission log is where a sharp interviewer focuses. The pandas attempt burned roughly four minutes (10:27–10:31) cycling through `ValueError: truth value of a Series is ambiguous` and malformed `.loc` / `.[[...]]` syntax, then the PySpark attempt repeated a parallel class of error with `=` vs `==` inside `filter`. These are not logic gaps, they are muscle-memory gaps: you should know cold that pandas/PySpark require `&` with parenthesized element-wise predicates and never Python `and`, and that comparisons use `==`. In a live loop, three minutes of syntax thrash on an Easy reads as "not fluent in the tool yet." Fix: drill the boolean-mask idiom until it is automatic so the Easy ones cost you zero debugging time and you bank that time for the Hard problem.

### 8.6 Verdict
This is a clean hire on output: all three solutions are correct, efficient, and in places more readable than the reference, and your `hour < 12` is the better defensive choice over the author's `BETWEEN`. The concern is fluency, not correctness. An Easy filter should be a ten-second reflex in every dialect, but you spent several minutes fighting `ValueError`, ambiguous-truth, and `=` vs `==` errors that a fluent practitioner never hits. Tighten the multi-language muscle memory, verbalize your assumptions about the `weather` field, and this becomes a bar-raiser-clean submission across the board.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Two-condition filter | `WHERE c1 AND c2` | `df[(c1) & (c2)]` | `df.filter((c1) & (c2))` |
| Boolean AND | `AND` | `&` (never `and`) | `&` (never `and`) |
| Equality test | `=` | `==` | `==` |
| Inclusive range | `hour BETWEEN 0 AND 11` | `df['hour'].between(0, 11)` | `F.col('hour').between(0, 11)` |
| Column reference | `hour` | `df['hour']` | `F.col('hour')` / `df['hour']` |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: simple multi-predicate row filter — the
  template `df[(cond1) & (cond2)]` ports identically to pandas and PySpark; SQL is
  just `WHERE c1 AND c2`.
- **Reusable snippet (pandas/PySpark mask)**:
  ```python
  # parentheses around EACH condition, & not and
  df[(df['a'] == x) & (df['b'] < y)]
  ```
- **Weakness I noticed**: boolean-mask syntax is not yet reflexive. Two separate
  classes of error (`and` vs `&`, `=` vs `==`) on a 30-second Easy. Drill until zero.

## 11. Related Notes

- Same category (Date/Time): 10005 "Hour Of Highest Gas Expense", 9688 "Churro
  Activity Date".
- Same boolean-mask muscle: every pandas/PySpark filter problem.
- Reference reading:
  - pandas boolean indexing: https://pandas.pydata.org/docs/user_guide/indexing.html#boolean-indexing
  - PySpark `Column.between`: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.between.html

---

*Generated 2026-06-03 by Ina (3-language solving) + Claude (author solution fetch via MCP, submission history analysis, FAANG reviewer subagent).*
