# 10024 — Wine varieties tasted by 'Roger Voss'

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 10024 |
| Category | Filter/Basic (distinct projection with NOT NULL guard) |
| Difficulty | Easy |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/10024-wine-varieties-tasted-by-roger-voss?code_type=1) · [Python](https://platform.stratascratch.com/coding/10024-wine-varieties-tasted-by-roger-voss?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/10024-wine-varieties-tasted-by-roger-voss?code_type=6) |
| Solved on | 2026-06-07 |
| Tables | `winemag_p2(country, description, designation, id, points, price, province, region_1, region_2, taster_name, taster_twitter_handle, title, variety, winery)` |

## 2. Problem Restatement (in my words)

Return the distinct `variety` values for rows where the taster is `'Roger Voss'`
**and** `region_1` is not NULL. Output one column, deduplicated.

Two quiet traps: the person's name lives in `taster_name` (not `variety`,
`winery`, or `description` — you have to find the right column first), and the
output column is `variety`, not the `region_1` you filter on.

## 3. My Approach

- **First instinct (SQL)**: hunt for where the name "Roger Voss" actually lives.
  I probed `variety`, `description LIKE '%Roger Voss%'`, and `winery LIKE` before
  `SELECT * WHERE taster_name = 'Roger Voss'` confirmed the column.
- **Where I got stuck (SQL)**: my first *submission* output `DISTINCT region_1`
  instead of `variety` — I filtered correctly but projected the wrong column.
- **Where I got stuck (pandas)**: NULL-check idioms — I wrote `!` for negation
  (R/JS habit), then `.notnull` without parentheses (a bound method, not a call).
- **Where I got stuck (PySpark)**: the real grind — ~8 failures from putting
  `# comments` **after** a `\` line-continuation, plus `=` vs `==`, `!= NULL`
  instead of `.isNotNull()`, and `.select(col.distinct())` (distinct is a
  DataFrame method, not a Column method).
- **Final strategy**: in all three engines — filter `taster_name == 'Roger Voss'`,
  filter `region_1` NOT NULL, project `variety`, dedupe.

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-06-07 12:05)

```sql
SELECT DISTINCT variety FROM winemag_p2
WHERE taster_name = 'Roger Voss' AND region_1 IS NOT NULL;
```

### 4.2 Python (pandas) ✅ (passed 2026-06-07 13:16)

```python
import pandas as pd

rv_taste = winemag_p2.loc[
    (winemag_p2['taster_name'] == 'Roger Voss') & (winemag_p2['region_1'].notna())
]
result = rv_taste['variety'].unique()   # NOTE: returns a 1-D ndarray; see Section 6
result
```

### 4.3 PySpark ✅ (passed 2026-06-07 13:27)

```python
import pyspark
from pyspark.sql import functions as f

result = winemag_p2.select("taster_name", "region_1", "variety") \
    .filter(f.col('taster_name') == 'Roger Voss') \
    .filter(f.col('region_1').isNotNull()) \
    .select('variety').distinct()

result.toPandas()
```

## 5. Author Solutions

Pulled via MCP `get_question(include_solution=True)`.

### 5.1 PostgreSQL

```sql
SELECT DISTINCT variety
FROM winemag_p2
WHERE taster_name = 'Roger Voss' AND region_1 IS NOT NULL;
```

### 5.2 Python (pandas)

```python
import pandas as pd
import numpy as np

result = winemag_p2[
    (winemag_p2['region_1'].notnull()) & (winemag_p2['taster_name'] == 'Roger Voss')
][['variety']].drop_duplicates()
```

### 5.3 PySpark

```python
import pyspark.sql.functions as F

result = winemag_p2.filter(
    (F.col('region_1').isNotNull()) & (F.col('taster_name') == 'Roger Voss')
).select('variety').distinct().toPandas()
```

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| SQL | `DISTINCT variety` + both filters | Identical | Same answer; mine matched the reference exactly once I fixed the projection |
| pandas dedup / output shape | `['variety'].unique()` → **1-D ndarray** | `[['variety']].drop_duplicates()` → **DataFrame** | Both pass here, but `unique()` is a Series-only array; `drop_duplicates()` keeps a tabular column and scales to multi-column output |
| pandas null check | `.notna()` | `.notnull()` | Identical — they're aliases |
| pandas filter style | `.loc[mask]` | boolean `[mask]` | Equivalent for row filtering |
| PySpark | extra `.select(3 cols)` before filtering, then `.select('variety')` | filter first, then `.select('variety')` | Same result; my leading `.select` is an unnecessary projection step |

**Key insight in one line:** this is a pure filter-and-project — the only real
decisions are *which column holds the name* and *which column to output*; I
stumbled on both before the logic, not on the logic itself.

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=10024)` — 33 attempts. Highlights:

| # | Time (UTC) | Language | Status | What happened |
|---|---|---|---|---|
| 1 | 11:41 | SQL | run | `SELECT * FROM winemag_p2` — inspect |
| 2 | 11:58 | SQL | run | `WHERE variety = 'Roger Voss'` — wrong column for the name |
| 3 | 12:00–12:04 | SQL | run | Probed `description LIKE`, `winery LIKE` for the name |
| 4 | 12:04:55 | SQL | run | `WHERE taster_name = 'Roger Voss'` — found the right column |
| 5 | 12:05:05 | SQL | ❌ submitted | Output `DISTINCT region_1` — **wrong projection** (filtered col, not variety) |
| 6 | 12:05:35 | SQL | ✅ submitted | `DISTINCT variety` + `taster_name` + `region_1 IS NOT NULL` |
| 7 | 13:03–13:10 | Python | run | `.head()`, then `.loc[taster_name == 'Roger Voss']` |
| 8 | 13:13:54 | Python | ❌ run | `(!winemag_p2['region_1'].isNA)` — `!` isn't Python negation → SyntaxError |
| 9 | 13:15:02 | Python | ❌ run | `.notnull` without `()` — bound method, not called → AssertionError |
| 10 | 13:15:15 | Python | run | `.notna()` correct |
| 11 | 13:16:46 | Python | ✅ submitted | `.loc[(==) & (.notna())]`, `['variety'].unique()` |
| 12 | 13:20:57 | PySpark | ❌ run | `!= NULL` + `# comment` after `\` → line-continuation error |
| 13 | 13:21:12 | PySpark | ❌ run | `.filter(f.col(...) = 'Roger Voss')` — single `=` |
| 14 | 13:21–13:25 | PySpark | ❌ run ×6 | Repeated `\ # comment` line-continuation errors; `.isNotNull` (no parens) |
| 15 | 13:26:53 | PySpark | ❌ run | `.select(f.col('variety').distinct())` — `'Column' object is not callable` |
| 16 | 13:27:14 | PySpark | run | `.select('variety').distinct()` — fixed |
| 17 | 13:27:32 | PySpark | ✅ submitted | Full chain, distinct on the DataFrame |

What the failed attempts taught me:
- **Find the column before filtering**: I spent 5 SQL runs locating that the name
  lives in `taster_name`. On a wide table, `SELECT *` once and scan the columns
  before guessing `LIKE` patterns.
- **Project the asked-for column**: my first SQL submission filtered right but
  output `region_1`. Filter column ≠ output column — read the "Output ..." line last.
- **pandas null idioms**: negation is `~` (not `!`); `.notna()`/`.notnull()` need
  the call parentheses.
- **PySpark line continuation**: nothing — not even a comment or a space — may
  follow a `\`. This single mistake cost ~8 attempts. Put comments on their own
  line, or drop the `\` by wrapping the chain in parentheses.
- **distinct is a DataFrame method**: `df.select('variety').distinct()`, never
  `col.distinct()`.

## 8. Senior DA / DS Review — Structured Rubric

> Structured review by Claude against a FAANG-tier rubric. Fresh-eyes pass over the
> three accepted solutions and the full attempt trail.

### 8.1 Correctness
- **Score**: 5/5
- Comment: All three accepted solutions return the correct distinct `variety` set with both predicates (`taster_name = 'Roger Voss'`, `region_1 IS NOT NULL`) and match the author logic exactly.

### 8.2 Performance & Efficiency
- **Score**: 4/5
- Comment: Single-pass filter-and-project in every engine — appropriate for the task. Minor: the PySpark version does a redundant leading `.select(3 cols)` before the real filter; filter first, then project the single output column as the author does.

### 8.3 Readability
- **Score**: 4/5
- Comment: Final code is clean and idiomatic in all three. The only smell is reachable only in the history — relying on `\` continuations with trailing comments in PySpark; wrapping the chain in parentheses removes the fragility entirely.

### 8.4 Edge Cases
- **Score**: 4/5
- Comment: `IS NOT NULL` / `.notna()` / `.isNotNull()` correctly handle the missing-region rows the prompt calls out, and `DISTINCT`/`unique`/`distinct` dedupe as required. You did not state the case-sensitivity assumption on the literal `'Roger Voss'` — worth a one-line callout in an interview.

### 8.5 Interview-Readiness
- **Score**: 3/5
- Comment: The logic was never the problem — column discovery and syntax were. Two avoidable signals: a first SQL submission projecting the wrong column, and an 8-attempt PySpark syntax loop. Say the contract out loud ("filter taster_name and non-null region_1, output distinct variety"), confirm the name column on the first inspect, then write once.

### 8.6 Verdict
This is a textbook Easy filter, and your final solutions are correct and idiomatic across all three engines — the gap is entirely in execution hygiene, not problem-solving. The highest-leverage fix is to separate *discovery* from *submission*: inspect the schema once to confirm `taster_name` holds the name, re-read the "Output ..." line so you project `variety` and not the column you filtered on, and in PySpark abandon backslash continuations for parenthesized chains so a stray comment can never cost you eight tries. Do those three things and a problem like this is a sub-two-minute, single-submission win.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Equality filter | `WHERE taster_name = 'Roger Voss'` | `df['taster_name'] == 'Roger Voss'` | `f.col('taster_name') == 'Roger Voss'` |
| NOT NULL guard | `region_1 IS NOT NULL` | `df['region_1'].notna()` / `.notnull()` | `f.col('region_1').isNotNull()` |
| Boolean AND | `AND` | `&` (parenthesize each side) | `&` (parenthesize each side) |
| Negation | `NOT` / `<>` | `~` (not `!`) | `~` |
| Distinct projection | `SELECT DISTINCT variety` | `['variety'].drop_duplicates()` (DataFrame) / `.unique()` (ndarray) | `.select('variety').distinct()` |
| Multi-line chain | (n/a) | wrap in `( ... )` | wrap in `( ... )` — avoid trailing `\` |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: filter-and-project. `WHERE <equality> AND
  <col> IS NOT NULL` → `SELECT DISTINCT <output col>`. The output column is named
  in the last sentence; the filter columns are named earlier — don't confuse them.

- **Reusable snippet (PySpark, no backslashes)**:
  ```python
  result = (
      winemag_p2
      .filter((f.col('taster_name') == 'Roger Voss') & f.col('region_1').isNotNull())
      .select('variety')
      .distinct()
  )
  ```

- **Weaknesses I noticed in myself**:
  1. I guess `LIKE` patterns to find a value instead of inspecting columns first.
  2. I project the column I filtered on, not the one asked for (SQL submission 1).
  3. pandas negation/null idioms (`~`, `.notna()` with parens) aren't automatic.
  4. PySpark `\` continuation + trailing comment is a recurring self-inflicted wound.

- **Drill for tomorrow morning** (5-min time-box):
  1. Re-write 10024 in PySpark cold, parenthesized chain, no `\`, first try.
  2. In pandas, output a **DataFrame** via `[['variety']].drop_duplicates()` instead
     of the `.unique()` ndarray.

## 11. Related Notes

- Same Filter/Basic + NOT NULL family in our queue: 2168 "Users Missing Phone
  Numbers", 2169 "Contact Information Completeness", 9620-style `IS NULL` checks.
- Same "find the right column then filter" discipline: any wide-table Easy.
- Reference reading:
  - PostgreSQL comparison & NULL: https://www.postgresql.org/docs/current/functions-comparison.html
  - pandas `notna`: https://pandas.pydata.org/docs/reference/api/pandas.Series.notna.html
  - PySpark `Column.isNotNull` / `DataFrame.distinct`: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html

---

*Generated 2026-06-07 by Ina (3-language solving) + Claude (author solution fetch
via MCP, submission history analysis, rubric review).*
