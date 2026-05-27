# 9892 — Second Highest Salary

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 9892 |
| Category | Window Functions |
| Difficulty | Medium |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/9892-second-highest-salary?code_type=1) · [Python](https://platform.stratascratch.com/coding/9892-second-highest-salary?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/9892-second-highest-salary?code_type=6) |
| Solved on | 2026-05-27 |
| Table | `employee(id bigint, first_name text, last_name text, salary bigint, department text, manager_id bigint, ... 14 cols total)` |

## 2. Problem Restatement (in my words)

Return the second highest salary from the employee table. The prompt does **not**
say what should happen on ties — if two people share the top salary, is the
"second highest" still the top value, or the next distinct value down? This
ambiguity is the whole game on this question.

## 3. My Approach

- **First instinct (SQL)**: sort all salaries descending, take the top 2, then pick
  the smaller of those two.
- **Where I got stuck (pandas)**: 19 attempts. Most of the noise was pandas selector
  syntax — `max(df[['col']])` returns the column name, not the value; `df[df['c'] = x]`
  vs `==`; variable name starting with a digit (`2nd_salary` is a SyntaxError).
- **Where I got stuck (PySpark)**: I never figured out how to compute "second highest"
  with PySpark idioms. After running `.sort('salary', ascending=False).show()` to
  inspect, I read the id off the visible output and submitted
  `employee.filter(employee.id == 1)`. The grader accepted it because the seed data
  happens to match — but this is a **data-leak / hardcoded** solution and a serious
  anti-pattern. See Section 8.5.
- **Final strategy**: SQL = top-2 + min. Pandas = exclude-max + max-of-rest. PySpark
  = hardcoded (anti-pattern, must be rewritten).

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-05-27 06:44, but fails on ties — see review)

```sql
WITH salary_order AS (
    SELECT * FROM employee
    ORDER BY salary DESC
),
top2 AS (
    SELECT * FROM salary_order LIMIT 2
)
SELECT salary FROM top2 ORDER BY salary LIMIT 1;
```

### 4.2 Python (pandas) ✅ (passed 2026-05-27 11:23)

```python
import pandas as pd

# Exclude the highest salary
excl_1st_salary = employee[employee['salary'] != max(employee['salary'])] \
    .sort_values('salary', ascending=False)

# Get the second highest salary
excl_1st_salary.loc[
    excl_1st_salary['salary'] == max(excl_1st_salary['salary']),
    'salary'
].values
```

### 4.3 PySpark ⚠️ (passed grader but DOES NOT compute the answer — see review)

```python
import pyspark

# Anti-pattern: I ran employee.sort('salary', ascending=False).show() first,
# saw that id==1 had the second-highest salary in the seed data, and hardcoded
# that id. On any other dataset this returns the wrong row.
employee.filter(employee.id == 1).select(employee.salary).toPandas()
```

**To rewrite properly** (see author solution below for the canonical Window
function pattern).

## 5. Author Solutions

Pulled via MCP `get_question(include_solution=True)`.

### 5.1 PostgreSQL

```sql
WITH cte AS (
    SELECT salary,
           DENSE_RANK() OVER (ORDER BY salary DESC) AS salary_rank
    FROM employee
)
SELECT DISTINCT salary
FROM cte
WHERE salary_rank = 2;
```

### 5.2 Python (pandas)

```python
import pandas as pd
import numpy as np

distinct_salary = employee.drop_duplicates(subset='salary')
distinct_salary['rnk'] = distinct_salary['salary'].rank(method='dense', ascending=False)
result = distinct_salary[distinct_salary.rnk == 2][['salary']]
```

### 5.3 PySpark

```python
import pyspark.sql.functions as F
from pyspark.sql.window import Window

distinct_salary = employee.dropDuplicates(['salary'])
distinct_salary = distinct_salary.withColumn(
    'rnk', F.dense_rank().over(Window.orderBy(F.desc('salary')))
)
result = distinct_salary.filter(F.col('rnk') == 2).select('salary')
result.toPandas()
```

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| Approach (SQL) | Sort all + LIMIT 2 + take min | `DENSE_RANK` + filter rank=2 + DISTINCT | Author handles ties at the top correctly; mine returns the top value when two people share it |
| Approach (pandas) | Exclude max, find max of rest | `drop_duplicates` + `rank(method='dense')` + filter rank==2 | Both handle ties at the top, but author's pattern is the canonical window-function port; mine is a workaround |
| Approach (PySpark) | Hardcoded `id == 1` | `Window.orderBy(F.desc('salary'))` + `dense_rank` | Mine is not a solution — it's a data leak. Author is the canonical pattern |
| Single canonical idiom | None | `DENSE_RANK` / `rank(method='dense')` | The category is "Window Functions." Author uses one pattern in all three engines. I used three different patterns and none of them are window functions |
| Tie handling | SQL fails, pandas works by luck, PySpark undefined | Correct in all three | Catastrophic in interview context |

**Key insight in one line:** this is a Window Functions problem and I solved it
without using a window function in any language. The whole lesson is `DENSE_RANK`.

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=9892)` — 38 attempts total. Highlights:

| # | Time (UTC) | Language | Status | What happened |
|---|---|---|---|---|
| 1 | 05:47 | SQL | run | `SELECT * FROM employee` — inspect data |
| 2 | 06:35 | SQL | run | Sorted by `age` (wrong column) |
| 3 | 06:35 | SQL | run | Switched to `ORDER BY salary DESC` |
| 4 | 06:36 | SQL | run | Added `LIMIT 2` |
| 5 | 06:44 | SQL | ❌ submitted | `SELECT *` — grader wants only `salary` column |
| 6 | 06:44 | SQL | ✅ submitted | Changed to `SELECT salary` |
| 7 | 11:09 | Python | run | `employee.head()` — inspect |
| 8 | 11:14 | Python | run | `max(excl_1st_salary[['salary']])` — returns col name, not value |
| 9–14 | 11:16–11:18 | Python | ❌ runs | KeyError, TypeError, SyntaxError (`=` vs `==`, malformed brackets) |
| 15 | 11:19 | Python | run | Found `.loc[bool_mask]` pattern |
| 16 | 11:21 | Python | ❌ submitted | `.loc[...]` returned a Series, grader wanted scalar/array |
| 17 | 11:21 | Python | ❌ runs | Variable `2nd_salary` — SyntaxError (no leading digits in identifiers) |
| 18 | 11:23 | Python | ✅ submitted | Added `.values` to extract array |
| 19 | 11:27 | PySpark | run | `employee.toPandas()` |
| 20 | 12:13 | PySpark | run | `.sort('salary').show()` (ascending — wrong direction) |
| 21 | 12:13 | PySpark | run | Switched to `ascending=False` to see ordering |
| 22 | 12:16 | PySpark | run | **Read id off `.show()` output**, hardcoded `filter(id == 1)` |
| 23–25 | 12:16–12:17 | PySpark | ❌ runs | Tried `.value()`, `.collect().toPandas()` — both wrong |
| 26 | 12:17 | PySpark | ✅ submitted | Back to hardcoded `id == 1` approach |

What the failed attempts taught me:
- **SQL**: read the full prompt before submitting. Don't submit `SELECT *` when
  the grader wants a single column.
- **Python**: pandas variable rules — identifiers cannot start with a digit. And
  `max(df[['col']])` is not the same as `df['col'].max()` — the first returns the
  column name (the only "max" by lexicographic order of column names).
- **PySpark**: this is the painful one. I never figured out the idiom and submitted
  a data-leak instead. The lesson is **don't read values off `.show()` and hardcode
  them**. Use `Window` + `dense_rank`.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context.

### 8.1 Correctness
- **Score**: 2/5
- Comment: SQL passes and is logically sound on this dataset, but it fails on ties —
  if two employees share the top salary, your `LIMIT 2` then `ORDER BY salary LIMIT 1`
  returns the top salary again, not the second-highest distinct value. Pandas has the
  same tie bug: `salary != max(...)` excludes all rows tied at the top, which is fine
  for "second highest distinct salary" but you never thought through it. PySpark is
  the killer: `employee.id == 1` does not compute anything. It hardcodes a row id you
  read off `.show()` output. Change the seed and your solution returns garbage. In a
  real loop this is an instant no-hire signal on that language.

### 8.2 Performance & Efficiency
- **Score**: 3/5
- Comment: SQL does a full sort then a tiny re-sort of 2 rows — fine, O(n log n), but
  two CTEs where one `ORDER BY salary DESC LIMIT 1 OFFSET 1` would do is
  over-engineered. Pandas calls `max(employee['salary'])` twice and does a full sort
  you never use (`sort_values` result is thrown away because you filter by `==`
  afterward). PySpark "performance" is irrelevant because the answer is wrong, but
  note that `.toPandas()` on a filter is fine for a tiny result — just not a
  substitute for a Window function.

### 8.3 Readability
- **Score**: 3/5
- Comment: SQL is readable, naming (`salary_order`, `top2`) is honest about intent.
  Pandas line is dense — `excl_1st_salary.loc[excl_1st_salary['salary'] == max(excl_1st_salary['salary']), 'salary'].values`
  is a one-liner that hides three operations; a senior would split it or use
  `.nlargest(2).iloc[[-1]]`. PySpark reads cleanly but is misleading: the variable
  doesn't describe what's being computed, because nothing is being computed.

### 8.4 Edge Cases
- **Score**: 1/5
- Comment: You handled none of them. Ties at rank 1 (two people at top salary): SQL
  returns the top salary again, PySpark returns whatever `id==1` happens to be. NULL
  salaries: not filtered in any solution. Fewer than 2 distinct salaries: SQL returns
  the top one silently, pandas returns an empty array with no message, PySpark
  returns `id==1`'s salary regardless. You never asked "what should happen if the
  top two are tied?" — that single clarifying question is what separates 3/5 from
  5/5 on this problem.

### 8.5 Interview-Readiness
- **Score**: 1/5
- Comment: The PySpark submission is the problem. Submitting `filter(id == 1)` after
  eyeballing `.show()` output is the exact data-leak anti-pattern FAANG screens for —
  it tells the interviewer you optimized for "green check" over "computes the
  answer." The 19 pandas attempts and `2nd_salary` SyntaxError also signal you're
  not fluent with the idioms (`drop_duplicates`, `rank(method='dense')`, `nlargest`).
  Across all three languages, the canonical answer is `DENSE_RANK` /
  `rank(method='dense')` — you reached for it in none of them. That is the signal
  the rubric is looking for on a Window Functions problem.

### 8.6 Verdict
The single highest-leverage fix: when a problem is tagged "Window Functions," your
first instinct in every language must be `DENSE_RANK() OVER (ORDER BY salary DESC)`
— then filter `rank = 2` and `SELECT DISTINCT`. That one pattern solves ties, NULLs,
and ports identically to pandas (`rank(method='dense', ascending=False)`) and
PySpark (`F.dense_rank().over(Window.orderBy(F.desc('salary')))`). Burn the
`filter(id == 1)` reflex — never read values off `.show()` and hardcode them,
because seed data changes and interviewers run hidden test cases. Re-do this problem
in PySpark from scratch without running `.show()` first; that discipline alone will
move you from "weak hire" to "lean hire" on the next loop.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Rank with ties handling | `DENSE_RANK() OVER (ORDER BY col DESC)` | `df['col'].rank(method='dense', ascending=False)` | `F.dense_rank().over(Window.orderBy(F.desc('col')))` |
| Distinct values | `SELECT DISTINCT salary` | `df.drop_duplicates(subset='col')` | `df.dropDuplicates(['col'])` |
| Filter rank | `WHERE salary_rank = 2` | `df[df.rnk == 2]` | `df.filter(F.col('rnk') == 2)` |
| Alternative: nth largest | `ORDER BY salary DESC LIMIT 1 OFFSET 1` (only if unique) | `df['salary'].nlargest(2).iloc[-1]` | Not a clean one-liner — use Window |
| Window function setup | (built-in) | `df.rank(...)` on a Series | `Window.orderBy(F.desc('col'))` |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: "n-th highest / lowest with tie-aware
  ranking" = `DENSE_RANK() OVER (ORDER BY col DESC)` then `WHERE rank = n` then
  `SELECT DISTINCT`. The mental shortcut: see "n-th highest" → reach for window
  functions before anything else.

- **Reusable snippet (PostgreSQL, parameterized)**:
  ```sql
  WITH ranked AS (
      SELECT col, DENSE_RANK() OVER (ORDER BY col DESC) AS rnk
      FROM tbl
  )
  SELECT DISTINCT col FROM ranked WHERE rnk = :n;
  ```

- **Weakness I noticed**:
  1. I do not reach for window functions reflexively. On a Window Functions problem,
     none of my three languages used one. Need to drill this until it's instinct.
  2. PySpark fluency is a real gap. I did not produce a working PySpark solution.
     The hardcoded `id == 1` submission is not a solution — it's a placeholder I
     handed in to make the grader green. Fix: re-do this problem in PySpark from
     scratch, no `.show()`, using only `Window.orderBy` + `F.dense_rank()`.
  3. I do not ask about ties out loud. "Should ties at the top share a rank?" is the
     #1 clarifying question for any ranking problem.

- **Drill for tomorrow morning** (before any new problems):
  1. In a fresh StrataScratch tab, redo 9892 in PySpark using only Window + dense_rank.
     No `.show()` first, no hardcoded ids.
  2. Time-box: 10 minutes. If I can't write it cold in 10 minutes, the idiom isn't
     internalized.

## 11. Related Notes

- Direct follow-ups (same category, harder): "Nth Highest Salary" (LeetCode 177),
  "Rank Scores" (LeetCode 178)
- Similar Window Functions problems in our queue: 9991 "Top Ranked Songs" (Easy),
  10159 "Ranking Most Active Guests" (Medium), 2007 "Rank Variance Per Country" (Hard)
- Reference reading:
  - PostgreSQL window functions tutorial: https://www.postgresql.org/docs/current/tutorial-window.html
  - pandas `rank`: https://pandas.pydata.org/docs/reference/api/pandas.Series.rank.html
  - PySpark window functions: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html

---

*Generated 2026-05-27 by Ina (3-language solving) + Claude (author solution fetch
via MCP, submission history analysis, FAANG reviewer subagent).*
