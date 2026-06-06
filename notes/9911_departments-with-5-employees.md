# 9911 — Departments With 5 Employees

> Standard note template (v2, 2026-05-27)

---

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 9911 |
| Category | Aggregation |
| Difficulty | Easy |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/9911-departments-with-5-employees?code_type=1) · [Python](https://platform.stratascratch.com/coding/9911-departments-with-5-employees?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/9911-departments-with-5-employees?code_type=6) |
| Solved on | 2026-06-06 |
| Tables | `employee(id, first_name, last_name, age, sex, employee_title, department, salary, target, bonus, email, city, address, manager_id)` |

## 2. Problem Restatement (in my words)

> Return the names of departments that have 5 or more employees. Output only the `department` column.

## 3. My Approach

- First instinct: `GROUP BY department` then filter the group count. (Correct instinct.)
- Where I got stuck:
  1. Tried `WHERE COUNT(*) >= 5` → aggregates aren't allowed in `WHERE`; this belongs in `HAVING`.
  2. First valid `HAVING` query passed locally but was **marked incorrect** because I returned two columns (`department, n_emp`) — the grader wanted **only** `department`.
  3. In pandas, burned ~10 attempts on filter/selection syntax (`df['col', mask]`, `[['id']]>=5`, `.alias`/`.rename` on a GroupBy).
- Final strategy: aggregate the count, filter on the count, then project **only** `department`.

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-06-06)

```sql
WITH emp_agg AS (
    SELECT
        department,
        COUNT(*) AS n_emp
    FROM employee
    GROUP BY department
    HAVING COUNT(*) >= 5
)
SELECT department FROM emp_agg;
```

### 4.2 Python (pandas) ✅ (passed 2026-06-06)

```python
import pandas as pd

# Count employees per department
n_emp_dep = employee.groupby('department')['id'].count().reset_index(name='n_emp')

result = n_emp_dep.loc[n_emp_dep['n_emp'] >= 5]
result = result[['department']]
```

### 4.3 PySpark ✅ (passed 2026-06-06)

```python
import pyspark
from pyspark.sql import functions as f

employee = (
    employee.groupBy('department')
    .agg(f.count('*').alias('n_emp'))
    .filter(f.col('n_emp') >= 5)
    .select(f.col('department'))
)

employee.toPandas()
```

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)` (Premium).

### 5.1 PostgreSQL

```sql
SELECT
    department
FROM employee
GROUP BY department
HAVING COUNT(DISTINCT id) >= 5;
```

### 5.2 Python

```python
import pandas as pd
import numpy as np

n_count = employee.groupby(['department'])['id'].nunique().to_frame('count').reset_index()
result = n_count[n_count['count'] >= 5][['department']]
```

### 5.3 PySpark

```python
import pyspark.sql.functions as F

n_count = employee.groupby('department').agg(F.countDistinct('id').alias('count'))
result = n_count.filter(n_count['count'] >= 5).select('department').toPandas()
result
```

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| Count method | `COUNT(*)` / `.count()` | `COUNT(DISTINCT id)` / `.nunique()` | Same result **only if `id` has no duplicates**. Author's is safer against duplicate rows. |
| Structure | CTE then `SELECT` | Single `GROUP BY ... HAVING` | CTE is fine but unnecessary here — author is leaner. |
| Output shape | `department` only (after fixing) | `department` only | Matches. This was my one failed submission. |

Key insight in one line: **Match the requested output columns exactly — returning the helper count column (`n_emp`) failed the grader even though the rows were right.**

## 7. Submission History (auto-captured)

> From MCP `get_my_attempts(question_id=9911)`. All times UTC, 2026-06-06.

| # | Time | Language | Status | Error / Note |
|---|---|---|---|---|
| 1 | 02:16 | SQL | run | `select * from employee` — explore |
| 2 | 02:19:23 | SQL | ❌ err | `COUNT(*)` in `WHERE` → GroupingError |
| 3 | 02:19:47 | SQL | ❌ submit | `HAVING` correct but returned 2 cols (`department, n_emp`) |
| 4 | 02:20:22 | SQL | ✅ submit | Wrapped in CTE, selected only `department` |
| 5–18 | 02:33–02:40 | Python | ❌ ×many | pandas filter/select syntax (`df['col', mask]`, `[['id']]>=5`, `.alias`/`.rename` on GroupBy) |
| 19 | 02:40:56 | Python | ✅ submit | `.loc[mask]` then `[['department']]` |
| 20 | 02:43–02:44 | PySpark | ✅ submit | `groupBy.agg(count).filter.select` |

What the failed attempts taught me:
- Lesson 1: Aggregate filters go in `HAVING`, never `WHERE`.
- Lesson 2: The grader checks **column set**, not just rows — drop helper columns before submitting.
- Lesson 3: In pandas you can't rename/alias on a `GroupBy` object; name the column at aggregation (`reset_index(name=...)` or `.agg(n=('id','count'))`), and boolean filtering is `df.loc[df['c'] >= 5]`, not `df['c', mask]`.

## 8. Senior DA / DS Review — Structured Rubric

### 8.1 Correctness
- **Score**: 5/5
- Comment: Final answers in all three engines are correct. Note the latent assumption that `id` is unique; author's `COUNT(DISTINCT id)` makes that explicit.

### 8.2 Performance & Efficiency
- **Score**: 4/5
- Comment: Single-pass aggregation, no joins — optimal. The CTE in SQL is cosmetic overhead; a bare `GROUP BY ... HAVING` is enough. `toPandas()` collects the full driver result, which is fine at this size.

### 8.3 Readability
- **Score**: 4/5
- Comment: Clear aliases (`n_emp`) and a logical flow. Minor: the standalone `employee.head()` lines and reassigning `employee` to the aggregated frame in PySpark can confuse a reader — prefer a new variable name like `result`.

### 8.4 Edge Cases
- **Score**: 3/5
- Comment: `COUNT(*)` counts rows, not distinct employees. If the table ever had duplicate employee rows, your count would overstate. Adopt `COUNT(DISTINCT id)` / `nunique()` as the default habit for "how many employees".

### 8.5 Interview-Readiness
- **Score**: 4/5
- Comment: You recovered from the `WHERE` vs `HAVING` slip quickly. In an interview, state the output contract up front ("return just the department names") — it would have caught the two-column miss before submitting.

### 8.6 Verdict
Solid, correct work across all three engines with a fast recovery from the classic `WHERE`/`HAVING` aggregate error. The single highest-leverage habit to build: **read the output spec as a contract** — confirm the exact columns expected before you submit, and default to `COUNT(DISTINCT <entity_id>)` whenever the question counts entities rather than rows. Both moves turn "right rows, wrong shape" near-misses into clean first-try passes.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Count per group | `COUNT(*)` / `COUNT(DISTINCT id)` | `.groupby('department')['id'].count()` / `.nunique()` | `F.count('*')` / `F.countDistinct('id')` |
| Filter on aggregate | `HAVING COUNT(*) >= 5` | `df.loc[df['n_emp'] >= 5]` | `.filter(F.col('n_emp') >= 5)` |
| Name the agg column | `AS n_emp` | `.reset_index(name='n_emp')` | `.alias('n_emp')` |
| Keep one column | `SELECT department` | `result[['department']]` | `.select('department')` |

## 10. Patterns to Remember

- Core pattern this problem teaches: **GROUP BY → HAVING (filter on aggregate) → project only the requested column.**
- Reusable snippet: `GROUP BY g HAVING COUNT(DISTINCT entity_id) >= n` for "groups with at least n entities".
- Weakness I noticed in myself: pandas group-filter-select syntax cost me ~10 attempts; drill `df.loc[mask, cols]` until automatic.

## 11. Related Notes

- Similar problems in this category: [`9892_second-highest-salary`](9892_second-highest-salary.md), [`10090_percentage-of-shipable-orders`](10090_percentage-of-shipable-orders.md)
- Reference material: GROUP BY / HAVING semantics; `COUNT(*)` vs `COUNT(DISTINCT col)`.

---

*Generated 2026-06-06 by Ina (problem solving) + Claude (author solution fetch, diff, rubric review).*
