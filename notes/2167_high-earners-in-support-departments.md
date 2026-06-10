# 2167 — High Earners in Support Departments

> Standard note template (v2, 2026-05-27)

---

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 2167 |
| Category | Filter/Basic (boolean logic & operator precedence) |
| Difficulty | Easy |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/2167-high-earners-in-support-departments?code_type=1) · [Python](https://platform.stratascratch.com/coding/2167-high-earners-in-support-departments?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/2167-high-earners-in-support-departments?code_type=6) |
| Solved on | 2026-06-09 |
| Tables | `techcorp_workforce(id, first_name, last_name, department, salary, joining_date, phone_number)` |

## 2. Problem Restatement (in my words)

Return name, department, and salary for everyone who earns more than $80,000 **and** sits in either the HR or Admin department. Two conditions: one numeric threshold, one membership-in-a-set.

## 3. My Approach

- **First instinct:** Plain `WHERE` filter — `department = 'HR' OR department = 'Admin' AND salary > 80000`. Looked right, ran, but returned wrong rows.
- **Where I got stuck:** Three separate precedence/syntax traps, one per language:
  - SQL: `AND` binds tighter than `OR`, so my unparenthesized condition silently meant "(HR) OR (Admin AND >80k)" — i.e. *every* HR employee regardless of salary.
  - pandas: used Python `and` between two Series → `ValueError: truth value of a Series is ambiguous`; also `isin('HR','Admin')` instead of `isin(['HR','Admin'])`.
  - PySpark: typos (`selct`, `import function`) — mechanical, not logical.
- **Final strategy:** Wrap the OR group in parentheses (SQL), switch to `&` with each comparison parenthesized (pandas/PySpark), pass a list to `isin`.

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-06-09)

```sql
SELECT DISTINCT
    first_name,
    last_name,
    department,
    salary
FROM techcorp_workforce
WHERE
    (department = 'HR' OR department = 'Admin')
    AND (salary > 80000);
```

### 4.2 Python (pandas) ✅ (passed 2026-06-09)

```python
import pandas as pd

depart = ['HR', 'Admin']
df = techcorp_workforce.loc[
    (techcorp_workforce['department'].isin(depart)) &
    (techcorp_workforce['salary'] > 80000)
]
result = df[['first_name', 'last_name', 'department', 'salary']]
```

### 4.3 PySpark ✅ (passed 2026-06-09)

```python
from pyspark.sql import functions as f

df = (techcorp_workforce
      .select('first_name', 'last_name', 'department', 'salary')
      .filter(f.col('department').isin('HR', 'Admin'))
      .filter(f.col('salary') > 80000))
df.toPandas()
```

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)` (Premium).

### 5.1 PostgreSQL

```sql
SELECT
    first_name,
    last_name,
    department,
    salary
FROM techcorp_workforce
WHERE salary > 80000
  AND (department = 'HR' OR department = 'Admin');
```

### 5.2 Python

```python
import pandas as pd

result = techcorp_workforce[
    (techcorp_workforce['salary'] > 80000) &
    (
        (techcorp_workforce['department'] == 'HR') |
        (techcorp_workforce['department'] == 'Admin')
    )
][['first_name', 'last_name', 'department', 'salary']]
```

### 5.3 PySpark

```python
from pyspark.sql import functions as F

result = techcorp_workforce.filter(
    (F.col('salary') > 80000)
    & ((F.col('department') == 'HR') | (F.col('department') == 'Admin'))
).select('first_name', 'last_name', 'department', 'salary')

result.toPandas()
```

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| OR group | `(department = 'HR' OR department = 'Admin')` | same | Identical — the fix that mattered |
| Set membership | `isin(['HR','Admin'])` (pandas/PySpark) | `== 'HR' \| == 'Admin'` | Equivalent; `isin` is cleaner for 2+ values |
| `DISTINCT` | Added `DISTINCT` in SQL | No `DISTINCT` | Mine is harmless here (no dup rows expected); author trusts the data. Don't reach for `DISTINCT` to "fix" a wrong-rows bug — it masks logic errors |
| Filter style | Two chained `.filter()` calls | One combined `&` expression | Both fine; chaining filters = implicit AND, very readable |
| Output shape | `df[['...']]` after filtering | inline column list | Same |

Key insight in one line: **In SQL, `AND` has higher precedence than `OR` — always parenthesize an OR group when mixing the two.**

## 7. Submission History (auto-captured)

> From MCP `get_my_attempts(question_id=2167)`. Times in KST. The path shows one real logic bug (SQL precedence) plus a string of mechanical slips.

| # | Time (KST) | Lang | Status | Error / Note |
|---|---|---|---|---|
| 1 | 15:16 | SQL | — | `select *` exploration |
| 2 | 15:45 | SQL | ❌ | No parens → `AND` bound to `Admin` only; returned all HR rows |
| 3 | 15:46 | SQL | ❌ | Added `DISTINCT` but still no parens — same wrong rows |
| 4 | 15:46 | SQL | ✅ | Wrapped `(department='HR' OR ...)` in parentheses |
| 5 | 17:59 | pandas | — | `.head()` exploration |
| 6 | 18:01 | pandas | ❌ | `isin('HR','Admin')` → TypeError (needs a list) |
| 7 | 18:02 | pandas | ❌ | `and` between Series → ValueError: ambiguous truth value |
| 8 | 18:03 | pandas | ❌ | Typo `techorp_workforce` → NameError; `df.loc['col',...]` wrong indexer |
| 9 | 18:04 | pandas | ✅ | `&` + `isin([...])` + `df[[cols]]` |
| 10 | 20:43 | PySpark | ❌ | `from pyspark.sql import function` → ImportError (it's `functions`) |
| 11 | 20:44 | PySpark | ❌ | Typo `.selct(...)` → AttributeError |
| 12 | 20:44 | PySpark | ✅ | Fixed import + method name |

What the failed attempts taught me:
- **Lesson 1 (the real one):** Boolean precedence is silent — wrong logic returns *a* result set, not an error. Parenthesize OR groups every time.
- **Lesson 2:** pandas needs `&`/`|` (not `and`/`or`) between Series, and each comparison wrapped in its own `()`. `isin` takes a **list**.
- **Lesson 3:** Most of my retries were typos (`selct`, `function`, `techorp`). Slow down by one beat before submitting — read the identifier back once.

## 8. Senior DA / DS Review — Structured Rubric

> Auto-filled by Claude in reviewer persona. Calibrated to a FAANG bar-raiser. Fresh read of the accepted solutions above.

### 8.1 Correctness
- **Score**: 5/5
- Comment: Final solutions in all three engines return the correct set. The two conditions are composed correctly once parentheses/operators are fixed.

### 8.2 Performance & Efficiency
- **Score**: 5/5
- Comment: Single full scan with a cheap predicate — optimal for this table. Chained `.filter()` in PySpark is fused by Catalyst, so no penalty vs. a combined expression.

### 8.3 Readability
- **Score**: 4/5
- Comment: Clean and idiomatic. Minor: the `DISTINCT` in SQL is unnecessary and slightly misleading (implies duplicates are expected). Naming `depart` is fine but `support_depts` would read better.

### 8.4 Edge Cases
- **Score**: 4/5
- Comment: Threshold is correctly strict (`> 80000`, not `>=`). Watch case/whitespace in department strings — `'hr'` or `' HR'` would be silently dropped; in an interview, name that assumption out loud.

### 8.5 Interview-Readiness
- **Score**: 4/5
- Comment: You reached the right answer in all three languages unaided. To raise this: catch the precedence bug *before* running by reading the WHERE clause aloud, and verbalize "I'll parenthesize the OR so it isn't swallowed by AND."

### 8.6 Verdict
Solid, correct, idiomatic across three engines. The single highest-leverage habit to build from this problem: **treat any `WHERE`/filter that mixes AND and OR as a parenthesization checkpoint** — the bug never raises an error, so it only gets caught by reading the logic or checking row counts. Pair that with a one-second identifier proofread before submitting, and your failed-attempt count on Easy problems should drop to near zero.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Boolean AND | `AND` | `&` (not `and`) | `&` (not `and`) |
| Boolean OR | `OR` | `\|` (not `or`) | `\|` (not `or`) |
| Group precedence | `( ... OR ... )` | wrap each comparison in `()` | wrap each comparison in `()` |
| Membership in set | `IN ('HR','Admin')` | `.isin(['HR','Admin'])` | `.isin('HR','Admin')` (varargs) |
| Numeric filter | `salary > 80000` | `df['salary'] > 80000` | `F.col('salary') > 80000` |
| Pick columns | `SELECT a, b, ...` | `df[['a','b',...]]` | `.select('a','b',...)` |

Note the `isin` asymmetry: pandas wants **one list** `['HR','Admin']`; PySpark `Column.isin` takes **varargs** `'HR','Admin'`. Easy to mix up.

## 10. Patterns to Remember

- **Core pattern:** Multi-condition filter mixing a set-membership test with a numeric threshold → the precedence trap when AND meets OR.
- **Reusable snippet (pandas):**
  ```python
  df[(df['col'].isin(['A','B'])) & (df['n'] > 80000)][['c1','c2']]
  ```
- **Weakness I noticed in myself:** Submitting before proofreading identifiers (3 typo failures), and not parenthesizing OR groups by reflex.

## 11. Related Notes

- Similar problems in this category: [[2172_customers-with-large-orders]] (filter + threshold), [[9911_departments-with-5-employees]] (HAVING vs WHERE).
- Reference: PostgreSQL operator precedence — `AND` > `OR`; pandas boolean indexing requires `&`/`|` with parentheses.

---

*Generated 2026-06-09 by Ina (problem solving) + Claude (author solution fetch, diff, rubric review).*
