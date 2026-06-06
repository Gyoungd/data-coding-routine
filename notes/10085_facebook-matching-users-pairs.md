# 10085 — Meta/Facebook Matching Users Pairs

> Standard note template (v2, 2026-05-27)

---

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 10085 |
| Category | Join (self-join) |
| Difficulty | Medium |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/10085-facebook-matching-users-pairs?code_type=1) · [Python](https://platform.stratascratch.com/coding/10085-facebook-matching-users-pairs?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/10085-facebook-matching-users-pairs?code_type=6) |
| Solved on | 2026-06-06 |
| Tables | `facebook_employees(id, age, gender, is_senior, location)` |

## 2. Problem Restatement (in my words)

> Self-join the employee table to find pairs that share the same `location` and `gender` but differ in `age` and `is_senior`. Output the two ids of each matching pair.

## 3. My Approach

- First instinct: split the table into `junior` (is_senior = FALSE) and `senior` (is_senior = TRUE) CTEs, then cross them on same location/gender with different age.
- Where I got stuck:
  1. **The junior/senior split failed — twice, even after adding `DISTINCT`.** It only produces pairs in one direction `(junior_id, senior_id)`; the grader expects the symmetric self-join, which emits **both** `(a, b)` and `(b, a)`. Half the rows were missing.
  2. First pandas submission failed because I dropped `id_x == id_y` but **forgot the seniority filter**.
  3. PySpark: tripped on `withColumnRenamed` (it takes two positional args, not a dict) and an `AMBIGUOUS_REFERENCE` from self-joining without renaming columns.
- Final strategy: plain self-join `FROM t a, t b WHERE a.location=b.location AND a.gender=b.gender AND a.age<>b.age AND a.is_senior<>b.is_senior`. The `is_senior <> is_senior` predicate also rules out self-pairs automatically (an employee can't differ in seniority from themselves), so no explicit `id <> id` is needed.

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-06-06)

```sql
-- Same Nation & Gender, but Different Age & seniority levels
SELECT
    j.id,
    s.id
FROM facebook_employees AS j, facebook_employees AS s
WHERE
    j.location = s.location
    AND j.gender = s.gender
    AND j.age != s.age
    AND j.is_senior != s.is_senior;
```

### 4.2 Python (pandas) ✅ (passed 2026-06-06)

```python
import pandas as pd

not_null = facebook_employees[facebook_employees['id'].notnull()]

full_join = not_null.merge(not_null, on=['location', 'gender'], how='inner')

# Drop self-pairs
drop_dup_id = full_join[full_join['id_x'] != full_join['id_y']]

# Apply seniority filter
filter_seniority = drop_dup_id[drop_dup_id['is_senior_x'] != drop_dup_id['is_senior_y']]

result = filter_seniority[['id_x', 'id_y']]
```

> ⚠️ Note: this passed **without** an explicit `age_x != age_y` filter — a quirk of this dataset (different seniority happened to coincide with different age). It is **not robust**; the author solution and my SQL/PySpark both keep the age condition. Add it.

### 4.3 PySpark ✅ (passed 2026-06-06)

```python
import pyspark
from pyspark.sql import functions as f

emp2 = (facebook_employees
        .withColumnRenamed('id', 'id2')
        .withColumnRenamed('age', 'age2')
        .withColumnRenamed('is_senior', 'is_senior2'))

result = (facebook_employees
          .join(emp2, on=['location', 'gender'])
          .filter(f.col('id') != f.col('id2'))
          .filter(f.col('age') != f.col('age2'))
          .filter(f.col('is_senior') != f.col('is_senior2'))
          .select('id', 'id2'))

result.toPandas()
```

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)` (Premium).

### 5.1 PostgreSQL

```sql
SELECT
    e1.id AS employee_1,
    e2.id AS employee_2
FROM facebook_employees e1
JOIN facebook_employees e2
    ON  e1.location = e2.location
    AND e1.age <> e2.age
    AND e1.gender = e2.gender
    AND e1.is_senior <> e2.is_senior
WHERE e1.id IS NOT NULL
  AND e2.id IS NOT NULL;
```

### 5.2 Python

```python
import pandas as pd
import numpy as np

not_null = facebook_employees[facebook_employees['id'].notnull()]
full_join = not_null.merge(not_null, on=['location', 'gender'], how='inner')
full_join = full_join[
    (full_join['age_x'] != full_join['age_y']) & (full_join['is_senior_x'] != full_join['is_senior_y'])]
result = full_join.rename(columns={'id_x': 'employee_1', 'id_y': 'employee_2'})[['employee_1', 'employee_2']]
```

### 5.3 PySpark

```python
import pyspark.sql.functions as F

df1 = facebook_employees
df2 = (facebook_employees
       .withColumnRenamed("id", "id2")
       .withColumnRenamed("location", "location2")
       .withColumnRenamed("age", "age2")
       .withColumnRenamed("gender", "gender2")
       .withColumnRenamed("is_senior", "is_senior2"))

result = (df1
          .join(df2, how='cross')
          .where(
              (df1.location == df2.location2) &
              (df1.age != df2.age2) &
              (df1.gender == df2.gender2) &
              (df1.is_senior != df2.is_senior2))
          .select(df1.id, df2.id2)
          .toPandas())
```

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| Pair direction | Full self-join (both orders) | Full self-join (both orders) | ✅ Matches. The junior/senior split I tried first only emits one direction → failed. |
| Age filter (pandas) | **Omitted** (passed by luck) | `age_x != age_y` explicit | Author is robust; mine is dataset-dependent. Fix it. |
| Self-pair exclusion (SQL) | Implicit via `is_senior <> is_senior` | Explicit-ish (same predicate) | Same effect; both rely on the seniority inequality. |
| NULL handling | `notnull()` in pandas only | `IS NOT NULL` / `notnull()` everywhere | Author guards ids in all engines. |
| Join idiom | comma-join (SQL), `on=[...]` (pandas/spark) | `JOIN ... ON` / `cross` | Equivalent; explicit `JOIN ... ON` reads better in interviews. |

Key insight in one line: **A symmetric self-join emits each pair in both orders `(a,b)` and `(b,a)`; splitting the table by the differing attribute (junior vs senior) gives only one direction and silently drops half the rows.**

## 7. Submission History (auto-captured)

> From MCP `get_my_attempts(question_id=10085)`. All times UTC, 2026-06-06.

| # | Time | Language | Status | Error / Note |
|---|---|---|---|---|
| 1 | 02:45 | SQL | run | `select *` — explore |
| 2 | 05:04:25 | SQL | ❌ err | CTE referenced alias `j` but `FROM junior, senior` (no alias) → UndefinedTable |
| 3 | 05:04:41 | SQL | ❌ submit | junior/senior split — only one pair direction |
| 4 | 05:05:27 | SQL | ❌ submit | added `DISTINCT`, still one direction |
| 5 | 05:06:45 | SQL | ✅ submit | Full self-join with `age !=` and `is_senior !=` |
| 6 | 05:31–05:33 | Python | ❌ submit | self-merge but **no seniority filter** |
| 7 | 05:35:44 | Python | ✅ submit | added `is_senior_x != is_senior_y` |
| 8 | 05:36–05:43 | PySpark | ❌ err ×several | `withColumnRenamed` dict, `=` vs `==`, AMBIGUOUS_REFERENCE (no rename) |
| 9 | 05:46:04 | PySpark | ✅ submit | renamed cols, join + 3 filters |

What the failed attempts taught me:
- Lesson 1: For "find matching pairs," reach for a **self-join first**, not a partition-by-attribute split — the split loses the symmetric direction.
- Lesson 2: Translate **every** condition in the prompt into a predicate. I dropped the seniority filter in pandas (and got away with omitting age only by luck).
- Lesson 3: PySpark self-joins need the right side's columns **renamed first**, or `f.col('x')` is ambiguous. `withColumnRenamed(old, new)` takes two args, not a dict.

## 8. Senior DA / DS Review — Structured Rubric

### 8.1 Correctness
- **Score**: 4/5
- Comment: All three final submissions pass, but the pandas version omits the `age` predicate and only passes because of a data coincidence. A correct-by-construction solution keeps every condition from the spec.

### 8.2 Performance & Efficiency
- **Score**: 3/5
- Comment: A self-join here is inherently O(n²) before filtering — unavoidable for pairwise matching. Pushing the most selective equality (`location`, `gender`) into the join keys (as you did in pandas/Spark) is the right move to shrink the intermediate. The SQL comma-join leaves all predicates in `WHERE`; the planner handles it, but an explicit `JOIN ... ON` with the equalities as join keys signals intent.

### 8.3 Readability
- **Score**: 4/5
- Comment: Clear step-by-step pandas with intermediate names (`not_null`, `full_join`, `drop_dup_id`). The SQL comma-join with `j`/`s` aliases is fine but `e1`/`e2` + `JOIN ... ON` (author style) is more conventional. Drop the stray `.head()` / `.toPandas()` debug lines before final submit.

### 8.4 Edge Cases
- **Score**: 3/5
- Comment: You handled NULL ids (pandas) and self-pairs (via `id !=` / `is_senior !=`). Gaps: (a) the missing age filter in pandas; (b) the pair-ordering issue that sank the first approach — worth internalizing as the default risk in any "pairs" problem.

### 8.5 Interview-Readiness
- **Score**: 4/5
- Comment: Good recovery arc. To tighten: before coding, say out loud "this is a symmetric self-join; each pair will appear twice unless I dedupe with `id1 < id2`" — that single sentence pre-empts both the direction bug and a 'should pairs be unordered?' clarifying question.

### 8.6 Verdict
Strong Medium-level work with a clean diagnosis once you abandoned the junior/senior split for a straight self-join. The highest-leverage habit: **on any "matching pairs" problem, default to a self-join and immediately decide the pair semantics — both directions, or unordered with `a.id < b.id`?** Second, treat the prompt's conditions as a checklist and encode every one (the dropped age filter passed only by luck). Nail those two and these go from four-submission grinds to first-try passes.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Self-join | `FROM t a JOIN t b ON ...` | `df.merge(df, on=[...])` → `_x`/`_y` suffixes | rename right side, then `df1.join(df2, on=[...])` |
| Equality join key | `a.location = b.location` | `on=['location','gender']` | `on=['location','gender']` |
| Inequality predicate | `a.age <> b.age` | `df['age_x'] != df['age_y']` | `f.col('age') != f.col('age2')` |
| Exclude self-pair | `a.is_senior <> b.is_senior` (implicit) | `df['id_x'] != df['id_y']` | `.filter(f.col('id') != f.col('id2'))` |
| NULL guard | `WHERE id IS NOT NULL` | `df[df['id'].notnull()]` | `.filter(f.col('id').isNotNull())` |

## 10. Patterns to Remember

- Core pattern this problem teaches: **symmetric self-join on equality keys + inequality predicates; decide pair direction up front.**
- Reusable snippet: `FROM t a JOIN t b ON a.key = b.key AND a.x <> b.x` — for "pairs sharing key but differing on x". Add `AND a.id < b.id` if unordered/unique pairs are wanted.
- Weakness I noticed in myself: (1) instinctively partitioned by attribute instead of self-joining; (2) dropped a required filter and got a false pass.

## 11. Related Notes

- Similar problems in this category: `9856-find-employees-with-the-same-salary` (self-join, same queue), `10078-matching-hosts-and-guests` (self-/cross-join with equality+inequality).
- Reference material: self-join semantics; ordered vs unordered pair dedup with `a.id < b.id`.

---

*Generated 2026-06-06 by Ina (problem solving) + Claude (author solution fetch, diff, rubric review).*
