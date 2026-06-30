# 9894 — Employee and Manager Salaries

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 9894 |
| Category | Join (multi-table) |
| Difficulty | Medium |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/9894-employee-and-manager-salaries?code_type=1) · [Python](https://platform.stratascratch.com/coding/9894-employee-and-manager-salaries?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/9894-employee-and-manager-salaries?code_type=6) |
| Solved on | 2026-05-27 (PostgreSQL; re-submitted clean 2026-05-30) |
| Tables | `employee(address text, age bigint, bonus bigint, city text, department text, email text, employee_title text, first_name text, id bigint, last_name text, manager_id bigint, salary bigint, sex text, target bigint)` |

## 2. Problem Restatement (in my words)

There is one `employee` table where each row carries both the person's own `id` and a `manager_id` that points back to another row in the **same** table. Find every employee whose `salary` is strictly greater than the salary of their own manager, and return that employee's `first_name` and `salary`. The whole problem is a single self-join: line the table up against itself, matching `employee.manager_id` to `manager.id`, then keep the rows where the employee out-earns the manager.

## 3. My Approach

- **First instinct (SQL)**: self-join `employee` to itself — alias `e` for the employee and `m` for the manager — on `e.manager_id = m.id`, pull both salaries side by side, then filter `emp_sal > manager_sal`. I wrapped the join in a CTE (`cte`) that exposes `emp_id, emp_name, emp_sal, manager_id, manager_name, manager_sal`, and selected the two required columns from it in the outer query. I had the right shape in my head from the very first SELECT.
- **Where I got stuck**: nothing conceptual — the self-join logic was right immediately. The only friction was **typo-level SQL syntax**:
  1. A stray comma in the SELECT list: `e.first_name, as emp_name` → `syntax error at or near "as"`.
  2. A trailing comma after the CTE definition: `... on e.manager_id = m.id),` then `select ...` → `syntax error at or near "select"` (a comma after the closing CTE paren makes Postgres expect another CTE, not the main query).
- **Final strategy**: same self-join-in-a-CTE, cleaned up — drop the stray commas, format it, and select `emp_name, emp_sal WHERE emp_sal > manager_sal`. Passed first submission. Three days later (2026-05-30) I re-solved it cold to warm up and then started porting it to **pandas**, but I tripped on `.loc` selection syntax and stopped after building the merged frame — I never wrote the comparison filter or submitted a pandas answer (see §7).

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-05-27 12:51, latest accepted 2026-05-30 15:24)

```sql
WITH cte AS (
    SELECT
        e.id AS emp_id,
        e.first_name AS emp_name,
        e.salary AS emp_sal,
        m.id AS manager_id,
        m.first_name AS manager_name,
        m.salary AS manager_sal
    FROM employee AS e INNER JOIN employee AS m ON e.manager_id = m.id
)

SELECT
    emp_name,
    emp_sal
FROM cte
WHERE emp_sal > manager_sal;
```

> Note: I kept all six columns in the CTE (manager's `id`/`name` included) even though the final answer only needs `emp_name, emp_sal`. It cost nothing here and made the join self-documenting while I debugged. The `INNER JOIN` is correct — an employee with no manager (`manager_id` that matches no `id`, or NULL) drops out, which is what we want since "more than their manager" is undefined without a manager.

### 4.2 Python (pandas)

⚠️ Not attempted / no accepted submission in my history. (I ran exploratory pandas on 2026-05-30 and built the self-merge, but never wrote the filter or submitted — see §7.)

### 4.3 PySpark

⚠️ Not attempted / no accepted submission in my history.

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)`.
> Premium-only feature. Verbatim per language.

### 5.1 PostgreSQL

```sql
WITH cte_managers AS
  (SELECT id,
          salary
   FROM employee)
SELECT e.first_name AS first_name,
       e.salary AS salary
FROM employee e
JOIN cte_managers m ON e.manager_id = m.id
WHERE e.salary > m.salary;
```

### 5.2 Python (pandas)

```python
import pandas as pd
import numpy as np

merged = pd.merge(employee,employee, left_on = 'manager_id', right_on = 'id')
result = merged[merged['salary_x'] > merged['salary_y']][['first_name_x','salary_x']]
```

### 5.3 PySpark

```python
import pyspark.sql.functions as F

merged = employee.alias("e1").join(employee.alias("e2"), F.col("e1.manager_id") == F.col("e2.id"))
result = merged.filter(F.col("e1.salary") > F.col("e2.salary")).select("e1.first_name", "e1.salary")
result.toPandas()
```

> StrataScratch also ships MySQL, MSSQL, Oracle, R, and Polars solutions for 9894 (the SQL dialects are byte-identical to 5.1; R uses a `dplyr` `left_join`-on-self with `suffix = c("mngr","emp")`; Polars self-joins on a renamed `manager_id`). The three above are the canonical engines I track.

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| CTE contents | CTE selects **all six** columns (both ids, both names, both salaries) from the joined pair | CTE (`cte_managers`) selects only `id, salary` from a **single, un-joined** copy of `employee`, then joins it in the outer query | Author's CTE is just a thin "managers lookup"; mine pre-joins everything. Both correct; author moves the join to the outer SELECT, I move it into the CTE |
| Where the join lives | Inside the CTE (`FROM employee e JOIN employee m`) | In the outer query (`FROM employee e JOIN cte_managers m`) | Two valid placements of the same self-join; functionally identical |
| Filter column reference | `WHERE emp_sal > manager_sal` (CTE aliases) | `WHERE e.salary > m.salary` (table-qualified) | Same comparison; mine reads off renamed CTE columns, author off qualified raw columns |
| Output column names | `emp_name, emp_sal` (renamed) | `first_name AS first_name, salary AS salary` (original names preserved) | The grader matches on position/values, so my renames pass — but author keeps the literal `first_name`/`salary` headers the prompt names |
| Output column order | `emp_name, emp_sal` | `first_name, salary` | Same two columns, same order, same meaning — both accepted |

**Key insight in one line:** the canonical idiom is "self-join `employee` to `employee` on `child.manager_id = parent.id`, then `WHERE child.salary > parent.salary`" — whether you stuff the join into a CTE (mine) or expose just the manager columns via a CTE and join in the outer query (author) is pure stylistic taste; the self-join + row-vs-row comparison is the whole problem.

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=9894)` — **16 attempts total**: PostgreSQL (code_type 1) across 2026-05-27 and 2026-05-30, plus a pandas (code_type 2) exploration session on 2026-05-30. Chronological:

| # | Time (UTC) | Language | Status | What happened |
|---|---|---|---|---|
| 1 | 2026-05-27 12:45 | SQL | run | `select * from employee` — inspect the table, spot `id` / `manager_id` / `salary` |
| 2 | 2026-05-27 12:49 | SQL | ❌ run | Self-join SELECT but stray comma `e.first_name, as emp_name` → `syntax error at or near "as"` |
| 3 | 2026-05-27 12:50 | SQL | run | Fixed the comma; clean self-join SELECT showing both salaries |
| 4 | 2026-05-27 12:51 | SQL | ❌ run | Wrapped in CTE but left a trailing comma after `...m.id),` → `syntax error at or near "select"` |
| 5 | 2026-05-27 12:51 | SQL | run | Removed the comma; lowercase CTE version runs clean |
| 6 | 2026-05-27 12:51 | SQL | ✅ **submitted** | Same query, formatted/uppercased → **accepted** (first solve) |
| 7 | 2026-05-30 09:11 | SQL | run | Re-ran the lowercase version cold (warm-up) |
| 8 | 2026-05-30 09:11 | SQL | ✅ **submitted** | Formatted version → **accepted** again |
| 9 | 2026-05-30 15:24 | SQL | run | Lowercase version once more |
| 10 | 2026-05-30 15:24 | SQL | ✅ **submitted** | Formatted version → **accepted** (latest accepted submission) |
| 11 | 2026-05-30 15:24 | pandas | run | Switched to pandas: `employee.head()` to see columns |
| 12 | 2026-05-30 15:32 | pandas | ❌ run | `employee.loc[["id","first_name",...]]` → `KeyError` (passing column names to `.loc` row-indexer) |
| 13 | 2026-05-30 15:35 | pandas | ❌ run | Same `.loc[[...]]` mistake → `KeyError` again |
| 14 | 2026-05-30 15:35 | pandas | ❌ run | Tried `employee.loc[,[...]]` → `SyntaxError: invalid syntax` (empty row slot) |
| 15 | 2026-05-30 15:35 | pandas | run | Switched to `employee[["id","first_name",...]]` — correct column selection, runs clean |
| 16 | 2026-05-30 15:36 | pandas | run | Built `emp_man = pd.merge(emp, man, on='manager_id')` and `.head()` — **stopped here**: no filter, no submission |

What the failed attempts taught me:
- **The self-join itself was never the problem — punctuation was.** Two of my three SQL errors were a misplaced comma (`first_name, as`) and a trailing comma after the CTE's closing paren. A comma right after `)` makes Postgres expect a *second* CTE, so it chokes on the `select`. When a CTE query errors "at or near select," check for a stray comma after the previous block before anything else.
- **`.loc[[...]]` is a row indexer, not a column selector.** Passing column names as a list to `.loc` makes pandas look for those *labels in the index* → `KeyError`. To pick columns, use plain bracket indexing `df[["col1","col2"]]` (or `.loc[:, ["col1","col2"]]` with the explicit row slot). I burned attempts 12–14 on this; the author just self-merges and never needs the column pre-selection at all.
- **I left pandas unfinished.** I got as far as the self-merge (`pd.merge(employee, employee, ...)`) but never wrote `merged[merged['salary_x'] > merged['salary_y']]` or submitted. The hard part (the self-join) was done; I quit one line before the filter. This is the real gap to close on this problem.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context.

### 8.1 Correctness
- **Score**: 5/5
- Comment: Your PostgreSQL self-join is exactly right — `e.manager_id = m.id` then `WHERE emp_sal > manager_sal` answers the prompt with no off-by-one or duplication risk. The strict `>` correctly excludes equal-salary pairs, and `INNER JOIN` correctly drops managerless rows. I can only score the engine you submitted; pandas (8 attempts, abandoned at the merge) and PySpark (untouched) are not assessed here, but the one solution you shipped is fully correct.

### 8.2 Performance & Efficiency
- **Score**: 4/5
- Comment: The self-join is the right shape and there is no window-function or subquery waste. One inefficiency: your CTE materializes **six columns** (`emp_id, manager_id, manager_name`) when the outer query reads only `emp_name, emp_sal, manager_sal`. The planner will usually prune these, but the author's `cte_managers` projecting just `id, salary` is the leaner pattern. Fix: trim the CTE projection to the three columns the outer query actually touches.

### 8.3 Readability
- **Score**: 5/5
- Comment: This is clean. Aliases `e`/`m` for child/parent are conventional, the `emp_*`/`manager_*` renames make the comparison `emp_sal > manager_sal` read in plain English, and your indentation and uppercased keywords are consistent. The inline note justifying `INNER JOIN` is the kind of self-documentation a reviewer wants to see.

### 8.4 Edge Cases
- **Score**: 4/5
- Comment: You reasoned about the two that matter — managerless employees (NULL/unmatched `manager_id` dropped by the inner join) and the strict-vs-non-strict boundary. The gap: a self-referencing `manager_id = id` row (someone listed as their own manager) would satisfy the join, and `salary > salary` is false so it harmlessly drops — but you did not name it. Fix: state out loud that the strict `>` also neutralizes the self-manager case, so a reviewer sees you checked it.

### 8.5 Interview-Readiness
- **Score**: 3/5
- Comment: Your write-up names the right assumptions (why `INNER JOIN`, why `>`), which would score well **if verbalized live**. But interview-readiness is multi-engine, and here the signal is thin: you shipped one language and stalled in pandas at attempt 16 — one line short of `merged[merged['salary_x'] > merged['salary_y']]`. In a real loop, "I got the self-merge but couldn't finish the filter" reads as a gap. Fix: drill the pandas/PySpark ports to completion so you can solve this cold in any of the three engines on demand.

### 8.6 Verdict
The self-join logic is locked — conceptually you saw the whole problem from the first SELECT, and every error in your history was punctuation (`first_name, as`, the trailing comma after `)`), not reasoning. That is a good place to be on a Medium. The ONE thing that moves you to the next level: stop quitting one step short in the non-SQL engines. You abandoned pandas at the merge and never opened PySpark, so a problem you genuinely understand still looks half-finished on paper. Finishing the port every time — even when SQL already passed — is what converts "I know the idiom" into "I can produce it under interview pressure in any engine."

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Self-join a table to itself | `FROM employee e JOIN employee m ON e.manager_id = m.id` | `pd.merge(employee, employee, left_on='manager_id', right_on='id')` | `employee.alias("e1").join(employee.alias("e2"), F.col("e1.manager_id")==F.col("e2.id"))` |
| Disambiguate the two copies | table aliases `e` / `m` | suffixes `_x` (left/employee) / `_y` (right/manager) auto-added by merge | `.alias("e1")` / `.alias("e2")` |
| Row-vs-row comparison filter | `WHERE e.salary > m.salary` | `merged[merged['salary_x'] > merged['salary_y']]` | `.filter(F.col("e1.salary") > F.col("e2.salary"))` |
| Select final two columns | `SELECT e.first_name, e.salary` | `[['first_name_x','salary_x']]` | `.select("e1.first_name", "e1.salary")` |
| Pick columns from a frame | n/a | `df[["col1","col2"]]` (NOT `df.loc[["col1","col2"]]`) | `.select("col1","col2")` |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: the **self-join** — when a table has a foreign key pointing back to its own primary key (`manager_id → id`, `parent_id → id`, `reply_to → comment_id`), join the table to a second copy of itself under two aliases, one playing the "child" role and one the "parent." Then any child-vs-parent comparison (`child.salary > parent.salary`) is an ordinary `WHERE` on the joined row. `INNER JOIN` naturally drops rows with no parent, which is usually what you want.

- **Reusable snippet (PostgreSQL, parameterized)**:
  ```sql
  SELECT c.first_name, c.salary
  FROM employee c                              -- child role
  JOIN employee p ON c.manager_id = p.id       -- parent role
  WHERE c.salary > p.salary;                   -- child-vs-parent comparison
  ```
  No CTE is actually required — the CTE in my answer (and the author's) is optional scaffolding.

- **Weakness I noticed in myself**:
  1. **I default to a CTE even when the query is a one-liner.** This problem is a 3-line self-join; I wrapped it in a CTE with six aliased columns. Harmless here, but the reflex adds noise. For a plain self-join, write the join directly and only reach for a CTE when there's a genuine intermediate result to name.
  2. **pandas column-selection muscle memory is weak.** `.loc[[...]]` for columns cost me three failed runs. `df[["a","b"]]` selects columns; `.loc[rows, cols]` needs both slots. I should drill basic pandas indexing — it's the thing that keeps blocking me before I even reach the actual logic.
  3. **I stop one step short in non-SQL engines.** I built the pandas self-merge and quit before the filter. Finishing the port (even when SQL already passed) is how the pandas/PySpark idioms actually stick.

- **Drill for tomorrow morning** (before any new problems):
  1. Re-solve 9894 in pandas end-to-end: `pd.merge(employee, employee, left_on='manager_id', right_on='id')`, then `merged[merged['salary_x'] > merged['salary_y']][['first_name_x','salary_x']]`. Time-box 5 minutes.
  2. Then PySpark: `.alias("e1").join(employee.alias("e2"), ...)` + `.filter(...)` + `.select(...)`. If I can do both cold, the self-join idiom is locked across all three engines.

## 11. Related Notes

- **Same category (Join — self-join on a hierarchy)**: this is the prototypical self-join problem. Pair it with any note where one table references itself — manager/employee hierarchies, comment/reply threads, or category/parent-category trees. The shared move is "join the table to a second alias of itself on the self-referencing key."
- **Adjacent skill — group aggregate vs. self-join**: contrast with the "salary vs. team average" family (e.g. the *Salary Less Than Twice The Average* note), which *also* joins `employee`-style data into manager and report roles but then **aggregates** the reports (`GROUP BY ... HAVING`). 9894 is the simpler sibling: row-vs-row, no aggregation. Knowing when a problem needs `GROUP BY`/`HAVING` versus a plain row comparison is the distinction to internalize.
- **Reference reading**:
  - PostgreSQL self-joins / table aliases: https://www.postgresql.org/docs/current/queries-table-expressions.html
  - pandas merging a frame with itself + `suffixes`: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html
  - pandas selecting columns vs. `.loc` indexing: https://pandas.pydata.org/docs/user_guide/indexing.html

---

*Generated 2026-06-30 by Ina (problem solving) + Claude (author solution fetch via MCP, submission history analysis, FAANG reviewer subagent).*
