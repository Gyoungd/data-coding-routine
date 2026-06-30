# 2110 — Salary Less Than Twice The Average

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 2110 |
| Category | Subquery / CTE |
| Difficulty | Medium |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/2110-salary-less-than-twice-the-average?code_type=1) · [Python](https://platform.stratascratch.com/coding/2110-salary-less-than-twice-the-average?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/2110-salary-less-than-twice-the-average?code_type=6) |
| Solved on | 2026-05-27 |
| Tables | `map_employee_hierarchy(empl_id text, manager_empl_id text)` · `dim_employee(empl_city text, empl_dob date, empl_id text, empl_name text, empl_pin bigint, salary bigint)` |

## 2. Problem Restatement (in my words)

Each manager has a set of direct reports (the `map_employee_hierarchy` table maps every `empl_id` to its `manager_empl_id`). For each manager, compute the average salary of the people who report to them. Keep only the managers whose **own** salary is strictly less than **twice** that average, and output the manager's ID, the manager's salary, and the team's average salary. The trap is that "salary" appears in two roles — the manager's salary and the reports' salaries — both living in the same `dim_employee` table, so you have to join `dim_employee` in twice (once as managers, once as employees), or aggregate first and then re-join for the manager's own salary.

## 3. My Approach

- **First instinct (SQL)**: build one wide table joining the hierarchy to `dim_employee` on `empl_id` to attach each report's salary, filter out rows with no manager (`manager_empl_id IS NOT NULL`), then `GROUP BY` manager to get the average. I reached for `FULL JOIN` from the very first query and never switched off it.
- **Where I got stuck**: two real walls, neither of them the join logic.
  1. **Getting the manager's own salary back.** After aggregating the team average, the manager's salary isn't in that result. I cycled through `LEFT JOIN` / `RIGHT JOIN` / `FULL JOIN` / `INNER JOIN` against `dim_employee` (attempts at 13:46–13:47) trying to re-attach `manager_sal`, and at one point referenced a table alias `d` that wasn't in the FROM clause (`missing FROM-clause entry for table "d"`).
  2. **The `^` operator.** I wrote the filter as `(emp_sal_avg)^2 > j.salary` meaning "twice the average," but in PostgreSQL `^` is exponentiation-like / actually resolves to a numeric power-style operator that gave me silently wrong numbers, NOT multiplication. I also tried `**2` (`operator does not exist: numeric ** integer`). I submitted the `^2` version **five separate times** and the grader rejected every one as incorrect — the SQL parsed fine, so I had no error message telling me why; it just kept coming back wrong.
- **Final strategy**: stop trying to be clever. CTE 1 (`join_table`) = hierarchy `FULL JOIN dim_employee` to attach report salaries, drop null managers. CTE 2 (`avg_sal`) = `GROUP BY manager_id, AVG(salary)`. Then join `avg_sal` back to `join_table` on `manager_id = empl_id` to recover the manager's own salary row, and filter with the corrected arithmetic `(emp_sal_avg) * 2 > j.salary`. The instant I changed `^2` to `*2`, it passed.

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-05-27 14:03)

```sql
WITH join_table AS (
    SELECT
        m.manager_empl_id AS manager_id,
        e.empl_id,
        e.salary
    FROM map_employee_hierarchy AS m FULL JOIN dim_employee AS e ON m.empl_id = e.empl_id
    WHERE m.manager_empl_id IS NOT NULL
),

avg_sal AS (
    SELECT
        manager_id,
        AVG(salary) AS emp_sal_avg
    FROM join_table
    GROUP BY manager_id
)

SELECT
    a.manager_id,
    j.salary AS manager_sal,
    emp_sal_avg
FROM avg_sal AS a INNER JOIN join_table AS j ON a.manager_id = j.empl_id
WHERE (emp_sal_avg) * 2 > j.salary;
```

> Note on the final join: `join_table` already contains every employee row keyed by `empl_id`, so joining `avg_sal.manager_id = join_table.empl_id` recovers the manager's own salary (a manager is also an employee row). This works because the seed data has each manager appear exactly once as an `empl_id`; if a manager could match multiple `empl_id` rows this re-join would duplicate. The author avoids this fragility by joining `dim_employee` directly for the manager — see Section 6.

### 4.2 Python (pandas)

⚠️ Not attempted / no accepted submission in my history.

### 4.3 PySpark

⚠️ Not attempted / no accepted submission in my history.

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)`.
> Premium-only feature.

### 5.1 PostgreSQL

```sql
SELECT h.manager_empl_id,
       managers.salary AS manager_salary,
       AVG(employees.salary) AS avg_employee_salary
FROM map_employee_hierarchy h
JOIN dim_employee managers ON h.manager_empl_id = managers.empl_id
JOIN dim_employee employees ON h.empl_id = employees.empl_id
GROUP BY 1,2
HAVING managers.salary < 2 * AVG(employees.salary)
```

### 5.2 Python (pandas)

```python
managers_salaries = map_employee_hierarchy.merge(dim_employee, left_on='manager_empl_id', right_on='empl_id', suffixes=('','_manager'))
managers_employees_salaries = managers_salaries.merge(dim_employee,on='empl_id', suffixes=('_manager','_employee'))[['manager_empl_id', 'salary_manager', 'salary_employee']]
aggregated_by_manager = managers_employees_salaries.groupby(['manager_empl_id', 'salary_manager']).mean().reset_index().rename({'salary_employee': 'avg_employee_salary'}, axis=1)
result = aggregated_by_manager[aggregated_by_manager['salary_manager'] < 2 * aggregated_by_manager['avg_employee_salary']]
```

### 5.3 PySpark

```python
# Import your libraries
import pyspark.sql.functions as F

# Start writing code
df=map_employee_hierarchy\
    .join(dim_employee, dim_employee.empl_id==map_employee_hierarchy.empl_id)\
    .groupBy('manager_empl_id')\
    .agg(F.avg('salary').alias('employees_avg_salary'))\
    .join(dim_employee, F.col('manager_empl_id')==F.col('empl_id'))\
    .selectExpr('manager_empl_id as manager', 'employees_avg_salary', 'salary')\
    .where('(salary)<employees_avg_salary*2')

df.toPandas()
```

> For reference, StrataScratch also ships MySQL, MSSQL, Oracle, R, and Polars
> solutions for this problem; the three above are the canonical engines I track.

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| Manager-salary source | Re-join `avg_sal` back to `join_table` on `manager_id = empl_id` (recovers the manager's row from the same flattened employee set) | Joins `dim_employee` **twice** with explicit aliases `managers` and `employees` — one join supplies the manager's salary, the other supplies report salaries | Author's intent is self-documenting; mine relies on the manager happening to appear as an `empl_id` row in `join_table` |
| Filter mechanism | `WHERE (emp_sal_avg) * 2 > j.salary` after a second CTE that aggregates | `HAVING managers.salary < 2 * AVG(employees.salary)` directly on the grouped query | Author filters the aggregate inline with `HAVING` — one fewer CTE, no re-join needed |
| Join type | `FULL JOIN` + `WHERE manager_empl_id IS NOT NULL` to discard the unmatched side | Plain `INNER JOIN` (×2) | A `FULL JOIN` then filtering nulls is functionally an inner join here; the author just writes the inner join directly. My `FULL JOIN` does extra work then throws half of it away |
| Number of passes | 2 CTEs + a final join = 3 logical steps | 1 grouped SELECT with `HAVING` = 1 step | Author is dramatically tighter |
| Output column order | `manager_id, manager_sal, emp_sal_avg` | `manager_empl_id, manager_salary, avg_employee_salary` | Same three columns, same order, same meaning — both accepted |

**Key insight in one line:** the canonical idiom is "self-join `dim_employee` twice (managers + employees), `GROUP BY` the manager, then `HAVING manager.salary < 2 * AVG(employee.salary)`" — `HAVING` is built for exactly this aggregate comparison and replaces my entire second CTE + re-join.

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=2110)` — **52 attempts total**, all PostgreSQL (code_type 1), all on 2026-05-27. Highlights:

| # | Time (UTC) | Language | Status | What happened |
|---|---|---|---|---|
| 1 | 13:27 | SQL | run | `SELECT * FROM map_employee_hierarchy` — inspect the mapping table |
| 2 | 13:29 | SQL | run | `SELECT * FROM dim_employee` — inspect employee table |
| 3 | 13:33 | SQL | ❌ run | Typo'd table name `emmap_employee_hierarchy` (relation does not exist) |
| 4–6 | 13:33 | SQL | run | Tried `LEFT` / `RIGHT` / `FULL JOIN` to see which keeps which side |
| 7 | 13:34 | SQL | run | Wrapped in CTE, checked `WHERE manager_id IS NULL` to find the top of the hierarchy |
| 8 | 13:39 | SQL | ❌ run | Referenced CTE alias `manager_id` in the `WHERE` of the same SELECT that defines it (column does not exist) |
| 9 | 13:39 | SQL | run | Added `avg_sal` CTE using `AVG(...) OVER (PARTITION BY manager_id)` (window-function approach) |
| 10–11 | 13:42 | SQL | ❌ run | `emp_sal_avg**2` — `operator does not exist: numeric ** integer`; also stray `select * from avg_sal` glued before the next SELECT |
| 12 | 13:43 | SQL | ❌ run | Still `**2`, same power-operator error |
| 13 | 13:44 | SQL | ❌ **submitted** | Switched to `^2`; parses but **grader says incorrect** (`^` is not multiplication) |
| 14 | 13:45 | SQL | ❌ **submitted** | Dropped `emp_sal_avg` from the SELECT list — wrong output shape, still `^2` |
| 15 | 13:45 | SQL | ❌ **submitted** | Re-added the column; still `^2` → incorrect |
| 16 | 13:46 | SQL | ❌ **submitted** | Re-joined `dim_employee` (`d`) for the manager salary; still `^2` → incorrect |
| 17 | 13:47 | SQL | ❌ run | Referenced alias `d` that wasn't in FROM (`missing FROM-clause entry for table "d"`) |
| 18 | 13:48 | SQL | ❌ **submitted** | Window-function version with `^2` → incorrect |
| 19–25 | 13:51–13:55 | SQL | run/❌ | Refactored `avg_sal` to plain `GROUP BY` (no window); misc syntax slips (`group by` before `from`, missing `select` keyword) |
| 26–32 | 13:54–13:56 | SQL | ❌ run | Side-quest: `empl_id == 'E851'` (`==` not valid in SQL), `empl_id = "E851"` (double quotes = identifier, not string), `empl_id like "E851"` — all string-literal mistakes |
| 33 | 13:52 | SQL | ❌ **submitted** | Plain `GROUP BY` version but STILL `^2` → incorrect |
| 34–40 | 13:57–14:01 | SQL | run/❌ | Tried `DISTINCT ON` / `distinct` / `unique` inside SELECT to dedupe `empl_id` (all syntax errors); `elect` typo |
| 41 | 14:01 | SQL | ❌ run | Final `^2` run before the fix |
| 42 | 14:03 | SQL | run | **Changed `^2` to `*2`** — first clean run of the correct query |
| 43 | 14:03 | SQL | ✅ **submitted** | Same query, formatted, `(emp_sal_avg) * 2 > j.salary` → **accepted** |

What the failed attempts taught me:
- **`^` is NOT exponentiation-as-I-expected, and definitely not multiplication, in PostgreSQL.** I burned ~30 minutes and **five rejected submissions** on `(emp_sal_avg)^2`. Because the SQL *parsed*, there was no error to read — the grader just kept saying "incorrect" with no hint. For "twice X" the operator is plain `* 2`. Lesson: when a query parses but the grader rejects it repeatedly, suspect a **silently-wrong operator or arithmetic**, not the join.
- **String literals use single quotes; `=` is the equality operator, not `==`.** My `empl_id == 'E851'`, `= "E851"`, and `like "E851"` detours were all the same root error (Python/pandas habits leaking into SQL). Double quotes mean "identifier," so `"E851"` was read as a column name.
- **You can't reference a CTE/SELECT alias in the `WHERE` of the same SELECT that creates it** (attempt 8: `WHERE manager_id IS NULL` where `manager_id` is the alias defined in that SELECT). Filter on the underlying column, or push the filter to an outer query.
- **`FULL JOIN` + `WHERE ... IS NOT NULL` was wasted motion** — it's an inner join with extra steps. When I only want matched rows, just write `JOIN`.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context.

### 8.1 Correctness
- **Score**: 3/5
- Comment: Your final PostgreSQL query returns the correct result set, but correctness here is fragile and partial. The re-join `a.manager_id = j.empl_id` only recovers `manager_sal` because each manager surfaces exactly once as an `empl_id` row — you flagged this yourself in the Section 4 note, which is the honest call. More to the point, you have no accepted pandas or PySpark submission, so on two of the three engines this problem is unsolved; I can only score the engine you shipped.

### 8.2 Performance & Efficiency
- **Score**: 3/5
- Comment: The `FULL JOIN ... WHERE m.manager_empl_id IS NOT NULL` materializes both unmatched sides and then discards them — that is an inner join paying full freight. Concrete fix: replace it with a plain `JOIN` so the planner never builds the outer rows. You also pay for a second CTE plus a final re-join to do what the author's single `HAVING managers.salary < 2 * AVG(employees.salary)` does in one grouped pass.

### 8.3 Readability
- **Score**: 4/5
- Comment: Clean CTE names (`join_table`, `avg_sal`), consistent aliasing, and explicit column lists — no `SELECT *` in the final query, good. One polish: in the outer `SELECT` you reference `emp_sal_avg` bare while qualifying `a.manager_id` and `j.salary`; qualify it as `a.emp_sal_avg` so a reader never has to trace which CTE owns the column.

### 8.4 Edge Cases
- **Score**: 2/5
- Comment: The duplicate-manager risk in the re-join is named but not defended — and `> j.salary` vs the author's `< 2 * AVG` quietly flips the comparison direction, which works only because you swapped operands too; one slip there is a silent wrong answer. Concrete fix: state the boundary out loud — "strictly less than twice" means a manager paid exactly `2 * avg` is excluded — and confirm your `*2 > salary` encodes that strict bound, not `>=`.

### 8.5 Interview-Readiness
- **Score**: 2/5
- Comment: Fifty-two attempts, five rejected submissions, all on the `^2` trap — and crucially you submitted blind five times against a grader with no feedback instead of building a two-row sanity check to see that `^` was returning power, not double. In a live loop, that loop is a no-hire signal on its own. Concrete fix: before submitting, narrate one assumption ("salary is the manager's own row, average is over reports") and run the arithmetic on one known manager to verify `*2` before trusting the grader.

### 8.6 Verdict
You got the right rows on PostgreSQL, but the path there — a `FULL JOIN` you immediately undo, a second CTE plus re-join standing in for one `HAVING`, and five identical blind submissions on a parsed-but-wrong operator — is the path of someone debugging by resubmission, not by reasoning. The one thing that moves you up: when a query parses but the grader rejects it, stop submitting and probe the arithmetic on a single known row — that habit would have killed the `^2` bug in one minute instead of thirty. Then close the real gap: you have zero accepted pandas and PySpark solutions here, so port this to `.merge` ×2 + boolean filter and `.groupBy().agg(F.avg)` + `.where`, because a FAANG loop will ask for at least one of them and right now that is unbuilt.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Compare aggregate to a row value | `HAVING managers.salary < 2 * AVG(employees.salary)` | filter after `groupby`: `df[df['salary_manager'] < 2 * df['avg_employee_salary']]` | `.where('salary < employees_avg_salary * 2')` after `.agg(F.avg(...))` |
| "Twice X" / multiply | `2 * x` or `x * 2` (NOT `x^2`, NOT `x**2`) | `2 * x` | `x * 2` |
| Self-join one table for two roles | `JOIN dim_employee managers ... JOIN dim_employee employees ...` | two `.merge(dim_employee, ...)` with `suffixes=('_manager','_employee')` | two `.join(dim_employee, ...)` chained |
| Group then average | `GROUP BY manager ... AVG(salary)` | `.groupby([...]).mean()` | `.groupBy('manager_empl_id').agg(F.avg('salary'))` |
| String literal | single quotes `'E851'` | `'E851'` / `"E851"` both fine | `'E851'` |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: "group-level aggregate vs. a per-group attribute" → join the dimension table in twice (one alias for the parent/manager, one for the children/reports), `GROUP BY` the parent, and put the comparison in **`HAVING`** (`HAVING parent.value < k * AVG(child.value)`). `HAVING` exists precisely to filter on an aggregate; it deletes the need for a second CTE that re-aggregates and re-joins.

- **Reusable snippet (PostgreSQL, parameterized)**:
  ```sql
  SELECT h.parent_id,
         p.metric AS parent_metric,
         AVG(c.metric) AS child_avg
  FROM hierarchy h
  JOIN dim p ON h.parent_id = p.id   -- parent role
  JOIN dim c ON h.child_id  = c.id   -- child role
  GROUP BY 1, 2
  HAVING p.metric < :k * AVG(c.metric);
  ```

- **Weakness I noticed in myself**:
  1. **Operator literacy gap.** I assumed `^` meant power (and that "power" was somehow what I wanted for "twice"). I never wanted exponentiation at all — "twice" is `* 2`. Five rejected submissions because the query parsed and I had no error to anchor on. Drill: keep a tiny cheat-line — *"twice = `*2`; power in Postgres = `power(x, n)`; `^` and `**` are traps."*
  2. **I default to `FULL JOIN` and window functions before reaching for the simplest tool.** The author's one-statement `GROUP BY ... HAVING` is the textbook answer; I built two CTEs, a window function, and a re-join to get there. Reflex to build: aggregate-comparison → `HAVING`.
  3. **SQL/pandas syntax bleed.** `==`, double-quoted strings, and referencing a SELECT alias in its own `WHERE` are all habits from other languages. When I switch into SQL, slow down on equality (`=`) and string quoting (`'...'`).

- **Drill for tomorrow morning** (before any new problems):
  1. Re-solve 2110 cold in PostgreSQL using the author's `JOIN dim_employee` ×2 + `HAVING` pattern — no CTEs, no window function. Time-box 8 minutes.
  2. Then port it to pandas (`.merge` ×2 + `groupby` + boolean filter) since I have zero pandas/PySpark attempts on this one. If I can't, that's the real gap to close.

## 11. Related Notes

- **Same category (Subquery / CTE — "compare to an aggregate")**: other Subquery/CTE notes in our queue that hinge on "value vs. the average / twice the average / same-group aggregate." This problem is the prototypical `HAVING aggregate-comparison` exercise; pair it with any "above/below average" problem.
- **Adjacent skills exercised**: self-join of a single dimension table into two roles (manager vs. employee) — see other Join (multi-table) notes that reuse one table under two aliases.
- **Reference reading**:
  - PostgreSQL math operators (why `^` is a trap, what `*` and `power()` do): https://www.postgresql.org/docs/current/functions-math.html
  - `GROUP BY` / `HAVING` (filtering on aggregates): https://www.postgresql.org/docs/current/sql-select.html#SQL-HAVING
  - pandas `merge` with `suffixes`: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html

---

*Generated 2026-06-30 by Ina (problem solving) + Claude (author solution fetch via MCP, submission history analysis, FAANG reviewer subagent).*
