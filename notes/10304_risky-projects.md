# 10304 — Risky Projects

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 10304 |
| Category | Join (multi-table) |
| Difficulty | Medium |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/10304-risky-projects?code_type=1) · [Python](https://platform.stratascratch.com/coding/10304-risky-projects?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/10304-risky-projects?code_type=6) |
| Solved on | 2025-07-07 (pandas only) |
| Tables | `linkedin_projects(budget bigint, end_date date, id bigint, start_date date, title text)` · `linkedin_emp_projects(emp_id bigint, project_id bigint)` · `linkedin_employees(first_name text, id bigint, last_name text, salary bigint)` |

## 2. Problem Restatement (in my words)

Three tables: projects (with a budget and a start/end date), employees (with an annual salary), and a bridge table that says which employees are assigned to which projects. For each project, take every assigned employee's annual salary and **prorate** it by how long the project ran — `salary × (days between start and end) / 365` — then sum those prorated amounts across all the project's employees. That sum is the project's true labour cost. Round it **up** to the next whole dollar (ceiling, not ordinary rounding), and keep only the projects where that cost is strictly greater than the budget. Output project title, budget, and the rounded-up total expense. Treat every year as 365 days. The two traps are (a) it's a three-table join through the bridge, and (b) "round up to the nearest dollar" means **ceiling**, not `.round()`.

## 3. My Approach

- **First instinct (pandas)**: chain two `merge`s — `linkedin_projects` → `linkedin_emp_projects` on `id = project_id`, then → `linkedin_employees` on `emp_id = id` — to get one flat row per (project, employee). Trim to the columns I need and rename `title` → `pjt_name`. Compute `period = (end_date − start_date).days`, a per-employee `unit_pay = salary/365`, and `payment = unit_pay × period`. Then `groupby` project, `sum` the payments, and filter `budget < total`.
- **Where I got stuck** — six distinct walls, in order:
  1. **`rename` syntax.** Burned three attempts on `.rename('title' = 'pjt_name')`, `.rename(df['title'] = ...)`, `.rename(title = ...)` before landing on the correct `.rename(columns={'title':'pjt_name'})`.
  2. **Datetime double-brackets.** `pd.to_datetime(df[['end_date']])` (DataFrame) raised `ValueError: to assemble mappings requires ... [year, month, day]`; single brackets (Series) fixed it.
  3. **Timedelta → number.** After subtracting dates I had a timedelta; I divided by `pd.Timedelta(days=1)` to get a day count (only switched to the cleaner `.dt.days` near the very end).
  4. **`groupby('pjt_name','budget')` passed positionally.** This was my most-repeated bug — pandas reads the second positional arg as `axis`, so it threw `ValueError: No axis named budget` five separate times. The fix is a **list**: `groupby(['pjt_name','budget'])`. I even fixed it once and then regressed back to the positional form.
  5. **Filtering on a column that no longer exists after aggregation.** After renaming the summed column to `total_emp_expense`, I kept writing `df2[df2['payment'] > ...]` → `KeyError: 'payment'`. I also shipped a no-op self-comparison (`total_emp_expense > total_emp_expense`, always empty) and once had the operator backwards (`budget > total`, which finds *under*-budget projects).
  6. **Rounding vs. ceiling — the real final blocker.** The spec says "round **up**," but I defaulted to `.round()`, then `.round(0)`, submitting four times and getting rejected each time because rounding ≠ ceiling. I tried `Series.ceil()` (doesn't exist → `AttributeError`), then `np.ceil(...)` (forgot the import → `NameError: name 'np' is not defined`).
- **Final strategy**: keep the two-merge flat table, `groupby(['pjt_name','budget'])['payment'].sum()`, and round the grouped total with **`np.ceil`** after adding `import numpy as np`. Filter `budget < total_emp_expense`. The instant numpy was imported so `np.ceil` could run, it passed. By that point everything except the rounding had already been correct for several attempts.

## 4. My Solutions

### 4.1 PostgreSQL

⚠️ Not attempted / no accepted submission in my history.

### 4.2 Python (pandas) ✅ (passed 2025-07-07 10:24)

```python
# Import your libraries
import pandas as pd
import numpy as np

# Start writing code
# allocate(=prorate) each emp annual salary
# output: list of overbudget pjt
# columns: pjt name, budget, total emp expense(round up the nearest dollar)

pjt_emp = linkedin_projects.merge(linkedin_emp_projects, left_on = 'id', right_on='project_id')
df = pjt_emp.merge(linkedin_employees, left_on = 'emp_id', right_on='id')

df = df[['title', 'budget','start_date','end_date','emp_id','salary']].rename(columns = { 'title' : 'pjt_name'})

# Calculate pjt period
df['period'] = (pd.to_datetime(df['end_date']) - pd.to_datetime(df['start_date'])).dt.days

# Calculate used salary
df['unit_pay'] = df['salary']/365

df['payment'] = (df['unit_pay'] * df['period'])


# Get the total_emp_expense per pjt
df2 = df.groupby(['pjt_name','budget'])['payment'].sum().reset_index(name = 'total_emp_expense')
df2['total_emp_expense'] = np.ceil(df2['total_emp_expense'])

result = df2[df2['budget'] < df2['total_emp_expense']]
result
```

### 4.3 PySpark

⚠️ Not attempted / no accepted submission in my history.

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)`.
> Premium-only feature.

### 5.1 PostgreSQL

```sql
SELECT a.title,
       a.budget,
       CEILING((a.end_date - a.start_date) * (SUM(c.salary) / 365)) AS prorated_employee_expense
FROM linkedin_projects a
INNER JOIN linkedin_emp_projects b ON a.id = b.project_id
INNER JOIN linkedin_employees c ON b.emp_id = c.id
GROUP BY a.title,
         a.budget,
         a.end_date,
         a.start_date
HAVING CEILING((a.end_date - a.start_date) * (SUM(c.salary) / 365)) > a.budget
ORDER BY a.title ASC;
```

### 5.2 Python (pandas)

```python
import pandas as pd
import numpy as np
from datetime import datetime

df = pd.merge(linkedin_projects, linkedin_emp_projects, how = 'inner',left_on = ['id'], right_on=['project_id'])
df1 = pd.merge(df, linkedin_employees, how = 'inner',left_on = ['emp_id'], right_on=['id'])
df1['project_duration'] = (pd.to_datetime(df1['end_date']) - pd.to_datetime(df1['start_date'])).dt.days
df_expense = df1.groupby('title')['salary'].sum().reset_index(name='expense')
df_budget_expense = pd.merge(df1, df_expense, how = 'left',left_on = ['title'], right_on=['title'])
df_budget_expense['prorated_expense'] = np.ceil(df_budget_expense['expense']*(df_budget_expense['project_duration'])/365)
df_budget_expense['budget_diff'] = df_budget_expense['prorated_expense'] - df_budget_expense['budget']
df_over_budget = df_budget_expense[df_budget_expense["budget_diff"] > 0]
result = df_over_budget[['title','budget','prorated_expense']]
result = result.drop_duplicates().sort_values('title')
```

### 5.3 PySpark

```python
import pyspark.sql.functions as F
from pyspark.sql import Window

df = linkedin_projects.join(linkedin_emp_projects, linkedin_projects.id == linkedin_emp_projects.project_id, "inner") \
    .join(linkedin_employees, linkedin_emp_projects.emp_id == linkedin_employees.id, "inner")

df = df.withColumn("project_duration", (F.to_date(df.end_date) - F.to_date(df.start_date)).cast("integer"))

df_expense = df.groupBy("title").agg(F.sum("salary").alias("expense"))
df_budget_expense = df.join(df_expense, "title", "left")

df_budget_expense = df_budget_expense.withColumn("prorated_expense", F.ceil(df_budget_expense.expense * df_budget_expense.project_duration / 365))
df_budget_expense = df_budget_expense.withColumn("budget_diff", df_budget_expense.prorated_expense - df_budget_expense.budget)

df_over_budget = df_budget_expense.filter(df_budget_expense.budget_diff > 0)

window = Window.partitionBy("title").orderBy("title")
result = df_over_budget.select("title", "budget", "prorated_expense").distinct().orderBy("title").withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")

result.toPandas()
```

> For reference, StrataScratch also ships MySQL, MSSQL, Oracle, R, and Polars
> author solutions for this problem; the three engines above are the canonical ones I track.
> One notable difference across them: the author's **pandas** and **PySpark** solutions sum
> `salary` first and *then* multiply by the per-row `project_duration/365` — which only gives
> the right answer because every employee on a given project shares that project's single
> duration. The **SQL/MySQL/Oracle** versions instead compute `(end - start) * SUM(salary) / 365`
> in one grouped expression. My pandas prorated each row individually (`unit_pay × period`) and
> *then* summed — algebraically identical here, and arguably more robust if employees ever had
> per-assignment periods.

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine (pandas) | Author (pandas) | Impact |
|---|---|---|---|
| Proration order | Per-row `payment = (salary/365) * period`, **then** `groupby.sum` | `groupby('title').sum(salary)` **then** `× project_duration/365` | Same result because all employees on a project share one duration; my per-row order is the more general/defensive shape |
| Ceiling location | `np.ceil` applied **once**, on the grouped `total_emp_expense` | `np.ceil` applied on the **merged row-level** `expense*duration/365`, before dropping duplicates | Both reach the same integers; mine ceils a single scalar per project, author ceils a repeated row value then de-dups |
| Dedup strategy | Not needed — `groupby` collapses to one row per project before the filter | Needs `.drop_duplicates()` because the expense was merged back onto every employee row | Author's "merge the aggregate back, then de-dup" pattern adds a `drop_duplicates` step mine avoids |
| Grouping keys | `groupby(['pjt_name','budget'])` (carries budget through) | `groupby('title')` then `merge` budget back from `df1` | Mine keeps `budget` in the group key so it survives aggregation; author re-attaches it via the left-merge |
| Filter expression | `df2[df2['budget'] < df2['total_emp_expense']]` | compute `budget_diff = prorated_expense − budget`, keep `budget_diff > 0` | Equivalent; author materializes a diff column, I compare inline |
| Sort | None (relied on default order; still accepted) | `.sort_values('title')` | Author sorts by title to match the SQL `ORDER BY a.title`; mine passed without an explicit sort |
| Output columns | `pjt_name, budget, total_emp_expense` | `title, budget, prorated_expense` | Same three columns, same order, just different names — both accepted |

**Key insight in one line:** because every employee on a project shares that project's single start/end window, "prorate-then-sum" (mine) and "sum-then-prorate" (author) are algebraically identical — but the real graded difference on this problem is **ceiling vs. ordinary rounding**, and `np.ceil` (not `Series.round()` and not the non-existent `Series.ceil()`) is the only thing that satisfies "round up to the nearest dollar."

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=10304)` — **55 attempts total**, every one **pandas (code_type 2)**, all in a single session on **2025-07-07 (UTC)**. 46 runs, 9 submissions, of which **1 accepted and 8 rejected**. Highlights (times UTC HH:MM):

| # | Time | Language | Status | What happened |
|---|---|---|---|---|
| 1–3 | 09:41–09:49 | pandas | run | Inspected each table with `.head()` (projects, emp_projects, employees) |
| 4–5 | 09:53–09:55 | pandas | run | Chained the two `merge`s; selected the working columns |
| 6 | 09:55 | pandas | ❌ run | `.rename('title' = 'pjt_name')` → SyntaxError (assignment in expression) |
| 7 | 09:56 | pandas | ❌ run | `.rename(df['title'] = 'pjt_name')` → SyntaxError |
| 8 | 09:56 | pandas | ❌ run | `.rename(title = 'pjt_name')` → TypeError: unexpected keyword 'title' |
| 9 | 09:56 | pandas | run | Fixed: `.rename(columns={'title':'pjt_name'})` |
| 10 | 09:59 | pandas | ❌ run | `pd.to_datetime(df[['end_date']])` (double brackets) → ValueError: requires [year, month, day] |
| 11 | 10:00 | pandas | run | Single brackets fix the datetime subtraction |
| 12 | 10:02 | pandas | run | Converted timedelta to days via `/ pd.Timedelta(days=1)` |
| 13–17 | 10:03–10:07 | pandas | run | Added `unit_pay`, `payment`; first tried an `overbudget` boolean flag column |
| 18 | 10:07 | pandas | ❌ **submitted** | `overbudget == True`, output row-level `[pjt_name,budget,payment]` → incorrect |
| 20 | 10:07 | pandas | ❌ **submitted** | Flipped to `overbudget == False` → incorrect |
| 21 | 10:09 | pandas | ❌ run | First `groupby('pjt_name','budget')` → ValueError: No axis named budget (positional args) |
| 23,26,27 | 10:10–10:11 | pandas | ❌ run | Same `No axis named budget` bug repeated (still passing keys positionally) |
| 29 | 10:12 | pandas | ❌ run | Fixed to `groupby(['pjt_name','budget'])` but filtered `df2['payment']` → KeyError (column renamed away) |
| 30 | 10:12 | pandas | ❌ run | **Regressed** back to positional groupby → No axis named budget again |
| 34 | 10:15 | pandas | ❌ run | `groupby('pjt_name)` missing closing quote → SyntaxError |
| 36 | 10:16 | pandas | ❌ run | `groupby(['pjt_name','budget','payment'])` → ValueError: cannot insert payment, already exists |
| 38 | 10:17 | pandas | run | `reset_index(name='total_emp_expense')` (correct grouped frame at last) |
| 39 | 10:17 | pandas | ❌ run | `df2['payment'] > ...` → KeyError: 'payment' (gone after rename) |
| 41 | 10:17 | pandas | ❌ **submitted** | No-op self-comparison `total_emp_expense > total_emp_expense` (empty) → incorrect |
| 43 | 10:18 | pandas | ❌ **submitted** | Filter direction now correct (`budget < total`) but rounding `.round()` → incorrect |
| 45 | 10:19 | pandas | ❌ **submitted** | Same logic resubmitted → incorrect (rounding still wrong) |
| 47 | 10:20 | pandas | ❌ **submitted** | Moved rounding to `sum().round()` → incorrect |
| 49 | 10:20 | pandas | ❌ **submitted** | `sum().round(0)` → incorrect (rounds, doesn't ceil) |
| 51 | 10:22 | pandas | ❌ **submitted** | Switched period calc to `.dt.days`, still `.round(0)` → incorrect |
| 52 | 10:22 | pandas | ❌ run | `Series.ceil()` → AttributeError: 'Series' object has no attribute 'ceil' |
| 53 | 10:23 | pandas | ❌ run | `np.ceil(...)` → NameError: name 'np' is not defined (forgot import) |
| 54 | 10:24 | pandas | run | Added `import numpy as np` — clean run |
| 55 | 10:24 | pandas | ✅ **submitted** | Same code → **accepted** |

(Each "submitted" row above is preceded ~1–2 s earlier by an identical-code "run" — StrataScratch fires a run right before each submit, which is why the run/submit counts are 46/9.)

What the failed attempts taught me:
- **"Round up to the nearest dollar" = ceiling, and in pandas that is `np.ceil`.** I lost four rejected submissions (`.round()`, `.round()`, `.round(0)`, `.round(0)`) plus two errored runs (`Series.ceil()` doesn't exist; `np.ceil` without `import numpy as np`) before getting it. Ordinary rounding silently passes the grader's *syntax* but fails its *values*, so I had no error to anchor on — exactly the "parses but wrong" trap. Lesson: read the rounding verb literally — "up" → `ceil`, "down" → `floor`, "nearest" → `round`.
- **`groupby` keys must be a LIST, not positional args.** `groupby('a','b')` makes pandas treat `'b'` as `axis` → `No axis named b`. I hit this five times and even regressed after fixing it once. `groupby(['a','b'])` always.
- **After `groupby(...).reset_index(name='X')`, the old column name is gone.** I kept filtering on `df2['payment']` after renaming the summed column to `total_emp_expense` → `KeyError`. Once you rename an aggregate, every downstream reference must use the new name.
- **Watch the comparison's direction and operands.** I shipped a no-op `total_emp_expense > total_emp_expense` and once had `budget > total` (under-budget) before settling on `budget < total_emp_expense`. Over-budget means *cost exceeds budget* → `budget < cost`.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context.

### 8.1 Correctness
- **Score**: 3/5
- Comment: Your pandas solution is correct and, on the proration math, arguably more defensive than the author's — prorating per row (`unit_pay * period`) before the `groupby.sum` survives the case where employees ever hold per-assignment durations, which the author's sum-then-multiply would silently break. But the rubric asks for three engines and you shipped one: PostgreSQL and PySpark are both "Not attempted." In a loop, an unanswered language is a non-pass on that language, so correctness is capped here regardless of how clean the pandas is.

### 8.2 Performance & Efficiency
- **Score**: 4/5
- Comment: The two-merge-then-`groupby` shape is the efficient one and you avoid the author's redundant `merge`-the-aggregate-back-then-`drop_duplicates` round trip entirely — fewer rows materialized, no de-dup pass. One nit: you carry `start_date`, `end_date`, and `emp_id` into `df` but never need `emp_id` past the merge; dropping it in the column projection on line 51 trims the frame you aggregate over.

### 8.3 Readability
- **Score**: 4/5
- Comment: The intermediate names (`period`, `unit_pay`, `payment`, `total_emp_expense`) read cleanly and the comment-per-step cadence is genuinely good. Two snags: `result` on line 67 as a bare trailing expression relies on the notebook auto-displaying it — in a script that returns nothing, so make it `print(result)` or an explicit return. And `df`/`df2` are throwaway names; `flat`/`per_project` would tell the reader what each frame *is*.

### 8.4 Edge Cases
- **Score**: 3/5
- Comment: The graded edge case here — "round up" meaning ceiling — you eventually nailed with `np.ceil` on line 64, and the strict `budget < total_emp_expense` correctly excludes the at-budget tie. But nothing in the code guards a NULL `salary`, `start_date`, or `end_date`: a single NULL date makes `period` NaT-driven and silently drops that project from the over-budget set with no flag. Add an explicit `.dropna(subset=['salary','start_date','end_date'])` or at least name the assumption that the join columns are complete.

### 8.5 Interview-Readiness
- **Score**: 2/5
- Comment: The 55-attempt, single-session history reads as trial-and-error, not narrated reasoning — you hit `No axis named budget` five times and *regressed* after fixing it (attempt 30), which signals editing-by-guess rather than reading the traceback that literally named the bug. The rounding blocker cost four rejected submissions with no error to anchor on; a single sentence up front — "round up means ceiling, so `np.ceil`, not `.round()`" — is exactly the assumption a FAANG interviewer wants verbalized *before* the keystroke. And solving one of three required languages is the dominant signal: you can't claim PostgreSQL or PySpark fluency on a problem you never opened in them.

### 8.6 Verdict
Your pandas instincts are sound — the per-row proration is the more general shape, the `groupby` keys-as-list is right, and you avoid the author's de-dup detour. The one thing that moves you up a level is to stop *converging by submission* and start *converging by narration*: before you type, say the rounding verb out loud ("up = ceil"), read the traceback verbatim instead of mutating the line, and on a three-engine problem treat an unwritten language as a failed question, not an optional extra. Re-solve 10304 cold in PostgreSQL (`JOIN ×2 + GROUP BY + HAVING CEILING(...) > budget`) and PySpark today — closing the language gap, not polishing the pandas, is where your next interview point is.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Round **up** to integer (ceiling) | `CEILING(x)` | `np.ceil(x)` (NOT `x.round()`, NOT `Series.ceil()`) | `F.ceil(x)` |
| Days between two dates | `end_date - start_date` (date subtraction yields integer days) | `(pd.to_datetime(end) - pd.to_datetime(start)).dt.days` | `(F.to_date(end) - F.to_date(start)).cast("integer")` |
| Inner-join three tables | `JOIN ... ON ... JOIN ... ON ...` | chained `.merge(..., how='inner', left_on=, right_on=)` | chained `.join(..., cond, "inner")` |
| Sum within group | `SUM(salary) ... GROUP BY` | `groupby([...])['salary'].sum()` | `.groupBy(...).agg(F.sum("salary"))` |
| Keep groups passing an aggregate test | `HAVING CEILING(...) > a.budget` | filter the grouped frame: `df2[df2['budget'] < df2['total_emp_expense']]` | `.filter(col(diff) > 0)` after the agg |
| Group keys (multiple) | `GROUP BY a.title, a.budget, ...` | `groupby(['title','budget'])` — **must be a list** | `.groupBy("title","budget")` |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: a **three-table fact join through a bridge table** (projects ⨝ emp_projects ⨝ employees), then a **prorated, grouped aggregate compared against a threshold**. In SQL the comparison lives in `HAVING CEILING(...) > budget`; in pandas you collapse with `groupby` and filter the resulting frame. The defining subtlety is the **rounding direction**: "round up to the nearest dollar" is **ceiling**, which the SQL author expresses with `CEILING(...)` and pandas requires `np.ceil`.

- **Reusable snippet (pandas, parameterized "over-threshold after prorated group-sum")**:
  ```python
  import numpy as np
  flat = a.merge(bridge, left_on='id', right_on='a_id').merge(b, left_on='b_id', right_on='id')
  flat['days']    = (pd.to_datetime(flat['end_date']) - pd.to_datetime(flat['start_date'])).dt.days
  flat['prorate'] = flat['salary'] * flat['days'] / 365
  g = flat.groupby(['title', 'budget'])['prorate'].sum().reset_index(name='cost')
  g['cost'] = np.ceil(g['cost'])              # "round UP" → ceil, never round()
  result = g[g['budget'] < g['cost']]          # over budget = cost exceeds budget
  ```

- **Weakness I noticed in myself**:
  1. **Rounding-verb literacy.** I reflexively reach for `.round()` whenever the word "round" appears, but the spec said "round **up**" = ceiling. Four rejected submissions, no error message (the grader only checks values), ~6 minutes lost. Drill: *"up = `np.ceil` / `CEILING` / `F.ceil`; down = floor; nearest = round."*
  2. **pandas API shape mistakes that throw real errors.** `groupby('a','b')` (positional), `Series.ceil()` (doesn't exist), `np.ceil` without `import numpy`, double-bracket `to_datetime`. These all error loudly, but I hit the positional-groupby one **five times** and even regressed after fixing it — a sign I was editing by trial-and-error instead of reading the traceback. The traceback (`No axis named budget`) literally named the bug.
  3. **Filter direction/operands under churn.** I shipped a self-comparison and a backwards operator before getting `budget < cost`. When the logic is "X exceeds Y," write it once, deliberately, and sanity-check on one project rather than flipping operators between submissions.

- **Drill for tomorrow morning** (before any new problems):
  1. Re-solve 10304 cold in **PostgreSQL** using the author's `JOIN ×2 + GROUP BY + HAVING CEILING(...) > budget` — I have zero SQL attempts on this one, so that's the real gap.
  2. Then re-solve the pandas version from a blank cell in under 6 minutes, getting `np.ceil` and the list-form `groupby` right on the first pass.

## 11. Related Notes

- **Same category (Join — multi-table fact/bridge join)**: other Join notes where a bridge/junction table connects two dimensions (here `linkedin_emp_projects` links projects to employees). Pair this with any problem that joins three tables through a many-to-many mapping and then aggregates the result.
- **Adjacent skills exercised**:
  - "Compare a grouped aggregate to a per-group threshold" — the same `HAVING aggregate > attribute` / `groupby → filter` shape seen in Subquery/CTE notes about above/below a budget or average.
  - **Date proration** — `salary × days/365` is a reusable Date/Time idiom; see any note that converts a date span to a fraction of a year.
- **Reference reading**:
  - PostgreSQL math functions (`CEILING`, `FLOOR`, `ROUND`): https://www.postgresql.org/docs/current/functions-math.html
  - NumPy `ceil` (why `Series.round()` is not "round up"): https://numpy.org/doc/stable/reference/generated/numpy.ceil.html
  - pandas `DataFrame.groupby` (keys must be a label or list of labels): https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.groupby.html
  - PySpark `ceil` / `to_date`: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html

---

*Generated 2026-06-30 by Ina (problem solving) + Claude (author solution fetch via MCP, submission history analysis, FAANG reviewer subagent).*
