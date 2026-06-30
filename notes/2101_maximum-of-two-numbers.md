# 2101 — Maximum of Two Numbers

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 2101 |
| Category | Join (multi-table) |
| Difficulty | Medium |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/2101-maximum-of-two-numbers?code_type=1) · [Python](https://platform.stratascratch.com/coding/2101-maximum-of-two-numbers?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/2101-maximum-of-two-numbers?code_type=6) |
| Solved on | 2026-06-16 |
| Tables | `deloitte_numbers(number bigint)` |

## 2. Problem Restatement (in my words)

There is one table with a single column of integers. Pair every number with every number — including each number with itself, and treating order as significant, so `(x, y)` and `(y, x)` are both kept and `(x, x)` is kept. (That is, all ordered pairs drawn from the column, with repetition allowed — `n²` rows for `n` distinct values.) For each pair output three columns: the first number, the second number, and the larger of the two (a tie returns that shared value). The whole problem is really "self-cross-join the table, then take a row-wise max of the two columns."

## 3. My Approach

- **First instinct**: cross-join the table to itself so I get every ordered pair, then compute the bigger of the two with `GREATEST(n1, n2)`. The cross join + `GREATEST` idea was right from the start — I never doubted that part.
- **Where I got stuck**: I over-read the phrase "two numbers" and added `WHERE t1.number != t2.number`, assuming the pair had to be of two *distinct* numbers. That filter drops every `(x, x)` self-pair. I submitted that version once and the grader rejected it as incorrect — "permutations with replacement" explicitly *includes* pairing a number with itself, so the equal-value pairs must stay.
- **Final strategy**: delete the `!= ` filter. Plain `CROSS JOIN` (keeps all `n²` ordered pairs, self-pairs included) wrapped in a CTE `p_table` that aliases the two columns `n1`/`n2`, then `SELECT *, GREATEST(n1, n2) AS max_num`. Removing the one `WHERE` line was the entire fix; it passed on the very next submission, ~1 minute later.

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-06-16 10:13)

```sql
WITH p_table AS (
    SELECT
        t1.number AS n1,
        t2.number AS n2
    FROM deloitte_numbers AS t1
    CROSS JOIN deloitte_numbers AS t2
)

SELECT
    *,
    GREATEST(n1, n2) AS max_num
FROM p_table;
```

> Note: `GREATEST` does the row-wise max in one call, so no `CASE` expression is needed. The `CROSS JOIN` with no `WHERE` keeps all `n²` ordered pairs, including each number paired with itself — which is exactly what "permutations with replacement" demands. My earlier rejected version had `WHERE t1.number != t2.number`, which silently dropped the self-pairs.

### 4.2 Python (pandas)

⚠️ Not attempted / no accepted submission in my history.

### 4.3 PySpark

⚠️ Not attempted / no accepted submission in my history.

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)`.
> Premium-only feature. This problem ships author solutions for all eight
> supported engines; the three canonical ones I track are below, with the
> remaining five quoted underneath for completeness.

### 5.1 PostgreSQL

```sql
SELECT dn1.number AS number1,
       dn2.number AS number2,
       CASE
           WHEN dn1.number > dn2.number THEN dn1.number
           ELSE dn2.number
       END AS max_number
FROM deloitte_numbers AS dn1
CROSS JOIN deloitte_numbers AS dn2
```

### 5.2 Python

```python
import pandas as pd

numbers = deloitte_numbers['number'].to_list()
all_combinations = {(x, y, max(x, y)) for x in numbers for y in numbers}
result = pd.DataFrame(all_combinations, columns=['number1', 'number2', 'max_number'])
```

### 5.3 PySpark

```python
from pyspark.sql import functions as F

df1 = deloitte_numbers.alias("df1")
df2 = deloitte_numbers.alias("df2")

pairs = df1.crossJoin(df2)

result = pairs.select(
    F.col("df1.number").alias("number1"),
    F.col("df2.number").alias("number2"),
    F.greatest(F.col("df1.number"), F.col("df2.number")).alias("max_number")
)

result_df = result.toPandas()
```

<details>
<summary>Other engines StrataScratch ships for this problem</summary>

**MySQL**

```sql
SELECT dn1.number AS number1,
       dn2.number AS number2,
       CASE
           WHEN dn1.number > dn2.number THEN dn1.number
           ELSE dn2.number
       END AS max_number
FROM deloitte_numbers AS dn1
CROSS JOIN deloitte_numbers AS dn2;
```

**MSSQL**

```sql
SELECT dn1.number AS number1,
       dn2.number AS number2,
       IIF(dn1.number > dn2.number, dn1.number, dn2.number) AS max_number
FROM deloitte_numbers AS dn1
CROSS JOIN deloitte_numbers AS dn2
```

**Oracle**

```sql
SELECT dn1."number" AS number1,
       dn2."number" AS number2,
       CASE
           WHEN dn1."number" > dn2."number" THEN dn1."number"
           ELSE dn2."number"
       END AS max_number
FROM deloitte_numbers dn1
CROSS JOIN deloitte_numbers dn2;
```

**R**

```r
library(dplyr)
numbers <- deloitte_numbers$number
all_combinations <- expand.grid(numbers, numbers) %>%
mutate(max_number = pmax(Var1, Var2)) %>%
select(number1 = Var1, number2 = Var2, max_number)
```

**Polars**

```python
import polars as pl

# Step 1: Create a LazyFrame from the numbers column
numbers_lazy = deloitte_numbers.lazy()

# Step 2: Cross join numbers with itself to create all (x, y) pairs
pairs = (
    numbers_lazy
    .join(
        numbers_lazy,
        how='cross'
    )
    .select([
        pl.col('number').alias('number1'),
        pl.col('number_right').alias('number2')
    ])
)

# Step 3: Calculate the max for each pair
result = (
    pairs.with_columns(
        pl.max_horizontal(['number1', 'number2']).alias('max_number')
    )
    .collect()
)

result
```

</details>

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| Row-wise max | `GREATEST(n1, n2)` | `CASE WHEN dn1.number > dn2.number THEN ... ELSE ... END` | Same result. `GREATEST` is one token and reads cleaner; the author's `CASE` is more portable (some engines lack `GREATEST`) and is explicit about tie-handling. Both correct. |
| Structure | Wrapped the join in a CTE `p_table`, then selected `*` + the max | Single flat `SELECT` straight off the `CROSS JOIN` | Author has no CTE — one fewer layer. Mine is fine but the CTE buys nothing here; it's a one-step problem. |
| Self-pairs (`x,x`) | Kept (after I removed my `!= ` filter) | Kept (no `WHERE` at all) | This is the whole problem. The author never filtered, so they never hit the trap I did. |
| Output columns | `n1, n2, max_num` (via `SELECT *`) | `number1, number2, max_number` | Same shape, same order, same meaning. Both accepted; the grader doesn't pin column names here. |
| Join clause | Explicit `CROSS JOIN` | Explicit `CROSS JOIN` | Identical — the canonical way to enumerate all ordered pairs. |

**Key insight in one line:** "all permutations with replacement" = a plain `CROSS JOIN` of the table with itself and **no** `WHERE` — the self-pairs `(x, x)` are part of the answer, and `GREATEST(a, b)` (or `CASE WHEN a > b`) gives the row-wise max in one expression.

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=2101)` — **7 attempts total**, all PostgreSQL (code_type 1), all on 2026-06-16. Ordered oldest → newest:

| # | Time (UTC) | Language | Status | What happened |
|---|---|---|---|---|
| 1 | 04:56 | SQL | run | `SELECT * FROM deloitte_numbers;` — inspect the single-column table |
| 2 | 09:58 | SQL | run | `CROSS JOIN` self, **with** `WHERE t1.number != t2.number` — first pass at the pairs (already carrying the distinct-values assumption) |
| 3 | 10:12:03 | SQL | run | Wrapped that join in CTE `p_table`, added `GREATEST(n1, n2)`; still the `!= ` filter |
| 4 | 10:12:06 | SQL | ❌ **submitted** | Same query with `WHERE t1.number != t2.number` → **grader says incorrect** (self-pairs `x,x` were dropped) |
| 5 | 10:13:09 | SQL | run | Stripped the CTE back to a plain `CROSS JOIN`, **removed** the `!= ` filter — sanity-checking the raw pair count |
| 6 | 10:13:12 | SQL | run | Re-wrapped in CTE + `GREATEST`, no filter — clean run of the correct query (lowercase) |
| 7 | 10:13:13 | SQL | ✅ **submitted** | Same query, formatted/uppercased → **accepted** |

What the failed attempts taught me:
- **"Two numbers" / "permutation" does not imply two *distinct* numbers.** The problem said "with replacement" and "`(x,y)` and `(y,x)` are different permutations," which together mean self-pairs `(x, x)` are valid rows. My instinct to add `WHERE t1.number != t2.number` came from reading "two numbers" as "two different numbers" — that one line was the only thing wrong, and it cost me a rejected submission. Lesson: in combinatorics-style prompts, read "with replacement" literally — it means *keep* the diagonal.
- **When a CROSS JOIN result looks wrong, suspect an over-eager `WHERE` before the join logic.** The join was never the problem; a filter I added was deleting rows the grader expected. The fix was *removing* code, not adding it.
- **I reflexively reach for a CTE on a one-step problem.** The author's answer is a single flat `SELECT`. The CTE I wrapped around the join added a layer without adding clarity — fine, but a reminder that not every problem needs the scaffolding.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context.

### 8.1 Correctness
- **Score**: 3/5
- Comment: Your PostgreSQL is correct — `CROSS JOIN` with no `WHERE` plus `GREATEST(n1, n2)` produces exactly the `n²` ordered pairs with self-pairs intact. But two of the three tracked engines (Python, PySpark) have no submission at all; on this axis I can only certify one of three deliverables, and a partial submission is not a complete answer.

### 8.2 Performance & Efficiency
- **Score**: 4/5
- Comment: A self-cross-join is inherently `O(n²)` — that is the problem, not your code, so there is nothing to prune. `GREATEST(n1, n2)` is a single scalar evaluation per row, cheaper than a branching `CASE`. One point off only because the `WITH p_table` CTE forces an extra logical materialization step the planner must collapse; the author's flat `SELECT` off the join is the leaner plan.

### 8.3 Readability
- **Score**: 4/5
- Comment: Clean indentation, explicit `CROSS JOIN`, and `n1`/`n2` aliases all read well. Two nits: `SELECT *, GREATEST(...)` leans on `*` to carry `n1`/`n2` — name them (`SELECT n1, n2, GREATEST(n1, n2) AS max_num`) so the output contract is visible without tracing the CTE. And `max_num` abbreviates where `max_number` would match the author and self-document.

### 8.4 Edge Cases
- **Score**: 3/5
- Comment: You eventually handled the one edge case that mattered — the `(x, x)` diagonal — but only after attempt #4 was rejected for the `WHERE t1.number != t2.number` filter that dropped it. Credit for the recovery; the deduction is for reaching for `!=` on a reflex before confirming the spec. You also never name the NULL case: if `deloitte_numbers.number` contained a NULL, `GREATEST` skips it (returns the non-NULL operand) — worth flagging out loud even when the column is `bigint NOT NULL` by assumption.

### 8.5 Interview-Readiness
- **Score**: 2/5
- Comment: In a live loop, two of three engines blank is a hard signal — the Python and PySpark cells are "⚠️ Not attempted," and the author ships a four-line pandas and a six-line PySpark solution you did not write. Concrete fix: write the pandas `set`-comprehension and the `df1.crossJoin(df2)` + `F.greatest` versions before your next review so all three are submission-ready. Your submission trail also shows silent solving — no narrated assumption about whether "two numbers" meant distinct values, which is the exact ambiguity that cost you attempt #4. State that assumption aloud next time ("I'll read 'with replacement' as keeping the diagonal").

### 8.6 Verdict
The SQL is genuinely solid and your post-mortem in Section 7 is sharper than most candidates ever produce — you correctly diagnosed that the fix was *removing* a filter, not adding logic. The one thing that moves you to the next level is breadth and narration under interview conditions: a FAANG loop rarely lets you pick your engine, so leaving Python and PySpark unattempted converts a clean win into a partial one. Before you mark a problem reviewed, ship all three engines and say the combinatorics assumption (ordered vs. unordered, with vs. without replacement) out loud — deciding the `WHERE` from the words instead of from reflex is the single habit that would have saved your rejected submission.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| All ordered pairs (self cross join) | `FROM t AS a CROSS JOIN t AS b` | `[(x, y) for x in nums for y in nums]` (or `itertools.product`) | `df1.crossJoin(df2)` |
| Row-wise max of two columns | `GREATEST(a, b)` or `CASE WHEN a > b THEN a ELSE b END` | `max(x, y)` per pair, or `df[['a','b']].max(axis=1)` | `F.greatest(F.col('a'), F.col('b'))` |
| Keep self-pairs `(x, x)` | no `WHERE` (do **not** add `a.col != b.col`) | iterate both loops fully, no `if x != y` | no `.filter()` on the cross join |
| Disambiguate same-table columns | table aliases `dn1` / `dn2` | n/a (lists) | `.alias("df1")` / `.alias("df2")` then `F.col("df1.number")` |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: enumerate **all ordered pairs** of a column's values → `CROSS JOIN` the table to itself; then a per-row comparison (`GREATEST` / `CASE`) gives the max. "Permutations with replacement, order matters" is the textbook description of an un-filtered self-cross-join (`n²` rows). If the prompt instead said "unordered, no repeats" you'd add `WHERE a.col < b.col`; "with replacement" / "order matters" means add **nothing**.
- **Reusable snippet (PostgreSQL)**:
  ```sql
  SELECT a.col AS first, b.col AS second, GREATEST(a.col, b.col) AS larger
  FROM t AS a
  CROSS JOIN t AS b;
  ```
- **Weakness I noticed in myself**: I default to filtering. I read "two numbers" and immediately wrote `!=`, deleting valid rows before I'd confirmed the spec wanted distinct values — and it didn't. The discipline to build: parse the combinatorics wording (*with replacement* vs *without*, *ordered* vs *unordered*) and decide the `WHERE` from the words, not from a reflex. Secondary note: I wrapped a one-line problem in a CTE; reach for the simplest flat query first.

## 11. Related Notes

- **Same category (Join — self-join / cross-join of one table)**: other Join (multi-table) notes where a single table is aliased twice and joined to itself — e.g. pairing rows for comparison, generating combinations, or "every row against every other row" problems. This 2101 note is the prototypical *un-filtered* cross-join; pair it with any problem that adds `a.col < b.col` to get *unordered distinct* pairs, so the contrast between the two filter conventions is explicit.
- **Adjacent skills exercised**: row-wise conditional value (`GREATEST` / `CASE WHEN`) — shared with any "pick the larger/smaller of two columns" problem.
- **Reference reading**:
  - PostgreSQL `CROSS JOIN`: https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-CROSS-JOINS
  - `GREATEST` / `LEAST`: https://www.postgresql.org/docs/current/functions-conditional.html#FUNCTIONS-GREATEST-LEAST
  - pandas `itertools.product` for pair enumeration: https://docs.python.org/3/library/itertools.html#itertools.product

---

*Generated 2026-06-30 by Ina (problem solving) + Claude (author solution fetch via MCP, submission history analysis, FAANG reviewer subagent).*
