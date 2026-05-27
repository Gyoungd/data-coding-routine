# 9805 — Find Drafts Which Contain the Word 'optimism'

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 9805 |
| Category | String |
| Difficulty | Easy |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/9805-find-drafts-which-contains-the-word-optimism?code_type=1) · [Python](https://platform.stratascratch.com/coding/9805-find-drafts-which-contains-the-word-optimism?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/9805-find-drafts-which-contains-the-word-optimism?code_type=6) |
| Solved on | 2026-05-27 |
| Table | `google_file_store(filename text, contents text)` |

## 2. Problem Restatement (in my words)

From a table of files, return the rows where the **filename looks like a draft**
(some "draft" marker in the name) **and** the **document body contains the word
"optimism"**. The problem doesn't define "draft" precisely, so the filter shape
is a judgment call.

## 3. My Approach

- First instinct: two-condition `WHERE` filter — one for filename, one for contents.
- Where I got stuck:
  - **SQL**: my first two submissions only filtered `contents`. Forgot the filename
    condition entirely. Two wrong submissions before I added `AND filename LIKE '%draft%'`.
  - **Python**: hit several pandas selector mistakes before landing on the right
    boolean-masking idiom (`df[df['col'].str.contains(...)]`).
  - **PySpark**: pandas habits leaked in — I tried `df[col].str.contains(...)`,
    `and` for boolean AND, and `&&`. PySpark needs `df.filter(cond1 & cond2)` and
    `.contains()` on a Column, not `.str.contains()`.
- Final strategy: a two-step filter (`drafts → opt_drafts`) in pandas for readability;
  a one-liner `.filter()` in PySpark; a straight two-condition `WHERE` in SQL.

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2026-05-27 05:43)

```sql
SELECT * FROM google_file_store
WHERE
    contents LIKE '%optimism%'
    AND filename LIKE '%draft%';
```

### 4.2 Python (pandas) ✅ (passed 2026-05-27 08:04)

```python
import pandas as pd

drafts = google_file_store[google_file_store['filename'].str.contains('draft')]
opt_drafts = drafts[drafts['contents'].str.contains('optimism')]
opt_drafts
```

### 4.3 PySpark ✅ (passed 2026-05-27 08:12)

```python
import pyspark

google_file_store.filter(
    google_file_store.filename.contains('draft')
    & google_file_store.contents.contains('optimism')
).toPandas()
```

## 5. Author Solutions

Pulled via MCP `get_question(include_solution=True)`.

### 5.1 PostgreSQL

```sql
SELECT *
FROM google_file_store
WHERE filename ILIKE 'draft%'
  AND contents ILIKE '%optimism%'
```

### 5.2 Python (pandas)

```python
import pandas as pd
import numpy as np

result = google_file_store[
    (google_file_store['filename'].str.contains('draft', case=False))
    & (google_file_store['contents'].str.contains('optimism', case=False))
]
```

### 5.3 PySpark

```python
import pyspark.sql.functions as F

result = google_file_store.filter(
    (F.col('filename').like('%draft%'))
    & (F.col('contents').like('%optimism%'))
).toPandas()
```

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| Case sensitivity (SQL) | `LIKE` | `ILIKE` | Author handles `Draft`, `OPTIMISM` and other case variants. Mine would miss them. |
| Case sensitivity (Python) | `.str.contains('draft')` | `.str.contains('draft', case=False)` | Same gap — mine is case-sensitive by default. |
| Case sensitivity (PySpark) | `.contains('draft')` | `.like('%draft%')` | Both are case-sensitive in PySpark. Neither is robust to `Draft`. Tie. |
| Filename pattern | `'%draft%'` (substring) | `'draft%'` (prefix-only, SQL) / `'%draft%'` (substring, PySpark) | Author was stricter in SQL ("the filename must *start* with `draft`") and more relaxed in PySpark. Author isn't fully consistent across engines. |
| Style (Python) | Two-step `drafts → opt_drafts` | One expression | Mine is easier to debug, author's is more compact. Both are valid. |
| Idiom (PySpark) | `df.col.contains('x')` (method on Column) | `F.col('col').like('%x%')` (SQL-style with explicit `F.col`) | Mine is more **PySpark-native**. Author's pattern is what you'd see when porting SQL to PySpark literally. Mine reads cleaner. |

**Key insight in one line:** the author covers case-insensitivity better, but my PySpark
idiom is actually closer to how PySpark code is written day-to-day.

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=9805)` — 30 attempts recorded. Highlights:

| # | Time (UTC) | Language | Status | What happened |
|---|---|---|---|---|
| 1 | 05:37 | SQL | run | `SELECT *` — exploring the data |
| 2 | 05:39 | SQL | run | Added `WHERE contents LIKE '%optimism%'` — filter contents only |
| 3 | 05:42 | SQL | ❌ submitted | Same as #2, **missing filename filter** |
| 4 | 05:43 | SQL | ✅ submitted | Added `AND filename LIKE '%draft%'` |
| 5 | 07:56 | Python | run | `head()` — exploring |
| 6 | 08:01 | Python | ❌ run | `[[filename]]` — variable name vs string literal confusion (NameError) |
| 7–11 | 08:01–02 | Python | ❌ run | Several wrong patterns: `[['filename']].str.contains(...)`, `[['filename']].contains(...)`, `[['filename'].str.contains(...)]` — wrong nesting of selectors |
| 12 | 08:03 | Python | run | Found the right idiom: `df[df['col'].str.contains(...)]` |
| 13 | 08:04 | Python | ✅ submitted | Added second filter step |
| 14 | 08:04 | PySpark | run | `toPandas()` only — exploring |
| 15 | 08:05 | PySpark | ❌ run | Tried `df[col].str.contains(...)` (pandas idiom — PySpark doesn't have `.str`) |
| 16 | 08:05 | PySpark | ❌ run | Used `and` instead of `&` (CANNOT_CONVERT_COLUMN_INTO_BOOL) |
| 17 | 08:05 | PySpark | ❌ run | Used `&&` (Python syntax error) |
| 18–20 | 08:10 | PySpark | ❌ run | `.contents('optimism')` — calling the Column object instead of `.contents.contains(...)` |
| 21 | 08:12 | PySpark | ✅ submitted | Final correct version |

What the failed attempts taught me:
- **SQL**: read the full prompt before submitting. The first two SQL submissions
  ignored the filename condition.
- **Python**: bracket grammar — `df[['col']]` returns a DataFrame, `df['col']`
  returns a Series. Only the Series has `.str`.
- **PySpark**: pandas habits don't carry over. PySpark wants Column methods
  (`.contains`, `.like`) and bitwise operators (`&`, `|`, `~`), not `and / or / not`.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context, no
> bias from the main chat. Scoring calibrated against bar-raiser standards.

### 8.1 Correctness
- **Score**: 3/5
- Comment: All three pass the grader, but only because the seed data is forgiving.
  Your `LIKE '%optimism%'` and `.contains('optimism')` are case-sensitive in Postgres
  and pandas/PySpark — a row with "Optimism" in the contents silently drops. The
  author's reference uses `ILIKE` / `case=False` for a reason. You also never defined
  what a "draft" is out loud; you assumed substring `draft` anywhere in `filename`,
  while the author anchors to `'draft%'` (prefix). Both are defensible, but on an
  Easy string problem the bar-raiser expects you to surface that ambiguity, not
  silently pick one.

### 8.2 Performance & Efficiency
- **Score**: 3/5
- Comment: `LIKE '%optimism%'` is a leading-wildcard scan — it cannot use a B-tree
  index and will full-scan `contents`. On a real Google-scale `file_store` you would
  mention `pg_trgm` + GIN, or full-text search (`to_tsvector @@ to_tsquery`). Not
  required for an Easy, but flagging it is what separates a 3 from a 5. The PySpark
  version is fine (single `.filter` fuses both predicates, lazy), but `SELECT *` /
  `toPandas()` on an unbounded set is a footgun you should at least name.

### 8.3 Readability
- **Score**: 3/5
- Comment: SQL is clean. The pandas version creates an intermediate `drafts` variable
  for no reason — one boolean mask with `&` is the idiom, and it matches how you'd
  write it in PySpark and SQL (one predicate, two AND-ed conditions). Keeping the
  three languages structurally parallel is a small thing FAANG reviewers notice.
  Also `SELECT *` is acceptable here because the prompt says "return the rows," but
  say that out loud — don't let the interviewer wonder if you know to enumerate
  columns in production.

### 8.4 Edge Cases
- **Score**: 2/5
- Comment: You handled none of them, and didn't name them. Misses: (1) case
  sensitivity — "Optimism", "OPTIMISM" drop; (2) the word "optimism" vs the
  substring — "optimismo", "anti-optimism-bill" match, which is probably not what
  the prompt means by "the word"; word-boundary regex (`~* '\moptimism\M'` in
  Postgres, `\b` in pandas with `regex=True`) is the honest fix; (3) NULLs in
  `contents` or `filename` — `LIKE` returns NULL not FALSE, which is fine in
  `WHERE` but worth saying; (4) what counts as a draft filename. On an Easy this
  is exactly where candidates differentiate themselves.

### 8.5 Interview-Readiness
- **Score**: 2/5
- Comment: Your submission history tells the real story: 2 wrong SQL tries (forgot
  the filename predicate), ~6 pandas tries fighting selector syntax
  (`df[['col']].str.contains` does not exist on a DataFrame — only on a Series),
  ~6 PySpark tries leaking pandas idioms (`and`, then `&&`). In a live loop those
  are screen-share moments the interviewer remembers. Internalize the three idioms
  cold: pandas = `df[df['col'].str.contains(...)]`, PySpark =
  `df.filter(F.col('col').contains(...))` with `&` `|` `~`, Postgres = `ILIKE` for
  case-insensitive. Also: you never stated assumptions or offered an alternative
  (trigram, FTS, regex word boundary). Silent solving on an Easy reads as "junior."

### 8.6 Verdict
You can get this problem green, but you got there by trial-and-error and you left
the two things a FAANG bar-raiser actually grades on the table: case-insensitivity
and the word-vs-substring distinction. The single highest-leverage change for
similar string problems is to open with 20 seconds of assumption-naming — "I'll
treat this case-insensitively, match 'draft' as a filename prefix, and match
'optimism' as a substring; flag me if you want word-boundary matching" — then
write one clean predicate per language using the idiomatic operator (`ILIKE`,
`case=False`, `F.col(...).rlike(r'\boptimism\b')`). Drill the three idioms until
the selector syntax is muscle memory; on an Easy, fluency is the whole signal.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Substring match | `LIKE '%x%'` or `ILIKE '%x%'` | `.str.contains('x', case=False)` | `F.col('c').like('%x%')` or `Column.contains('x')` |
| Boolean AND | `AND` | `&` (vectorized — not `and`) | `&` (Column-level — not `and`) |
| Case-insensitive | `ILIKE` or `~*` | `case=False` flag on `.str.contains` | `lower(F.col('c')).like(...)` or `.rlike('(?i)x')` |
| Output | rows from `SELECT *` | filtered DataFrame | DataFrame `.filter(...)`, then `.toPandas()` for the grader |

## 10. Patterns to Remember

- **Core pattern**: "filter a table where multiple text columns match conditions" — daily
  bread-and-butter SQL. Worth being fluent in all three engines.
- **Reusable snippet** (case-insensitive word match in PostgreSQL):
  ```sql
  WHERE col ~* '\\moptimism\\M'   -- word-bounded, case-insensitive
  ```
- **Weakness I noticed**: I rush to a solution before stating assumptions out loud.
  Need to slow down by ~30 seconds and name the ambiguities before typing.
- **PySpark mental model fix**: PySpark Columns are not pandas Series. They use
  `.contains` / `.like` / `.rlike` directly on the Column, and combine with `& | ~`.

## 11. Related Notes

- Similar String-category problems to try next: 9817 "Count Occurrences Of Words In
  Drafts" (Medium), 9842 "First Names With Six Letters Ending in 'h'" (Easy)
- Reference reading:
  - PostgreSQL pattern matching: https://www.postgresql.org/docs/current/functions-matching.html
  - pandas `str.contains`: https://pandas.pydata.org/docs/reference/api/pandas.Series.str.contains.html
  - PySpark Column methods: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.html

---

*Generated 2026-05-27 by Ina (3-language solving) + Claude (author solution fetch via MCP, diff analysis, Senior rubric review).*
