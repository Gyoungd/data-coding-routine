# {Problem ID} — {Problem Title}

> Standard note template (v2, 2026-05-27)
> Filename pattern: `notes/{id}_{slug}.md` (e.g. `9805_find-drafts-optimism.md`)

---

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | {e.g. 9805} |
| Category | {e.g. String} |
| Difficulty | Easy / Medium / Hard |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/{slug}?code_type=1) · [Python](.../coding/{slug}?code_type=2) · [PySpark](.../coding/{slug}?code_type=6) |
| Solved on | YYYY-MM-DD |
| Tables | `table1(col1, col2, ...)` |

## 2. Problem Restatement (in my words)

> Rewrite the problem in one or two short sentences instead of copying the original.
> Helps with licensing and forces real understanding.

## 3. My Approach

- First instinct:
- Where I got stuck:
- Final strategy:

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed YYYY-MM-DD)

```sql
-- Final accepted code
```

### 4.2 Python (pandas) ✅ (passed YYYY-MM-DD)

```python
import pandas as pd

# Final accepted code
```

### 4.3 PySpark ✅ (passed YYYY-MM-DD)

```python
import pyspark.sql.functions as F

# Final accepted code
```

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)`.
> Premium-only feature.

### 5.1 PostgreSQL

```sql
```

### 5.2 Python

```python
```

### 5.3 PySpark

```python
```

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| Case sensitivity | | | |
| Pattern shape | | | |
| Idiom choice | | | |
| Output shape | | | |

Key insight in one line:

## 7. Submission History (auto-captured)

> Pulled from MCP `get_my_attempts(question_id=...)`. Shows trial-and-error path.

| # | Time | Language | Status | Error / Note |
|---|---|---|---|---|
| 1 | HH:MM | SQL | ❌ | Missing filename filter |
| 2 | HH:MM | SQL | ✅ | Added `AND filename LIKE '%draft%'` |
| ... | | | | |

What the failed attempts taught me:
- Lesson 1:
- Lesson 2:

## 8. Senior DA / DS Review — Structured Rubric

> Auto-filled by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer
> persona). Prompt definition: [`docs/_reviewer_agent.md`](_reviewer_agent.md).
> Fresh context, no main-chat bias. Calibrated against bar-raiser standards.

### 8.1 Correctness
- **Score**: __/5
- Comment:

### 8.2 Performance & Efficiency
- **Score**: __/5
- Comment (scans, joins, window misuse, etc.):

### 8.3 Readability
- **Score**: __/5
- Comment (CTE breakdown, aliases, comments, indentation):

### 8.4 Edge Cases
- **Score**: __/5
- Comment (NULLs, ties, empty result, duplicates, case sensitivity):

### 8.5 Interview-Readiness
- **Score**: __/5
- Comment (verbalized assumptions, named ambiguities, 5-minute whiteboard test):

### 8.6 Verdict
<One paragraph (3–5 sentences). The single highest-leverage coaching note. What
would move you to the next level on similar problems.>

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Substring match | `LIKE '%x%'` / `ILIKE` | `.str.contains('x', case=False)` | `F.col('c').like('%x%')` or `.contains('x')` |
| Boolean AND | `AND` | `&` (not `and`) | `&` (not `and`) |
| Case-insensitive | `ILIKE` / `~*` | `case=False` flag | `lower(col) like` or `rlike` |

(Fill in only the rows relevant to this problem.)

## 10. Patterns to Remember

- Core pattern this problem teaches:
- Reusable snippet (if any):
- Weakness I noticed in myself:

## 11. Related Notes

- Similar problems in this category:
- Reference material (docs, blogs):

---

*Generated YYYY-MM-DD by Ina (problem solving) + Claude (author solution fetch, diff, rubric review).*
