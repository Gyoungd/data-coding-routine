# 10285 — Acceptance Rate By Date

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 10285 |
| Category | Date / Time |
| Difficulty | Medium |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/10285-acceptance-rate-by-date?code_type=1) · [Python](https://platform.stratascratch.com/coding/10285-acceptance-rate-by-date?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/10285-acceptance-rate-by-date?code_type=6) |
| Solved on | 2025-06-25 (PostgreSQL only) |
| Tables | `fb_friend_requests(action text, date date, user_id_receiver text, user_id_sender text)` |

> Category note: the morning heuristic matches on body/title/table names, and the title literally contains "rate by date" — an exact keyword in the **Date / Time** bucket (rules apply top-to-bottom, first match wins, and nothing above it fires). Honest caveat: the *actual skill* this problem drills is a **self LEFT JOIN of one table onto itself** (sent rows vs. accepted rows) plus a conditional count ratio — i.e. it leans far more "Join / Aggregation" than "Date." The `date` is only a `GROUP BY` key, not where the difficulty lives. I'm keeping the heuristic label for consistency with the pipeline, but flagging that it under-describes the real work.

## 2. Problem Restatement (in my words)

One table logs every friend-request event as a row. A `sent` row means user A sent user B a request; an `accepted` row means that same (sender, receiver) pair was later accepted. A non-accepted request simply has no `accepted` row — there's no "rejected" flag. For each **date a request was sent**, compute the acceptance rate = (number of those requests that were ever accepted) ÷ (number sent that day). The catch: acceptance can land on a *different* date than the send, so I can't just group both actions by date and divide — I have to **match each sent request to its acceptance by the (sender, receiver) pair**, not by date. Output only dates that had at least one acceptance.

## 3. My Approach

- **First instinct**: split the table into two sets — `request` (action = 'sent') and `accept` (action = 'accepted') — as CTEs. Match them on the pair `(user_id_sender, user_id_receiver)`. My very first join attempt was an **INNER `JOIN` with a `WHERE r.date <= a.date`** filter (attempts at 01:44–01:46), because I was thinking "an acceptance must come on or after the send date." I was building toward listing the matched rows first, then aggregating.
- **Where I got stuck**: two distinct things.
  1. **A trailing comma after the last CTE.** Every single one of my 6 failed attempts died with the *same* error: `syntax error at or near "select"`. The cause was a stray comma after the closing `)` of the `accept` CTE — `accept as ( ... ),` immediately followed by the final `SELECT`. PostgreSQL expects either another CTE name or the main query after that comma, so it choked on `select`. I stared at the join logic when the bug was one character of punctuation between the CTE block and the main query.
  2. **Join shape: INNER + date filter vs. LEFT JOIN.** My early version (`JOIN ... WHERE r.date <= a.date`) only kept matched pairs, which would have computed the *wrong denominator* — it drops sent requests that were never accepted, so the rate would always look like 100% per surviving date. I pivoted to a **LEFT JOIN** so every sent row survives (denominator = all sends that day) and only the accepted ones contribute to the numerator via `count(a.user_id_receiver)` (NULLs from unmatched LEFT-JOIN rows aren't counted by `COUNT(col)`).
- **Final strategy**: two CTEs (`request` = sent, `accept` = accepted), `request LEFT JOIN accept` on the **pair** `(sender, receiver)`, then `GROUP BY r.date`. Numerator = `count(a.user_id_receiver)` (counts only matched/accepted rows), denominator = `count(r.user_id_sender)` (counts all sent rows that day). Cast the denominator to `decimal` so the division isn't integer-truncated to 0. The moment I deleted the trailing comma, it ran and passed on the first clean execution — the join logic had been correct for several attempts; only the comma was blocking it.

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2025-06-25 01:51 UTC)

```sql
with request as (select date, user_id_sender,
            user_id_receiver
from fb_friend_requests
where action = 'sent'),
accept as (
    select date, user_id_sender, user_id_receiver
    from fb_friend_requests
    where action = 'accepted'
)
select r.date,
count(a.user_id_receiver)/cast(count(r.user_id_sender) as decimal) as percentage_accept
from request r
left join accept a on r.user_id_sender = a.user_id_sender
and r.user_id_receiver = a.user_id_receiver
group by r.date;
```

> Note: the numerator `count(a.user_id_receiver)` works because `COUNT(column)` ignores NULLs — every sent request that found no acceptance produces a LEFT-JOIN row with NULL on the `a.*` columns, so it's excluded from the count but still present in the denominator `count(r.user_id_sender)`. The `cast(... as decimal)` is load-bearing: without it, integer ÷ integer truncates (e.g. 3/5 → 0) in PostgreSQL.

### 4.2 Python (pandas)

⚠️ Not attempted / no accepted submission in my history.

### 4.3 PySpark

⚠️ Not attempted / no accepted submission in my history.

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)`.
> Premium-only feature. StrataScratch ships solutions for all eight supported engines on this problem; the three canonical engines I track are below, with the other dialects summarized after.

### 5.1 PostgreSQL

```sql
WITH sent_cte AS
  (SELECT date, user_id_sender,
                user_id_receiver
   FROM fb_friend_requests
   WHERE action='sent' ),
     accepted_cte AS
  (SELECT date, user_id_sender,
                user_id_receiver
   FROM fb_friend_requests
   WHERE action='accepted' )
SELECT a.date,
       count(b.user_id_receiver)/CAST(count(a.user_id_sender) AS decimal) AS percentage_acceptance
FROM sent_cte a
LEFT JOIN accepted_cte b ON a.user_id_sender=b.user_id_sender
AND a.user_id_receiver=b.user_id_receiver
GROUP BY a.date
```

### 5.2 Python (pandas)

```python
import pandas as pd
import numpy as np

df_sent=fb_friend_requests[fb_friend_requests.action == 'sent']
df_accepted=fb_friend_requests[fb_friend_requests.action == 'accepted']
new_df = pd.merge(df_sent, df_accepted,  how='left', left_on=['user_id_sender','user_id_receiver'], right_on = ['user_id_sender','user_id_receiver'])
accepted_count=new_df.groupby(["date_x"]).count().reset_index()
accepted_count["acceptance_rate"] = accepted_count["action_y"]/accepted_count["action_x"]
result = accepted_count[["date_x","acceptance_rate"]]
```

### 5.3 PySpark

```python
from pyspark.sql.functions import col, count

# Assuming `fb_friend_requests` is a PySpark DataFrame already loaded

# Filter sent and accepted requests into separate DataFrames
sent_df = fb_friend_requests.filter(col("action") == "sent").alias("sent")
accepted_df = fb_friend_requests.filter(col("action") == "accepted").alias("accepted")

# Perform a left join between sent_df and accepted_df
joined_df = sent_df.join(
    accepted_df,
    (col("sent.user_id_sender") == col("accepted.user_id_sender")) &
    (col("sent.user_id_receiver") == col("accepted.user_id_receiver")),
    "left"
).select(
    col("sent.date").alias("request_date"),
    col("sent.user_id_sender").alias("sent_user_id_sender"),
    col("accepted.user_id_receiver").alias("accepted_user_id_receiver")
)

# Aggregate by request_date to calculate acceptance rate
result_df = joined_df.groupBy("request_date").agg(
    count("accepted_user_id_receiver").alias("accepted_count"),
    count("sent_user_id_sender").alias("sent_count")
).withColumn(
    "percentage_acceptance",
    col("accepted_count") / col("sent_count")
)

# Sort the results by request_date and select required columns
output_df = result_df.select("request_date", "percentage_acceptance").orderBy("request_date")

# Convert to Pandas DataFrame
output_pd = output_df.toPandas()
```

> Other dialects shipped by StrataScratch for this problem (for reference; not engines I track):
> - **MySQL**: same idea but written as `RIGHT JOIN` from `accepted` onto `sent`, and relies on MySQL's `/` returning a float automatically (no explicit cast).
> - **MSSQL**: identical to the PostgreSQL CTE solution.
> - **Oracle**: same CTE shape, but `date` is quoted as `"date"` (reserved word) and cast is to `FLOAT`, with an explicit `ORDER BY`.
> - **R (dplyr)**: `accepted %>% right_join(sent, ...)` then `summarise(acceptance_rate = sum(!is.na(date_accept))/n())` — note it counts non-NULL accept dates instead of `COUNT(col)`.
> - **Polars**: `df_sent.join(df_accepted, on=[...], how='left')` then `group_by('date').agg(count of action vs action_right)`.

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author (PostgreSQL) | Impact |
|---|---|---|---|
| Overall structure | `request` / `accept` CTEs, `request LEFT JOIN accept`, `GROUP BY r.date` | `sent_cte` / `accepted_cte`, `sent LEFT JOIN accepted`, `GROUP BY a.date` | **Essentially identical** — same algorithm, same join direction, same grouping. This is the rare case where my accepted answer matches the canonical solution almost line-for-line. |
| Numerator | `count(a.user_id_receiver)` | `count(b.user_id_receiver)` | Same idiom: count the receiver column from the *accepted* side, which is NULL for unmatched sends and therefore skipped. |
| Denominator + cast | `cast(count(r.user_id_sender) as decimal)` | `CAST(count(a.user_id_sender) AS decimal)` | Same — both cast the denominator to `decimal` to dodge integer-division truncation. |
| Join keys | `(sender, receiver)` pair | `(sender, receiver)` pair | Same — both correctly match on the *pair*, not on date, so a cross-date acceptance still matches its send. |
| Output column name | `percentage_accept` | `percentage_acceptance` | Cosmetic; grader accepts either. |
| Alias naming | `request` / `accept` (`r` / `a`) | `sent_cte` / `accepted_cte` (`a` / `b`) | Mine read slightly clearer (`request`/`accept` is more semantic than `a`/`b`). Trivial. |

**Key insight in one line:** my final solution *is* the canonical pattern — split one event table into "sent" and "accepted" by `action`, **LEFT JOIN on the (sender, receiver) pair** so every send is kept, and divide `COUNT(accepted_col)` by a `CAST(... AS decimal)` denominator; the only thing that ever stood between me and "accepted" was a stray comma after the last CTE.

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=10285)` — **11 attempts total**, all PostgreSQL (code_type 1). One inspection run on 2025-06-23, the rest on 2025-06-25. Of these, **2 are solution submissions** (the duplicate accepted pair at 01:51) and the rest are runs.

| # | Time (UTC) | Language | Status | What happened |
|---|---|---|---|---|
| 1 | 2025-06-23 10:22 | SQL | run | `SELECT * FROM fb_friend_requests` — first look at the table, two days before the real attempt |
| 2 | 01:28 | SQL | run | `SELECT * ... WHERE action = 'accepted'` — inspect the accepted rows |
| 3 | 01:28 | SQL | run | `SELECT * ... WHERE action = 'sent'` — inspect the sent rows |
| 4 | 01:44 | SQL | ❌ run | First join attempt: INNER `JOIN` listing matched pairs with `WHERE r.date <= a.date`; died on `syntax error at or near "select"` — **trailing comma after the `accept` CTE** |
| 5 | 01:45 | SQL | ❌ run | Same INNER + `r.date <= a.date` idea, reformatted onto one line; same trailing-comma syntax error |
| 6 | 01:46 | SQL | ❌ run | Wrapped the join condition in parentheses `on (r.sender = a.sender and r.receiver = a.receiver)`; still the trailing comma → same error |
| 7 | 01:48 | SQL | ❌ run | Pivoted to the LEFT-JOIN + `GROUP BY date` ratio (using `SELECT *` CTEs); correct logic now, but **comma still there** → same syntax error |
| 8 | 01:49 | SQL | ❌ run | Slimmed CTEs to select only the 3 needed columns; comma still present → same error |
| 9 | 01:50 | SQL | ❌ run | Effectively final query, but **the comma after `accept as (...)` was still there** → `syntax error at or near "select"` |
| 10 | 01:51 | SQL | run | **Deleted the trailing comma** — first clean run of the correct query |
| 11 | 01:51 | SQL | ✅ **submitted** | Same query, submitted → **accepted** (`percentage_accept` ratio per date) |

What the failed attempts taught me:
- **A CTE chain has commas *between* CTEs, never *before* the main query.** Six of my failures were the identical error from one comma: `accept as ( ... ),` then `SELECT`. The fix is to drop the comma after the *last* CTE. Lesson: when I see `syntax error at or near "select"` right after a `WITH` block, the first thing to check is the punctuation joining the CTEs to the main query — not the join logic, which I wrongly suspected for ~7 minutes.
- **INNER JOIN + a date filter would have silently broken the denominator.** My first instinct (`JOIN ... WHERE r.date <= a.date`) keeps only accepted pairs, so every surviving date would show ~100% acceptance. The correct denominator needs *all* sent rows, which only a **LEFT JOIN** preserves. I caught this myself before submitting, but it's the conceptual trap of the problem: the join must keep the full "sent" population.
- **Match on the entity pair, not on the date.** Because acceptance can occur on a later date, joining on `date` would lose those matches. The whole point is to join on `(sender, receiver)` and let `date` be only the `GROUP BY` key.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context.

### 8.1 Correctness
- **Score**: 5/5
- Comment: Your PostgreSQL solution is exactly right — the `LEFT JOIN accept a ON r.user_id_sender = a.user_id_sender AND r.user_id_receiver = a.user_id_receiver` keeps every sent row in the denominator, and `count(a.user_id_receiver)` counts only matched accepts because COUNT ignores the NULLs from unmatched LEFT-JOIN rows. The pair-key join (not a date join) correctly captures cross-date acceptances. This is the canonical answer. The 5/5 is for the one language you submitted; pandas and PySpark are unattempted and carry no signal here.

### 8.2 Performance & Efficiency
- **Score**: 4/5
- Comment: Clean shape — two filtered scans on `action`, one hash join on the pair, one `GROUP BY r.date`. No window misuse, no nested subqueries, no redundant passes. The one thing to name in an interview: `fb_friend_requests` is scanned twice (`where action = 'sent'` and `where action = 'accepted'`). A single pass with conditional aggregation (`count(...) FILTER (WHERE action='accepted')` over a self-grouped set) is an alternative, but the two-CTE form is more readable and the planner handles it well — say that trade-off out loud rather than leaving it implicit.

### 8.3 Readability
- **Score**: 4/5
- Comment: Your `request`/`accept` names (aliased `r`/`a`) read more semantically than the author's `a`/`b` — good instinct. The deduction is indentation: the `request` CTE on lines 33-36 wraps `user_id_receiver` to a ragged second line while the `accept` CTE is block-formatted, so the two CTEs don't visually rhyme. Pick one style — align both CTE bodies the same way — and the query scans in one glance.

### 8.4 Edge Cases
- **Score**: 3/5
- Comment: The `cast(count(r.user_id_sender) as decimal)` is the load-bearing edge guard and you nailed it — without it `3/5` truncates to `0`. NULL handling via `count(col)` is correct. The gap: no `DISTINCT`. If a `(sender, receiver)` pair appears more than once on the accepted side (a duplicate or re-accept event), the LEFT JOIN fan-out double-counts the numerator and the rate can exceed 1.0. The author solution shares this assumption, but a bar-raiser expects you to flag it. Fix: either `count(DISTINCT a.user_id_sender, a.user_id_receiver)` or state aloud "I'm assuming at most one accepted row per pair."

### 8.5 Interview-Readiness
- **Score**: 3/5
- Comment: The logic that reached "accepted" was sound, but the path raises two flags. First, breadth: pandas and PySpark are marked "⚠️ Not attempted" — in a loop that can ask you to port to a DataFrame API on the spot, one engine is a real gap, and the author's `pd.merge(how='left')` + `groupby().count()` is a five-minute port. Second, debugging discipline: six attempts (01:44-01:50) died on the identical `syntax error at or near "select"` from a trailing comma after the `accept` CTE, while you suspected the join. Concrete fix: when a `WITH` block throws an error pointing at `select`, read the punctuation between the last CTE and the main query first — and narrate the assumption ("at most one accept per pair") before you write the join.

### 8.6 Verdict
Your final query is the canonical pattern, and your self-caught pivot from `INNER JOIN ... WHERE r.date <= a.date` to a LEFT JOIN — preserving the full sent population in the denominator — shows you understand the actual trap of this problem. The one thing that moves you to the next level is verbalized edge-case discipline: a strong senior states the de-duplication assumption ("one accept per pair, else the rate breaks 1.0") and the two-scan trade-off *before* the join goes on the board, so the interviewer never has to ask. Pair that with closing the pandas/PySpark gap, and a near-identical-to-canonical solution becomes a clear hire signal instead of a quiet one.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Split one table by a flag | two CTEs `WHERE action='sent'` / `='accepted'` | `df[df.action=='sent']` / `df[df.action=='accepted']` | `.filter(col("action")=="sent")` / `=="accepted"` |
| LEFT JOIN on a multi-column key | `LEFT JOIN ... ON a.sender=b.sender AND a.receiver=b.receiver` | `pd.merge(..., how='left', left_on=[...], right_on=[...])` | `.join(other, (col("...")==col("...")) & (col("...")==col("...")), "left")` |
| Count only matched rows (skip NULLs) | `count(b.user_id_receiver)` (COUNT ignores NULL) | `groupby().count()` on the accepted-side column (`action_y`) | `count("accepted_user_id_receiver")` |
| Ratio without integer truncation | `count(...)/CAST(count(...) AS decimal)` | float division is default (`action_y/action_x`) | `col("accepted_count")/col("sent_count")` (float default) |
| Group by the send date | `GROUP BY a.date` | `groupby(["date_x"])` (the sent-side date after merge suffix) | `.groupBy("request_date")` |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: "**self-join one event log into two states, then take a ratio per group**." When a single table records both a request and its fulfilment as separate rows distinguished by a flag column, split it into two filtered sets, **LEFT JOIN the fulfilment side onto the request side on the entity key** (here the `(sender, receiver)` pair, *not* the date), and compute `COUNT(fulfilment_col) / COUNT(request_col)` grouped by the dimension you care about. The LEFT JOIN guarantees the denominator stays the full request population; `COUNT(col)` skipping NULLs gives the numerator for free.

- **Reusable snippet (PostgreSQL, parameterized)**:
  ```sql
  WITH requested AS (SELECT key1, key2, dim FROM events WHERE action = 'sent'),
       fulfilled AS (SELECT key1, key2      FROM events WHERE action = 'accepted')
  SELECT r.dim,
         COUNT(f.key1) / CAST(COUNT(r.key1) AS decimal) AS rate
  FROM requested r
  LEFT JOIN fulfilled f ON r.key1 = f.key1 AND r.key2 = f.key2
  GROUP BY r.dim;
  ```

- **Weakness I noticed in myself**:
  1. **Punctuation blindness in CTE chains.** Six identical failures from one trailing comma after the last CTE, and I spent the time suspecting my join instead of the syntax. The error message even pointed at `select` — i.e. *right after* the comma. Drill: when a `WITH` query throws `syntax error at or near "select"`, scan the comma between the final CTE and the main query *first*.
  2. **Reaching for a date filter when the join key is an entity.** My instinct to add `WHERE r.date <= a.date` shows I was thinking temporally when the matching is relational (sender+receiver). For "rate of X that became Y" questions, match on the *thing*, group by the *date* — don't conflate the two.
  3. **No pandas / PySpark attempt.** I solved only PostgreSQL. The author's `pd.merge(how='left')` + `groupby().count()` and the PySpark `.join(..., "left")` + `count()` are short; porting this would cost a few minutes and close a real gap for interview breadth.

- **Drill for tomorrow morning**:
  1. Re-solve 10285 cold in PostgreSQL — get the comma right on the first pass and time-box to 5 minutes.
  2. Port it to pandas using `pd.merge(how='left')` + `groupby(["date_x"]).count()` + a float ratio (the author's exact shape), since I have zero pandas attempts here.

## 11. Related Notes

- **Same category (Date / Time — "rate / metric by date")**: other Date/Time notes where `date` is the `GROUP BY` key and the work is a per-date ratio or running metric. Pair this with any "X per day / acceptance or conversion rate over time" problem.
- **Adjacent skills exercised (the real meat)**: **self-join of one table into two roles** (sent vs. accepted) — see Join (multi-table) notes that reuse one table under two aliases; and **conditional-count ratios** (`COUNT(matched)/COUNT(total)`) — see Aggregation notes that compute rates with NULL-skipping `COUNT(col)`. This problem is mislabeled by the heuristic; conceptually it belongs with those.
- **Reference reading**:
  - PostgreSQL `WITH` / CTE syntax (commas go *between* CTEs): https://www.postgresql.org/docs/current/queries-with.html
  - `COUNT(col)` ignores NULLs (the numerator trick): https://www.postgresql.org/docs/current/functions-aggregate.html
  - Integer vs. numeric division / `CAST` (why the denominator cast matters): https://www.postgresql.org/docs/current/functions-math.html
  - pandas `merge` with `how='left'`: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html

---

*Generated 2026-06-30 by Ina (problem solving) + Claude (author solution fetch via MCP, submission history analysis, FAANG reviewer subagent).*
