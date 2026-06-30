# 10352 — Users By Average Session Time

## 1. Problem Metadata

| Field | Value |
|---|---|
| ID | 10352 |
| Category | Date / Time |
| Difficulty | Medium |
| StrataScratch links | [PostgreSQL](https://platform.stratascratch.com/coding/10352-users-by-avg-session-time?code_type=1) · [Python](https://platform.stratascratch.com/coding/10352-users-by-avg-session-time?code_type=2) · [PySpark](https://platform.stratascratch.com/coding/10352-users-by-avg-session-time?code_type=6) |
| Solved on | 2025-06-23 (PostgreSQL only) |
| Tables | `facebook_web_log(action text, timestamp timestamp without time zone, user_id bigint)` |

## 2. Problem Restatement (in my words)

`facebook_web_log` is an event stream: every row is one user firing one `action` (`page_load`, `page_exit`, and other action types like scroll/click) at some `timestamp`. A "session" is one user's stretch from when they loaded the page to when they exited it, and we're told each user has at most one session per calendar day. Because a single day can carry several `page_load` or `page_exit` rows, collapse them: take the **latest** `page_load` and the **earliest** `page_exit` of that day. Keep the day only if that load actually happened **before** that exit (drop malformed days where exit precedes load). Session duration = exit − load. Finally, average each user's per-day durations and output `user_id` with that average session time.

The two traps: (1) you must reduce many same-day events down to one load and one exit *before* differencing — not just blindly pair every load with every exit; (2) the load↔exit pairing has to be scoped to the **same user AND same day**, not across days.

## 3. My Approach

- **First instinct**: inspect the raw table (`select * from facebook_web_log`), then immediately reach for a window function — `rank() over (partition by user_id, to_char(timestamp,'YYYY-MM-DD') order by timestamp)` to number events within each user-day. My mental model was "rank the events in a day, the first one is the load, the last one is the exit." I spent the first ~10 attempts just getting the window syntax to parse (stray closing paren `))`, `partition by user_id AND to_char(...)` which SQL read as a boolean, etc.).
- **Where I got stuck** — three distinct walls:
  1. **Window syntax mechanics.** Repeated `syntax error at or near ")"` from an extra paren after the `OVER(...)` clause, and `argument of AND must be type boolean` because I wrote `partition by user_id and to_char(...)` instead of comma-separating the partition keys.
  2. **`GROUP BY` after the join.** Several runs threw `column "l.user_id" must appear in the GROUP BY clause` — I selected `l.user_id, avg(...)` but forgot the `GROUP BY l.user_id`, or I had it on the wrong side. Also a `==` slip (`action_order == 1` → `operator does not exist: bigint == integer`).
  3. **The real conceptual wall: how to pick "the" load and "the" exit per day.** My rank-based filtering kept producing the *wrong rows*. I tried `action_order = 1` for the load and `max(action_order)` for the exit, but `action_order` ranked *all* actions in the day (including scroll/click rows), so "rank 1" wasn't guaranteed to be a `page_load` and "max rank" wasn't guaranteed to be a `page_exit`. I then tried `min(action_order)`/`max(action_order)` *as window functions inside* the page_load/page_exit CTEs — but I left them as bare window columns I never filtered on, so the CTEs still returned every matching row, and the join produced a cartesian-ish blow-up of load×exit pairs per day. Five of these variants got **submitted and rejected as incorrect** (the SQL ran fine, just wrong numbers). I also tried `age(e.timestamp, l.timestamp)` instead of plain subtraction — still wrong because the underlying pairing was wrong, not the differencing.
- **Final strategy (what actually passed)**: I abandoned the per-day MIN/MAX-rank framing entirely and switched to a **sequential-pairing** idea. Filter to just `('page_load','page_exit')`, number them per user in timestamp order with `row_number()`, then **self-join consecutive rows** (`a.seq_num = b.seq_num - 1`) keeping only the pairs where `a.action='page_load'` and `b.action='page_exit'`. Each kept pair is one load immediately followed by its exit; `b.timestamp - a.timestamp` is the duration; `avg(...) group by user_id` finishes it. It passed on the first submission of this new shape. (Honest caveat: this works on the seed data because each user's load/exit events interleave cleanly one-after-another; it is *not* the same logic as the official "latest load / earliest exit per day" rule — see Section 6.)

## 4. My Solutions

### 4.1 PostgreSQL ✅ (passed 2025-06-23 08:27)

```sql
with action_rank as(
    select user_id, timestamp, action,
        row_number() over (partition by user_id order by timestamp) as seq_num
        from facebook_web_log
        where action in ('page_load', 'page_exit')
),
matched_sessions as (
    select 
        a.user_id,
        a.timestamp as load_time,
        b.timestamp as exit_time,
        b.timestamp - a.timestamp as session_duration
        from action_rank a
        join action_rank b on a.user_id = b.user_id and a.seq_num = b.seq_num - 1
        where a.action = 'page_load' and b.action = 'page_exit'
)
select user_id, avg(session_duration) as avg_session_duration
from matched_sessions
group by user_id;
```

> Note on the pairing: `row_number()` is partitioned by `user_id` only (not by day), so `seq_num` runs continuously across all of a user's load/exit events. The self-join on `seq_num = seq_num - 1` pairs each event with the *immediately following* one, and the `WHERE a.action='page_load' AND b.action='page_exit'` keeps only load→exit adjacencies. This returns the correct answer on the seed data because each session's load and exit are adjacent in time, but it is structurally different from the spec's "latest load / earliest exit per day" reduction. If a user had two `page_load`s in a row before an exit, or a `page_exit` that came first, this pairing would behave differently from the author's. See Section 6 for the contrast.

### 4.2 Python (pandas)

⚠️ Not attempted / no accepted submission in my history.

### 4.3 PySpark

⚠️ Not attempted / no accepted submission in my history.

## 5. Author Solutions

> Pulled from StrataScratch via MCP `get_question(include_solution=True)`.
> Premium-only feature.

### 5.1 PostgreSQL

```sql
WITH loads AS (
  SELECT
    user_id,
    DATE(timestamp) AS day,
    MAX(timestamp) AS load_time
  FROM facebook_web_log
  WHERE action = 'page_load'
  GROUP BY user_id, DATE(timestamp)
),
exits AS (
  SELECT
    user_id,
    DATE(timestamp) AS day,
    MIN(timestamp) AS exit_time
  FROM facebook_web_log
  WHERE action = 'page_exit'
  GROUP BY user_id, DATE(timestamp)
),
sessions AS (
  SELECT
    l.user_id,
    l.load_time,
    e.exit_time,
    EXTRACT(EPOCH FROM (e.exit_time - l.load_time)) AS session_duration
  FROM loads l
  JOIN exits e
    ON l.user_id = e.user_id AND l.day = e.day
  WHERE l.load_time < e.exit_time
)
SELECT
  user_id,
  AVG(session_duration) AS avg_session_duration
FROM sessions
GROUP BY user_id;
```

### 5.2 Python (pandas)

```python
import pandas as pd

facebook_web_log['timestamp'] = pd.to_datetime(facebook_web_log['timestamp'])
facebook_web_log['day'] = facebook_web_log['timestamp'].dt.floor('d')

loads = facebook_web_log.loc[facebook_web_log['action'] == 'page_load'] \
    .groupby(['user_id', 'day']) \
    .agg(load_time=('timestamp', 'max')) \
    .reset_index()

exits = facebook_web_log.loc[facebook_web_log['action'] == 'page_exit'] \
    .groupby(['user_id', 'day']) \
    .agg(exit_time=('timestamp', 'min')) \
    .reset_index()

sessions = pd.merge(loads, exits, on=['user_id', 'day'])
sessions = sessions[sessions['load_time'] < sessions['exit_time']]

sessions['session_duration'] = (sessions['exit_time'] - sessions['load_time']).dt.total_seconds()

result = sessions.groupby('user_id')['session_duration'].mean().reset_index()
result.rename(columns={'session_duration': 'avg_session_duration'}, inplace=True)
result
```

### 5.3 PySpark

```python
from pyspark.sql import functions as F

facebook_web_log = facebook_web_log.withColumn('day', F.date_trunc('day', F.col('timestamp')))

loads = facebook_web_log.filter(F.col('action') == 'page_load') \
    .groupBy('user_id', 'day') \
    .agg(F.max('timestamp').alias('load_time'))

exits = facebook_web_log.filter(F.col('action') == 'page_exit') \
    .groupBy('user_id', 'day') \
    .agg(F.min('timestamp').alias('exit_time'))

sessions = loads.join(exits, on=['user_id', 'day']) \
    .filter(F.col('load_time') < F.col('exit_time')) \
    .withColumn('session_duration',
                F.unix_timestamp('exit_time') - F.unix_timestamp('load_time'))

agg_sessions = sessions.groupBy('user_id') \
    .agg(F.avg('session_duration').alias('avg_session_duration'))

result = agg_sessions.toPandas()

result
```

> For reference, StrataScratch also ships MySQL, MSSQL, Oracle, R, and Polars solutions for this problem. The MySQL/MSSQL/Oracle variants are the same `loads`/`exits`/`sessions` CTE structure with engine-specific duration math (`TIMESTAMPDIFF(SECOND, …)`, `DATEDIFF(SECOND, …)`, `EXTRACT(... ) * 60/3600/86400` respectively). The three engines above are the canonical ones I track.

## 6. Diff Analysis (mine vs. author)

| Aspect | Mine | Author | Impact |
|---|---|---|---|
| Per-day reduction | None — I never collapse same-day duplicates. `row_number()` is partitioned by `user_id` only and I pair *adjacent events in time* | Two CTEs reduce each user-day to exactly one row: `loads` = `MAX(timestamp)` (latest load), `exits` = `MIN(timestamp)` (earliest exit), both `GROUP BY user_id, DATE(timestamp)` | Author implements the spec literally ("latest load / earliest exit per day"); mine implements a *different rule* (consecutive load→exit) that happens to match on this data |
| Pairing mechanism | Self-join `action_rank` to itself on `seq_num = seq_num - 1`, then `WHERE a.action='page_load' AND b.action='page_exit'` | Join `loads` to `exits` on `user_id = user_id AND day = day` | Author's join key is explicit and day-scoped; mine relies on temporal adjacency holding |
| "load before exit" guard | Implicit — a load→exit adjacency can only have the load first | Explicit `WHERE l.load_time < e.exit_time` drops malformed days | Author defends the edge case the spec calls out; mine never checks it |
| Duration expression | `b.timestamp - a.timestamp` → returns a Postgres `interval`; `AVG(interval)` returns an interval | `EXTRACT(EPOCH FROM (e.exit_time - l.load_time))` → seconds as a number; `AVG` returns a numeric | Both were accepted by the grader, so an interval average is fine here; author's epoch-seconds is the more portable/explicit form |
| Window function | `ROW_NUMBER()` central to the whole approach | None — pure `GROUP BY` aggregation + join | Author needs no window at all; the problem is fundamentally aggregate-per-group, not ranking |
| Number of passes | 1 window CTE + 1 self-join CTE + final group = 3 steps | 2 aggregation CTEs + 1 join CTE + final group = 4 steps | Comparable length; the difference is *correct-by-construction* vs. *correct-by-luck-of-data* |

**Key insight in one line:** the canonical idiom is "split the stream by action, reduce each `(user, day)` to one `MAX(load)` and one `MIN(exit)`, join on `user_id+day`, guard `load < exit`, then average the difference" — a pure aggregation pattern; my `ROW_NUMBER()` consecutive-pairing produced the right answer on the seed data but encodes a different rule than the problem actually specifies.

## 7. Submission History (auto-captured)

From MCP `get_my_attempts(question_id=10352)` — **38 attempts total**, all PostgreSQL (code_type 1), all on 2025-06-23. Of these, 8 were graded submissions (`is_solution_submission=true`): 7 incorrect, 1 correct. Highlights:

| # | Time (UTC) | Language | Status | What happened |
|---|---|---|---|---|
| 1 | 07:39 | SQL | run | `select * from facebook_web_log` — inspect the raw event stream |
| 2 | 07:44 | SQL | ❌ run | `rank() over (... order by timestamp))` — stray extra `)` → `syntax error at or near ")"` |
| 3 | 07:44 | SQL | ❌ run | `partition by user_id and to_char(...)` → `argument of AND must be type boolean, not bigint` (need comma, not AND) |
| 4 | 07:45 | SQL | run | Fixed the window: `partition by user_id, to_char(timestamp,'YYYY-MM-DD') order by timestamp` |
| 5 | 07:52 | SQL | ❌ run | `action_order == 1` → `operator does not exist: bigint == integer` (`==` is not SQL equality) |
| 6 | 07:52–07:55 | SQL | ❌ run | Building `page_load`/`page_exit` CTEs with `where action_order = (select max(action_order) from log_rank)`; misc syntax slips (`max(action_order from log_rank)`, extra `)`) |
| 7 | 08:04 | SQL | ❌ **submitted** | First real submission: `sum((e.timestamp - l.timestamp))` (SUM not AVG) + exit picked via `max(action)` window → incorrect |
| 8 | 08:05 | SQL | ❌ **submitted** | Swapped to `avg(...)` but exit CTE still used `max(action) over (...)` un-filtered → wrong pairing → incorrect |
| 9 | 08:06–08:09 | SQL | ❌ run | `select l.user_id, avg(...)` with no `GROUP BY` → `column "l.user_id" must appear in the GROUP BY clause` (several times) |
| 10 | 08:09 | SQL | ❌ **submitted** | Added `group by l.user_id`, but `page_exit` CTE still returns every row (un-filtered window) → wrong → incorrect |
| 11 | 08:14 | SQL | ❌ **submitted** | `page_load` via `action_order = 1` (ranks *all* actions, not just loads) + `page_exit` via `max(action_order)` window → incorrect |
| 12 | 08:15 | SQL | ❌ **submitted** | Switched difference to `age(e.timestamp, l.timestamp)` — still wrong because the *pairing* was wrong, not the diff → incorrect |
| 13 | 08:15 | SQL | ❌ **submitted** | Same as #12, `age()` form re-submitted → incorrect (7th rejected submission) |
| 14 | 08:27 | SQL | ✅ **submitted** | **New approach**: `row_number() over (partition by user_id order by timestamp)`, self-join `seq_num = seq_num - 1`, keep `a.action='page_load' AND b.action='page_exit'`, `avg(b.ts - a.ts) group by user_id` → **accepted** |

What the failed attempts taught me:
- **My `RANK()` "first action = load, last action = exit" model was wrong from the start.** `action_order` ranked *every* action in the day (scrolls, clicks, etc.), so `action_order = 1` was not necessarily a `page_load` and `max(action_order)` was not necessarily a `page_exit`. I burned ~6 submissions on variants of this broken assumption before abandoning it. Lesson: when filtering to "the load" and "the exit," filter to the action type **first**, *then* rank/aggregate within that type — exactly what the author's split-into-`loads`/`exits` CTEs do.
- **A window function in the SELECT list does nothing unless you filter on it.** I wrote `max(action_order) over (partition by ...)` inside the `page_exit` CTE and assumed it would "pick" the exit row — but a window column just *adds a column*; the CTE still returned every row, so the join exploded into many load×exit pairs. To actually pick one row you need `WHERE` / `QUALIFY` / a `GROUP BY MAX`, not a bare window column.
- **SQL hygiene I keep slipping on**: `==` (not valid; use `=`), comma-separated `PARTITION BY` keys (not `AND`), balanced parens around `OVER(...)`, and `GROUP BY` must list every non-aggregated SELECT column. Five-plus of my 38 attempts died on these mechanical errors before I ever got to the logic.
- **The grader gives no hint on a wrong-but-valid query.** All 7 rejected submissions parsed and ran; they just returned wrong numbers with no message. I should have built a tiny sanity check (eyeball one user's load/exit rows and hand-compute the expected duration) instead of resubmitting six variations of the same broken pairing.

## 8. Senior DA / DS Review — Structured Rubric

> Produced by the **Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona).
> Prompt: [`docs/_reviewer_agent.md`](../docs/_reviewer_agent.md). Fresh context.

### 8.1 Correctness
- **Score**: 3/5
- Comment: Your query passes the grader, but it is correct-by-data, not correct-by-spec, and you know it — your own Section 6 admits the `seq_num = seq_num - 1` consecutive-pairing "encodes a different rule." The prompt says *latest* load and *earliest* exit per day; your `ROW_NUMBER() ... partition by user_id` (no day key) pairs whatever event happens to follow. On a day with two `page_load`s before one `page_exit`, your join keeps the load→load adjacency out and pairs the second load, silently diverging from the author's `MAX(timestamp)` load. Concrete fix: re-implement with the author's `loads`/`exits` split (`MAX`/`MIN` grouped by `user_id, DATE(timestamp)`) and diff the two correct-by-construction queries against your output — if they disagree on any seeded user, your version is wrong.

### 8.2 Performance & Efficiency
- **Score**: 3/5
- Comment: The self-join of `action_rank` to itself is an extra pass the aggregate approach never needs: the author hits the base table twice with `WHERE action = '...' GROUP BY` (two index-eligible scans) and joins two already-reduced row sets. Your `ROW_NUMBER()` window sorts every load/exit event per user, then the self-join on `a.seq_num = b.seq_num - 1` re-materializes a paired set. At seed scale it is fine; at production scale the window sort plus self-join dominates. Concrete fix: replace the window+self-join with the two-CTE `GROUP BY MAX/MIN` reduction — fewer rows enter the join, and the grouping keys (`user_id`, `DATE(timestamp)`) are what you'd realistically index on.

### 8.3 Readability
- **Score**: 4/5
- Comment: This reads cleanly — `action_rank` and `matched_sessions` are well-named CTEs, the join condition is explicit, and `load_time`/`exit_time`/`session_duration` are good aliases. One blemish: the CTE name `action_rank` advertises ranking, but you use `row_number()`, not `rank()` — a reviewer skims the name and expects tie-handling semantics that aren't there. Concrete fix: rename the CTE to `ordered_events` or `seq_events` so the label matches `row_number()`'s actual contract.

### 8.4 Edge Cases
- **Score**: 2/5
- Comment: You never guard the malformed-day case the spec explicitly calls out. The author writes `WHERE l.load_time < e.exit_time` to drop days where exit precedes load; your version has no equivalent and you concede it is only "implicit." It is not actually safe — your pairing assumes load and exit interleave one-after-another, so an orphan `page_exit` with no preceding load, or a load with no exit, is handled by luck of ordering, not logic. Concrete fix: after pairing, add `WHERE a.timestamp < b.timestamp` (or adopt the day-scoped join) so an out-of-order or unmatched event cannot produce a negative or phantom duration.

### 8.5 Interview-Readiness
- **Score**: 3/5
- Comment: The strongest part of this note is your candor — explicitly flagging in Sections 3 and 6 that your approach diverges from the spec is exactly the assumption-naming a FAANG interviewer rewards. But in the loop itself you would lose signal: 7 wrong-but-valid submissions (#7–#13) show you debugged by resubmission against a silent grader instead of hand-verifying one user's load/exit rows first. You also solved only PostgreSQL; Python and PySpark are "⚠️ Not attempted," so you have no cross-engine fluency to demonstrate. Concrete fix: before any resubmit, hand-compute one user's expected average from the raw rows and assert your query matches it — that single habit kills the six-variant resubmit loop.

### 8.6 Verdict
The one thing that moves you up: reach for `GROUP BY MIN/MAX` before `ROW_NUMBER()`. This is a pure aggregate-per-group problem, and your reflex to window-and-pair produced an answer that is right on this data and wrong on the rule — a distinction a bar-raiser will probe in thirty seconds. Filter to the action type *first*, reduce each `(user, day)` to one row, then join; the window function disappears and the edge guard becomes trivial. Your self-awareness in the writeup is genuinely above bar — now make the code itself correct-by-construction, and port it to pandas and PySpark so the idiom sticks across all three engines.

## 9. Cross-Language Idioms (same problem, three engines)

| Idiom | PostgreSQL | Python (pandas) | PySpark |
|---|---|---|---|
| Truncate timestamp to the day | `DATE(timestamp)` | `df['timestamp'].dt.floor('d')` | `F.date_trunc('day', F.col('timestamp'))` |
| Latest load per group | `MAX(timestamp) ... GROUP BY user_id, DATE(timestamp)` | `.groupby(['user_id','day']).agg(load_time=('timestamp','max'))` | `.groupBy('user_id','day').agg(F.max('timestamp'))` |
| Earliest exit per group | `MIN(timestamp) ... GROUP BY user_id, DATE(timestamp)` | `.groupby([...]).agg(exit_time=('timestamp','min'))` | `.groupBy(...).agg(F.min('timestamp'))` |
| Join load to exit on user+day | `JOIN ... ON l.user_id=e.user_id AND l.day=e.day` | `pd.merge(loads, exits, on=['user_id','day'])` | `loads.join(exits, on=['user_id','day'])` |
| Duration in seconds | `EXTRACT(EPOCH FROM (exit - load))` | `(exit - load).dt.total_seconds()` | `F.unix_timestamp('exit') - F.unix_timestamp('load')` |
| Guard load before exit | `WHERE l.load_time < e.exit_time` | `sessions[sessions['load_time'] < sessions['exit_time']]` | `.filter(F.col('load_time') < F.col('exit_time'))` |
| Average per user | `AVG(session_duration) ... GROUP BY user_id` | `.groupby('user_id')['session_duration'].mean()` | `.groupBy('user_id').agg(F.avg('session_duration'))` |
| Equality / partition keys | `=` (not `==`); `PARTITION BY a, b` (comma, not `AND`) | `==` | `==` inside `F.col(...) == ...` |

## 10. Patterns to Remember

- **Core pattern this problem teaches**: "sessionize an event log → one duration per (entity, day)." The robust recipe is **split-by-action, reduce, join, difference, average**: (1) filter to `page_load`, aggregate `MAX(timestamp)` per `(user, day)`; (2) filter to `page_exit`, aggregate `MIN(timestamp)` per `(user, day)`; (3) join the two on `user_id + day`; (4) guard `load < exit`; (5) `AVG(exit − load)` per user. The key discipline: **filter to the event type before you rank/aggregate**, so "the load" and "the exit" are guaranteed to be the right action.
- **Reusable snippet (PostgreSQL, parameterized)**:
  ```sql
  WITH starts AS (
    SELECT entity_id, DATE(ts) AS day, MAX(ts) AS start_ts
    FROM events WHERE action = 'start' GROUP BY entity_id, DATE(ts)
  ),
  ends AS (
    SELECT entity_id, DATE(ts) AS day, MIN(ts) AS end_ts
    FROM events WHERE action = 'end' GROUP BY entity_id, DATE(ts)
  )
  SELECT s.entity_id,
         AVG(EXTRACT(EPOCH FROM (e.end_ts - s.start_ts))) AS avg_seconds
  FROM starts s
  JOIN ends e ON s.entity_id = e.entity_id AND s.day = e.day
  WHERE s.start_ts < e.end_ts
  GROUP BY s.entity_id;
  ```
- **Weakness I noticed in myself**:
  1. **I reach for window functions reflexively, even when plain `GROUP BY` is the right tool.** This whole problem is aggregate-per-group; the author uses zero window functions. I built a `ROW_NUMBER()` pipeline and got the right answer by accident (consecutive pairing happened to align with the data) rather than by encoding the actual "latest load / earliest exit" rule. Reflex to build: "pick the min/max event per group" → `GROUP BY group_key` with `MIN`/`MAX`, *not* a window I then have to filter.
  2. **I add window columns and forget they don't filter.** `max(x) over (...)` in the SELECT just appends a column; it never reduces rows. To actually keep one row per group I need `GROUP BY`, `WHERE rk = 1`, or `QUALIFY`.
  3. **I debugged by resubmission.** 7 rejected submissions, all parsed-but-wrong, no grader hint. I should hand-verify one user's expected duration before resubmitting.

## 11. Related Notes

- **Same category (Date / Time — "event-log sessionization & timestamp differencing")**: pair this with any problem that computes a duration between two timestamped events (login→logout, start→end, first→last action of a day), or that buckets events by `DATE(ts)` and reduces to a per-day MIN/MAX. The "latest X / earliest Y per day, then difference" shape recurs across web-log and session-analytics questions.
- **Adjacent skills exercised**: self-joining one event table into two roles (load rows vs. exit rows) — overlaps with Join (multi-table) notes that reuse a single table under two aliases; and `GROUP BY` MIN/MAX reduction — overlaps with Aggregation notes on "earliest/latest per group."
- **Reference reading**:
  - PostgreSQL date/time functions (`DATE()`, `EXTRACT(EPOCH …)`, interval arithmetic): https://www.postgresql.org/docs/current/functions-datetime.html
  - `GROUP BY` semantics (why every non-aggregated column must be grouped): https://www.postgresql.org/docs/current/sql-select.html#SQL-GROUPBY
  - Window functions vs. aggregation (when a window column does *not* filter rows): https://www.postgresql.org/docs/current/tutorial-window.html
  - pandas `merge` / `groupby().agg`: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html

---

*Generated 2026-06-30 by Ina (problem solving) + Claude (author solution fetch via MCP, submission history analysis, FAANG reviewer subagent).*
