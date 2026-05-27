# Category Heuristic Rules

The StrataScratch MCP does not return any category metadata. We confirmed this on
2026-05-27 by inspecting the actual responses of `list_questions` and `get_question`
— neither has a `category` field.

So every morning, the scheduled task fetches each candidate problem's full body and
classifies it with keyword matching against the problem text (`question`), the title
(`question_short`), and table names.

## Seven Category Buckets

Rules are applied top-to-bottom. First match wins.

### 1. Window Functions
Keywords: `RANK`, `ROW_NUMBER`, `LAG`, `LEAD`, `OVER`, `PARTITION BY`, `DENSE_RANK`,
`NTILE`, `FIRST_VALUE`, `LAST_VALUE`,
"rank", "consecutive", "running total", "moving average", "n-th", "percentile"

### 2. Subquery / CTE
Keywords: `WITH`, `EXISTS`, `NOT EXISTS`, `NOT IN (SELECT`,
"in the same", "compared to all", "twice the average", "nested"

### 3. Join (multi-table)
Triggers:
- `tables` field has length ≥ 2, or
- Body mentions `JOIN`, `INNER`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN`,
  "two tables", "both tables", or contains self-referencing columns
  (e.g. `manager_id` pointing back to the same table = self-join hint)

### 4. String / Text
Keywords: `LIKE`, `SUBSTRING`, `REPLACE`, `CONCAT`, `SPLIT`, `LENGTH`, `UPPER`,
`LOWER`, `REGEXP`,
"contains", "starts with", "ends with", "the word", "name", "text", "char"

### 5. Date / Time
Keywords: `DATE`, `MONTH`, `YEAR`, `WEEK`, `DAY`, `INTERVAL`, `EXTRACT`,
`DATE_TRUNC`,
"per month", "per day", "between", "consecutive days", "since", "rate by date",
"quarter", "recent"

### 6. Aggregation
Keywords: `GROUP BY`, `SUM`, `AVG`, `COUNT`, `MAX`, `MIN`, `HAVING`,
"total", "per", "average", "highest", "lowest", "distinct count", "unique"

### 7. Filter / Basic
Fallback bucket when nothing else matches. Typical signals: `WHERE`, `IN`, `BETWEEN`,
`IS NULL`, "find", "list", "return".

## Daily Balancing Rule

When picking the day's 5 problems, the scheduler greedy-selects so that the 5
problems cover **5 different buckets** as much as possible. If the candidate pool
runs short, repeats are allowed.

Every problem's chosen category goes into the daily JSON under `category`, so
misclassifications are visible at a glance.

## Confidence — kept honest

This is keyword heuristic, not semantic understanding. Misclassifications happen.
Two common failure modes:

- A problem that *should* be solved with window functions but the body never says
  `RANK` or "consecutive" — falls into Filter or Aggregation by default.
- A problem with multiple legitimate categories (e.g. "running total per month"
  is both Window and Date) — the first matching rule wins, so the rest are silent.

When I notice a misclassification, I edit the rules above and add the missing
keyword. The rules file is part of the workflow, not a fixed config.

## Change log

- 2026-05-27: Initial draft (Ina + Claude)
