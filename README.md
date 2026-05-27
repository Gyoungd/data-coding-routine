# Data Coding Routine

Daily practice repo for **Data Analyst / Data Scientist / Data Engineer** interview coding.
Focus areas: **PostgreSQL**, **Python (pandas / Polars)**, and **PySpark (DataFrame + Spark SQL)**.

The goal here is not just to *solve* — it is to **learn out loud**.
Every problem comes back to this repo as a structured note: what I tried, what I missed,
what a senior reviewer would say, and what pattern I want to remember.

## How the routine works

1. **10:00 AM daily** (when Cowork is open): a scheduled task curates today's 5 problems
   from the StrataScratch freemium educational pool and saves them to
   `stratascratch_daily/YYYY-MM-DD.json`.
   - Weekdays: **1 Easy + 4 Medium**
   - Mondays: **1 Easy + 3 Medium + 1 Hard** (only 5 freemium Hards exist, so a 5-week cycle)
   - The 5 problems are picked to cover **different categories** (heuristic, see `docs/_category_rules.md`).
2. In Cowork chat, type `morning` (or `yesterday's set`, `Monday's set`, `May 25 set`)
   to load the queue. Each problem shows up with **direct StrataScratch links** for all three languages.
3. **Solve on the StrataScratch website.** Run / Check Solution are faster there than over MCP.
   I solve the same problem in PostgreSQL → Python → PySpark.
4. Back in Cowork: `review {problem_id}` triggers Claude to:
   - Pull my own submission history via MCP (`get_my_attempts`)
   - Pull the author solutions via MCP (`get_question(include_solution=True)`)
   - Diff mine against the author's
   - **Spawn the Senior DA/DS Reviewer subagent** (FAANG-tier interviewer persona,
     fresh context) to produce Section 8 of the note. Prompt:
     [`docs/_reviewer_agent.md`](docs/_reviewer_agent.md)
   - Write everything into `notes/{id}_{slug}.md`
5. I commit and push with GitHub Desktop.

## Repository layout

```
├─ stratascratch_daily/   # Daily curated set (one JSON per day)
├─ notes/                 # Learning notes per problem (markdown)
└─ docs/
   ├─ _category_rules.md  # Keyword heuristic for category bucketing
   ├─ _note_template.md   # Standard learning-note template
   └─ _reviewer_agent.md  # Senior DA/DS reviewer subagent prompt (FAANG persona)
```

## Conventions

- Note filename: `notes/{id}_{slug}.md` (e.g. `9805_find-drafts-optimism.md`)
- Original problem text and datasets are **never copied verbatim** — I paraphrase in my own words.
  This avoids licensing issues and forces me to prove I actually read the problem.
- StrataScratch submission history is always reachable through MCP `get_my_attempts`.
  `source='web'` means I solved it on the StrataScratch site. `source='mcp'` means I solved
  it through Cowork chat. Both count.

## Senior DA / DS Rubric (5 axes)

Every note scores my solution on:

1. **Correctness**
2. **Performance & Efficiency**
3. **Readability**
4. **Edge Cases**
5. **Interview-Readiness**

Each axis is 1–5 with a short comment. Full template in
[`docs/_note_template.md`](docs/_note_template.md).

## Known limitations (kept honest)

- **Only 5 Hard problems** in the freemium pool. Monday rotation will start repeating after
  five weeks.
- The StrataScratch public MCP exposes **freemium and non-hidden questions only**, even on
  Premium accounts.
- Category bucketing is keyword-based and **can misclassify** — `docs/_category_rules.md`
  is editable, just add keywords as they come up.
- The scheduled task only runs while the Cowork app is open. If the app is closed at 10 AM,
  the task fires once on next launch.
