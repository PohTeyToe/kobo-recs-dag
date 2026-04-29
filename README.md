# kobo-recs-dag

A Better-than-Quiz book-recommendations pipeline. Built as a portfolio piece
for [Abdallah Safi](https://abdallah-safi.vercel.app)'s application to the
Rakuten Kobo Data Engineer Co-op (Toronto, Apr 2026).

**Live:** https://kobo-recs-dag.vercel.app
**Source:** https://github.com/PohTeyToe/kobo-recs-dag

---

## The pitch

Kobo's Spring 2026 update (April 2) shipped personalized recommendations
driven by a single onboarding quiz. Power users on r/kobo and MobileRead
flagged the gaps: no genre exclusion, no dislike, and no learning from what
you actually read.

This demo is a working sketch of what a richer signal looks like. It runs
end-to-end on the same stack the JD calls out: **Airflow, dbt, SQL, Python,
BI**. The pipeline ingests Open Library (CC0 bibliographic data), generates
a synthetic reader-event stream, and produces top-10 recommendations from
two strategies side-by-side so they can be compared like-for-like.

> **Synthetic event data is for demonstration only and does not represent
> real Kobo telemetry.** No Kobo internal data was used or implied.

---

## Layout

```
dags/recs_dag.py             # Airflow DAG — production shape
models/
  _schema.yml                # dbt sources + schema/funnel tests
  staging/stg_books.sql      # Cleaned Open Library records
  staging/stg_events.sql     # Cleaned synthetic reader events
  intermediate/int_user_genre_affinity.sql
  marts/mart_recs.sql        # Top-10 per user × strategy
scripts/build-data.ts        # Node.js materialization for the demo
src/app/
  page.tsx                   # Landing
  dag/page.tsx               # Stage-by-stage pipeline view
  recs/page.tsx              # Interactive A/B explorer
  quality/page.tsx           # Schema + funnel tests
src/data/computed.json       # Materialized output (generated at build)
```

The Airflow DAG and the dbt SQL files describe the **production** shape.
The Vercel demo is materialized at build-time by a Node.js script
(`scripts/build-data.ts`) that runs the same five stages and writes a JSON
snapshot the static pages render. This keeps the deploy simple (no Python /
Airflow control plane) while keeping every model and test visible in the repo.

---

## Pipeline stages

| # | Stage | Operator | What it does |
|-|-|-|-|
| 1 | `ingest_open_library`  | KubernetesPodOperator | 100 books per subject from openlibrary.org/search.json |
| 2 | `generate_events`      | PythonOperator        | SYNTHETIC: ~5000 weighted events across 50 users |
| 3 | `dbt_run_staging`      | BashOperator → dbt    | `stg_books`, `stg_events` |
| 4 | `dbt_run_intermediate` | BashOperator → dbt    | `int_user_genre_affinity` (Σ ≤ 1 per user) |
| 5 | `dbt_run_marts`        | BashOperator → dbt    | `mart_recs` (top-10, two strategies) |
| 6 | `dbt_test`             | BashOperator → dbt    | Schema + funnel tests; halts on failure |
| 7 | `publish_to_redis`     | KubernetesPodOperator | Reverse-ETL `mart_recs` → Redis |
| 8 | `refresh_bi_dashboard` | SimpleHttpOperator    | POSTs Domo dataset refresh for Tolino BI |

## The two strategies

| Strategy | Signal | Why it matters |
|-|-|-|
| `quiz`   | Onboarding-quiz subjects only, ranked by recency | Mirrors the Apr 2026 Kobo rollout — single signal, no learning |
| `collab` | Cosine similarity over user × subject affinity → peer-weighted candidates with content backstop | Uses the actual reader-event stream; falls back to top-subject content matches when the pool is thin |

The `/recs` page lets you pick any synthetic user and see both columns
side-by-side with explanations on every row.

## Tests

Schema tests in `models/_schema.yml`:

- `unique`, `not_null` on every key column
- `accepted_values` on subject and event_type
- `relationships` from `stg_events.book_id` and `mart_recs.book_id` → `stg_books.book_id`
- `dbt_utils.unique_combination_of_columns` on `mart_recs(user_id, book_id, strategy)`

Funnel tests:

- `int_user_genre_affinity_sum_lte_1` — Σ normalized_score per user ≤ 1.0
- `mart_recs_coverage_both_strategies` — every user has recs from both strategies
- `mart_recs_relationship_book_id` — every recommendation references a real catalog book

All visible at `/quality` on the live site.

## Run locally

```bash
git clone https://github.com/PohTeyToe/kobo-recs-dag
cd kobo-recs-dag
npm install
npm run build:data    # fetches Open Library, writes src/data/computed.json
npm run dev           # http://localhost:3000
```

`npm run build` chains `build:data` then `next build`, so every Vercel deploy
gets a fresh dataset.

## Acknowledgements

Open Library for CC0/public-domain bibliographic data via the
[/search.json API](https://openlibrary.org/dev/docs/api/search). Architectural
pattern lifted from my SearchFlow portfolio project (Airflow + dbt +
reverse-ETL); this repo re-targets that shape at book recommendations.

## License

MIT.
