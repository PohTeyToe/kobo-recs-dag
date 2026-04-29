import data from "@/data/computed.json";

const STAGES = [
  {
    id: "ingest",
    label: "ingest_open_library",
    op: "KubernetesPodOperator",
    desc: "Pulls 100 books per subject from openlibrary.org/search.json across fiction, mystery, sci-fi, romance, fantasy, young-adult. Lands raw to raw.openlibrary_search.",
  },
  {
    id: "events",
    label: "generate_events",
    op: "PythonOperator (synthetic)",
    desc: "Produces ~5000 weighted reader events. SYNTHETIC — replace with real Kafka consumer in prod.",
  },
  {
    id: "stg",
    label: "dbt_run_staging",
    op: "BashOperator → dbt",
    desc: "stg_books, stg_events. Type cast, deduplicate, filter to accepted event_types.",
  },
  {
    id: "int",
    label: "dbt_run_intermediate",
    op: "BashOperator → dbt",
    desc: "int_user_genre_affinity. Per-user × subject weighted score, normalized so Σ ≤ 1.",
  },
  {
    id: "mart",
    label: "dbt_run_marts",
    op: "BashOperator → dbt",
    desc: "mart_recs. Top-10 per user from each strategy (quiz, collab). Final output for downstream consumers.",
  },
  {
    id: "test",
    label: "dbt_test",
    op: "BashOperator → dbt",
    desc: "Schema tests + funnel-integrity tests. Halts the pipeline on first failure — no silently broken recs ship.",
  },
  {
    id: "etl",
    label: "publish_to_redis",
    op: "KubernetesPodOperator",
    desc: "Reverse-ETL: mart_recs → Redis cache for low-latency in-app reads.",
  },
  {
    id: "bi",
    label: "refresh_bi_dashboard",
    op: "SimpleHttpOperator → Domo",
    desc: "POSTs a dataset-refresh to Domo so Tolino BI dashboards reflect the latest run.",
  },
];

export default function DAGPage() {
  return (
    <div className="space-y-10">
      <header>
        <p className="text-xs uppercase tracking-widest text-stone-500 mb-2">
          /dag
        </p>
        <h1 className="text-3xl font-semibold tracking-tight">
          The pipeline, stage by stage
        </h1>
        <p className="mt-3 text-stone-700 max-w-3xl">
          Hourly schedule, 15-minute SLA, halts on dbt-test failure. The full
          Airflow file lives at{" "}
          <a
            className="underline"
            href="https://github.com/PohTeyToe/kobo-recs-dag/blob/main/dags/recs_dag.py"
          >
            dags/recs_dag.py
          </a>
          . The demo here materializes the same pipeline to static JSON so it
          can run on Vercel without an Airflow control plane.
        </p>
      </header>

      <ol className="space-y-3">
        {STAGES.map((s, i) => (
          <li
            key={s.id}
            className="bg-white border border-stone-200 rounded-lg p-5 grid md:grid-cols-[3rem_1fr_auto] gap-4 items-start"
          >
            <div className="text-stone-400 font-mono text-sm tabular-nums">
              {String(i + 1).padStart(2, "0")}
            </div>
            <div>
              <p className="font-mono font-semibold">{s.label}</p>
              <p className="text-sm text-stone-600 mt-1.5">{s.desc}</p>
            </div>
            <span className="text-xs text-stone-500 bg-stone-100 px-2 py-1 rounded font-mono">
              {s.op}
            </span>
          </li>
        ))}
      </ol>

      <section className="bg-white border border-stone-200 rounded-lg p-6">
        <h2 className="font-semibold mb-3">Run snapshot</h2>
        <dl className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <div>
            <dt className="text-stone-500 text-xs uppercase tracking-widest">
              Generated at
            </dt>
            <dd className="mt-1 tabular-nums">
              {new Date(data.generated_at).toUTCString()}
            </dd>
          </div>
          <div>
            <dt className="text-stone-500 text-xs uppercase tracking-widest">
              stg_books
            </dt>
            <dd className="mt-1 tabular-nums">
              {data.counts.books.toLocaleString()} rows
            </dd>
          </div>
          <div>
            <dt className="text-stone-500 text-xs uppercase tracking-widest">
              stg_events
            </dt>
            <dd className="mt-1 tabular-nums">
              {data.counts.events.toLocaleString()} rows
            </dd>
          </div>
          <div>
            <dt className="text-stone-500 text-xs uppercase tracking-widest">
              mart_recs
            </dt>
            <dd className="mt-1 tabular-nums">
              {data.counts.rec_rows.toLocaleString()} rows
            </dd>
          </div>
        </dl>
      </section>
    </div>
  );
}
