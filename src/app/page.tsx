import Link from "next/link";
import data from "@/data/computed.json";

export default function Home() {
  const passed = data.tests.filter((t) => t.status === "pass").length;
  return (
    <div className="space-y-12">
      <section>
        <p className="text-xs uppercase tracking-widest text-stone-500 mb-3">
          Portfolio submission · Rakuten Kobo Data Engineer Co-op
        </p>
        <h1 className="text-4xl md:text-5xl font-semibold tracking-tight leading-tight">
          A book-recommendations pipeline that does more than ask one onboarding
          quiz.
        </h1>
        <p className="mt-6 text-lg text-stone-700 max-w-3xl">
          Kobo&apos;s Spring 2026 update (April 2) shipped personalized
          recommendations driven by a single onboarding quiz. Power users on
          r/kobo and MobileRead pointed out the gaps: no genre exclusion, no
          dislike, and no learning from what you actually read. This demo is a
          working sketch of what a richer signal looks like — built end-to-end
          on the same stack the JD calls out: Airflow, dbt, SQL, Python, BI.
        </p>
      </section>

      <section className="grid md:grid-cols-3 gap-4">
        <Stat
          label="Books ingested"
          value={data.counts.books.toLocaleString()}
          sub="Open Library /search.json across 6 subjects"
        />
        <Stat
          label="Synthetic reader events"
          value={data.counts.events.toLocaleString()}
          sub={`${data.counts.users} users · weighted page-read / finish / abandon / rate`}
        />
        <Stat
          label="dbt tests passing"
          value={`${passed} / ${data.tests.length}`}
          sub="schema + funnel-integrity"
        />
      </section>

      <section className="grid md:grid-cols-2 gap-6">
        <Card
          href="/dag"
          title="Pipeline"
          body="Five-stage DAG: Open Library ingest → staging → intermediate (user × subject affinity) → marts → reverse-ETL to a recs cache. The Airflow file in /dags/recs_dag.py shows the production shape."
        />
        <Card
          href="/recs"
          title="Recs explorer"
          body="Pick a synthetic user. See the quiz-only strategy alongside a collaborative-filtering strategy with content backstop. Each row carries an explanation."
        />
        <Card
          href="/quality"
          title="Data quality"
          body="Schema tests on every model (unique, not_null, accepted_values, relationships) plus funnel tests catching genre-affinity overflow, orphan recs, and duplicate output rows."
        />
        <Card
          href="https://github.com/PohTeyToe/kobo-recs-dag"
          title="Source on GitHub"
          body="Public repo — MIT. Includes the dbt SQL models in /models, the Airflow DAG in /dags, and the Node.js build pipeline in /scripts/build-data.ts."
          external
        />
      </section>

      <section className="bg-white border border-stone-200 rounded-lg p-6">
        <h2 className="text-lg font-semibold mb-2">Why this shape fits Tolino BI</h2>
        <ul className="list-disc list-inside space-y-1.5 text-stone-700 text-sm">
          <li>
            Cross-border partner reporting (Hugendubel, Thalia, Weltbild) needs
            referential integrity guaranteed by schema tests, not goodwill.
          </li>
          <li>
            Per-page-read royalty allocation under Kobo Plus is the same shape
            of problem: weight events, normalize per actor, materialize a mart.
          </li>
          <li>
            Reverse-ETL from <code>mart_recs</code> to a Redis cache is the
            production pattern that keeps an in-app recs surface low-latency
            without coupling reads to the warehouse.
          </li>
        </ul>
      </section>
    </div>
  );
}

function Stat({
  label,
  value,
  sub,
}: {
  label: string;
  value: string;
  sub: string;
}) {
  return (
    <div className="bg-white border border-stone-200 rounded-lg p-5">
      <p className="text-xs uppercase tracking-widest text-stone-500">{label}</p>
      <p className="text-3xl font-semibold mt-2 tabular-nums">{value}</p>
      <p className="text-xs text-stone-500 mt-2">{sub}</p>
    </div>
  );
}

function Card({
  href,
  title,
  body,
  external,
}: {
  href: string;
  title: string;
  body: string;
  external?: boolean;
}) {
  const cls =
    "block bg-white border border-stone-200 rounded-lg p-6 hover:border-stone-400 transition-colors";
  if (external)
    return (
      <a href={href} className={cls} target="_blank" rel="noreferrer">
        <h3 className="font-semibold mb-1.5">{title} →</h3>
        <p className="text-sm text-stone-600">{body}</p>
      </a>
    );
  return (
    <Link href={href} className={cls}>
      <h3 className="font-semibold mb-1.5">{title} →</h3>
      <p className="text-sm text-stone-600">{body}</p>
    </Link>
  );
}
