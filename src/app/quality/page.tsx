import data from "@/data/computed.json";

export default function QualityPage() {
  const passed = data.tests.filter((t) => t.status === "pass").length;
  const failed = data.tests.length - passed;
  const byModel = new Map<string, typeof data.tests>();
  for (const t of data.tests) {
    if (!byModel.has(t.model)) byModel.set(t.model, []);
    byModel.get(t.model)!.push(t);
  }

  return (
    <div className="space-y-10">
      <header>
        <p className="text-xs uppercase tracking-widest text-stone-500 mb-2">
          /quality
        </p>
        <h1 className="text-3xl font-semibold tracking-tight">
          Schema and funnel-integrity tests
        </h1>
        <p className="mt-3 text-stone-700 max-w-3xl">
          Every model carries tests in <code>models/_schema.yml</code>. dbt
          halts on first failure so a broken pipeline never publishes a broken
          mart. Funnel tests catch the bugs that schema tests don&apos;t —
          double-counted weights, orphan recs, missing strategy coverage.
        </p>
      </header>

      <section className="grid grid-cols-3 gap-4">
        <Stat label="passed" value={passed} tone="ok" />
        <Stat label="failed" value={failed} tone={failed > 0 ? "fail" : "ok"} />
        <Stat label="total" value={data.tests.length} tone="neutral" />
      </section>

      {[...byModel.entries()].map(([model, tests]) => (
        <section key={model}>
          <h2 className="font-mono text-sm uppercase tracking-widest text-stone-500 mb-3">
            {model}
          </h2>
          <div className="bg-white border border-stone-200 rounded-lg divide-y divide-stone-200">
            {tests.map((t) => (
              <div
                key={t.test_id}
                className="p-4 grid md:grid-cols-[6rem_1fr_auto] gap-4 items-start"
              >
                <span
                  className={`text-xs font-mono uppercase rounded px-2 py-1 inline-block w-fit ${
                    t.status === "pass"
                      ? "bg-emerald-100 text-emerald-800"
                      : "bg-red-100 text-red-800"
                  }`}
                >
                  {t.status}
                </span>
                <div>
                  <p className="font-mono text-sm font-semibold">{t.test_id}</p>
                  <p className="text-sm text-stone-600 mt-1">{t.description}</p>
                </div>
                <span className="text-xs text-stone-500 font-mono">
                  {t.observed}
                </span>
              </div>
            ))}
          </div>
        </section>
      ))}

      <section className="bg-stone-100 border border-stone-200 rounded-lg p-6 text-sm">
        <p className="font-semibold mb-2">Note on the funnel tests</p>
        <p className="text-stone-700">
          Schema tests (unique, not_null, accepted_values, relationships) are
          table-stakes. The interesting ones are the <em>funnel</em> tests —
          properties that have to hold across the whole pipeline. Catching{" "}
          <em>Σ normalized_score &gt; 1</em> means a weighting bug introduced
          upstream surfaces here, not in a partner&apos;s Tolino BI dashboard.
        </p>
      </section>
    </div>
  );
}

function Stat({
  label,
  value,
  tone,
}: {
  label: string;
  value: number;
  tone: "ok" | "fail" | "neutral";
}) {
  const colour =
    tone === "ok"
      ? "text-emerald-700"
      : tone === "fail"
        ? "text-red-700"
        : "text-stone-700";
  return (
    <div className="bg-white border border-stone-200 rounded-lg p-5">
      <p className="text-xs uppercase tracking-widest text-stone-500">{label}</p>
      <p className={`text-3xl font-semibold mt-2 tabular-nums ${colour}`}>
        {value}
      </p>
    </div>
  );
}
