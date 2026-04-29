"use client";

import { useMemo, useState } from "react";
import data from "@/data/computed.json";

type Book = (typeof data.books)[number];
type Rec = (typeof data.recs)[number];

const SUBJECT_LABEL: Record<string, string> = {
  fiction: "Fiction",
  mystery: "Mystery",
  science_fiction: "Sci-Fi",
  romance: "Romance",
  fantasy: "Fantasy",
  young_adult: "Young Adult",
};

export default function RecsPage() {
  const [userId, setUserId] = useState(data.users[0].user_id);
  const user = data.users.find((u) => u.user_id === userId)!;

  const bookById = useMemo(
    () => new Map(data.books.map((b) => [b.book_id, b] as const)),
    [],
  );
  const userRecs = useMemo(
    () => data.recs.filter((r) => r.user_id === userId),
    [userId],
  );
  const quiz = userRecs.filter((r) => r.strategy === "quiz").sort((a, b) => a.rank - b.rank);
  const collab = userRecs.filter((r) => r.strategy === "collab").sort((a, b) => a.rank - b.rank);
  const affinity = data.affinity
    .filter((a) => a.user_id === userId)
    .sort((a, b) => b.normalized_score - a.normalized_score);

  return (
    <div className="space-y-8">
      <header>
        <p className="text-xs uppercase tracking-widest text-stone-500 mb-2">
          /recs
        </p>
        <h1 className="text-3xl font-semibold tracking-tight">
          Two strategies, side by side
        </h1>
        <p className="mt-3 text-stone-700 max-w-3xl">
          Pick a synthetic user. The left column is a quiz-only strategy
          (mirrors the Apr 2026 Kobo rollout — content match on the user&apos;s
          onboarding quiz subjects, ranked by recency). The right column adds
          collaborative filtering over the synthetic event matrix with a
          content backstop.
        </p>
      </header>

      <div className="bg-white border border-stone-200 rounded-lg p-5 flex flex-wrap items-center gap-4">
        <label className="text-sm font-medium" htmlFor="user-select">
          User
        </label>
        <select
          id="user-select"
          value={userId}
          onChange={(e) => setUserId(e.target.value)}
          className="border border-stone-300 rounded px-3 py-1.5 text-sm font-mono bg-white"
        >
          {data.users.map((u) => (
            <option key={u.user_id} value={u.user_id}>
              {u.user_id} — quiz: {u.quiz_subjects.map((s) => SUBJECT_LABEL[s]).join(", ")}
            </option>
          ))}
        </select>
        <div className="flex-1" />
        <span className="text-xs text-stone-500">
          Quiz subjects: {user.quiz_subjects.map((s) => SUBJECT_LABEL[s]).join(" · ")}
        </span>
      </div>

      <section className="bg-white border border-stone-200 rounded-lg p-5">
        <h2 className="text-sm font-semibold uppercase tracking-widest text-stone-500 mb-3">
          User × subject affinity (normalized, from event history)
        </h2>
        <div className="space-y-2">
          {affinity.map((a) => (
            <div key={a.subject} className="grid grid-cols-[8rem_1fr_4rem] gap-3 items-center text-sm">
              <span>{SUBJECT_LABEL[a.subject]}</span>
              <div className="h-2 bg-stone-100 rounded">
                <div
                  className="h-full bg-stone-700 rounded"
                  style={{ width: `${Math.min(100, a.normalized_score * 100)}%` }}
                />
              </div>
              <span className="text-right tabular-nums text-stone-600">
                {(a.normalized_score * 100).toFixed(1)}%
              </span>
            </div>
          ))}
        </div>
      </section>

      <div className="grid md:grid-cols-2 gap-5">
        <RecColumn title="Strategy A — quiz-only" subtitle="content match on quiz subjects, ranked by recency" recs={quiz} bookById={bookById} accent="stone" />
        <RecColumn title="Strategy B — collab + content backstop" subtitle="cosine similarity over affinity vectors → peer-weighted candidates" recs={collab} bookById={bookById} accent="emerald" />
      </div>
    </div>
  );
}

function RecColumn({
  title,
  subtitle,
  recs,
  bookById,
  accent,
}: {
  title: string;
  subtitle: string;
  recs: Rec[];
  bookById: Map<string, Book>;
  accent: "stone" | "emerald";
}) {
  return (
    <section className="bg-white border border-stone-200 rounded-lg p-5">
      <header className="mb-4">
        <h3 className="font-semibold">{title}</h3>
        <p className="text-xs text-stone-500 mt-1">{subtitle}</p>
      </header>
      <ol className="space-y-2.5">
        {recs.map((r) => {
          const b = bookById.get(r.book_id);
          if (!b) return null;
          return (
            <li
              key={`${r.user_id}-${r.book_id}-${r.strategy}`}
              className="grid grid-cols-[2rem_1fr] gap-3 items-start text-sm border-l-2 pl-3 py-1"
              style={{
                borderColor: accent === "emerald" ? "#10b981" : "#a8a29e",
              }}
            >
              <span className="text-stone-400 font-mono tabular-nums pt-0.5">
                #{r.rank}
              </span>
              <div>
                <p className="font-medium leading-tight">{b.title}</p>
                <p className="text-xs text-stone-500 mt-0.5">
                  {b.author} {b.year ? `· ${b.year}` : ""} · {SUBJECT_LABEL[b.primary_subject] ?? b.primary_subject}
                </p>
                <p className="text-xs text-stone-600 mt-1 italic">{r.reason}</p>
              </div>
            </li>
          );
        })}
      </ol>
    </section>
  );
}
