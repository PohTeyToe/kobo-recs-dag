/**
 * build-data.ts — runs at build-time to assemble the static dataset for the demo.
 *
 * Pipeline (mirrors the dbt model layer in /models/*.sql):
 *   1. Ingest ~500 books from Open Library subject API           -> stg_books
 *   2. Generate 50 synthetic users + ~5000 synthetic events       -> stg_events
 *   3. Compute per-user genre affinity                             -> int_user_genre_affinity
 *   4. Build top-10 recs with two strategies (quiz vs collab)     -> mart_recs
 *   5. Run schema + funnel-integrity tests                        -> tests/results
 *   6. Write everything to src/data/computed.json
 *
 * SYNTHETIC EVENT DATA IS FOR DEMONSTRATION ONLY.
 * Open Library data is CC0/public-domain bibliographic metadata.
 */

import { writeFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";

type OLDoc = {
  key: string;
  title: string;
  author_name?: string[];
  authors?: { name: string }[];
  first_publish_year?: number;
  cover_i?: number;
  cover_id?: number;
  subject?: string[];
};

type Book = {
  book_id: string;
  title: string;
  author: string;
  year: number | null;
  cover_id: number | null;
  primary_subject: string;
  subjects: string[];
};

type Event = {
  event_id: string;
  user_id: string;
  book_id: string;
  event_type: "page_read" | "finish" | "abandon" | "rate";
  weight: number;
  ts: string;
};

type AffinityRow = {
  user_id: string;
  subject: string;
  raw_score: number;
  normalized_score: number;
};

type Rec = {
  user_id: string;
  book_id: string;
  rank: number;
  score: number;
  strategy: "quiz" | "collab";
  reason: string;
};

type TestResult = {
  test_id: string;
  model: string;
  description: string;
  status: "pass" | "fail";
  observed: string;
};

const SUBJECTS = [
  "fiction",
  "mystery",
  "science_fiction",
  "romance",
  "fantasy",
  "young_adult",
];

const SUBJECT_LABEL: Record<string, string> = {
  fiction: "Fiction",
  mystery: "Mystery",
  science_fiction: "Sci-Fi",
  romance: "Romance",
  fantasy: "Fantasy",
  young_adult: "Young Adult",
};

// ---------- 1. Ingest Open Library ----------
function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchSubject(subject: string, limit = 100): Promise<Book[]> {
  // Try the /search.json endpoint first; fall back to /subjects/{name}.json
  // which has a slightly different shape but the same data we need.
  const candidates = [
    `https://openlibrary.org/search.json?subject=${subject}&limit=${limit}&fields=key,title,author_name,first_publish_year,cover_i,subject`,
    `https://openlibrary.org/subjects/${subject}.json?limit=${limit}`,
  ];
  let lastErr: unknown;
  for (const url of candidates) {
    for (let attempt = 0; attempt < 4; attempt++) {
      try {
        const res = await fetch(url, {
          headers: { "User-Agent": "kobo-recs-dag-demo (github.com/PohTeyToe/kobo-recs-dag)" },
        });
        if (!res.ok) throw new Error(`OpenLibrary ${subject} ${res.status}`);
        const json = (await res.json()) as
          | { docs: OLDoc[] }
          | { works: OLDoc[] };
        const docs = "docs" in json ? json.docs : json.works;
        return docs
          .map((d: OLDoc) => {
            const author =
              (d.author_name && d.author_name[0]) ||
              (d.authors && d.authors[0]?.name) ||
              null;
            if (!d.title || !author) return null;
            return {
              book_id: d.key.replace("/works/", ""),
              title: d.title.slice(0, 120),
              author,
              year: d.first_publish_year ?? null,
              cover_id: d.cover_i ?? d.cover_id ?? null,
              primary_subject: subject,
              subjects: (d.subject ?? []).slice(0, 8),
            } as Book;
          })
          .filter((b): b is Book => b !== null);
      } catch (err) {
        lastErr = err;
        await sleep(800 * (attempt + 1));
      }
    }
  }
  throw lastErr ?? new Error(`OpenLibrary ${subject} failed`);
}

async function ingestBooks(): Promise<Book[]> {
  const all: Book[] = [];
  for (const s of SUBJECTS) {
    process.stdout.write(`  ingest ${s}... `);
    const books = await fetchSubject(s, 100);
    console.log(`${books.length} books`);
    all.push(...books);
    await sleep(400); // be polite to Open Library
  }
  // dedupe by book_id, keeping first occurrence (preserves primary_subject)
  const seen = new Set<string>();
  const dedup: Book[] = [];
  for (const b of all) {
    if (seen.has(b.book_id)) continue;
    seen.add(b.book_id);
    dedup.push(b);
  }
  return dedup;
}

// ---------- 2. Synthetic users + events ----------
let rngState = 42;
function rand() {
  // mulberry32 - deterministic so build is reproducible
  rngState |= 0;
  rngState = (rngState + 0x6d2b79f5) | 0;
  let t = rngState;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}

function pick<T>(arr: T[]): T {
  return arr[Math.floor(rand() * arr.length)];
}

function generateUsers() {
  const users: { user_id: string; quiz_subjects: string[] }[] = [];
  for (let i = 1; i <= 50; i++) {
    // simulate the Kobo onboarding quiz: user picks 1-3 subjects
    const n = 1 + Math.floor(rand() * 3);
    const picked = new Set<string>();
    while (picked.size < n) picked.add(pick(SUBJECTS));
    users.push({
      user_id: `u_${String(i).padStart(3, "0")}`,
      quiz_subjects: [...picked],
    });
  }
  return users;
}

function generateEvents(
  users: { user_id: string; quiz_subjects: string[] }[],
  books: Book[],
): Event[] {
  const bySubject = new Map<string, Book[]>();
  for (const b of books) {
    const arr = bySubject.get(b.primary_subject) ?? [];
    arr.push(b);
    bySubject.set(b.primary_subject, arr);
  }
  const eventTypes: { type: Event["event_type"]; weight: number; p: number }[] = [
    { type: "page_read", weight: 0.5, p: 0.55 },
    { type: "finish", weight: 1.0, p: 0.2 },
    { type: "abandon", weight: -0.4, p: 0.15 },
    { type: "rate", weight: 0.8, p: 0.1 },
  ];
  const events: Event[] = [];
  let eid = 0;
  for (const u of users) {
    // every user has a true latent preference that is broader than the quiz
    // 80% of events come from latent prefs, 20% are exploration
    const latent = new Set(u.quiz_subjects);
    // expand latent: ~50% of users actually like a subject they didn't quiz for
    if (rand() > 0.5) latent.add(pick(SUBJECTS));
    const latentArr = [...latent];
    const events_per_user = 80 + Math.floor(rand() * 40); // 80-120
    for (let i = 0; i < events_per_user; i++) {
      const fromLatent = rand() < 0.8;
      const subj = fromLatent ? pick(latentArr) : pick(SUBJECTS);
      const pool = bySubject.get(subj);
      if (!pool || pool.length === 0) continue;
      const book = pick(pool);
      // sample event type
      const r = rand();
      let cum = 0;
      let chosen = eventTypes[0];
      for (const e of eventTypes) {
        cum += e.p;
        if (r <= cum) {
          chosen = e;
          break;
        }
      }
      const ts = new Date(
        Date.now() - Math.floor(rand() * 60 * 24 * 3600 * 1000),
      ).toISOString();
      events.push({
        event_id: `e_${String(++eid).padStart(6, "0")}`,
        user_id: u.user_id,
        book_id: book.book_id,
        event_type: chosen.type,
        weight: chosen.weight,
        ts,
      });
    }
  }
  return events;
}

// ---------- 3. Affinity ----------
function computeAffinity(
  users: { user_id: string; quiz_subjects: string[] }[],
  events: Event[],
  books: Book[],
): AffinityRow[] {
  const bookSubj = new Map(books.map((b) => [b.book_id, b.primary_subject] as const));
  const acc = new Map<string, Map<string, number>>();
  for (const e of events) {
    const subj = bookSubj.get(e.book_id);
    if (!subj) continue;
    if (!acc.has(e.user_id)) acc.set(e.user_id, new Map());
    const m = acc.get(e.user_id)!;
    m.set(subj, (m.get(subj) ?? 0) + e.weight);
  }
  const out: AffinityRow[] = [];
  for (const u of users) {
    const m = acc.get(u.user_id) ?? new Map();
    // floor at 0 (negative scores zeroed before normalization)
    const positive = new Map<string, number>();
    for (const [s, v] of m.entries()) positive.set(s, Math.max(0, v));
    const total = [...positive.values()].reduce((a, b) => a + b, 0) || 1;
    for (const s of SUBJECTS) {
      const raw = positive.get(s) ?? 0;
      out.push({
        user_id: u.user_id,
        subject: s,
        raw_score: Number(raw.toFixed(3)),
        normalized_score: Number((raw / total).toFixed(4)),
      });
    }
  }
  return out;
}

// ---------- 4. Recs ----------
function buildRecs(
  users: { user_id: string; quiz_subjects: string[] }[],
  affinity: AffinityRow[],
  books: Book[],
  events: Event[],
): Rec[] {
  const out: Rec[] = [];
  const affByUser = new Map<string, Map<string, number>>();
  for (const a of affinity) {
    if (!affByUser.has(a.user_id)) affByUser.set(a.user_id, new Map());
    affByUser.get(a.user_id)!.set(a.subject, a.normalized_score);
  }
  // Build a co-occurrence-style item popularity per subject (collab proxy):
  // a book's "collab score" for user U = sum over events from users who share
  // U's top-2 subjects.
  const eventsByUser = new Map<string, Event[]>();
  for (const e of events) {
    if (!eventsByUser.has(e.user_id)) eventsByUser.set(e.user_id, []);
    eventsByUser.get(e.user_id)!.push(e);
  }
  // top subject per user
  const topSubj = new Map<string, string[]>();
  for (const [uid, m] of affByUser.entries()) {
    const sorted = [...m.entries()].sort((a, b) => b[1] - a[1]);
    topSubj.set(
      uid,
      sorted.slice(0, 2).map((s) => s[0]),
    );
  }

  // Strategy A — "quiz" (Kobo current): content-based on quiz_subjects only,
  // ranked by raw book year recency (proxy for "newer first").
  for (const u of users) {
    const quizSubs = new Set(u.quiz_subjects);
    const candidates = books
      .filter((b) => quizSubs.has(b.primary_subject))
      .sort((a, b) => (b.year ?? 0) - (a.year ?? 0))
      .slice(0, 10);
    candidates.forEach((b, i) => {
      out.push({
        user_id: u.user_id,
        book_id: b.book_id,
        rank: i + 1,
        score: Number((1 - i * 0.05).toFixed(3)),
        strategy: "quiz",
        reason: `Matches quiz pick: ${SUBJECT_LABEL[b.primary_subject]}`,
      });
    });
  }

  // Strategy B — "collab": collaborative-flavored.
  //   For each user, look at peers (other users) and weight each candidate book
  //   by similarity of normalized affinity vectors (cosine), then add a
  //   content-based backstop on the user's strongest subject.
  const userVec = new Map<string, number[]>();
  for (const u of users) {
    const m = affByUser.get(u.user_id) ?? new Map();
    userVec.set(
      u.user_id,
      SUBJECTS.map((s) => m.get(s) ?? 0),
    );
  }
  function cosine(a: number[], b: number[]) {
    let dot = 0,
      na = 0,
      nb = 0;
    for (let i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      na += a[i] * a[i];
      nb += b[i] * b[i];
    }
    return dot / (Math.sqrt(na) * Math.sqrt(nb) + 1e-9);
  }

  for (const u of users) {
    // peers ranked by cosine similarity, top 8
    const peers = users
      .filter((p) => p.user_id !== u.user_id)
      .map((p) => ({
        uid: p.user_id,
        sim: cosine(userVec.get(u.user_id)!, userVec.get(p.user_id)!),
      }))
      .sort((a, b) => b.sim - a.sim)
      .slice(0, 8);
    // candidate book scores: sum sim * weight from peer events
    const score = new Map<string, number>();
    for (const peer of peers) {
      const peerEvents = eventsByUser.get(peer.uid) ?? [];
      for (const e of peerEvents) {
        if (e.weight <= 0) continue;
        score.set(
          e.book_id,
          (score.get(e.book_id) ?? 0) + peer.sim * e.weight,
        );
      }
    }
    // remove books the user already touched
    const seen = new Set((eventsByUser.get(u.user_id) ?? []).map((e) => e.book_id));
    const ranked = [...score.entries()]
      .filter(([bid]) => !seen.has(bid))
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);
    // backfill from top-subject if collab pool is thin
    if (ranked.length < 10) {
      const top = topSubj.get(u.user_id)?.[0];
      if (top) {
        const fillers = books
          .filter((b) => b.primary_subject === top && !seen.has(b.book_id))
          .filter((b) => !ranked.some(([bid]) => bid === b.book_id))
          .slice(0, 10 - ranked.length);
        for (const f of fillers) ranked.push([f.book_id, 0.001]);
      }
    }
    const bookById = new Map(books.map((b) => [b.book_id, b] as const));
    ranked.forEach(([bid, s], i) => {
      const book = bookById.get(bid);
      const reason = book
        ? `Peers with similar reading patterns finished/rated this; subject ${SUBJECT_LABEL[book.primary_subject] ?? book.primary_subject}`
        : "Collaborative-filter candidate";
      out.push({
        user_id: u.user_id,
        book_id: bid,
        rank: i + 1,
        score: Number(s.toFixed(4)),
        strategy: "collab",
        reason,
      });
    });
  }
  return out;
}

// ---------- 5. Tests ----------
function runTests(
  books: Book[],
  events: Event[],
  affinity: AffinityRow[],
  recs: Rec[],
  users: { user_id: string }[],
): TestResult[] {
  const r: TestResult[] = [];
  const bookIds = new Set(books.map((b) => b.book_id));

  // schema: stg_books unique
  const seen = new Set<string>();
  let dupBooks = 0;
  for (const b of books) {
    if (seen.has(b.book_id)) dupBooks++;
    seen.add(b.book_id);
  }
  r.push({
    test_id: "stg_books_unique_book_id",
    model: "stg_books",
    description: "book_id is unique across the catalog",
    status: dupBooks === 0 ? "pass" : "fail",
    observed: `${dupBooks} duplicates in ${books.length} rows`,
  });

  // schema: stg_books not_null
  const nullTitles = books.filter((b) => !b.title).length;
  r.push({
    test_id: "stg_books_not_null_title",
    model: "stg_books",
    description: "title is not null",
    status: nullTitles === 0 ? "pass" : "fail",
    observed: `${nullTitles} null titles`,
  });

  // schema: stg_books accepted_values primary_subject
  const allowed = new Set(SUBJECTS);
  const badSubj = books.filter((b) => !allowed.has(b.primary_subject)).length;
  r.push({
    test_id: "stg_books_accepted_values_primary_subject",
    model: "stg_books",
    description: `primary_subject in (${SUBJECTS.join(", ")})`,
    status: badSubj === 0 ? "pass" : "fail",
    observed: `${badSubj} rows out of accepted set`,
  });

  // schema: stg_events accepted_values event_type
  const eAllowed = new Set(["page_read", "finish", "abandon", "rate"]);
  const badE = events.filter((e) => !eAllowed.has(e.event_type)).length;
  r.push({
    test_id: "stg_events_accepted_values_event_type",
    model: "stg_events",
    description: "event_type in (page_read, finish, abandon, rate)",
    status: badE === 0 ? "pass" : "fail",
    observed: `${badE} bad rows in ${events.length}`,
  });

  // schema: stg_events relationship to stg_books
  const orphan = events.filter((e) => !bookIds.has(e.book_id)).length;
  r.push({
    test_id: "stg_events_relationship_book_id",
    model: "stg_events",
    description: "every event.book_id references stg_books.book_id",
    status: orphan === 0 ? "pass" : "fail",
    observed: `${orphan} orphan events`,
  });

  // funnel: int_user_genre_affinity normalized_score sums to <= 1 per user
  const sumByUser = new Map<string, number>();
  for (const a of affinity)
    sumByUser.set(a.user_id, (sumByUser.get(a.user_id) ?? 0) + a.normalized_score);
  const overOne = [...sumByUser.values()].filter((v) => v > 1.001).length;
  r.push({
    test_id: "int_user_genre_affinity_sum_lte_1",
    model: "int_user_genre_affinity",
    description: "Σ normalized_score per user ≤ 1.0 (catches double-counting)",
    status: overOne === 0 ? "pass" : "fail",
    observed: `${overOne} users with sum > 1`,
  });

  // funnel: mart_recs every recommendation references a real book
  const recOrphans = recs.filter((rec) => !bookIds.has(rec.book_id)).length;
  r.push({
    test_id: "mart_recs_relationship_book_id",
    model: "mart_recs",
    description: "every recommendation references a real catalog book",
    status: recOrphans === 0 ? "pass" : "fail",
    observed: `${recOrphans} recs without a matching book`,
  });

  // funnel: mart_recs no duplicate (user, book, strategy)
  const key = new Set<string>();
  let dupRecs = 0;
  for (const rec of recs) {
    const k = `${rec.user_id}|${rec.book_id}|${rec.strategy}`;
    if (key.has(k)) dupRecs++;
    key.add(k);
  }
  r.push({
    test_id: "mart_recs_unique_user_book_strategy",
    model: "mart_recs",
    description: "no duplicate (user_id, book_id, strategy) recommendations",
    status: dupRecs === 0 ? "pass" : "fail",
    observed: `${dupRecs} duplicates`,
  });

  // funnel: every active user gets at least 1 rec from each strategy
  const userStrat = new Map<string, Set<string>>();
  for (const rec of recs) {
    if (!userStrat.has(rec.user_id)) userStrat.set(rec.user_id, new Set());
    userStrat.get(rec.user_id)!.add(rec.strategy);
  }
  const missingStrategy = users.filter(
    (u) => (userStrat.get(u.user_id)?.size ?? 0) < 2,
  ).length;
  r.push({
    test_id: "mart_recs_coverage_both_strategies",
    model: "mart_recs",
    description: "every user has recs from BOTH quiz and collab strategies",
    status: missingStrategy === 0 ? "pass" : "fail",
    observed: `${missingStrategy} users missing a strategy`,
  });

  return r;
}

// ---------- 6. Write ----------
async function main() {
  console.log("[1/6] ingesting Open Library...");
  const books = await ingestBooks();
  console.log(`     ${books.length} unique books in catalog`);

  console.log("[2/6] generating synthetic users + events...");
  const users = generateUsers();
  const events = generateEvents(users, books);
  console.log(`     ${users.length} users, ${events.length} events`);

  console.log("[3/6] computing int_user_genre_affinity...");
  const affinity = computeAffinity(users, events, books);

  console.log("[4/6] computing mart_recs (quiz + collab)...");
  const recs = buildRecs(users, affinity, books, events);
  console.log(`     ${recs.length} rec rows`);

  console.log("[5/6] running schema + funnel tests...");
  const tests = runTests(books, events, affinity, recs, users);
  const passed = tests.filter((t) => t.status === "pass").length;
  console.log(`     ${passed}/${tests.length} tests passed`);

  console.log("[6/6] writing src/data/computed.json...");
  const out = {
    generated_at: new Date().toISOString(),
    counts: {
      books: books.length,
      users: users.length,
      events: events.length,
      affinity_rows: affinity.length,
      rec_rows: recs.length,
    },
    books,
    users,
    affinity,
    recs,
    tests,
    // sample only a slice of events for the UI to keep payload manageable
    sample_events: events.slice(0, 200),
  };
  const dir = join(process.cwd(), "src", "data");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "computed.json"), JSON.stringify(out));
  console.log("done.");
}

main().catch((e) => {
  console.error("[build-data] live ingest failed:", e?.message ?? e);
  // If the materialized snapshot already exists (committed to the repo),
  // continue the build with the cached snapshot rather than failing the
  // deploy on a transient Open Library outage.
  const out = join(process.cwd(), "src", "data", "computed.json");
  try {
    const stat = require("node:fs").statSync(out);
    if (stat && stat.size > 0) {
      console.warn(
        `[build-data] using cached snapshot at ${out} (${stat.size} bytes)`,
      );
      process.exit(0);
    }
  } catch {
    // no cached snapshot — fall through to fail
  }
  process.exit(1);
});
