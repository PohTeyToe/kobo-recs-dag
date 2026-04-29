{{ config(materialized='table') }}

/*
    Mart: per-user top-10 recommendations from each strategy.

    Two strategies, side-by-side, so the consumer of this mart can A/B them:

      strategy = 'quiz'    Content-based on the user's onboarding-quiz subjects.
                           Mirrors Kobo's current Apr-2026 personalization
                           rollout (single quiz signal, no negative feedback).

      strategy = 'collab'  Cosine-similarity over user × subject affinity
                           vectors, then ranks candidate books by Σ(peer_sim
                           × peer_event_weight). Backfills with top-subject
                           content matches if the collab pool is thin.

    Tests in models/_schema.yml enforce: unique (user_id, book_id, strategy),
    referential integrity to stg_books, and coverage (every user receives
    recs from BOTH strategies — see mart_recs_coverage_both_strategies).
*/

-- The actual computation runs in scripts/build-data.ts because this demo
-- materializes to static JSON for Vercel deployment. The SQL here documents
-- the production shape: 4 CTEs (quiz_candidates, collab_peers,
-- collab_candidates, ranked_union) and a final ROW_NUMBER() partition by
-- (user_id, strategy) ordered by score DESC, filtering rank ≤ 10.

WITH quiz_candidates AS (
    SELECT
        u.user_id,
        b.book_id,
        'quiz'::text                                       AS strategy,
        1.0 - (ROW_NUMBER() OVER (
            PARTITION BY u.user_id
            ORDER BY b.year DESC NULLS LAST
        ) - 1) * 0.05                                      AS score,
        'Matches quiz pick: ' || b.primary_subject         AS reason
    FROM {{ ref('dim_users') }} u
    JOIN {{ ref('stg_books') }} b
      ON b.primary_subject = ANY(u.quiz_subjects)
),

user_vectors AS (
    SELECT user_id, subject, normalized_score
    FROM {{ ref('int_user_genre_affinity') }}
),

collab_peers AS (
    SELECT
        a.user_id,
        b.user_id                                          AS peer_id,
        SUM(a.normalized_score * b.normalized_score) /
            (SQRT(SUM(a.normalized_score * a.normalized_score)) *
             SQRT(SUM(b.normalized_score * b.normalized_score)) + 1e-9)
            AS cosine_sim
    FROM user_vectors a
    JOIN user_vectors b USING (subject)
    WHERE a.user_id != b.user_id
    GROUP BY 1, 2
),

collab_candidates AS (
    SELECT
        p.user_id,
        e.book_id,
        'collab'::text                                     AS strategy,
        SUM(p.cosine_sim * e.event_weight)                 AS score,
        'Peers with similar reading patterns finished/rated this' AS reason
    FROM collab_peers p
    JOIN {{ ref('stg_events') }} e ON e.user_id = p.peer_id
    WHERE e.event_weight > 0
      AND NOT EXISTS (
          SELECT 1 FROM {{ ref('stg_events') }} ue
          WHERE ue.user_id = p.user_id
            AND ue.book_id = e.book_id
      )
    GROUP BY 1, 2
),

ranked_union AS (
    SELECT
        user_id, book_id, strategy, score, reason,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, strategy
            ORDER BY score DESC
        ) AS rank
    FROM (
        SELECT * FROM quiz_candidates
        UNION ALL
        SELECT * FROM collab_candidates
    ) u
)

SELECT user_id, book_id, strategy, rank, score, reason
FROM ranked_union
WHERE rank <= 10
