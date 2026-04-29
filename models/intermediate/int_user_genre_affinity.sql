{{ config(materialized='table') }}

/*
    Intermediate: per-user genre affinity, weighted and normalized.

    A user's affinity for a subject is the sum of weighted reader-events
    (page_read=+0.5, finish=+1.0, abandon=-0.4, rate=+0.8) on books in
    that subject. Negative totals are floored at 0 before normalization.

    The normalized_score column sums to ≤ 1.0 per user (verified by
    the int_user_genre_affinity_sum_lte_1 funnel test).
*/

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

books AS (
    SELECT * FROM {{ ref('stg_books') }}
),

raw_scores AS (
    SELECT
        e.user_id,
        b.primary_subject AS subject,
        SUM(e.event_weight) AS raw_score
    FROM events e
    JOIN books b USING (book_id)
    GROUP BY 1, 2
),

floored AS (
    SELECT
        user_id,
        subject,
        GREATEST(raw_score, 0) AS raw_score
    FROM raw_scores
),

totals AS (
    SELECT
        user_id,
        SUM(raw_score) AS total
    FROM floored
    GROUP BY 1
)

SELECT
    f.user_id,
    f.subject,
    f.raw_score,
    CASE
        WHEN t.total = 0 THEN 0
        ELSE f.raw_score / t.total
    END AS normalized_score
FROM floored f
JOIN totals t USING (user_id)
