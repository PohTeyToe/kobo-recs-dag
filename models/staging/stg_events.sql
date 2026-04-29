{{ config(materialized='view') }}

/*
    Staging model for synthetic reader events.

    !!! SYNTHETIC DATA — for demonstration only. Not real Kobo telemetry.

    Source: raw.reader_events
    Transformations:
      - Type cast event_weight as FLOAT
      - Filter to known event_type values
      - Deduplicate on event_id (keep most-recent ingestion)
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'reader_events') }}
),

dedup AS (
    SELECT
        event_id,
        user_id,
        book_id,
        event_type,
        CAST(weight AS FLOAT)         AS event_weight,
        CAST(ts AS TIMESTAMP)         AS event_ts,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY ingested_at DESC
        ) AS row_num
    FROM source
    WHERE event_type IN ('page_read', 'finish', 'abandon', 'rate')
)

SELECT
    event_id,
    user_id,
    book_id,
    event_type,
    event_weight,
    event_ts,
    ingested_at
FROM dedup
WHERE row_num = 1
