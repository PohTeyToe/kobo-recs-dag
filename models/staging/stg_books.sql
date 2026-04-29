{{ config(materialized='view') }}

/*
    Staging model for Open Library bibliographic records.

    Source: raw.openlibrary_search (one row per Open Library search-API doc)
    Transformations:
      - Strip /works/ prefix from Open Library key
      - Truncate title to 120 chars
      - Pick first author when array has multiple
      - Cast first_publish_year to INTEGER
      - Filter out records missing title or author
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'openlibrary_search') }}
),

cleaned AS (
    SELECT
        REPLACE(key, '/works/', '')              AS book_id,
        SUBSTRING(title, 1, 120)                 AS title,
        author_name[1]                           AS author,
        CAST(first_publish_year AS INTEGER)      AS year,
        cover_i                                  AS cover_id,
        primary_subject,
        subject                                  AS subjects,
        ingested_at
    FROM source
    WHERE title IS NOT NULL
      AND author_name IS NOT NULL
      AND ARRAY_LENGTH(author_name) > 0
)

SELECT * FROM cleaned
