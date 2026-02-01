{{
  config(
    materialized='view',
    tags=['staging', 'titles']
  )
}}

/*
Staging model for title basics.

Transforms the raw IMDB title_basics table into a clean, movie-only dataset
with standardized column names and proper data types.

Filters applied:
- Only movies (excludes TV series, shorts, etc.)
- Non-adult content only
- Released after 1950 (modern cinema era)
- Valid release year (not NULL)

Source: bronze.title_basics
Output: View in silver dataset
*/

WITH source AS (
  SELECT * FROM {{ source('bronze', 'title_basics') }}
),

filtered AS (
  SELECT
    -- Primary key
    tconst AS movie_id,
    
    -- Title information
    primaryTitle AS title,
    originalTitle AS original_title,
    
    -- Metadata
    SAFE_CAST(runtimeMinutes AS INT64) AS runtime_minutes,
    SAFE_CAST(startYear AS INT64) AS release_year,
    
    genres
    
  FROM source
  WHERE
    titleType = 'movie'
    AND isAdult = 0
    AND SAFE_CAST(startYear AS INT64) > {{ var('min_year', 1950) }}
    AND startYear IS NOT NULL
    AND primaryTitle IS NOT NULL
    AND primaryTitle != ''
)

SELECT * FROM filtered