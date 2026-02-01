{{
  config(
    materialized='view',
    tags=['staging', 'actors', 'cast']
  )
}}

/*
Staging model for movie actors.

Transforms the raw IMDB title_principals table by:
1. Filtering for actors/actresses only (excludes directors, writers, etc.)
2. Joining with person data to enrich with actor names and biographical info
3. Extracting character names from JSON-like strings
4. Deriving gender from actor/actress category

Source: bronze.title_principals + stg_person
Output: View in silver dataset
*/

WITH source AS (
  SELECT * FROM {{ source('bronze', 'title_principals') }}
),

actor_principals AS (
  /*
  Filter title_principals for acting roles only
  IMDB uses 'actor' for male actors and 'actress' for female actors
  */
  SELECT 
    tconst AS movie_id,
    ordering,
    nconst AS actor_id,
    category,
    characters
    
  FROM source
  WHERE
    category IN ('actor', 'actress')
    AND nconst IS NOT NULL
    AND tconst IS NOT NULL
),

enriched AS (
  /*
  Join with person staging to get actor details
  LEFT JOIN preserves all acting credits even if person info is missing
  */
  SELECT 
    ap.movie_id,
    ap.ordering,
    ap.actor_id,
    
    -- Person information (from stg_person)
    p.name AS actor_name,
    
    -- Acting-specific fields
    ap.category,
    CASE 
      WHEN ap.category = 'actor' THEN 'Male'
      WHEN ap.category = 'actress' THEN 'Female'
      ELSE NULL
    END AS gender,
    
    -- Character information
    ap.characters,
    
    -- Biographical information
    p.birth_year,
    p.death_year,
    p.profession
    
  FROM actor_principals ap
  LEFT JOIN {{ ref('stg_person') }} p
    ON ap.actor_id = p.person_id
)

SELECT * FROM enriched
