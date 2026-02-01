{{
  config(
    materialized='view',
    tags=['staging', 'crew', 'directors']
  )
}}

/*
Staging model for movie crew (directors).

Transforms the raw IMDB title_crew table by:
1. Exploding comma-separated director IDs into separate rows
2. Joining with person data to enrich with director names and info

Note: Writers are excluded in this model (focus on directors only).

Source: bronze.title_crew + stg_person
Output: View in silver dataset
*/

WITH source AS (
  SELECT * FROM {{ source('bronze', 'title_crew') }}
),

exploded_directors AS (
  /*
  IMDB stores multiple directors as comma-separated IDs (e.g., "nm0001,nm0002")
  We explode this into one row per director-movie relationship
  */
  SELECT 
    tconst AS movie_id,
    TRIM(director_id) AS director_id
  FROM source,
  UNNEST(SPLIT(directors, ',')) AS director_id
  WHERE
    -- Exclude titles with no directors
    directors != r'\N'
    AND directors IS NOT NULL
    AND directors != ''
    
    -- Exclude empty strings from split
    AND TRIM(director_id) != ''
),

enriched AS (
  /*
  Join with person staging to get director details
  LEFT JOIN preserves all director relationships even if person info is missing
  */
  SELECT 
    ed.movie_id,
    ed.director_id,
    
    -- Person information (from stg_person)
    p.name AS director_name,
    p.birth_year,
    p.death_year,
    p.profession
    
  FROM exploded_directors ed
  LEFT JOIN {{ ref('stg_person') }} p
    ON ed.director_id = p.person_id
)

SELECT * FROM enriched