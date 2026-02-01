{{
  config(
    materialized='table',
    tags=['marts', 'fact-table'],
    partition_by={
      'field': 'release_year',
      'data_type': 'int64',
      'range': {
        'start': 1950,
        'end': 2030,
        'interval': 10
      }
    },
    cluster_by=['num_votes'] 
  )
}}

/*
Fact table: movies

This is the main analytics table combining movie information, ratings,
directors, and actors into a denormalized structure optimized for queries.

Key features:
- One row per movie
- Nested arrays for directors and actors (BigQuery STRUCT/ARRAY)
- Partitioned by release_year for query performance
- Clustered by rating metrics for filtering efficiency

Source: Multiple staging models (stg_title_basics, stg_ratings, stg_crew, stg_actor)
Output: Table in silver dataset
*/

WITH directors_agg AS (
  /*
  Aggregate directors into an array of structs
  Each movie can have multiple directors 
  */
  SELECT 
    movie_id,
    ARRAY_AGG(
      STRUCT(
        director_id AS id,
        director_name AS name,
        birth_year AS birth_year,
        death_year AS death_year
      ) 
      ORDER BY director_name  
    ) AS directors
  FROM {{ ref('stg_crew') }}
  WHERE
    -- Only include directors with known names
    director_name IS NOT NULL
    AND director_id IS NOT NULL
  GROUP BY movie_id
),

actors_agg AS (
  /*
  Aggregate actors into an array of structs
  Ordered by billing (top-billed actors first)
  Limited to top 10 actors per movie for performance
  */
  SELECT 
    movie_id,
    ARRAY_AGG(
      STRUCT(
        actor_id AS id,
        actor_name AS name,
        birth_year AS birth_year,
        death_year AS death_year,
        characters AS characters,
        gender AS gender
      ) 
      IGNORE NULLS           
      ORDER BY ordering      
      LIMIT 10               
    ) AS actors
  FROM {{ ref('stg_actor') }}
  WHERE
    -- Only include actors with known names
    actor_name IS NOT NULL
    AND actor_id IS NOT NULL
  GROUP BY movie_id
),

final AS (
  SELECT
    /* ===================================================================
       Movie Information
       =================================================================== */
    t.movie_id,
    t.title,
    t.original_title,
    t.runtime_minutes,
    t.release_year,
    t.genres,

    /* ===================================================================
       Rating Metrics
       =================================================================== */
    r.average_rating,
    r.num_votes,
    
    -- Derived rating category for easy filtering
    CASE
      WHEN r.average_rating >= 8.5 THEN 'Excellent'
      WHEN r.average_rating >= 7.5 THEN 'Very Good'
      WHEN r.average_rating >= 6.5 THEN 'Good'
      WHEN r.average_rating >= 5.5 THEN 'Average'
      ELSE 'Below Average'
    END AS rating_category,

    /* ===================================================================
       People (Nested Arrays)
       =================================================================== */
    d.directors,
    a.actors,
    
    -- Derived counts for convenience
    ARRAY_LENGTH(d.directors) AS director_count,
    ARRAY_LENGTH(a.actors) AS actor_count

  FROM {{ ref('stg_title_basics') }} t
  
  -- INNER JOIN: Only movies with ratings (filters out unrated movies)
  INNER JOIN {{ ref('stg_ratings') }} r
    ON t.movie_id = r.movie_id
  
  -- LEFT JOIN: Keep movies even without director/actor info
  LEFT JOIN directors_agg d
    ON d.movie_id = t.movie_id
    
  LEFT JOIN actors_agg a
    ON a.movie_id = t.movie_id
)

SELECT * FROM final