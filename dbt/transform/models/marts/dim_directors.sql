{{
  config(
    materialized='table',
    tags=['marts', 'dimension']
  )
}}

/*
Dimension table: directors

This table contains one row per director who has directed movies in our
fact table (movies). It includes biographical information and aggregated
statistics about their filmography.

Purpose:
- Enable director-based filtering and analysis
- Provide director details for BI tools
- Calculate director-level metrics (avg rating, total movies, etc.)

Source: movies (fact table) + stg_person
Output: Table in silver dataset
*/

WITH movie_directors AS (
  /*
  Extract unique directors from the movies fact table
  Uses UNNEST to flatten the directors array
  */
  SELECT DISTINCT director.id AS director_id
  FROM {{ ref('movies') }},
  UNNEST(directors) AS director
  WHERE
    -- Exclude NULL director IDs (data quality)
    director.id IS NOT NULL
),

director_stats AS (
  /*
  Calculate aggregated statistics per director
  */
  SELECT
    director.id AS director_id,
    
    -- Filmography statistics
    COUNT(DISTINCT m.movie_id) AS total_movies,
    ROUND(AVG(m.average_rating), 2) AS avg_movie_rating,
    SUM(m.num_votes) AS total_votes,
    
    -- Career span
    MIN(m.release_year) AS first_movie_year,
    MAX(m.release_year) AS latest_movie_year,
    MAX(m.release_year) - MIN(m.release_year) AS career_span_years,
    
    -- Best work
    MAX(m.average_rating) AS best_movie_rating,
    
    -- Movie with highest rating (take first if tie)
    ARRAY_AGG(
      STRUCT(m.movie_id, m.title, m.average_rating, m.release_year)
      ORDER BY m.average_rating DESC, m.num_votes DESC
      LIMIT 1
    )[OFFSET(0)] AS best_movie
    
  FROM {{ ref('movies') }} m,
  UNNEST(m.directors) AS director
  WHERE director.id IS NOT NULL
  GROUP BY director.id
),

final AS (
  /*
  Combine director personal information with their statistics
  */
  SELECT 
    -- Primary key
    p.person_id AS director_id,
    
    -- Personal information
    p.name AS director_name,
    p.birth_year,
    p.death_year,
    p.profession,
    p.known_for_titles,
    
    -- Age (if alive) or age at death
    CASE
      WHEN p.death_year IS NOT NULL THEN p.death_year - p.birth_year
      ELSE EXTRACT(YEAR FROM CURRENT_DATE()) - p.birth_year
    END AS age,
    
    -- Career statistics
    s.total_movies,
    s.avg_movie_rating,
    s.total_votes,
    s.first_movie_year,
    s.latest_movie_year,
    s.career_span_years,
    s.best_movie_rating,
    s.best_movie.title AS best_movie_title,
    s.best_movie.movie_id AS best_movie_id,
    s.best_movie.release_year AS best_movie_year,
    
    -- Derived categories for analysis
    CASE
      WHEN s.avg_movie_rating >= 8.0 THEN 'Elite'
      WHEN s.avg_movie_rating >= 7.0 THEN 'Excellent'
      WHEN s.avg_movie_rating >= 6.0 THEN 'Good'
      ELSE 'Average'
    END AS director_tier,
    
    CASE
      WHEN s.total_movies >= 20 THEN 'Prolific'
      WHEN s.total_movies >= 10 THEN 'Established'
      WHEN s.total_movies >= 5 THEN 'Emerging'
      ELSE 'New'
    END AS career_stage
    
  FROM movie_directors md
  INNER JOIN {{ ref('stg_person') }} p
    ON md.director_id = p.person_id
  LEFT JOIN director_stats s
    ON md.director_id = s.director_id
)

SELECT * FROM final