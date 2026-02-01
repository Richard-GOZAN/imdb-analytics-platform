{{
  config(
    materialized='table',
    tags=['marts', 'dimension']
  )
}}

/*
Dimension table: actors

This table contains one row per actor who has acted in movies in our
fact table (movies). It includes biographical information, aggregated
statistics about their filmography, and gender-based analysis.

Purpose:
- Enable actor-based filtering and analysis
- Provide actor details for BI tools
- Calculate actor-level metrics (avg rating, total movies, career span)
- Analyze gender representation in cinema

Source: movies (fact table) + stg_person
Output: Table in silver dataset
*/

WITH movie_actors AS (
  /*
  Extract unique actors from the movies fact table
  Uses UNNEST to flatten the actors array
  */
  SELECT DISTINCT
    actor.id AS actor_id,
    actor.gender AS gender  
  FROM {{ ref('movies') }},
  UNNEST(actors) AS actor
  WHERE
    actor.id IS NOT NULL
),

actor_stats AS (
  /*
  Calculate aggregated statistics per actor
  */
  SELECT
    actor.id AS actor_id,
    
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
    )[OFFSET(0)] AS best_movie,
    
    -- Top billing statistics (how often they're the lead)
    COUNTIF(actor_position = 1) AS lead_role_count,
    ROUND(COUNTIF(actor_position = 1) / COUNT(*) * 100, 1) AS lead_role_percentage
    
  FROM {{ ref('movies') }} m,
  UNNEST(m.actors) AS actor WITH OFFSET AS actor_position
  WHERE actor.id IS NOT NULL
  GROUP BY actor.id
),

final AS (
  /*
  Combine actor personal information with their statistics
  */
  SELECT 
    -- Primary key
    p.person_id AS actor_id,
    
    -- Personal information
    p.name AS actor_name,
    p.birth_year,
    p.death_year,
    p.profession,
    p.known_for_titles,
    
    -- Gender (from movie_actors, could be NULL if not consistently coded)
    ma.gender,
    
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
    s.lead_role_count,
    s.lead_role_percentage,
    
    -- Derived categories for analysis
    CASE
      WHEN s.avg_movie_rating >= 8.0 THEN 'Elite'
      WHEN s.avg_movie_rating >= 7.0 THEN 'Excellent'
      WHEN s.avg_movie_rating >= 6.0 THEN 'Good'
      ELSE 'Average'
    END AS actor_tier,
    
    CASE
      WHEN s.total_movies >= 30 THEN 'Prolific'
      WHEN s.total_movies >= 15 THEN 'Established'
      WHEN s.total_movies >= 5 THEN 'Emerging'
      ELSE 'New'
    END AS career_stage,
    
    CASE
      WHEN s.lead_role_percentage >= 50 THEN 'Leading Actor'
      WHEN s.lead_role_percentage >= 25 THEN 'Mixed Roles'
      ELSE 'Supporting Actor'
    END AS role_type
    
  FROM movie_actors ma
  INNER JOIN {{ ref('stg_person') }} p
    ON ma.actor_id = p.person_id
  LEFT JOIN actor_stats s
    ON ma.actor_id = s.actor_id
)

SELECT * FROM final
