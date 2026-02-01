{{
  config(
    materialized='view',
    tags=['staging', 'ratings']
  )
}}

/*
Staging model for title ratings.

Transforms the raw IMDB title_ratings table into a clean dataset
with only well-rated titles (sufficient votes for statistical significance).

Filters applied:
- Minimum vote threshold (configurable, default: 10,000 votes)
- Valid ratings only (not NULL)

Source: bronze.title_ratings
Output: View in silver dataset
*/

WITH source AS (
  SELECT * FROM {{ source('bronze', 'title_ratings') }}
),

filtered AS (
  SELECT
    -- Primary key (foreign key to stg_title_basics)
    tconst AS movie_id,
    
    -- Rating metrics
    averageRating AS average_rating,
    numVotes AS num_votes
    
  FROM source
  WHERE
    -- Exclude titles with NULL ratings
    averageRating IS NOT NULL
    
    -- Only include titles with sufficient votes for statistical significance
    -- Configurable via dbt variable (defaults to 10,000)
    AND numVotes > {{ var('min_votes', 10000) }}
    
    -- Sanity checks
    AND averageRating >= 0
    AND averageRating <= 10
    AND numVotes >= 0
)

SELECT * FROM filtered

/*
Notes:
- min_votes is configurable via dbt variable
- Filters out obscure titles with too few ratings
- 10K votes ensures statistically significant ratings
- Sanity checks prevent data quality issues
*/
