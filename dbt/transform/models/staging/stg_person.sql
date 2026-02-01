{{
  config(
    materialized='view',
    tags=['staging', 'people']
  )
}}

/*
Staging model for people (actors, directors, writers, etc.).

Transforms the raw IMDB name_basics table into a clean dataset
with valid biographical information and known works.

Filters applied:
- Must have valid birth year (excludes unknown birth dates)
- Must have known titles (excludes people without notable works)

Source: bronze.name_basics
Output: View in silver dataset
*/

WITH source AS (
  SELECT * FROM {{ source('bronze', 'name_basics') }}
),

cleaned AS (
  SELECT
    -- Primary key
    nconst AS person_id,
    
    -- Personal information
    primaryName AS name,
    SAFE_CAST(birthYear AS INT64) AS birth_year,
    SAFE_CAST(deathYear AS INT64) AS death_year,
    
    -- Professional information
    primaryProfession AS profession,
    knownForTitles AS known_for_titles
    
  FROM source
  WHERE
    -- Must have valid birth year 
    birthYear IS NOT NULL
    AND SAFE_CAST(birthYear AS INT64) IS NOT NULL
    
    -- Must have known works (excludes obscure people)
    AND knownForTitles IS NOT NULL
    AND knownForTitles != ''
    
    -- Sanity checks
    AND primaryName IS NOT NULL
    AND primaryName != ''
    
    -- Birth year should be reasonable (1800-2020)
    AND SAFE_CAST(birthYear AS INT64) BETWEEN 1800 AND 2020
)

SELECT * FROM cleaned