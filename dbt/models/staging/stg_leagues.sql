{{ config(materialized='view')}}

WITH temp_table AS (
SELECT
    row_number() OVER(PARTITION BY leagueId ORDER BY last_updated DESC) AS rownum, --This is to ensure that accounts will only receive the latest record
    leagueId,
    region,
    queue,
    tier,
    division
FROM
    leagues
)
SELECT 
    leagueId,
    region,
    queue,
    tier,
    division
FROM temp_table
WHERE rownum = 1

    