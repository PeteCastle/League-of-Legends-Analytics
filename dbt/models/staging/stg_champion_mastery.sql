{{ config(materialized='view')}}

WITH temp_table AS (
SELECT
    row_number() OVER(PARTITION BY summonerName, region, championId ORDER BY championPoints DESC) AS rownum, --This is to ensure that accounts will only receive the latest record
    summonerName,
    region,
    championId,
    championLevel,
    championPoints,
    chestGranted,
    tokensEarned
FROM champion_mastery
)
SELECT 
    summonerName,
    region,
    championId,
    championLevel,
    championPoints,
    chestGranted,
    tokensEarned
FROM temp_table
WHERE rownum = 1
    