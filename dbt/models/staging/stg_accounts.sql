{{ config(materialized='view')}}
WITH temp_table AS (
SELECT
    row_number() OVER(PARTITION BY name, region ORDER BY last_updated DESC) AS rownum, --This is to ensure that accounts will only receive the latest record
    leagueId,
    name,
    region,
    summonerLevel,
    leaguePoints,
    wins,
    losses,
    last_updated
from
    accounts
)

SELECT 
    leagueId,
    name,
    region,
    summonerLevel,
    leaguePoints,
    wins,
    losses
FROM temp_table
WHERE rownum = 1