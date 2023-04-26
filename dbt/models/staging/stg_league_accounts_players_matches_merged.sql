{{ config(materialized='view')}}
--  Note that not all accounts in each matched are extracted during the ETL process so missing values are expected.  A workaround is needed

WITH temp_table AS (
    SELECT 
        matchId, 
        summonerName,
        LOWER(SUBSTRING_INDEX(matchId,'_',1)) AS region
    FROM {{ ref('stg_players_match')}} 
)
SELECT 
    temp_table.matchId,
    temp_table.summonerName,
    temp_table.region,
    league_accounts.queue,
    league_accounts.tier,
    league_accounts.division
FROM temp_table
LEFT JOIN {{ ref('stg_league_accounts_merged')}} AS league_accounts
ON temp_table.summonerName = league_accounts.name
    AND temp_table.region = league_accounts.region
ORDER BY temp_table.matchId