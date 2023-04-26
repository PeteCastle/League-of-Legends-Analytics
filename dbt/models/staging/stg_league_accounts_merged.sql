{{ config(materialized='view')}}

SELECT
    stg_accounts.name,
    stg_accounts.region,
    stg_accounts.summonerLevel,
    stg_accounts.leaguePoints,
    stg_accounts.wins,
    stg_accounts.losses,
    stg_leagues.queue,
    stg_leagues.tier,
    stg_leagues.division

FROM {{ ref('stg_accounts')}} AS stg_accounts
LEFT JOIN {{ ref('stg_leagues')}} AS stg_leagues
ON stg_accounts.leagueId = stg_leagues.leagueId 
    AND stg_accounts.region = stg_leagues.region
-- GROUP BY
--     stg_accounts.name,
--     stg_accounts.region,
--     stg_accounts.summonerLevel,
--     stg_accounts.leaguePoints,
--     stg_accounts.wins,
--     stg_accounts.losses,
--     stg_leagues.queue,
--     stg_leagues.tier,
--     stg_leagues.division