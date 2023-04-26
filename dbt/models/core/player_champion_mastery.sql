{{ config(materialized='table')}}

SELECT
    stg_champion_mastery.summonerName,
    stg_champion_mastery.region,
    league_accounts.queue,
    league_accounts.tier,
    league_accounts.division,
    champion_info.id,
    champion_info.key AS champion_id,
    champion_info.name AS champion_name,
    champion_info.primary_class AS champion_primary_class,
    champion_info.secondary_class AS champion_secondary_class,
    stg_champion_mastery.championLevel,
    stg_champion_mastery.championPoints

FROM {{ ref('stg_champion_mastery')}} AS stg_champion_mastery
LEFT JOIN {{ ref('stg_league_accounts_merged')}} AS league_accounts
ON stg_champion_mastery.summonerName = league_accounts.name
    AND stg_champion_mastery.region = league_accounts.region 
LEFT JOIN {{ source('data_dragon','champion_info')}} AS champion_info
    ON stg_champion_mastery.championId = champion_info.key
ORDER BY summonerName,region,id