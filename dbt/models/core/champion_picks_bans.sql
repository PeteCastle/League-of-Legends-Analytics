{{ config(materialized='table') }}

WITH champion_picks_unpivoted AS (
    {{ dbt_utils.unpivot(
    ref('champion_picks'),
    cast_to = 'int',
    exclude = ['matchId','tier','division'],
    field_name = 'championId',
    value_name = 'isPicked'
)}}
),

champion_picks_filtered AS (
    SELECT 
        matchId,
        tier,
        division,
        championId
    FROM champion_picks_unpivoted
    WHERE isPicked = 1
),

 champion_bans_unpivoted AS (
    {{ dbt_utils.unpivot(
    ref('champion_bans'),
    cast_to = 'int',
    exclude = ['matchId','tier','division'],
    field_name = 'championId',
    value_name = 'isBanned')
    }}
),

 champion_bans_filtered AS (
    SELECT 
        matchId,
        tier,
        division,
        championId
    FROM champion_bans_unpivoted
    WHERE isBanned = 1
),

 champion_picks_bans_merged AS (
    SELECT 
        matchId,
        tier,
        division,
        championId,
        -- *,
        "ban" AS type
    FROM champion_bans_filtered
    UNION ALL
    SELECT 
        matchId,
        tier,
        division,
        championId,
        -- *,
        "pick" AS type
    FROM champion_picks_filtered
)
SELECT
    champion_picks_bans_merged.matchId,
    champion_picks_bans_merged.tier,
    champion_picks_bans_merged.division,
    champion_picks_bans_merged.championId,
    champion_picks_bans_merged.type,
    champion_info.name AS champion_name,
    champion_info.title AS champion_title
FROM champion_picks_bans_merged
LEFT JOIN {{source('data_dragon','champion_info') }} AS champion_info
ON champion_info.id = champion_picks_bans_merged.championId