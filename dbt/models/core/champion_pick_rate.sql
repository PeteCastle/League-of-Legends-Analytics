-- DEPRECATED DONT USE


{{ config(materialized='table')}}

WITH champion_pick_count AS (
    {{ dbt_utils.unpivot(
        relation=ref('champion_pick_count__unclean'),
        cast_to='int',
        field_name = 'champion_id_raw',
        value_name = 'champion_pick_count'
) }}
), champion_pick_count_clean AS (
    SELECT 
        SUBSTRING_INDEX(champion_id_raw,'_',1) AS champion_id,
        champion_pick_count
    FROM champion_pick_count
), total_match_count AS (
    SELECT
        COUNT(DISTINCT(matchId)) AS total
    FROM {{ ref('stg_general_match')}}
)

SELECT 
    champion_pick_count_clean.champion_id AS key,
    -- champion_pick_count_clean.champion_pick_count AS pick_count,
    
    -- champion_info.id,
    champion_info.name,
    ((champion_pick_count_clean.champion_pick_count / (total_match_count.total))*100) AS pick_rate
    
FROM champion_pick_count_clean
LEFT JOIN {{ source('data_dragon', 'champion_info')}} AS champion_info
ON champion_pick_count_clean.champion_id = champion_info.key
CROSS JOIN total_match_count
ORDER BY pick_rate DESC




