{{ config(materialized='table')}}


SELECT 
    summonerName,
    region,
    {{ 
        dbt_utils.pivot(
            'championId',
            dbt_utils.get_column_values(source('data_dragon','champion_info'), 'key'),
            agg = 'sum',
            then_value = 'championPoints'
        )
    }}
FROM {{ ref('stg_champion_mastery')}}
GROUP BY summonerName, region
