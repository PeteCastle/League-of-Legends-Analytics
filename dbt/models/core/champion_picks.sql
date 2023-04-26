{{ config(materialized='table')}}
WITH temp_table AS (
    SELECT 
        matchId,
        {{ generate_champion_pick_columns() }}
    FROM {{ref("stg_players_match")}}
)
{% set champion_list = run_query("SELECT id FROM champion_info ORDER BY key") %}
, temp_table_2 AS(
    SELECT 
        matchId,
        {% for champion in champion_list  %}
            MAX({{champion[0]}}) AS {{champion[0]}}
            {% if not loop.last -%}
            ,
            {% endif -%}
        {% endfor %}
    FROM temp_table
    GROUP BY matchId
)

SELECT 
    temp_table_2.matchId,
    match_details.tier,
    match_details.division,
    {% for champion in champion_list  %}
    temp_table_2.{{champion[0]}}
    {% if not loop.last -%}
            ,
            {% endif -%}
    {% endfor %}
FROM temp_table_2
LEFT JOIN {{ ref('match_league_points')}} AS match_details
ON match_details.matchId = temp_table_2.matchId
  