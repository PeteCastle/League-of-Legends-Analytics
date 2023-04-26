{{ config(materialized='table')}}

-- Note that not all players stg_league_accounts_players_matches_merged have values in queue, tier, and division.
WITH temp_table AS (
    SELECT
        matchId,
        {{ generate_league_ranking_info_to_points() }}
    FROM {{ ref('stg_league_accounts_players_matches_merged') }}
),

temp_table_2 AS (
SELECT
    matchId,
    ROUND(AVG(player_league_point)) AS avg_match_league_point
FROM temp_table
GROUP BY matchId

)

SELECT
    matchId,
    {{ generate_league_ranking_points_to_info() }}
FROM temp_table_2