{{ config(materialized='table') }}
SELECT 
    match_teams.matchId,
    match_details.tier,
    match_details.division,
    {{ generate_champion_ban_columns() }}
FROM {{ ref("stg_teams_match") }} AS match_teams
LEFT JOIN {{ ref('match_league_points')}} AS match_details
ON match_details.matchId = match_teams.matchId