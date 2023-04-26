{{ config(materialized='table')}}

SELECT
    name,
    region,
    ((wins) / (wins + losses))*100 AS  win_rate_pcnt,
    queue,
    tier,
    division
FROM {{ ref('stg_league_accounts_merged') }}
ORDER BY win_rate_pcnt ASC
