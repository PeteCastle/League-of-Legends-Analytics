{{ config(materialized='view')}}
WITH temp_table AS(
    SELECT
        row_number() OVER(PARTITION BY matchId ORDER BY dataVersion) AS rownum, --This is to ensure that MATCHES will only receive the latest record
        matchId,
        gameMode,
        gameType,
        gameCreation,
        gameDuration,
        gameVersion
    FROM general_match
)
SELECT 
    matchId,
    gameMode,
    gameType,
    gameCreation,
    gameDuration,
    gameVersion
FROM temp_table
WHERE rownum = 1
