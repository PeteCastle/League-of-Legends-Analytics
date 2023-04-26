-- DEPRECATED DONT USE
{{ config(materialized="view") }}

with
    temp_table as (
        select
            row_number() over (
                partition by matchid, championid order by championid
            ) as rownum,
            matchid,
            championid
        from {{ ref("stg_players_match") }}
    ),
    temp_table_2 as (  -- Removal is necessary in matches where same champion is picked twice (only in unranked games), it will only count as one (not two)
        select matchid, championid from temp_table where rownum = 1
    )

select
    {{
        dbt_utils.pivot(
            "championId",
            dbt_utils.get_column_values(
                table=source("data_dragon", "champion_info"), column="key"
            ),
            suffix="_champion",
            agg="sum",
        )
    }}
from temp_table_2
