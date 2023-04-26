{% macro generate_champion_pick_columns() %}
    {% set sql_statement %}
    SELECT key, id FROM champion_info ORDER BY key
    {% endset %}
    {% set champion_list = run_query(sql_statement) %}
    {% for champion in champion_list  %}
        CASE
            WHEN championId = {{ champion[0] }} THEN 1 
            ELSE 0 
        END AS {{champion[1]}}
        {%  if not loop.last -%}
         ,
        {% endif -%}
    {% endfor %}
{% endmacro %} 