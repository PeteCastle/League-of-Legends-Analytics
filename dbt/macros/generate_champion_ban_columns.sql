{% macro generate_champion_ban_columns() %}
    {% set sql_statement %}
    SELECT key, id FROM champion_info ORDER BY key
    {% endset %}
    {% set champion_list = run_query(sql_statement) %}
    {% for champion in champion_list  %}
        CASE
            {% for n in range(1,11) %}
               WHEN bans_turn_{{n}} = {{champion[0]}} THEN 1
            {% endfor %}
            ELSE 0 
        END AS {{champion[1]}} {{"," if not loop.last}}
     
    {% endfor %}
{% endmacro %} 