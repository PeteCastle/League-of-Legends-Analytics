{% macro generate_champion_id_mappings() %}

    {% set sql_statement %}
    SELECT key, id FROM champion_info
    {% endset %}

    {%- set champion_mappings = dbt_utils.get_query_results_as_dict(sql_statement) -%}
    
    aaa
    {% for champion,id in champion_mappings  -%}
    {{ champion }} {{id}}
    {% endfor %}

  

{% endmacro %} 