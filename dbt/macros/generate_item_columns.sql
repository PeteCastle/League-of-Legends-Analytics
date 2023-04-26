{% macro generate_item_columns() %}
    {% set sql_statement %}
    SELECT id,name FROM item_info
    {% endset %}
    {% set item_list = run_query(sql_statement) %}
    {% for item in item_list  %}
        CASE
            {% for n in range(0,6) %}
               WHEN item{{n}} = {{champion[0]}} THEN 1
            {% endfor %}
            ELSE 0 
        END AS {{champion[1]}}
        {%  if not loop.last -%}
         ,
        {% endif -%}
    {% endfor %}
{% endmacro %} 