{# This macro collects data from a query and inserts it into a list #}

{% macro query_to_list(query) %}
    {% set query_to_process %}
        {{ query }}
    {% endset %}

    {% set results = run_query(query_to_process) %}

    {% if execute %}
    {% set results_list = results.rows %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}

    {{ return(results_list) }}

{% endmacro %}