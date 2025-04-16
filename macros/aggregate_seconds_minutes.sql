{% macro aggregate_seconds_to_minutes_with_column(table_name, column_name, timestamp_column, renamed_column_name=None) %}
    {% set aggregation_query %}
        SELECT
            ID AS user_id,
            DATE_TRUNC('minute', {{ timestamp_column }}) AS activity_minute,  
            AVG({{ column_name }}) AS avg_{{ renamed_column_name or column_name }},
            MIN({{ column_name }}) AS min_{{ renamed_column_name or column_name }},
            MAX({{ column_name }}) AS max_{{ renamed_column_name or column_name }},
            COUNT(*) AS {{ renamed_column_name or column_name }}_readings
        FROM {{ ref(table_name) }}
        GROUP BY ID, activity_minute
    {% endset %}

    {{ return(aggregation_query) }}
{% endmacro %}
