{% macro aggregate_single_table(table_name, column_name, time_granularity='hour', renamed_column_name=None) %}
    {% set time_truncation %}
        {% if time_granularity == 'hour' %}
            DATE_TRUNC('hour', ACTIVITYHOUR)  -- Truncates to the hour
        {% elif time_granularity == 'minute' %}
            DATE_TRUNC('minute', ACTIVITYHOUR)  -- Truncates to the minute
        {% elif time_granularity == 'day' %}
            DATE_TRUNC('day', ACTIVITYHOUR)  -- Truncates to the day
        {% else %}
            DATE_TRUNC('hour', ACTIVITYHOUR)
        {% endif %}
    {% endset %}

    {% set final_column_name = renamed_column_name if renamed_column_name else column_name %}

    {% set aggregation_query %}
        SELECT
            ID AS user_id,
            {{ time_truncation }} AS activity_{{time_granularity}},
            AVG({{ column_name }}) AS avg_{{ final_column_name }},
            MIN({{ column_name }}) AS min_{{ final_column_name }},
            MAX({{ column_name }}) AS max_{{ final_column_name }},
            COUNT(*) AS {{ final_column_name }}_readings
        FROM {{ ref(table_name) }}
        GROUP BY ID, activity_{{time_granularity}}
    {% endset %}

    {{ return(aggregation_query) }}
{% endmacro %}
