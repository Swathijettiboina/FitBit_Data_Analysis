-- for general types 
{% macro cast_col(col_name, col_type) %}
    CAST("{{ col_name }}" AS {{ col_type }}) AS {{ col_name }}
{% endmacro %}

{% macro cast_col_precise(col_name, col_type) %}
    CAST("{{ col_name }}" AS {{ col_type }}) AS {{ col_name }}
{% endmacro %}

-- for standardizing date columns 
{% macro cast_date(column_name) %}
    COALESCE(
        TRY_TO_DATE("{{ column_name }}", 'YYYY-MM-DD'),
        TRY_TO_DATE("{{ column_name }}", 'MM/DD/YYYY'),
        TRY_TO_DATE("{{ column_name }}")
    ) AS {{ column_name }}
{% endmacro %}




{% macro cast_timestamp(column_name) %}
    COALESCE(
        TRY_TO_TIMESTAMP("{{ column_name }}", 'YYYY-MM-DD HH24:MI:SS'),        
        TRY_TO_TIMESTAMP("{{ column_name }}", 'YYYY-MM-DD"T"HH24:MI:SS'),     
        TRY_TO_TIMESTAMP("{{ column_name }}", 'MM/DD/YYYY HH12:MI:SS AM'),      
        TRY_TO_TIMESTAMP("{{ column_name }}", 'DD-MM-YYYY HH24:MI:SS'),       
        TRY_TO_TIMESTAMP("{{ column_name }}", 'MM-DD-YYYY HH24:MI:SS'),       
        TRY_TO_TIMESTAMP("{{ column_name }}", 'YYYY-MM-DD'),                  
        TRY_TO_TIMESTAMP("{{ column_name }}", 'MM/DD/YYYY'),                  
        TRY_TO_TIMESTAMP("{{ column_name }}", 'DD-MM-YYYY'),                   
        TRY_TO_TIMESTAMP("{{ column_name }}", 'YYYY-MM-DD"T"HH:MI:SS'),         
        TRY_TO_TIMESTAMP("{{ column_name }}")                                     
    ) AS {{ column_name }}
{% endmacro %}


