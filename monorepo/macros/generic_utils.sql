# macros/dynamic_schema.sql
{% macro get_dynamic_schema() %}
  {% if target.name == 'dev' %}
    {{ var('user') }}  # Dynamic schema based on the user's name
  {% else %}
    data  # Fixed schema for production
  {% endif %}
{% endmacro %}

# macros/date_threshold.sql
{% macro get_date_threshold(days_threshold = var('global_days_threshold', 30) ) %}
    dateadd(day, {{ -days_threshold }}, current_date)
{% endmacro %}

# macros/incremental_config.sql
{% macro apply_incremental_config(unique_key, partition_field, data_type='date') %}
    {{
        config(
            materialized='incremental',
            unique_key=unique_key,
            incremental_strategy='merge',
            partition_by={
                "field": partition_field,
                "data_type": data_type
            }
        )
    }}
{% endmacro %}
