{% macro decode_base64(field) %}
    -- Implementation for PostgreSQL
     DECODE({{ field }}, 'base64')
{% endmacro %}
