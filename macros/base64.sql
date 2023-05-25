{% macro decode_base64(field) %}
    -- Implementation for PostgreSQL
    CONVERT_FROM({{ field }}::bytea, 'UTF8')
{% endmacro %}
