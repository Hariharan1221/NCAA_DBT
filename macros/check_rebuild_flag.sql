{% macro check_rebuild_flag() %}

{% set query %}
    SELECT COUNT(*)
    FROM {{ source('control_tables','HIERARCHY_REBUILD_CTRL') }}
    WHERE SILVER_TABLE_NAME = '{{ this.name }}'
    AND HIERARCHY_REBUILD_FLAG = 'Y'
{% endset %}

{% set result = run_query(query) %}

{% if execute %}
    {% set rebuild_count = result.columns[0].values()[0] %}
{% else %}
    {% set rebuild_count = 1 %}
{% endif %}

{{ return(rebuild_count > 0) }}

{% endmacro %}
