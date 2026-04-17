{% macro update_rebuild_flag(bronze_table, source_name) %}
 
UPDATE {{ source('control_tables','HIERARCHY_REBUILD_CTRL') }}
SET
    HIERARCHY_REBUILD_FLAG = 'Y',
    EDW_LAST_UPDATE_DATE_TIME = CURRENT_TIMESTAMP
WHERE BRONZE_TABLE_NAME = '{{ bronze_table }}'
  AND (
        SILVER_LAST_PROCESSED_DATE_TIME < (
            SELECT MAX(EDW_LAST_UPDATE_DATE_TIME)
            FROM {{ this }}
        )
        OR
        (SELECT COUNT(*) FROM {{ this }})
>
        (SELECT COUNT(*) FROM {{ source(source_name, bronze_table) }})
      );
 
{% endmacro %}