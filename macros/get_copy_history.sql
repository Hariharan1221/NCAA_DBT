{% macro get_copy_history(fq_table, job_name, src_sys_cd, src_entity, lookback_hours=1) %}

    SELECT
        LAST_LOAD_TIME::DATE                            AS AS_OF_DT,
        NULL                                            AS BATCH_ID,
        '{{ job_name }}'                                AS JOB_NAME,
        '{{ src_sys_cd }}'                              AS SRC_SYS_CD,

        CASE
            WHEN '{{ src_entity }}' <> '' THEN '{{ src_entity }}'
            ELSE REGEXP_REPLACE(SPLIT_PART(FILE_NAME, '/', -1), '\\.[^.]+$', '')
        END                                             AS SRC_ENTITY,

        STAGE_LOCATION || FILE_NAME                     AS SRC_FILE_PATH,

        CASE STATUS
            WHEN 'Loaded'           THEN 'LOADED'
            WHEN 'Load in progress' THEN 'LOAD IN PROGRESS'
            WHEN 'Partially loaded' THEN 'PARTIALLY LOADED'
            ELSE                         'FAILED'
        END                                             AS FILE_STATUS,

        '{{ fq_table }}'                                AS TARGET_TABLE,
        COALESCE(ROW_PARSED, 0)                         AS SOURCE_COUNT,
        COALESCE(ROW_COUNT,  0)                         AS TARGET_LOADED_COUNT,
        LAST_LOAD_TIME                                  AS JOB_STRT_DTTM,
        LAST_LOAD_TIME                                  AS JOB_END_DTTM,
        CURRENT_TIMESTAMP()                             AS CRTD_DT_TM,
        CURRENT_USER()                                  AS CRTD_BY,
        CURRENT_TIMESTAMP()                             AS LST_UPD_DT_TM,
        CURRENT_USER()                                  AS LST_UPD_BY

    FROM TABLE(
        INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => '{{ fq_table }}',
            START_TIME => DATEADD('HOUR', -{{ lookback_hours }}, CURRENT_TIMESTAMP())
        )
    )

{% endmacro %}