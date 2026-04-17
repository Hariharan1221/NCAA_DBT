{% macro generate_dimension_hierarchy(dimension, system_name, source_name) %}

WITH RECURSIVE SOURCE_DEDUP AS (

    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY {{ dimension }}_metadata_key_hash
                   ORDER BY src_last_update_date_time DESC
               ) AS rn
        FROM {{ source(source_name, system_name ~ '_' ~ dimension ~ '_METADATA') }}
    )
    WHERE rn = 1

),

/* ---------------------------------------------------
ROOT MEMBERS
--------------------------------------------------- */

ROOTS AS (

    SELECT *
    FROM SOURCE_DEDUP s
    WHERE s.{{ dimension }}_metadata_parent IS NULL
       OR NOT EXISTS (
            SELECT 1
            FROM SOURCE_DEDUP p
            WHERE p.{{ dimension }} = s.{{ dimension }}_metadata_parent
       )

),

/* ---------------------------------------------------
RECURSIVE HIERARCHY
--------------------------------------------------- */

HIERARCHY AS (

    SELECT
        r.{{ dimension }},
        r.{{ dimension }}_metadata_parent,
        r.{{ dimension }}_metadata_key_hash,
        ARRAY_CONSTRUCT(r.{{ dimension }}) AS path_array,
        1 AS level,
        r.{{ dimension }} AS leaf_{{ dimension }},
        IFF(UPPER(r.data_storage) = 'SHARED',1,0) AS stop_flag
    FROM ROOTS r

    UNION ALL

    SELECT
        c.{{ dimension }},
        c.{{ dimension }}_metadata_parent,
        c.{{ dimension }}_metadata_key_hash,
        ARRAY_APPEND(h.path_array,c.{{ dimension }}),
        h.level + 1,
        c.{{ dimension }},
        IFF(h.stop_flag = 1 OR UPPER(c.data_storage)='SHARED',1,0)
    FROM HIERARCHY h
    JOIN SOURCE_DEDUP c
      ON c.{{ dimension }}_metadata_parent = h.{{ dimension }}
    WHERE h.level < 11
      AND h.stop_flag = 0

),

/* ---------------------------------------------------
FLATTEN HIERARCHY
--------------------------------------------------- */

FINAL_DATA AS (

SELECT

    path_array[0]::VARCHAR(80) AS SELECTION,

    IFF(ARRAY_SIZE(path_array)>=2,path_array[1]::VARCHAR(80),NULL) AS GENERATION_1,
    IFF(ARRAY_SIZE(path_array)>=3,path_array[2]::VARCHAR(80),NULL) AS GENERATION_2,
    IFF(ARRAY_SIZE(path_array)>=4,path_array[3]::VARCHAR(80),NULL) AS GENERATION_3,
    IFF(ARRAY_SIZE(path_array)>=5,path_array[4]::VARCHAR(80),NULL) AS GENERATION_4,
    IFF(ARRAY_SIZE(path_array)>=6,path_array[5]::VARCHAR(80),NULL) AS GENERATION_5,
    IFF(ARRAY_SIZE(path_array)>=7,path_array[6]::VARCHAR(80),NULL) AS GENERATION_6,
    IFF(ARRAY_SIZE(path_array)>=8,path_array[7]::VARCHAR(80),NULL) AS GENERATION_7,
    IFF(ARRAY_SIZE(path_array)>=9,path_array[8]::VARCHAR(80),NULL) AS GENERATION_8,
    IFF(ARRAY_SIZE(path_array)>=10,path_array[9]::VARCHAR(80),NULL) AS GENERATION_9,
    IFF(ARRAY_SIZE(path_array)>=11,path_array[10]::VARCHAR(80),NULL) AS GENERATION_10,

    CASE
        WHEN EXISTS (
            SELECT 1
            FROM SOURCE_DEDUP p
            WHERE p.{{ dimension }}_metadata_parent = h.leaf_{{ dimension }}
        )
        THEN NULL
        ELSE h.leaf_{{ dimension }}
    END::VARCHAR(80) AS {{ dimension }}_CODE,

    b.alias_default::VARCHAR(255) AS {{ dimension }}_NAME,

    '{{ dimension | upper }}' AS DIMENSION,

    b.{{ dimension }}_metadata_key_hash AS {{ dimension }}_KEY_HASH,
    b.{{ dimension }}_metadata_nonkey_hash AS {{ dimension }}_NONKEY_HASH,

    b.src_create_date_time,
    b.src_last_update_date_time,
    b.src_created_by,
    b.src_last_updated_by,

    b.batch_id,
    b.raw_edw_create_date_time,
    b.edw_create_date_time AS BRONZE_EDW_CREATE_DATE_TIME,
    b.file_name,

    ROW_NUMBER() OVER (
        PARTITION BY b.{{ dimension }}_metadata_key_hash, path_array
        ORDER BY b.src_last_update_date_time DESC
    ) AS rn

FROM HIERARCHY h
JOIN SOURCE_DEDUP b
  ON b.{{ dimension }}_metadata_key_hash = h.{{ dimension }}_metadata_key_hash

),

HIERARCHY_DEDUP AS (

SELECT *
FROM FINAL_DATA
WHERE rn = 1

)

/* ---------------------------------------------------
FINAL OUTPUT
--------------------------------------------------- */

SELECT

    COALESCE(h.SELECTION,b.{{ dimension }})::VARCHAR(80) AS SELECTION,

    h.GENERATION_1::VARCHAR(80) AS GENERATION_1,
    h.GENERATION_2::VARCHAR(80) AS GENERATION_2,
    h.GENERATION_3::VARCHAR(80) AS GENERATION_3,
    h.GENERATION_4::VARCHAR(80) AS GENERATION_4,
    h.GENERATION_5::VARCHAR(80) AS GENERATION_5,
    h.GENERATION_6::VARCHAR(80) AS GENERATION_6,
    h.GENERATION_7::VARCHAR(80) AS GENERATION_7,
    h.GENERATION_8::VARCHAR(80) AS GENERATION_8,
    h.GENERATION_9::VARCHAR(80) AS GENERATION_9,
    h.GENERATION_10::VARCHAR(80) AS GENERATION_10,

    h.{{ dimension }}_CODE,

    b.alias_default::VARCHAR(255) AS {{ dimension }}_NAME,

    '{{ dimension | upper }}' AS DIMENSION,

    b.{{ dimension }}_metadata_key_hash AS {{ dimension }}_KEY_HASH,
    b.{{ dimension }}_metadata_nonkey_hash AS {{ dimension }}_NONKEY_HASH,

    b.src_create_date_time,
    b.src_last_update_date_time,
    b.src_created_by,
    b.src_last_updated_by,

    CURRENT_TIMESTAMP() AS EDW_CREATE_DATE_TIME,
    CURRENT_TIMESTAMP() AS EDW_LAST_UPDATE_DATE_TIME,
    'SYSTEM' AS EDW_CREATED_BY,
    'SYSTEM' AS EDW_LAST_UPDATED_BY,

    '{{ system_name }}' AS SRC_SYS_CD,

    b.batch_id,
    b.raw_edw_create_date_time,
    b.edw_create_date_time AS BRONZE_EDW_CREATE_DATE_TIME,
    b.file_name

FROM SOURCE_DEDUP b
LEFT JOIN HIERARCHY_DEDUP h
ON b.{{ dimension }}_metadata_key_hash = h.{{ dimension }}_KEY_HASH

{% endmacro %}