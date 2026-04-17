{% macro time_to_timestamp_ltz(date_column, time_column) -%}

COALESCE(

  /* ==========================================================
     1) 12-hour formats with A/P (and optional AM/PM)
     Handles: 1150A, 0530P, 930P, 1213PM, 12:13P, "00630A"
     ========================================================== */
  TRY_TO_TIMESTAMP_LTZ(
    TO_VARCHAR(TO_DATE({{ date_column }})) || ' ' ||
    (
      WITH t AS (
        SELECT UPPER(REGEXP_REPLACE(TRIM({{ time_column }}), '[^0-9APM]', '')) AS s0
      ),
      n AS (
        SELECT
          -- drop trailing M (PM/AM -> P/A)
          REGEXP_REPLACE(s0, 'M$', '') AS s1
        FROM t
      ),
      m AS (
        SELECT
          -- normalize 5-digit + A/P: 00630A -> 0630A
          REGEXP_REPLACE(s1, '^0([0-9]{4})([AP])$', '\\1\\2') AS s2
        FROM n
      ),
      f AS (
        SELECT
          -- pad 3-digit + A/P: 930P -> 0930P
          REGEXP_REPLACE(s2, '^([0-9]{3})([AP])$', '0\\1\\2') AS s
        FROM m
      )
      SELECT
        CASE
          WHEN NOT REGEXP_LIKE(s, '^[0-9]{4}[AP]$') THEN NULL
          WHEN TRY_TO_NUMBER(SUBSTR(s, 3, 2)) > 59 THEN NULL
          ELSE
            LPAD(
              CASE
                WHEN TRY_TO_NUMBER(SUBSTR(s, 1, 2)) = 12 AND RIGHT(s, 1) = 'A' THEN 0
                WHEN RIGHT(s, 1) = 'P' AND TRY_TO_NUMBER(SUBSTR(s, 1, 2)) <> 12
                  THEN TRY_TO_NUMBER(SUBSTR(s, 1, 2)) + 12
                ELSE TRY_TO_NUMBER(SUBSTR(s, 1, 2))
              END
            , 2, '0')
            || ':' || SUBSTR(s, 3, 2) || ':00'
        END
      FROM f
    ),
    'YYYY-MM-DD HH24:MI:SS'
  ),

  /* ==========================================================
     2) 24-hour numeric formats (no A/P)
     Handles: 525, 0730, 1010, 1530, 00630, 00730, 00835, 00525
     ========================================================== */
  TRY_TO_TIMESTAMP_LTZ(
    TO_VARCHAR(TO_DATE({{ date_column }})) || ' ' ||
    (
      WITH x AS (
        SELECT REGEXP_REPLACE(TRIM({{ time_column }}), '[^0-9]', '') AS d
      ),
      y AS (
        SELECT
          CASE
            WHEN LENGTH(d) = 5 THEN RIGHT(d, 4)              -- 00630 -> 0630
            WHEN LENGTH(d) = 4 THEN d
            WHEN LENGTH(d) = 3 THEN LPAD(d, 4, '0')          -- 525 -> 0525
            ELSE NULL
          END AS hhmm
        FROM x
      )
      SELECT
        CASE
          WHEN hhmm IS NULL THEN NULL
          WHEN TRY_TO_NUMBER(SUBSTR(hhmm, 3, 2)) > 59 THEN NULL
          ELSE REGEXP_REPLACE(hhmm, '^([0-9]{2})([0-9]{2})$', '\\1:\\2:00')
        END
      FROM y
    ),
    'YYYY-MM-DD HH24:MI:SS'
  )

)

{%- endmacro %}
