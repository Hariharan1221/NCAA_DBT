{% macro log_model_run() %}
{% if execute %}

    {% set log_table = target.database ~ "." ~ target.schema ~ ".MODEL_RUN_LOG" %}

    {% set model_name = model.name if model is defined else this.identifier %}
    {% set model_schema = this.schema %}
    {% set target_database = this.database %}
    {% set target_schema = this.schema %}
    {% set target_table = this.identifier %}

    {% set query %}
        insert into {{ log_table }} (
            RUN_ID,
            INVOCATION_ID,
            MODEL_NAME,
            MODEL_SCHEMA,
            TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            SOURCE_DATABASE,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            START_TIME,
            END_TIME,
            STATUS,
            TARGET_INSERT_COUNT,
            TARGET_UPD_COUNT,
            TARGET_DEL_COUNT,
            PROCESSED_COUNT,
            TARGET_COUNT,
            SOURCE_COUNT,
            WAREHOUSE,
            EXECUTED_BY
        )
        with ts as (
            select current_timestamp()::timestamp_ltz(9) as start_ts
        )
        select
            start_ts as RUN_ID,
            '{{ invocation_id }}' as INVOCATION_ID,
            '{{ model_name }}' as MODEL_NAME,
            '{{ model_schema }}' as MODEL_SCHEMA,
            '{{ target_database }}' as TARGET_DATABASE,
            '{{ target_schema }}' as TARGET_SCHEMA,
            '{{ target_table }}' as TARGET_TABLE,
            null as SOURCE_DATABASE,
            null as SOURCE_SCHEMA,
            null as SOURCE_TABLE,
            start_ts as START_TIME,
            null as END_TIME,
            'STARTED' as STATUS,
            null as TARGET_INSERT_COUNT,
            null as TARGET_UPD_COUNT,
            null as TARGET_DEL_COUNT,
            null as PROCESSED_COUNT,
            null as TARGET_COUNT,
            null as SOURCE_COUNT,
            current_warehouse() as WAREHOUSE,
            current_user() as EXECUTED_BY
        from ts
    {% endset %}

    {{ return(query) }}

{% endif %}
{% endmacro %}