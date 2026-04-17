{% macro log_run_end(results) %}
{% if execute %}

    {% set log_table = target.database ~ "." ~ target.schema ~ ".MODEL_RUN_LOG" %}

    {% for res in results %}
        {% if res.node is not none and res.node.resource_type == 'model' %}

            {% set model_name   = res.node.name %}
            {% set model_schema = res.node.schema %}
            {% set model_db     = res.node.database %}
            {% set model_alias  = res.node.alias %}

            {% set rel = adapter.get_relation(
                database=model_db,
                schema=model_schema,
                identifier=model_alias
            ) %}

            {% if rel is not none %}

                {% set start_time_sql %}
                    select max(START_TIME) as START_TIME
                    from {{ log_table }}
                    where INVOCATION_ID = '{{ invocation_id }}'
                      and MODEL_NAME = '{{ model_name }}'
                      and MODEL_SCHEMA = '{{ model_schema }}'
                      and TARGET_DATABASE = '{{ model_db }}'
                      and TARGET_SCHEMA = '{{ model_schema }}'
                      and TARGET_TABLE = '{{ model_alias }}'
                      and STATUS = 'STARTED'
                {% endset %}

                {% set start_time_result = run_query(start_time_sql) %}
                {% if start_time_result is not none and start_time_result.rows | length > 0 %}
                    {% set start_time = start_time_result.columns[0].values()[0] %}
                {% else %}
                    {% set start_time = none %}
                {% endif %}

                {% set target_count_sql %}
                    select count(*) as CNT
                    from {{ rel }}
                {% endset %}

                {% set target_count_result = run_query(target_count_sql) %}
                {% if target_count_result is not none and target_count_result.rows | length > 0 %}
                    {% set target_count = target_count_result.columns[0].values()[0] | int %}
                {% else %}
                    {% set target_count = 0 %}
                {% endif %}

                {% set insert_count = 0 %}
                {% set update_count = 0 %}
                {% set delete_count = 0 %}
                {% set processed_count = 0 %}

                {# =======================================================
                   SOURCE DATABASE / SCHEMA / TABLE population
                   ======================================================= #}

                {% set ns = namespace(
    src_count_list  = [],
    src_db_list     = [],
    src_schema_list = [],
    src_table_list  = []
) %}

{% for node_id in res.node.depends_on.nodes %}

    {# ── source() nodes → Bronze ── #}
    {% if node_id.startswith('source.') %}

        {% set src_db     = none %}
        {% set src_schema = none %}
        {% set src_table  = none %}

        {% set src_node = graph.sources.get(node_id) %}
        {% if src_node %}
            {% set src_db          = src_node.database %}
            {% set src_schema      = src_node.schema %}
            {% set src_table       = src_node.identifier %}
            {% set src_source_name = src_node.source_name %}
        {% endif %}

        {% if not src_db %}
            {% for k, v in graph.sources.items() %}
                {% if v.get('unique_id') == node_id %}
                    {% set src_db          = v.database %}
                    {% set src_schema      = v.schema %}
                    {% set src_table       = v.identifier %}
                    {% set src_source_name = v.source_name %}
                {% endif %}
            {% endfor %}
        {% endif %}

        {% if not src_db %}
            {% set parts = node_id.split('.') %}
            {% if parts | length >= 4 %}
                {% set src_db          = target.database %}
                {% set src_schema      = parts[2] %}
                {% set src_table       = parts[3] %}
                {% set src_source_name = parts[2] %}
            {% endif %}
        {% endif %}

        {# Exclude control tables from source logging #}
        {% if src_db and src_schema and src_table 
           and src_table != 'HIERARCHY_REBUILD_CTRL' 
           and src_source_name != 'control_tables' %}
            {% set _ = ns.src_db_list.append(src_db) %}
            {% set _ = ns.src_schema_list.append(src_schema) %}
            {% set _ = ns.src_table_list.append(src_table) %}

            {% set src_relation = adapter.get_relation(
                database=src_db,
                schema=src_schema,
                identifier=src_table
            ) %}
            {% if src_relation %}
                {% set src_cnt_sql %}select count(*) from {{ src_relation }}{% endset %}
            {% else %}
                {% set src_cnt_sql %}select count(*) from {{ src_db }}.{{ src_schema }}.{{ src_table }}{% endset %}
            {% endif %}
            {% set src_cnt_res = run_query(src_cnt_sql) %}
            {% if src_cnt_res and src_cnt_res.rows | length > 0 %}
                {% set _ = ns.src_count_list.append(src_cnt_res.columns[0].values()[0] | int) %}
            {% endif %}
        {% endif %}

    {# ── ref() nodes → Silver / upstream models ── #}
    {% elif node_id.startswith('model.') %}

        {% set silver_db     = none %}
        {% set silver_schema = none %}
        {% set silver_table  = none %}

        {% set silver_node = graph.nodes.get(node_id) %}
        {% if silver_node %}
            {% set silver_db     = silver_node.database %}
            {% set silver_schema = silver_node.schema %}
            {% set silver_table  = silver_node.alias %}
        {% endif %}

        {% if not silver_db %}
            {% set parts = node_id.split('.') %}
            {% if parts | length >= 3 %}
                {% set silver_db     = target.database %}
                {% set silver_schema = target.schema %}
                {% set silver_table  = parts[2] %}
            {% endif %}
        {% endif %}

        {# Exclude control tables from source logging if they are models #}
        {% if silver_db and silver_schema and silver_table and silver_table != 'HIERARCHY_REBUILD_CTRL' %}
            {% set _ = ns.src_db_list.append(silver_db) %}
            {% set _ = ns.src_schema_list.append(silver_schema) %}
            {% set _ = ns.src_table_list.append(silver_table) %}

            {% set silver_relation = adapter.get_relation(
                database=silver_db,
                schema=silver_schema,
                identifier=silver_table
            ) %}
            {% if silver_relation %}
                {% set silver_cnt_sql %}select count(*) from {{ silver_relation }}{% endset %}
            {% else %}
                {% set silver_cnt_sql %}select count(*) from {{ silver_db }}.{{ silver_schema }}.{{ silver_table }}{% endset %}
            {% endif %}
            {% set silver_cnt_res = run_query(silver_cnt_sql) %}
            {% if silver_cnt_res and silver_cnt_res.rows | length > 0 %}
                {% set _ = ns.src_count_list.append(silver_cnt_res.columns[0].values()[0] | int) %}
            {% endif %}
        {% endif %}

    {% endif %}

{% endfor %}

{# ── sum the count list outside the loop — no namespace mutation needed ── #}
{% set source_count    = ns.src_count_list | sum %}
{% set source_database = ns.src_db_list     | unique | join(', ') %}
{% set source_schema   = ns.src_schema_list | unique | join(', ') %}
{% set source_table    = ns.src_table_list  | join(', ') %}

                {# ======================================================= #}

                {% if start_time is not none %}

                    {% set insert_sql %}
                        select count(*) as CNT
                        from {{ rel }}
                        where EDW_CREATE_DATE_TIME > '{{ start_time }}'::TIMESTAMP_LTZ(9)
                          and EDW_LAST_UPDATE_DATE_TIME is not null
                          and EDW_CREATE_DATE_TIME = EDW_LAST_UPDATE_DATE_TIME
                    {% endset %}

                    {% set insert_result = run_query(insert_sql) %}
                    {% if insert_result is not none and insert_result.rows | length > 0 %}
                        {% set insert_count = insert_result.columns[0].values()[0] | int %}
                    {% endif %}

                    {% set update_count_sql %}
                        select count(*) as CNT
                        from {{ rel }}
                        where EDW_LAST_UPDATE_DATE_TIME > '{{ start_time }}'::TIMESTAMP_LTZ(9)
                          and EDW_CREATE_DATE_TIME is not null
                          and EDW_LAST_UPDATE_DATE_TIME is not null
                          and EDW_CREATE_DATE_TIME < EDW_LAST_UPDATE_DATE_TIME
                    {% endset %}

                    {% set update_result = run_query(update_count_sql) %}
                    {% if update_result is not none and update_result.rows | length > 0 %}
                        {% set update_count = update_result.columns[0].values()[0] | int %}
                    {% endif %}

                    {% set start_count_sql %}
                        select count(*) as CNT
                        from {{ rel }} at (timestamp => '{{ start_time }}'::TIMESTAMP_LTZ(9))
                    {% endset %}

                    {% set start_count_result = run_query(start_count_sql) %}
                    {% if start_count_result is not none and start_count_result.rows | length > 0 %}
                        {% set start_count = start_count_result.columns[0].values()[0] | int %}
                    {% else %}
                        {% set start_count = 0 %}
                    {% endif %}

                    {% set delete_count = start_count + insert_count - target_count %}
                    {% if delete_count < 0 %}
                        {% set delete_count = 0 %}
                    {% endif %}

                    {% set processed_count = insert_count + update_count + delete_count %}

                {% endif %}

                {% set final_status = 'SUCCESS' if (res.status | string | lower) == 'success' else 'FAILED' %}

                {% set update_sql %}
                    update {{ log_table }}
                    set
                        END_TIME = CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(9),
                        STATUS = '{{ final_status }}',
                        TARGET_INSERT_COUNT = {{ insert_count }},
                        TARGET_UPD_COUNT = {{ update_count }},
                        TARGET_DEL_COUNT = {{ delete_count }},
                        PROCESSED_COUNT = {{ processed_count }},
                        TARGET_COUNT = {{ target_count }},
                        SOURCE_COUNT = {{ source_count if ns.src_db_list | length > 0 else 'null' }},
                        SOURCE_DATABASE = {{ "'" ~ source_database ~ "'" if source_database else 'null' }},
                        SOURCE_SCHEMA = {{ "'" ~ source_schema ~ "'" if source_schema else 'null' }},
                        SOURCE_TABLE = {{ "'" ~ source_table ~ "'" if source_table else 'null' }},
                        WAREHOUSE = CURRENT_WAREHOUSE(),
                        EXECUTED_BY = CURRENT_USER()
                    where INVOCATION_ID = '{{ invocation_id }}'
                      and MODEL_NAME = '{{ model_name }}'
                      and MODEL_SCHEMA = '{{ model_schema }}'
                      and TARGET_DATABASE = '{{ model_db }}'
                      and TARGET_SCHEMA = '{{ model_schema }}'
                      and TARGET_TABLE = '{{ model_alias }}'
                      and STATUS = 'STARTED'
                {% endset %}

                {% do run_query(update_sql) %}

            {% endif %}
        {% endif %}
    {% endfor %}

{% endif %}
{% endmacro %}