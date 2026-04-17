{% macro send_failure_alert() %}

  {% if execute %}

    {% set ns = namespace(failed_models=[], model_paths=[], error_messages=[]) %}

    {% for result in results %}
      {% if result.status == 'error' %}
        {% do ns.failed_models.append(result.node.name) %}
        {% set model_path = result.node.original_file_path.split('/')[1:-1] | join(' -- ') %}
        {% do ns.model_paths.append(model_path) %}
        {% set error_msg = (result.message | string)[:200] + '...' if (result.message | string | length) > 200 else (result.message | string) %}
        {% do ns.error_messages.append(result.node.name ~ ': ' ~ error_msg) %}
      {% endif %}
    {% endfor %}

    {% if ns.failed_models | length > 0 %}

      {% set failed_list = ns.failed_models | join(', ') %}
      {% set fail_count = ns.failed_models | length %}
      {% set path_list = ns.model_paths | unique | join(', ') %}

      {% set email_subject = target.database ~ '_' ~ target.name ~ '_' ~ target.schema ~ '_' ~ failed_list ~ '_' ~ run_started_at.strftime('%m-%d-%Y') %}

      {% set email_body = 'Dear Team,\\n\\n'
        ~ 'This is an automated notification to inform you that the DBT pipeline has encountered failures during execution. Please review the details below and take appropriate action.\\n\\n'
        ~ 'FAILURE SUMMARY:\\n'
        ~ '   • Environment: ' ~ target.name ~ '\\n'
        ~ '   • Database: ' ~ target.database ~ '\\n'
        ~ '   • Schema: ' ~ target.schema ~ '\\n'
        ~ '   • Failed Models (' ~ fail_count ~ '): ' ~ failed_list ~ '\\n'
        ~ '   • Execution Time: ' ~ run_started_at.strftime('%Y-%m-%d %H:%M:%S UTC') ~ '\\n\\n'
        ~ 'ERROR DETAILS:\\n'
        ~ (ns.error_messages | join('\\n')) ~ '\\n\\n'
        ~ 'Next Steps:\\n'
        ~ '   1. Check dbt logs for detailed error messages\\n'
        ~ '   2. Review model SQL and dependencies\\n'
        ~ '   3. Fix issues and re-run the pipeline\\n\\n'
        ~ 'Do not reply to this email - it is sent from an unmonitored address\\n\\n'
        ~ 'Regards,\\n'
        ~ 'Snowflake AMS Team\\n'
      %}

      {% set safe_email_body = email_body | replace("'", "''") %}
      {% set alert_query = "CALL SYSTEM$SEND_EMAIL('dbt_failure_email', 'scheemakurthi@enquotech.com', '" ~ email_subject ~ "', '" ~ safe_email_body ~ "');" %}

      {% do run_query(alert_query) %}

      {{ log("ALERT: Failure email sent for models: " ~ failed_list, info=true) }}

    {% else %}
      {{ log("All models ran successfully. No alert needed.", info=true) }}
    {% endif %}

  {% endif %}

{% endmacro %}