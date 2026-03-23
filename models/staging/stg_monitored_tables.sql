{{
    config(
        materialized="table",
        tags=["anomaly_detection", "metadata"],
        full_refresh=(
            false if not var("allow_full_refresh_anomaly_detection", false) else none
        ),
    )
}}

/*
    Staging: Monitored Tables

    Extracts the list of tables and sources enrolled in anomaly detection by scanning:
    - graph.nodes (dbt models)
    - graph.sources (source tables from external databases)

    Finds all tables with volume_anomaly or freshness_anomaly tests via the shared
    get_enrolled_tables macro.

    **Cross-Database Support:**
    Fully supports source tables from external databases (e.g., FIVETRAN_SALESFORCE_DB,
    FIVETRAN_HEAP_DB). Each source's database property is captured and used to query
    the correct INFORMATION_SCHEMA.

    **Full Refresh Protection:**
    This model ignores --full-refresh flag by default to prevent accidental rebuilds.
    Override: dbt run --select stg_monitored_tables --vars '{allow_full_refresh_anomaly_detection: true}'

    This model serves as the enrollment registry - only tables appearing here will be
    tracked by the snap_monitored_table_metadata snapshot.
*/
{%- set all_tables = get_enrolled_tables(["volume_anomaly", "freshness_anomaly"]) -%}
{%- set monitored_tables = [] -%}
{%- for t in all_tables -%}
    {%- set table_key = {
        "database": t.database,
        "schema": t.schema,
        "name": t.name,
        "full_name": t.full_name,
    } -%}
    {%- if table_key not in monitored_tables -%}
        {%- do monitored_tables.append(table_key) -%}
    {%- endif -%}
{%- endfor -%}

{%- if monitored_tables | length > 0 %}
    {%- for table in monitored_tables %}
        {% if loop.first %} select
        {% else %}
            union all
            select
        {% endif %}
            '{{ table.database | upper }}' as database_name,
            '{{ table.schema | upper }}' as schema_name,
            '{{ table.name | upper }}' as table_name,
            upper('{{ table.full_name }}') as full_table_name,
            current_timestamp() as _loaded_at
    {%- endfor %}
{%- else %}
    -- No monitored tables found, return empty result
    select
        cast(null as varchar) as database_name,
        cast(null as varchar) as schema_name,
        cast(null as varchar) as table_name,
        cast(null as varchar) as full_table_name,
        cast(null as timestamp_ntz) as _loaded_at
    where false
{%- endif %}
