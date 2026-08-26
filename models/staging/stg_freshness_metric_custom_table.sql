{{
    config(
        materialized="incremental",
        unique_key="_snapshot_id",
        full_refresh=False,
        tags=["anomaly_detection", "metadata"],
        on_schema_change="sync_all_columns",
    )
}}
-- this model periodically takes a snapshot of custom-table freshness metrics.
-- --full-refresh mode is disabled here to avoid loss of data
{%- set all_tables = get_enrolled_tables(["freshness_anomaly"]) -%}
{%- set custom_tables = [] -%}
{%- for t in all_tables -%}
    {%- if t.kwargs.get("timestamp_column") -%}
        {%- do custom_tables.append(
            {
                "database": t.database,
                "schema": t.schema,
                "name": t.name,
                "full_name": t.full_name,
                "timestamp_column": t.kwargs.get("timestamp_column"),
                "timezone": t.kwargs.get("timezone", "America/Los_Angeles"),
                "sensitivity": t.kwargs.get("sensitivity", "medium"),
                "training_period_days": t.kwargs.get(
                    "training_period_days", 90
                ),
            }
        ) -%}
    {%- endif -%}
{%- endfor %}

with
    {%- if custom_tables | length > 0 %}
        {%- for table in custom_tables %}
            -- First, get max timestamp currently and consider this
            -- table_last_timestamp
            hourly_data_{{ loop.index }} as (
                select
                    date_trunc('hour', current_timestamp())::timestamp_ntz
                    as snapshot_timestamp,
                    {%- if table.timezone != "America/Los_Angeles" %}
                        max(
                            convert_timezone(
                                '{{ table.timezone }}',
                                'America/Los_Angeles',
                                {{ table.timestamp_column }}
                            )
                        )::timestamp_ntz
                    {%- else %} max({{ table.timestamp_column }})::timestamp_ntz
                    {%- endif %} as table_last_timestamp
                from {{ table.database }}.{{ table.schema }}.{{ table.name }}
            ),

            -- Calculate freshness metrics
            metrics_{{ loop.index }} as (
                select
                    upper('{{ table.database }}') as database_name,
                    upper('{{ table.schema }}') as schema_name,
                    upper('{{ table.name }}') as table_name,
                    upper('{{ table.full_name }}') as full_table_name,
                    snapshot_timestamp,
                    table_last_timestamp,
                    upper('{{ table.sensitivity }}') as sensitivity,
                    '{{ table.timestamp_column }}' as timestamp_column,
                    {{ table.training_period_days }} as training_period_days
                from hourly_data_{{ loop.index }}
            ),
        {%- endfor %}

        -- Union all custom table metrics
        final as (
            select *
            from metrics_1
            {%- for i in range(2, custom_tables | length + 1) %}
                union all
                select *
                from metrics_{{ i }}
            {%- endfor %}
        )
    {% else %}
        -- No custom-timestamp enrolled tables: return an empty, schema-compatible set
        final as (
            select
                cast(null as varchar) as database_name,
                cast(null as varchar) as schema_name,
                cast(null as varchar) as table_name,
                cast(null as varchar) as full_table_name,
                cast(null as timestamp_ntz) as snapshot_timestamp,
                cast(null as timestamp_ntz) as table_last_timestamp,
                cast(null as varchar) as sensitivity,
                cast(null as varchar) as timestamp_column,
                cast(null as number) as training_period_days
            where false
        )
    {%- endif %}

select
    {{
        dbt_utils.generate_surrogate_key(
            [
                "full_table_name",
                "current_timestamp()",
            ]
        )
    }} as _snapshot_id, *
from final
