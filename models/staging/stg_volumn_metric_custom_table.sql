{{
    config(
        materialized="view",
        tags=["anomaly_detection", "volume"],
    )
}}
-- different from freshness metrics, in which the historical max(timestamp_column) at
-- snapshot time needs to be saved, justifying the usage of incremental models, volume
-- metrics check row counts by buckets of time period, which is always available
-- retroactively
-- depends_on: {{ ref('snap_monitored_table_metadata') }}
{#- Use shared macro to get enrolled tables -#}
{%- set all_tables = get_enrolled_tables(["volume_anomaly"]) -%}
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
                "training_period_days": t.kwargs.get(
                    "training_period_days", 90
                ),
                "sensitivity": t.kwargs.get("sensitivity", "very_low"),
            }
        ) -%}
    {%- endif -%}
{%- endfor -%}

-- Sensitivity to MAD multiplier mapping (read from vars in dbt_project.yml).
-- The value is the number of MADs (median absolute deviations) used downstream to set
-- the anomaly band MED +/- multiplier * MAD.
-- Separate settings for row_count_change (growth rate) vs absolute row_count
{% set row_count_change_sensitivity_map = {
    "very_low": var(
        "volume_anomaly_detection.row_count_change_sensitivity_very_low",
        7.0,
    ),
    "low": var(
        "volume_anomaly_detection.row_count_change_sensitivity_low", 6.0
    ),
    "medium": var(
        "volume_anomaly_detection.row_count_change_sensitivity_medium", 5.0
    ),
    "high": var(
        "volume_anomaly_detection.row_count_change_sensitivity_high", 4.0
    ),
    "very_high": var(
        "volume_anomaly_detection.row_count_change_sensitivity_very_high",
        3.0,
    ),
} %}


-- Query tables with custom timestamp columns (hourly granularity for backfill)
with
    {%- if custom_tables | length > 0 %}
        -- Hourly spine for complete coverage
        {%- for table in custom_tables %}
            -- Base count: rows before the backfill window (single scan)
            base_count_{{ loop.index }} as (
                select count(*) as cnt
                from {{ table.database }}.{{ table.schema }}.{{ table.name }} t
                where
                    t.{{ table.timestamp_column }} is not null
                    and t.{{ table.timestamp_column }} < dateadd(
                        'day',
                        -{{ table.training_period_days }},
                        date_trunc('hour', current_timestamp())
                    )
            ),
            -- Hourly incremental counts within the backfill window
            hourly_counts_{{ loop.index }} as (
                select
                    date_trunc(
                        'hour', {{ table.timestamp_column }}
                    ) as snapshot_timestamp,
                    count(*) as hourly_count
                from {{ table.database }}.{{ table.schema }}.{{ table.name }}
                where
                    {{ table.timestamp_column }} is not null
                    and {{ table.timestamp_column }} >= dateadd(
                        'day',
                        -{{ table.training_period_days }},
                        date_trunc('hour', current_timestamp())
                    )
                group by 1
            ),

            -- Cumulative row count via running SUM (replaces correlated subquery)
            custom_table_metrics_{{ loop.index }} as (
                select
                    upper('{{ table.database }}') as database_name,
                    upper('{{ table.schema }}') as schema_name,
                    upper('{{ table.name }}') as table_name,
                    upper('{{ table.full_name }}') as full_table_name,

                    -- Cumulative row count: base count + running sum of hourly counts
                    (select cnt from base_count_{{ loop.index }}) + coalesce(
                        sum(hourly_count) over (
                            order by snapshot_timestamp
                            rows between unbounded preceding and current row
                        ),
                        0
                    ) as row_count,
                    hourly_count as row_count_change,
                    snapshot_timestamp,

                    'table' as source_type,
                    '{{ table.timestamp_column }}' as timestamp_column,
                    '{{ table.sensitivity }}' as sensitivity,
                    {{ row_count_change_sensitivity_map[table.sensitivity] }}
                    as row_count_change_mad_multiplier

                from hourly_counts_{{ loop.index }}
            ),
        {%- endfor %}

        -- Union all custom table metrics
        prep as (
            select *
            from custom_table_metrics_1
            {%- for i in range(2, custom_tables | length + 1) %}
                union all
                select *
                from custom_table_metrics_{{ i }}
            {%- endfor %}
        )

    {% else %}
        prep as (
            select
                cast(null as varchar) as database_name,
                cast(null as varchar) as schema_name,
                cast(null as varchar) as table_name,
                cast(null as varchar) as full_table_name,
                cast(null as number) as row_count,
                cast(null as number) as row_count_change,
                cast(null as timestamp_ntz) as snapshot_timestamp,
                cast(null as varchar) as source_type,
                cast(null as varchar) as timestamp_column,
                cast(null as varchar) as sensitivity,
                cast(null as number) as row_count_change_mad_multiplier
            where false
        )
    {%- endif %},
    final as (
        -- the last hour bucket has incomplete data, so to be excluded in the
        -- downstream model
        select
            *,
            row_number() over (
                partition by full_table_name, timestamp_column
                order by snapshot_timestamp desc
            ) as snapshot_nth_order
        from prep
    )
select *
from final
