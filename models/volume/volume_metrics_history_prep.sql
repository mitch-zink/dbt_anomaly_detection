{{
    config(
        materialized="table",
        tags=["anomaly_detection", "volume"],
    )
}}
-- different from freshness metrics, in which the historical max(timestamp_column) at
-- snapshot time needs to be saved, justifying the usage of incremental models, volumn
-- metrics check row counts by buckets of time period, which is always available
-- depends_on: {{ ref('snap_monitored_table_metadata') }}
{#- Use shared macro to get enrolled tables -#}
{%- set all_tables = get_enrolled_tables(["volume_anomaly"]) -%}
{%- set metadata_tables = [] -%}
{%- for t in all_tables -%}
    {%- if not t.kwargs.get("timestamp_column") -%}
        {%- do metadata_tables.append(
            {
                "database": t.database,
                "schema": t.schema,
                "name": t.name,
                "full_name": t.full_name,
                "sensitivity": t.kwargs.get("sensitivity", "very_low"),
                "training_period_days": t.kwargs.get(
                    "training_period_days", 90
                ),
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


with
    custom_table_metrics as (
        select
            database_name,
            schema_name,
            table_name,
            full_table_name,
            row_count,
            snapshot_timestamp,
            source_type,
            timestamp_column,
            sensitivity,
            row_count_change_mad_multiplier
        from {{ ref("stg_volumn_metric_custom_table") }}
        where snapshot_nth_order > 1
    ),
    -- Query information_schema for enrolled tables (those with volume tests but no
    -- timestamp_column)
    metadata_metrics as (
        {%- if metadata_tables | length > 0 %}
            select
                -- Table identification
                upper(t.table_catalog) as database_name,
                upper(t.table_schema) as schema_name,
                upper(t.table_name) as table_name,
                upper(t.full_table_name) as full_table_name,

                -- Volume metric
                t.row_count,

                t.snapshot_timestamp,  -- Hourly snapshot timestamp
                'metadata' as source_type,
                cast(null as varchar) as timestamp_column,

                -- Sensitivity from test config
                case
                    upper(t.full_table_name)
                    {%- for table in metadata_tables %}
                        when upper('{{ table.full_name }}')
                        then '{{ table.sensitivity }}'
                    {%- endfor %}
                end as sensitivity,

                case
                    upper(t.full_table_name)
                    {%- for table in metadata_tables %}
                        when upper('{{ table.full_name }}')
                        then {{ row_count_change_sensitivity_map[table.sensitivity] }}
                    {%- endfor %}
                end as row_count_change_mad_multiplier,

            from {{ ref("snap_monitored_table_metadata") }} t
            where
                true
                and t.table_type = 'BASE TABLE'
                and t.row_count is not null
                and t.snapshot_timestamp is not null
                -- Keep only snapshots within each enrolled table's training window
                -- (per-table training_period_days from the volume_anomaly test config)
                and t.snapshot_timestamp >= dateadd(
                    'day',
                    -1 * (
                        case
                            upper(t.full_table_name)
                            {%- for table in metadata_tables %}
                                when upper('{{ table.full_name }}')
                                then {{ table.training_period_days }}
                            {%- endfor %}
                        end
                    ),
                    current_timestamp()
                )
                -- Only include enrolled tables (case-insensitive comparison)
                and upper(t.full_table_name) in (
                    {%- for table in metadata_tables %}
                        upper('{{ table.full_name }}'){% if not loop.last %},{% endif %}
                    {%- endfor %}
                )
        {%- else %}
            -- No metadata-enrolled tables, return empty result set
            select
                cast(null as varchar) as database_name,
                cast(null as varchar) as schema_name,
                cast(null as varchar) as table_name,
                cast(null as varchar) as full_table_name,
                cast(null as number) as row_count,
                cast(null as timestamp_ntz) as snapshot_timestamp,
                cast(null as varchar) as source_type,
                cast(null as varchar) as timestamp_column,
                cast(null as varchar) as sensitivity,
                cast(null as number) as row_count_change_mad_multiplier
            where false
        {%- endif %}
    ),

    -- Union both sources. custom_table_metrics always exists (it reads the staging
    -- table, which returns an empty set when there are no custom-timestamp tables), so
    -- the union is unconditional.
    unioned as (
        select *
        from custom_table_metrics
        union all
        select *
        from metadata_metrics
    ),
    added_id as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "full_table_name",
                        "snapshot_timestamp",
                        "source_type",
                        "timestamp_column",
                    ]
                )
            }} as metric_id,
            -- timestamp_column is part of the object identity: switching which column
            -- measures staleness redefines the metric, so it must yield a new object_id
            -- (fresh baseline/history) rather than reusing the old one.
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "full_table_name",
                        "source_type",
                        "timestamp_column",
                    ]
                )
            }} as object_id,
            current_timestamp() as _loaded_at,
            *
        from unioned
        qualify
            row_number() over (
                partition by
                    full_table_name, source_type, timestamp_column, snapshot_timestamp
                -- Dedup duplicate snapshots of the same hour; keep the highest observed
                -- row_count (there is no table_last_altered_at at the volume grain).
                order by row_count desc
            )
            = 1
    ),
    final as (
        select
            *,

            -- Change vs the previous snapshot for this table (new rows since the
            -- last observation; may be negative if rows were deleted). Metadata
            -- row_count is cumulative, so the period delta is a lag difference
            -- 0 row_count_change is converted to null because we exclude 0
            -- row_count_change rows in calculating MED/MAD
            nullif(
                row_count - lag(row_count) over (
                    partition by object_id order by snapshot_timestamp
                ),
                0
            ) as row_count_change
        from added_id
    )
select *
from final
