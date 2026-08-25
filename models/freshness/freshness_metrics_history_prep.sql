{{
    config(
        materialized="incremental",
        unique_key="metric_id",
        on_schema_change="append_new_columns",
        tags=["anomaly_detection", "freshness"],
        full_refresh=var("allow_full_refresh_anomaly_detection", false),
    )
}}
-- depends_on: {{ ref('snap_monitored_table_metadata') }}
/*
    Freshness Metrics History (prep) — dual-source per-snapshot history.

    **Purpose:**
    Builds the per-snapshot freshness history that feeds freshness_metrics_history_final.
    One row per (object, snapshot) recording when each enrolled table was last updated.
    This model does NOT decide staleness — the percentile baseline (normal_gap_upper),
    is_stale, and is_training all live downstream in freshness_metrics_history_final.

    **Data sources (unioned into one shape):**
    1. Custom timestamp columns — MAX(timestamp_column) per snapshot, read from
       stg_freshness_metric_custom_table. source_type = 'table'. For tables that declare
       a timestamp_column on their freshness_anomaly test.
    2. Information-schema snapshots — LAST_ALTERED from snap_monitored_table_metadata.
       source_type = 'metadata'. For enrolled tables without a custom timestamp_column.

    **Enrollment:**
    Only tables enrolled via the freshness_anomaly test (scanned from graph.nodes by the
    get_enrolled_tables macro). The Jinja below selects the metadata-source tables (those
    with no timestamp_column); custom-timestamp tables arrive pre-filtered from staging.

    **Incremental:**
    Incremental on metric_id. Incremental runs read the last 7 days of snapshots; a full
    refresh reads the last 31 days. Full refresh is gated behind
    allow_full_refresh_anomaly_detection (default off) because the sub-daily
    custom-timestamp snapshots are point-in-time and cannot be reconstructed after the fact.

    **Grain & keys:**
    One row per object per snapshot.
    - metric_id  = hash(full_table_name, snapshot_timestamp, source_type, timestamp_column)
    - object_id  = hash(full_table_name, source_type, timestamp_column)
    timestamp_column is part of the identity, so switching the column used to measure
    staleness yields a new object (fresh baseline/history) rather than reusing the old one.

    **Output columns:**
    database_name, schema_name, table_name, full_table_name, table_created_at,
    table_last_altered_at, snapshot_timestamp, source_type, timestamp_column, sensitivity,
    training_period_days, metric_id, object_id, _loaded_at.
*/
{#- Use shared macro to get enrolled tables -#}
{%- set all_tables = get_enrolled_tables(["freshness_anomaly"]) -%}
{%- set metadata_tables = [] -%}
{%- for t in all_tables -%}
    {%- if not t.kwargs.get("timestamp_column") -%}
        {%- do metadata_tables.append(
            {
                "database": t.database,
                "schema": t.schema,
                "name": t.name,
                "full_name": t.full_name,
                "sensitivity": t.kwargs.get("sensitivity", "medium"),
                "training_period_days": t.kwargs.get(
                    "training_period_days", 90
                ),
            }
        ) -%}
    {%- endif -%}
{%- endfor -%}


-- Source 1: custom-timestamp tables — per-snapshot MAX(timestamp_column) from staging
with
    custom_table_metrics as (
        select
            database_name,
            schema_name,
            table_name,
            full_table_name,
            cast(null as timestamp_ntz) as table_created_at,  -- Not available for custom timestamp tables
            table_last_timestamp as table_last_altered_at,
            snapshot_timestamp,
            'table' as source_type,
            timestamp_column,
            sensitivity,
            training_period_days
        from {{ ref("stg_freshness_metric_custom_table") }}
        where
            true
            {% if is_incremental() %}
                -- On incremental runs, only query last 7 days from snapshot
                and snapshot_timestamp >= dateadd('day', -7, current_timestamp())
            {% else %}
                -- On full refresh run, only query last 31 days from snapshot
                and snapshot_timestamp >= dateadd('day', -31, current_timestamp())
            {% endif %}
    ),

    -- Source 2: metadata tables — LAST_ALTERED from the info-schema snapshot, for
    -- enrolled tables without a custom timestamp_column (up to 31-day history)
    metadata_metrics as (
        {%- if metadata_tables | length > 0 %}
            select
                -- Table identification
                upper(snap.table_catalog) as database_name,
                upper(snap.table_schema) as schema_name,
                upper(snap.table_name) as table_name,
                upper(snap.full_table_name) as full_table_name,

                -- Freshness metrics
                snap.created as table_created_at,
                -- Cap at snapshot_timestamp to prevent future timestamps from causing
                -- negative staleness
                least(
                    snap.last_altered, snap.snapshot_timestamp
                ) as table_last_altered_at,

                snap.snapshot_timestamp,
                'metadata' as source_type,
                cast(null as varchar) as timestamp_column,
                -- Sensitivity from test config
                case
                    upper(snap.full_table_name)
                    {%- for table in metadata_tables %}
                        when upper('{{ table.full_name }}')
                        then upper('{{ table.sensitivity }}')
                    {%- endfor %}
                end as sensitivity,
                -- Training window (days) from the freshness_anomaly test config
                case
                    upper(snap.full_table_name)
                    {%- for table in metadata_tables %}
                        when upper('{{ table.full_name }}')
                        then {{ table.training_period_days }}
                    {%- endfor %}
                end as training_period_days
            from {{ ref("snap_monitored_table_metadata") }} snap
            where
                true
                -- Read ALL snapshot records (including historical) for
                -- frequency-agnostic detection
                and snap.last_altered is not null
                and snap.snapshot_timestamp is not null
                {% if is_incremental() %}
                    -- On incremental runs, only query last 7 days from snapshot
                    and snap.snapshot_timestamp
                    >= dateadd('day', -7, current_timestamp())
                {% else %}
                    -- On full refresh run, only query last 31 days from snapshot
                    and snap.snapshot_timestamp
                    >= dateadd('day', -31, current_timestamp())
                {% endif %}
                -- Only include enrolled tables (case-insensitive comparison)
                and full_table_name in (
                    {%- for table in metadata_tables %}
                        upper('{{ table.full_name }}')
                        {% if not loop.last %},{% endif %}
                    {%- endfor %}
                )
        {%- else %}
            -- No metadata-enrolled tables: return an empty set with the SAME 11-column
            -- shape as the populated branch (and custom_table_metrics) so the UNION ALL
            -- stays valid. metric_id / object_id / _loaded_at are added later in
            -- `final`.
            select
                cast(null as varchar) as database_name,
                cast(null as varchar) as schema_name,
                cast(null as varchar) as table_name,
                cast(null as varchar) as full_table_name,
                cast(null as timestamp_ntz) as table_created_at,
                cast(null as timestamp_ntz) as table_last_altered_at,
                cast(null as timestamp_ntz) as snapshot_timestamp,
                cast(null as varchar) as source_type,
                cast(null as varchar) as timestamp_column,
                cast(null as varchar) as sensitivity,
                cast(null as number) as training_period_days
            where false
        {%- endif %}
    ),

    -- Union both sources
    unioned as (
        select *
        from custom_table_metrics
        union all
        select *
        from metadata_metrics
    ),
    final as (
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
                order by table_last_altered_at desc nulls last
            )
            = 1
    )
select *
from final
