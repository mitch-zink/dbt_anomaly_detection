{{
    config(
        materialized="incremental",
        unique_key="metric_id",
        on_schema_change="sync_all_columns",
        tags=["anomaly_detection", "freshness"],
    )
}}

-- depends_on: {{ ref('snap_monitored_table_metadata') }}
/*
    Freshness Metrics History - Dual Source Architecture

    **Purpose:**
    Tracks data staleness (time since last update) for enrolled tables and detects freshness anomalies.

    **Full Refresh:**
    This model supports --full-refresh for iterative development and sensitivity tuning.
    The snapshot (snap_monitored_table_metadata) is protected separately and preserves all history.

    **Data Sources:**
    1. **Custom timestamp columns** - MAX(timestamp_column) from table for instant 30-day backfill
    2. **Information schema snapshots** - LAST_ALTERED metadata from snap_monitored_table_metadata

    **Enrollment:**
    - Only processes tables enrolled via freshness_anomaly test in schema.yml
    - Enrollment determined by scanning graph.nodes (compiled at runtime via stg_monitored_tables)

    **Detection Logic:**
    - Calculates hours_since_last_update (staleness metric)
    - Rolling 28-day baseline statistics (mean + stddev)
    - Upper bound only: Flags if staleness > (mean + σ × stddev)
    - No lower bound (tables being "too fresh" is not a problem)

    **Output:**
    - Unified table with source_type to distinguish between timestamp and metadata sources
    - Boolean is_stale flag when data is staler than expected
    - Sensitivity levels control sigma multipliers (very_low=3σ recommended)
*/
{%- set custom_tables = [] -%}
{%- set metadata_tables = [] -%}
{%- if execute -%}
    {%- for node in graph.nodes.values() -%}
        {%- if node.resource_type == "test" -%}
            {%- if node.test_metadata and node.test_metadata.name == "freshness_anomaly" -%}
                {%- set test_kwargs = node.test_metadata.kwargs -%}
                {%- if node.depends_on.nodes -%}
                    {%- for dep_node_id in node.depends_on.nodes -%}
                        {%- if dep_node_id.startswith(
                            "model."
                        ) or dep_node_id.startswith("source.") -%}
                            {# Check graph.sources for source tables, graph.nodes for models #}
                            {%- if dep_node_id.startswith("source.") -%}
                                {%- set ref_node = graph.sources.get(dep_node_id) -%}
                            {%- else -%}
                                {%- set ref_node = graph.nodes.get(dep_node_id) -%}
                            {%- endif -%}
                            {%- if ref_node -%}
                                {%- set table_info = {
                                    "database": ref_node.database,
                                    "schema": ref_node.schema,
                                    "name": (
                                        ref_node.name
                                        if ref_node.resource_type == "model"
                                        else ref_node.identifier
                                    ),
                                    "full_name": ref_node.database
                                    ~ "."
                                    ~ ref_node.schema
                                    ~ "."
                                    ~ (
                                        ref_node.name
                                        if ref_node.resource_type == "model"
                                        else ref_node.identifier
                                    ),
                                } -%}

                                {%- if table_info.name not in [
                                    "volume_metrics_history",
                                    "freshness_metrics_history",
                                ] -%}
                                    {%- if test_kwargs.get("timestamp_column") -%}
                                        {# Custom timestamp column - add to custom_tables #}
                                        {%- do custom_tables.append(
                                            {
                                                "database": table_info.database,
                                                "schema": table_info.schema,
                                                "name": table_info.name,
                                                "full_name": table_info.full_name,
                                                "timestamp_column": test_kwargs.get(
                                                    "timestamp_column"
                                                ),
                                                "timezone": test_kwargs.get(
                                                    "timezone", "UTC"
                                                ),
                                                "training_period_days": test_kwargs.get(
                                                    "training_period_days",
                                                    90,
                                                ),
                                                "sensitivity": test_kwargs.get(
                                                    "sensitivity", "very_low"
                                                ),
                                            }
                                        ) -%}
                                    {%- else -%}
                                        {# No timestamp column - add to metadata_tables #}
                                        {%- do metadata_tables.append(
                                            {
                                                "database": table_info.database,
                                                "schema": table_info.schema,
                                                "name": table_info.name,
                                                "full_name": table_info.full_name,
                                                "sensitivity": test_kwargs.get(
                                                    "sensitivity", "very_low"
                                                ),
                                            }
                                        ) -%}
                                    {%- endif -%}
                                {%- endif -%}
                            {%- endif -%}
                        {%- endif -%}
                    {%- endfor -%}
                {%- endif -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

-- Sensitivity to sigma multiplier mapping (read from vars in dbt_project.yml)
-- Freshness uses different thresholds than volume because staleness patterns are more
-- stable
-- Universal defaults balanced for production use - catch abandoned tables without
-- excessive noise
-- Override in your project's dbt_project.yml under freshness_anomaly_detection vars
{%- set sensitivity_map = {
    "very_low": var(
        "freshness_anomaly_detection.sensitivity_very_low", 10.0
    ),
    "low": var("freshness_anomaly_detection.sensitivity_low", 6.0),
    "medium": var("freshness_anomaly_detection.sensitivity_medium", 4.0),
    "high": var("freshness_anomaly_detection.sensitivity_high", 2.5),
    "very_high": var(
        "freshness_anomaly_detection.sensitivity_very_high", 1.5
    ),
} -%}

-- Generate hourly spine and query tables with custom timestamp columns
with
    {%- if custom_tables | length > 0 %}
        hourly_spine as (
            select
                dateadd(
                    'hour',
                    seq4(),
                    dateadd('day', -30, date_trunc('hour', current_timestamp()))
                ) as snapshot_timestamp
            from table(generator(rowcount => 30 * 24))  -- 30 days * 24 hours (matches rolling window period)
        ),

        {%- for table in custom_tables %}
            -- First, get max timestamp for each hour where data exists
            hourly_data_{{ loop.index }} as (
                select
                    date_trunc(
                        'hour',
                        {%- if table.timezone != "UTC" %}
                            convert_timezone(
                                '{{ table.timezone }}',
                                'UTC',
                                {{ table.timestamp_column }}
                            )
                        {%- else %} {{ table.timestamp_column }}
                        {%- endif %}
                    ) as data_hour,
                    {%- if table.timezone != "UTC" %}
                        max(
                            convert_timezone(
                                '{{ table.timezone }}',
                                'UTC',
                                {{ table.timestamp_column }}
                            )
                        ) as max_timestamp
                    {%- else %} max({{ table.timestamp_column }}) as max_timestamp
                    {%- endif %}
                from {{ table.database }}.{{ table.schema }}.{{ table.name }}
                where
                    {{ table.timestamp_column }} is not null
                    and {{ table.timestamp_column }} >= dateadd(
                        'day', -{{ table.training_period_days }}, current_timestamp()
                    )
                group by 1
            ),

            -- Join to spine and forward-fill last known timestamp
            filled_{{ loop.index }} as (
                select
                    spine.snapshot_timestamp,
                    -- Cap table_last_altered_at at snapshot_timestamp to prevent
                    -- future timestamps
                    -- from causing negative staleness calculations
                    least(
                        max(data.max_timestamp) over (
                            order by spine.snapshot_timestamp
                            rows between unbounded preceding and current row
                        ),
                        spine.snapshot_timestamp
                    ) as table_last_altered_at
                from hourly_spine spine
                left join
                    hourly_data_{{ loop.index }} data
                    on spine.snapshot_timestamp = data.data_hour
                where spine.snapshot_timestamp <= current_timestamp()
            ),

            -- Calculate freshness metrics
            metrics_{{ loop.index }} as (
                select
                    '{{ table.database }}' as database_name,
                    '{{ table.schema }}' as schema_name,
                    '{{ table.name }}' as table_name,
                    '{{ table.full_name }}' as full_table_name,
                    cast(null as timestamp_ntz) as table_created_at,  -- Not available for custom timestamp tables
                    table_last_altered_at,
                    snapshot_timestamp,
                    'table' as source_type,
                    '{{ table.timestamp_column }}' as timestamp_column,

                    -- Staleness = hours since table was last updated (as of this
                    -- snapshot hour). Use GREATEST to handle cases where data arrives
                    -- within the same hour as snapshot.
                    greatest(
                        0, datediff('hour', table_last_altered_at, snapshot_timestamp)
                    ) as hours_since_last_update,
                    greatest(
                        0, datediff('minute', table_last_altered_at, snapshot_timestamp)
                    ) as minutes_since_last_update,

                    '{{ table.sensitivity }}' as sensitivity,
                    cast(
                        {{ sensitivity_map[table.sensitivity] }} as decimal(5, 2)
                    ) as sigma_multiplier,

                    current_timestamp() as _loaded_at
                from filled_{{ loop.index }}
                where table_last_altered_at is not null  -- Only after first data arrives
            ),
        {%- endfor %}

        -- Union all custom table metrics
        custom_table_metrics as (
            select *
            from metrics_1
            {%- for i in range(2, custom_tables | length + 1) %}
                union all
                select *
                from metrics_{{ i }}
            {%- endfor %}
        ),

    {% else %}
    {%- endif %}
    -- Query snapshot for enrolled tables (those with freshness tests but no
    -- timestamp_column) - provides 30-day history even on full-refresh
    metadata_metrics as (
        {%- if metadata_tables | length > 0 %}
            select
                -- Table identification
                snap.table_catalog as database_name,
                snap.table_schema as schema_name,
                snap.table_name,
                snap.table_catalog
                || '.'
                || snap.table_schema
                || '.'
                || snap.table_name as full_table_name,

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

                -- Staleness metrics
                datediff(
                    'hour',
                    least(snap.last_altered, snap.snapshot_timestamp),
                    snap.snapshot_timestamp
                ) as hours_since_last_update,
                datediff(
                    'minute',
                    least(snap.last_altered, snap.snapshot_timestamp),
                    snap.snapshot_timestamp
                ) as minutes_since_last_update,

                -- Sensitivity from test config
                case
                    upper(snap.full_table_name)
                    {%- for table in metadata_tables %}
                        when upper('{{ table.full_name }}')
                        then '{{ table.sensitivity }}'
                    {%- endfor %}
                end as sensitivity,

                case
                    upper(snap.full_table_name)
                    {%- for table in metadata_tables %}
                        when upper('{{ table.full_name }}')
                        then {{ sensitivity_map[table.sensitivity] }}
                    {%- endfor %}
                end as sigma_multiplier,

                current_timestamp() as _loaded_at

            from {{ ref("snap_monitored_table_metadata") }} snap
            where
                true
                -- Read ALL snapshot records (including historical) for
                -- frequency-agnostic detection
                and snap.last_altered is not null
                and snap.snapshot_timestamp is not null
                {% if is_incremental() %}
                    -- On incremental runs, only query last 30 days from snapshot
                    and snap.snapshot_timestamp
                    >= dateadd('day', -30, current_timestamp())
                {% endif %}
                -- Only include enrolled tables (case-insensitive comparison)
                and upper(
                    snap.table_catalog
                    || '.'
                    || snap.table_schema
                    || '.'
                    || snap.table_name
                ) in (
                    {%- for table in metadata_tables %}
                        upper('{{ table.full_name }}'){% if not loop.last %},{% endif %}
                    {%- endfor %}
                )
            -- Deduplicate snapshot source in case of duplicate snapshot_timestamp
            -- timestamps
            qualify
                row_number() over (
                    partition by
                        snap.table_catalog,
                        snap.table_schema,
                        snap.table_name,
                        snap.snapshot_timestamp
                    order by snap.dbt_updated_at desc nulls last
                )
                = 1
        {%- else %}
            -- No metadata-enrolled tables, return empty result set
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
                cast(null as number) as hours_since_last_update,
                cast(null as number) as minutes_since_last_update,
                cast(null as varchar) as sensitivity,
                cast(null as decimal(5, 2)) as sigma_multiplier,
                cast(null as timestamp_ntz) as _loaded_at
            where false
        {%- endif %}
    ),

    -- Union both sources
    unioned as (
        {%- if custom_tables | length > 0 %}
            select *
            from custom_table_metrics
            union all
        {%- endif %}
        select *
        from metadata_metrics
    ),

    -- Include historical data for metadata mode to enable proper window function
    -- calculations
    -- IMPORTANT: Always include historical snapshot data (even on full-refresh)
    -- to ensure rolling window statistics have sufficient observations
    with_history as (
        {% if is_incremental() %}
            -- Incremental: Load from existing table
            select
                database_name,
                schema_name,
                table_name,
                full_table_name,
                table_created_at,
                table_last_altered_at,
                snapshot_timestamp,
                source_type,
                timestamp_column,
                hours_since_last_update,
                minutes_since_last_update,
                sensitivity,
                sigma_multiplier,
                _loaded_at
            from {{ this }}
            where snapshot_timestamp >= dateadd('day', -30, current_timestamp())
            union all
        {% else %}
            -- Full-refresh: Load from snapshot to preserve historical context
            -- Without this, rolling windows would only see current run's data
            select
                snap.table_catalog as database_name,
                snap.table_schema as schema_name,
                snap.table_name,
                snap.full_table_name,
                snap.created as table_created_at,
                snap.last_altered as table_last_altered_at,
                snap.snapshot_timestamp,
                'metadata' as source_type,
                cast(null as varchar) as timestamp_column,
                greatest(
                    0, datediff('hour', snap.last_altered, snap.snapshot_timestamp)
                ) as hours_since_last_update,
                greatest(
                    0, datediff('minute', snap.last_altered, snap.snapshot_timestamp)
                ) as minutes_since_last_update,
                -- Map sensitivity from test configuration
                case
                    upper(snap.full_table_name)
                    {%- for table in metadata_tables %}
                        when upper('{{ table.full_name }}')
                        then '{{ table.sensitivity }}'
                    {%- endfor %}
                end as sensitivity,
                -- Map sigma multiplier from sensitivity_map
                case
                    upper(snap.full_table_name)
                    {%- for table in metadata_tables %}
                        when upper('{{ table.full_name }}')
                        then {{ sensitivity_map[table.sensitivity] }}
                    {%- endfor %}
                end as sigma_multiplier,
                snap.snapshot_timestamp as _loaded_at
            from {{ ref("snap_monitored_table_metadata") }} as snap
            inner join
                {{ ref("stg_monitored_tables") }} as mt
                on snap.full_table_name = mt.full_table_name
            where
                snap.dbt_valid_to is null
                and snap.snapshot_timestamp >= dateadd('day', -30, current_timestamp())
                -- Exclude current hour to prevent duplicates with unioned CTE
                and snap.snapshot_timestamp < date_trunc('hour', current_timestamp())
                -- Only include enrolled tables (case-insensitive comparison)
                and upper(snap.full_table_name) in (
                    {%- for table in metadata_tables %}
                        upper('{{ table.full_name }}'){% if not loop.last %},{% endif %}
                    {%- endfor %}
                )
            union all
        {% endif %}
        select *
        from unioned
    ),

    -- Calculate anomaly detection metrics using rolling window
    anomaly_detection as (
        select
            database_name,
            schema_name,
            table_name,
            full_table_name,
            table_created_at,
            table_last_altered_at,
            hours_since_last_update,
            minutes_since_last_update,
            snapshot_timestamp,
            source_type,
            timestamp_column,
            sensitivity,
            sigma_multiplier,
            _loaded_at,

            -- Calculate rolling statistics (28-day time window)
            -- Uses INTERVAL '1 second' PRECEDING to exclude current row from baseline
            -- calculation
            -- (equivalent to ROWS BETWEEN ... AND 1 PRECEDING in row-based windows)
            avg(hours_since_last_update) over (
                partition by full_table_name, source_type
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as expected_staleness_mean,

            stddev(hours_since_last_update) over (
                partition by full_table_name, source_type
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as expected_staleness_stddev,

            count(*) over (
                partition by full_table_name, source_type
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as observation_count

        from with_history
    ),

    -- Add metric_id and anomaly flags
    final as (
        select
            -- Identifiers
            database_name,
            schema_name,
            table_name,
            full_table_name,
            source_type,
            timestamp_column,
            snapshot_timestamp,

            -- Metrics
            table_created_at,
            table_last_altered_at,
            hours_since_last_update,
            minutes_since_last_update,

            -- Statistical baselines
            round(expected_staleness_mean, 0) as expected_staleness_mean,
            round(expected_staleness_stddev, 0) as expected_staleness_stddev,
            round(
                expected_staleness_mean
                - (sigma_multiplier * expected_staleness_stddev),
                0
            ) as expected_min_hours,
            round(
                expected_staleness_mean
                + (sigma_multiplier * expected_staleness_stddev),
                0
            ) as expected_max_hours,

            observation_count,

            -- Anomaly detection flag (upper bound only - too stale)
            case
                -- Not enough historical data
                when
                    observation_count
                    < {{
                        var(
                            "freshness_anomaly_detection.min_historical_observations",
                            7,
                        )
                    }}
                then false
                -- No variation in staleness (perfectly consistent updates)
                when expected_staleness_stddev = 0 or expected_staleness_stddev is null
                then false
                -- Insufficient variation to detect anomalies (< 1 hour stddev)
                when
                    expected_staleness_stddev
                    < {{ var("freshness_anomaly_detection.min_stddev_hours", 1.0) }}
                then false
                -- Below minimum absolute staleness threshold (filter out normal
                -- refresh lag)
                when
                    hours_since_last_update
                    < {{ var("freshness_anomaly_detection.min_staleness_hours", 6) }}
                then false
                -- Stale beyond expected upper bound
                when
                    hours_since_last_update > (
                        expected_staleness_mean
                        + (sigma_multiplier * expected_staleness_stddev)
                    )
                then true
                else false
            end as is_stale,

            -- Configuration
            sensitivity,
            sigma_multiplier,

            -- System columns
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "database_name",
                        "schema_name",
                        "table_name",
                        "snapshot_timestamp",
                        "source_type",
                    ]
                )
            }} as metric_id,
            _loaded_at

        from anomaly_detection
    )

select *
from final

{% if is_incremental() %}
    -- On incremental runs, only return new snapshots (filter out historical data used
    -- for window calculations)
    where snapshot_timestamp > (select max(snapshot_timestamp) from {{ this }})
{% endif %}

-- Deduplicate in case of overlapping data (e.g., full-refresh loading historical +
-- new)
qualify row_number() over (partition by metric_id order by _loaded_at desc) = 1
