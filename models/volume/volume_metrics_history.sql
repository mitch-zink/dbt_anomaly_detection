{{
    config(
        materialized="incremental",
        unique_key="metric_id",
        on_schema_change="append_new_columns",
        tags=["anomaly_detection", "volume"],
    )
}}

-- depends_on: {{ ref('snap_monitored_table_metadata') }}
/*
    Volume Metrics History - Dual Detection Architecture

    **Purpose:**
    Tracks row count metrics over time for enrolled tables and detects volume anomalies using
    statistical analysis combined with materiality filters.

    **Full Refresh:**
    This model supports --full-refresh for iterative development and sensitivity tuning.
    The snapshot (snap_monitored_table_metadata) is protected separately and preserves all history.

    **Data Sources:**
    1. **Custom timestamp columns** - Direct table queries for instant 30-day backfill (hourly granularity)
    2. **Information schema snapshots** - row_count metadata from snap_monitored_table_metadata

    **Enrollment:**
    - Only processes tables enrolled via volume_anomaly test in schema.yml
    - Enrollment determined by scanning graph.nodes (compiled at runtime via get_enrolled_tables macro)

    **Detection Logic:**
    Two-stage filtering (Statistical + Materiality):

    1. **Change-based detection** - Monitors growth rate (row_count_change):
       - Statistical: change > mean + (σ × stddev) OR change < mean - (σ × stddev)
       - Materiality: configurable absolute + relative thresholds (default >1,000 rows AND >200%)
       - Output: 'Row Count Change - Spike' or 'Row Count Change - Dip'

    2. **Size-based detection** - Monitors absolute row count:
       - Statistical: count > mean + (σ × stddev) OR count < mean - (σ × stddev)
       - Materiality: configurable absolute + relative thresholds (default >100 rows AND >5%)
       - Output: 'Row Count - Spike' or 'Row Count - Dip'

    **Output:**
    - 4 boolean flags: is_spike_high, is_drop_low, is_row_count_anomaly, is_anomaly
    - is_anomaly is guaranteed consistent: simply (is_spike_high OR is_drop_low OR is_row_count_anomaly)
    - 3 reason columns: row_count_change_anomaly_reason, row_count_anomaly_reason, anomaly_reason
    - Rolling 28-day window statistics for dynamic baseline calculation
*/
{#- Use shared macro to get enrolled tables -#}
{%- set all_tables = get_enrolled_tables(["volume_anomaly"]) -%}
{%- set custom_tables = [] -%}
{%- set metadata_tables = [] -%}
{%- for t in all_tables -%}
    {%- if t.kwargs.get("timestamp_column") -%}
        {%- do custom_tables.append(
            {
                "database": t.database,
                "schema": t.schema,
                "name": t.name,
                "full_name": t.full_name,
                "timestamp_column": t.kwargs.get("timestamp_column"),
                "timezone": t.kwargs.get("timezone", "UTC"),
                "training_period_days": t.kwargs.get(
                    "training_period_days", 30
                ),
                "sensitivity": t.kwargs.get("sensitivity", "very_low"),
            }
        ) -%}
    {%- else -%}
        {%- do metadata_tables.append(
            {
                "database": t.database,
                "schema": t.schema,
                "name": t.name,
                "full_name": t.full_name,
                "sensitivity": t.kwargs.get("sensitivity", "very_low"),
            }
        ) -%}
    {%- endif -%}
{%- endfor -%}

-- Sensitivity to sigma multiplier mapping (read from vars in dbt_project.yml)
-- Separate settings for row_count_change (growth rate) vs absolute row_count
{%- set row_count_change_sensitivity_map = {
    "very_low": var(
        "volume_anomaly_detection.row_count_change_sensitivity_very_low",
        20.0,
    ),
    "low": var(
        "volume_anomaly_detection.row_count_change_sensitivity_low", 12.0
    ),
    "medium": var(
        "volume_anomaly_detection.row_count_change_sensitivity_medium", 8.0
    ),
    "high": var(
        "volume_anomaly_detection.row_count_change_sensitivity_high", 6.0
    ),
    "very_high": var(
        "volume_anomaly_detection.row_count_change_sensitivity_very_high",
        5.0,
    ),
} -%}

{%- set row_count_sensitivity_map = {
    "very_low": var(
        "volume_anomaly_detection.row_count_sensitivity_very_low", 16.0
    ),
    "low": var("volume_anomaly_detection.row_count_sensitivity_low", 8.0),
    "medium": var(
        "volume_anomaly_detection.row_count_sensitivity_medium", 4.0
    ),
    "high": var("volume_anomaly_detection.row_count_sensitivity_high", 2.0),
    "very_high": var(
        "volume_anomaly_detection.row_count_sensitivity_very_high", 1.0
    ),
} -%}

-- Materiality thresholds (configurable via vars)
{%- set mat_change_rows = var(
    "volume_anomaly_detection.materiality_change_rows", 1000
) -%}
{%- set mat_change_pct = var("volume_anomaly_detection.materiality_change_pct", 2.0) -%}
{%- set mat_count_rows = var("volume_anomaly_detection.materiality_count_rows", 100) -%}
{%- set mat_count_pct = var("volume_anomaly_detection.materiality_count_pct", 0.05) -%}
{%- set min_obs = var("volume_anomaly_detection.min_historical_observations", 7) -%}
{%- set seasonality_enabled = var('seasonality_enabled', true) -%}
{%- set min_seasonal_obs = var('min_seasonal_observations', 4) -%}

-- Query tables with custom timestamp columns (hourly granularity for backfill)
with
    {%- if custom_tables | length > 0 %}
        -- Hourly spine for complete coverage
        hourly_spine as (
            select
                dateadd(
                    'hour',
                    seq4(),
                    dateadd('day', -30, date_trunc('hour', current_timestamp()))
                )::timestamp_ntz as snapshot_timestamp
            from table(generator(rowcount => 30 * 24))  -- 30 days * 24 hours
        ),

        {%- for table in custom_tables %}
            -- Base count: rows before the backfill window (single scan)
            base_count_{{ loop.index }} as (
                select count(*) as cnt
                from {{ table.database }}.{{ table.schema }}.{{ table.name }} t
                where
                    t.{{ table.timestamp_column }} is not null
                    and t.{{ table.timestamp_column }}
                    < dateadd('day', -30, date_trunc('hour', current_timestamp()))
            ),

            -- Hourly incremental counts within the backfill window
            hourly_counts_{{ loop.index }} as (
                select
                    date_trunc('hour', {{ table.timestamp_column }}) as data_hour,
                    count(*) as hourly_count
                from {{ table.database }}.{{ table.schema }}.{{ table.name }}
                where
                    {{ table.timestamp_column }} is not null
                    and {{ table.timestamp_column }}
                    >= dateadd('day', -30, date_trunc('hour', current_timestamp()))
                group by 1
            ),

            -- Cumulative row count via running SUM (replaces correlated subquery)
            custom_table_metrics_{{ loop.index }} as (
                select
                    '{{ table.database }}' as database_name,
                    '{{ table.schema }}' as schema_name,
                    '{{ table.name }}' as table_name,
                    '{{ table.full_name }}' as full_table_name,

                    -- Cumulative row count: base count + running sum of hourly counts
                    (select cnt from base_count_{{ loop.index }}) + coalesce(
                        sum(hc.hourly_count) over (
                            order by spine.snapshot_timestamp
                            rows between unbounded preceding and current row
                        ),
                        0
                    ) as row_count,

                    spine.snapshot_timestamp,

                    'table' as source_type,
                    '{{ table.timestamp_column }}' as timestamp_column,
                    '{{ table.sensitivity }}' as sensitivity,
                    {{ row_count_change_sensitivity_map[table.sensitivity] }}
                    as row_count_change_sigma_multiplier,
                    {{ row_count_sensitivity_map[table.sensitivity] }}
                    as row_count_sigma_multiplier,

                    current_timestamp() as _loaded_at

                from hourly_spine spine
                left join
                    hourly_counts_{{ loop.index }} hc
                    on spine.snapshot_timestamp = hc.data_hour
                where spine.snapshot_timestamp <= current_timestamp()
            ),
        {%- endfor %}

        -- Union all custom table metrics
        custom_table_metrics as (
            select *
            from custom_table_metrics_1
            {%- for i in range(2, custom_tables | length + 1) %}
                union all
                select *
                from custom_table_metrics_{{ i }}
            {%- endfor %}
        ),

    {% else %}
    {%- endif %}
    -- Query information_schema for enrolled tables (those with volume tests but no
    -- timestamp_column)
    metadata_metrics as (
        {%- if metadata_tables | length > 0 %}
            select
                -- Table identification
                t.table_catalog as database_name,
                t.table_schema as schema_name,
                t.table_name,
                t.table_catalog
                || '.'
                || t.table_schema
                || '.'
                || t.table_name as full_table_name,

                -- Volume metric
                t.row_count,

                t.snapshot_timestamp,  -- Hourly snapshot timestamp
                'metadata' as source_type,
                cast(null as varchar) as timestamp_column,

                -- Sensitivity from test config
                case
                    upper(
                        t.table_catalog || '.' || t.table_schema || '.' || t.table_name
                    )
                    {%- for table in metadata_tables %}
                        when upper('{{ table.full_name }}')
                        then '{{ table.sensitivity }}'
                    {%- endfor %}
                end as sensitivity,

                case
                    upper(
                        t.table_catalog || '.' || t.table_schema || '.' || t.table_name
                    )
                    {%- for table in metadata_tables %}
                        when upper('{{ table.full_name }}')
                        then {{ row_count_change_sensitivity_map[table.sensitivity] }}
                    {%- endfor %}
                end as row_count_change_sigma_multiplier,

                case
                    upper(
                        t.table_catalog || '.' || t.table_schema || '.' || t.table_name
                    )
                    {%- for table in metadata_tables %}
                        when upper('{{ table.full_name }}')
                        then {{ row_count_sensitivity_map[table.sensitivity] }}
                    {%- endfor %}
                end as row_count_sigma_multiplier,

                current_timestamp() as _loaded_at

            from {{ ref("snap_monitored_table_metadata") }} t
            where
                true
                and t.table_type = 'BASE TABLE'
                and t.row_count is not null
                and t.snapshot_timestamp is not null
                -- Read ALL snapshot records (including historical versions for
                -- backfill)
                -- Only include enrolled tables (case-insensitive comparison)
                and upper(
                    t.table_catalog || '.' || t.table_schema || '.' || t.table_name
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
                        t.table_catalog,
                        t.table_schema,
                        t.table_name,
                        t.snapshot_timestamp
                    order by t.dbt_updated_at desc nulls last
                )
                = 1
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
                cast(null as number) as row_count_change_sigma_multiplier,
                cast(null as number) as row_count_sigma_multiplier,
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

    -- Include historical data for proper window function calculations
    -- with_history_raw unions old + new, then dedup removes overlap
    with_history_raw as (
        {% if is_incremental() %}
            -- On incremental runs, union new snapshots with historical data (last 30
            -- days)
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
                row_count_change_sigma_multiplier,
                row_count_sigma_multiplier,
                _loaded_at
            from {{ this }}
            where snapshot_timestamp >= dateadd('day', -30, current_timestamp())
            union all
        {% endif %}
        select *
        from unioned
    ),

    -- Deduplicate in case historical and new data overlap on the same timestamp
    with_history as (
        select *
        from with_history_raw
        qualify
            row_number() over (
                partition by full_table_name, source_type, snapshot_timestamp
                order by _loaded_at desc nulls last
            )
            = 1
    ),

    -- Calculate row count change from previous snapshot
    with_change as (
        select
            *,
            row_count - lag(row_count) over (
                partition by full_table_name, source_type order by snapshot_timestamp
            ) as row_count_change
        from with_history
    ),

    -- Calculate rolling statistics on row_count_CHANGE (unified for both sources)
    change_stats as (
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
            row_count_change_sigma_multiplier,
            row_count_sigma_multiplier,
            _loaded_at,
            row_count_change,

            -- Base (non-seasonal) rolling statistics (28-day time window)
            -- Uses INTERVAL '1 second' PRECEDING to exclude current row from baseline
            avg(row_count_change) over (
                partition by full_table_name, source_type
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as base_expected_change_mean,

            stddev(row_count_change) over (
                partition by full_table_name, source_type
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as base_expected_change_stddev,

            avg(row_count) over (
                partition by full_table_name, source_type
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as base_expected_row_count_mean,

            stddev(row_count) over (
                partition by full_table_name, source_type
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as base_expected_row_count_stddev,

            count(*) over (
                partition by full_table_name, source_type
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as base_observation_count

            {% if seasonality_enabled %}
            ,
            -- Seasonal (day-of-week partitioned) rolling statistics
            -- Compares each day only to the same weekday in the lookback window
            avg(row_count_change) over (
                partition by full_table_name, source_type, dayofweek(snapshot_timestamp)
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as seasonal_expected_change_mean,

            stddev(row_count_change) over (
                partition by full_table_name, source_type, dayofweek(snapshot_timestamp)
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as seasonal_expected_change_stddev,

            avg(row_count) over (
                partition by full_table_name, source_type, dayofweek(snapshot_timestamp)
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as seasonal_expected_row_count_mean,

            stddev(row_count) over (
                partition by full_table_name, source_type, dayofweek(snapshot_timestamp)
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as seasonal_expected_row_count_stddev,

            count(*) over (
                partition by full_table_name, source_type, dayofweek(snapshot_timestamp)
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as seasonal_observation_count
            {% endif %}

        from with_change
    ),

    -- Resolve seasonal vs base statistics
    -- Falls back to base (non-seasonal) when insufficient day-of-week observations
    resolved_stats as (
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
            row_count_change_sigma_multiplier,
            row_count_sigma_multiplier,
            _loaded_at,
            row_count_change,

            {% if seasonality_enabled %}
            case when seasonal_observation_count >= {{ min_seasonal_obs }}
                 then seasonal_expected_change_mean
                 else base_expected_change_mean
            end as expected_change_mean,

            case when seasonal_observation_count >= {{ min_seasonal_obs }}
                 then seasonal_expected_change_stddev
                 else base_expected_change_stddev
            end as expected_change_stddev,

            case when seasonal_observation_count >= {{ min_seasonal_obs }}
                 then seasonal_expected_row_count_mean
                 else base_expected_row_count_mean
            end as expected_row_count_mean,

            case when seasonal_observation_count >= {{ min_seasonal_obs }}
                 then seasonal_expected_row_count_stddev
                 else base_expected_row_count_stddev
            end as expected_row_count_stddev,

            case when seasonal_observation_count >= {{ min_seasonal_obs }}
                 then seasonal_observation_count
                 else base_observation_count
            end as observation_count
            {% else %}
            base_expected_change_mean as expected_change_mean,
            base_expected_change_stddev as expected_change_stddev,
            base_expected_row_count_mean as expected_row_count_mean,
            base_expected_row_count_stddev as expected_row_count_stddev,
            base_observation_count as observation_count
            {% endif %}

        from change_stats
    ),

    -- Compute individual detection flags with materiality filters
    with_flags as (
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
            row_count,
            row_count_change,

            -- Statistical baselines for row_count_CHANGE
            round(expected_change_mean, 0) as expected_change_mean,
            round(expected_change_stddev, 0) as expected_change_stddev,
            round(
                expected_change_mean
                - (row_count_change_sigma_multiplier * expected_change_stddev),
                0
            ) as expected_change_min,
            round(
                expected_change_mean
                + (row_count_change_sigma_multiplier * expected_change_stddev),
                0
            ) as expected_change_max,

            -- Statistical baselines for ABSOLUTE row_count
            round(expected_row_count_mean, 0) as expected_row_count_mean,
            round(expected_row_count_stddev, 0) as expected_row_count_stddev,
            round(
                expected_row_count_mean
                - (row_count_sigma_multiplier * expected_row_count_stddev),
                0
            ) as expected_row_count_min,
            round(
                expected_row_count_mean
                + (row_count_sigma_multiplier * expected_row_count_stddev),
                0
            ) as expected_row_count_max,

            observation_count,

            -- Detection flags with materiality filters
            -- Change-based: statistical threshold AND absolute + relative materiality
            (
                {{
                    is_spike_high(
                        metric_change="row_count_change",
                        expected_change_mean="expected_change_mean",
                        expected_change_stddev="expected_change_stddev",
                        sigma_multiplier="row_count_change_sigma_multiplier",
                        observation_count="observation_count",
                        min_observations=min_obs,
                    )
                }}
                and abs(row_count_change - expected_change_mean)
                >= {{ mat_change_rows }}
                and (
                    case
                        when expected_change_mean = 0
                        then abs(row_count_change) >= {{ mat_change_rows }}
                        else
                            abs(
                                (row_count_change - expected_change_mean)
                                / nullif(abs(expected_change_mean), 0)
                            )
                            >= {{ mat_change_pct }}
                    end
                )
            ) as is_spike_high,

            (
                {{
                    is_drop_low(
                        metric_change="row_count_change",
                        expected_change_mean="expected_change_mean",
                        expected_change_stddev="expected_change_stddev",
                        sigma_multiplier="row_count_change_sigma_multiplier",
                        observation_count="observation_count",
                        min_observations=min_obs,
                    )
                }}
                and abs(row_count_change - expected_change_mean)
                >= {{ mat_change_rows }}
                and (
                    case
                        when expected_change_mean = 0
                        then abs(row_count_change) >= {{ mat_change_rows }}
                        else
                            abs(
                                (row_count_change - expected_change_mean)
                                / nullif(abs(expected_change_mean), 0)
                            )
                            >= {{ mat_change_pct }}
                    end
                )
            ) as is_drop_low,

            -- Absolute row count out of range detection with materiality filter
            case
                when observation_count < {{ min_obs }}
                then false
                when expected_row_count_stddev = 0 or expected_row_count_stddev is null
                then false
                when
                    (
                        row_count < (
                            expected_row_count_mean
                            - (row_count_sigma_multiplier * expected_row_count_stddev)
                        )
                        or row_count > (
                            expected_row_count_mean
                            + (row_count_sigma_multiplier * expected_row_count_stddev)
                        )
                    )
                    and (
                        abs(row_count - expected_row_count_mean) >= {{ mat_count_rows }}
                        or case
                            when expected_row_count_mean = 0
                            then abs(row_count) >= {{ mat_count_rows }}
                            else
                                abs(
                                    (row_count - expected_row_count_mean)
                                    / nullif(expected_row_count_mean, 0)
                                )
                                >= {{ mat_count_pct }}
                        end
                    )
                then true
                else false
            end as is_row_count_anomaly,

            -- Materiality checks (diagnostic columns)
            abs(row_count_change - expected_change_mean)
            >= {{ mat_count_rows }} as is_material_absolute_change,
            abs(row_count - expected_row_count_mean)
            >= {{ mat_count_rows }} as is_material_absolute_count,

            case
                when expected_change_mean = 0
                then abs(row_count_change) >= {{ mat_count_rows }}
                else
                    abs(
                        (row_count_change - expected_change_mean)
                        / nullif(abs(expected_change_mean), 0)
                    )
                    >= 0.5
            end as is_material_relative_change,
            case
                when expected_row_count_mean = 0
                then abs(row_count) >= {{ mat_count_rows }}
                else
                    abs(
                        (row_count - expected_row_count_mean)
                        / nullif(expected_row_count_mean, 0)
                    )
                    >= {{ mat_count_pct }}
            end as is_material_relative_count,

            -- Statistical anomaly flags (without materiality filter)
            {{
                is_spike_high(
                    metric_change="row_count_change",
                    expected_change_mean="expected_change_mean",
                    expected_change_stddev="expected_change_stddev",
                    sigma_multiplier="row_count_change_sigma_multiplier",
                    observation_count="observation_count",
                    min_observations=min_obs,
                )
            }} as is_spike_high_statistical,

            {{
                is_drop_low(
                    metric_change="row_count_change",
                    expected_change_mean="expected_change_mean",
                    expected_change_stddev="expected_change_stddev",
                    sigma_multiplier="row_count_change_sigma_multiplier",
                    observation_count="observation_count",
                    min_observations=min_obs,
                )
            }} as is_drop_low_statistical,

            (
                observation_count >= {{ min_obs }}
                and expected_row_count_stddev > 0
                and expected_row_count_stddev is not null
                and (
                    row_count < (
                        expected_row_count_mean
                        - (row_count_sigma_multiplier * expected_row_count_stddev)
                    )
                    or row_count > (
                        expected_row_count_mean
                        + (row_count_sigma_multiplier * expected_row_count_stddev)
                    )
                )
            ) as is_row_count_anomaly_statistical,

            -- Configuration
            sensitivity,
            row_count_change_sigma_multiplier,
            row_count_sigma_multiplier,

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

        from resolved_stats
    ),

    -- Derive is_anomaly and reason columns from individual flags
    -- This guarantees is_anomaly is always consistent with individual flags
    final as (
        select
            database_name,
            schema_name,
            table_name,
            full_table_name,
            source_type,
            timestamp_column,
            snapshot_timestamp,
            row_count,
            row_count_change,
            expected_change_mean,
            expected_change_stddev,
            expected_change_min,
            expected_change_max,
            expected_row_count_mean,
            expected_row_count_stddev,
            expected_row_count_min,
            expected_row_count_max,
            observation_count,
            is_spike_high,
            is_drop_low,
            is_row_count_anomaly,
            is_material_absolute_change,
            is_material_absolute_count,
            is_material_relative_change,
            is_material_relative_count,
            is_spike_high_statistical,
            is_drop_low_statistical,
            is_row_count_anomaly_statistical,

            -- Master anomaly flag: guaranteed consistent with individual flags
            (is_spike_high or is_drop_low or is_row_count_anomaly) as is_anomaly,

            -- Anomaly reasons derived from flags
            case
                when is_spike_high
                then 'Row Count Change - Spike'
                when is_drop_low
                then 'Row Count Change - Dip'
                else null
            end as row_count_change_anomaly_reason,

            case
                when is_row_count_anomaly and row_count < expected_row_count_mean
                then 'Row Count - Dip'
                when is_row_count_anomaly
                then 'Row Count - Spike'
                else null
            end as row_count_anomaly_reason,

            -- Combined anomaly reason (comma-separated if multiple types detected)
            -- Note: Snowflake's concat_ws returns NULL if ANY arg is NULL,
            -- so we use array_construct_compact (which skips NULLs) instead
            nullif(
                array_to_string(
                    array_construct_compact(
                        case
                            when is_spike_high
                            then 'Row Count Change - Spike'
                            when is_drop_low
                            then 'Row Count Change - Dip'
                            else null
                        end,
                        case
                            when
                                is_row_count_anomaly
                                and row_count < expected_row_count_mean
                            then 'Row Count - Dip'
                            when is_row_count_anomaly
                            then 'Row Count - Spike'
                            else null
                        end
                    ),
                    ', '
                ),
                ''
            ) as anomaly_reason,

            sensitivity,
            row_count_change_sigma_multiplier,
            row_count_sigma_multiplier,
            metric_id,
            _loaded_at

        from with_flags
    )

select *
from final

{% if is_incremental() %}
    -- On incremental runs, only return new snapshots (filter out historical data used
    -- for window calculations)
    where snapshot_timestamp > (select max(snapshot_timestamp) from {{ this }})
{% endif %}
