{{
    config(
        materialized="incremental",
        unique_key="metric_id",
        on_schema_change="sync_all_columns",
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
    - Enrollment determined by scanning graph.nodes (compiled at runtime via stg_monitored_tables)

    **Detection Logic:**
    Two-stage filtering (Statistical + Materiality):

    1. **Change-based detection** - Monitors growth rate (row_count_change):
       - Statistical: change > mean + (σ × stddev) OR change < mean - (σ × stddev)
       - Materiality: >1,000 rows deviation AND >200% relative change
       - Output: 'Row Count Change - Spike' or 'Row Count Change - Dip'

    2. **Size-based detection** - Monitors absolute row count:
       - Statistical: count > mean + (σ × stddev) OR count < mean - (σ × stddev)
       - Materiality: >100 rows deviation AND >5% relative change
       - Output: 'Row Count - Spike' or 'Row Count - Dip'

    **Sensitivity Levels:**
    Configurable sigma multipliers:
    - Row Count Change: very_low=4.0σ, low=3.0σ, medium=2.0σ, high=1.5σ, very_high=1.0σ
    - Absolute Row Count: very_low=6.0σ, low=4.0σ, medium=3.0σ, high=2.0σ, very_high=1.0σ

    **Output:**
    - 4 boolean flags: is_spike_high, is_drop_low, is_row_count_anomaly, is_anomaly
    - 3 reason columns: row_count_change_anomaly_reason, row_count_anomaly_reason, anomaly_reason
    - Rolling 28-day window statistics for dynamic baseline calculation
*/
{%- set custom_tables = [] -%}
{%- set metadata_tables = [] -%}
{%- if execute -%}
    {%- for node in graph.nodes.values() -%}
        {%- if node.resource_type == "test" -%}
            {%- if node.test_metadata and node.test_metadata.name == "volume_anomaly" -%}
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
                                                    30,
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
                ) as snapshot_timestamp
            from table(generator(rowcount => 30 * 24))  -- 30 days * 24 hours
        ),

        {%- for table in custom_tables %}
            -- Get cumulative row count as of each hour (running total)
            custom_table_metrics_{{ loop.index }} as (
                select
                    '{{ table.database }}' as database_name,
                    '{{ table.schema }}' as schema_name,
                    '{{ table.name }}' as table_name,
                    '{{ table.full_name }}' as full_table_name,

                    -- Cumulative row count: all rows with timestamp <= this hour
                    (
                        select count(*)
                        from {{ table.database }}.{{ table.schema }}.{{ table.name }} t
                        where
                            t.{{ table.timestamp_column }} is not null
                            and t.{{ table.timestamp_column }}
                            <= spine.snapshot_timestamp
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

    -- Include historical data for metadata mode to enable proper window function
    -- calculations
    with_history as (
        {% if is_incremental() %}
            -- On incremental runs, union new snapshots with historical data (last 30
            -- days)
            -- Only select base columns to match structure of unioned CTE
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

            -- Calculate rolling statistics on the change column (28-day time window)
            -- Uses INTERVAL '1 second' PRECEDING to exclude current row from baseline
            -- calculation
            -- (equivalent to ROWS BETWEEN ... AND 1 PRECEDING in row-based windows)
            avg(row_count_change) over (
                partition by full_table_name, source_type
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as expected_change_mean,

            stddev(row_count_change) over (
                partition by full_table_name, source_type
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as expected_change_stddev,

            -- Calculate rolling statistics on ABSOLUTE row_count (28-day time window)
            avg(row_count) over (
                partition by full_table_name, source_type
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as expected_row_count_mean,

            stddev(row_count) over (
                partition by full_table_name, source_type
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as expected_row_count_stddev,

            count(*) over (
                partition by full_table_name, source_type
                order by
                    snapshot_timestamp
                    range
                    between interval '28 days' preceding
                    and interval '1 second' preceding
            ) as observation_count

        from with_change
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
            row_count,
            row_count_change,

            -- Statistical baselines for row_count_CHANGE (uses
            -- row_count_change_sigma_multiplier)
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

            -- Statistical baselines for ABSOLUTE row_count (uses
            -- row_count_sigma_multiplier)
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
            -- For change-based: Require BOTH absolute (>1000) AND relative (>200%)
            -- thresholds
            (
                {{
                    is_spike_high(
                        metric_change="row_count_change",
                        expected_change_mean="expected_change_mean",
                        expected_change_stddev="expected_change_stddev",
                        sigma_multiplier="row_count_change_sigma_multiplier",
                        observation_count="observation_count",
                        min_observations=var(
                            "volume_anomaly_detection.min_historical_observations", 7
                        ),
                    )
                }}
                and abs(row_count_change - expected_change_mean) >= 1000
                and (
                    case
                        when expected_change_mean = 0
                        then abs(row_count_change) >= 1000
                        else
                            abs(
                                (row_count_change - expected_change_mean)
                                / nullif(abs(expected_change_mean), 0)
                            )
                            >= 2.0
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
                        min_observations=var(
                            "volume_anomaly_detection.min_historical_observations", 7
                        ),
                    )
                }}
                and abs(row_count_change - expected_change_mean) >= 1000
                and (
                    case
                        when expected_change_mean = 0
                        then abs(row_count_change) >= 1000
                        else
                            abs(
                                (row_count_change - expected_change_mean)
                                / nullif(abs(expected_change_mean), 0)
                            )
                            >= 2.0
                    end
                )
            ) as is_drop_low,

            -- Absolute row count out of range detection with materiality filter
            case
                when
                    observation_count
                    <
                    {{ var("volume_anomaly_detection.min_historical_observations", 7) }}
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
                        abs(row_count - expected_row_count_mean) >= 100
                        or case
                            when expected_row_count_mean = 0
                            then abs(row_count) >= 100
                            else
                                abs(
                                    (row_count - expected_row_count_mean)
                                    / nullif(expected_row_count_mean, 0)
                                )
                                >= 0.05
                        end
                    )
                then true
                else false
            end as is_row_count_anomaly,

            -- Materiality checks for filtering out statistically significant but
            -- meaningless changes
            -- Check 1: Absolute magnitude (raw change is large enough to matter)
            abs(row_count_change - expected_change_mean)
            >= 100 as is_material_absolute_change,
            abs(row_count - expected_row_count_mean)
            >= 100 as is_material_absolute_count,

            -- Check 2: Relative magnitude (% deviation from expected is large enough)
            case
                when expected_change_mean = 0
                then abs(row_count_change) >= 100
                else
                    abs(
                        (row_count_change - expected_change_mean)
                        / nullif(abs(expected_change_mean), 0)
                    )
                    >= 0.5
            end as is_material_relative_change,
            case
                when expected_row_count_mean = 0
                then abs(row_count) >= 100
                else
                    abs(
                        (row_count - expected_row_count_mean)
                        / nullif(expected_row_count_mean, 0)
                    )
                    >= 0.05
            end as is_material_relative_count,

            -- Statistical anomaly flags (without materiality filter)
            {{
                is_spike_high(
                    metric_change="row_count_change",
                    expected_change_mean="expected_change_mean",
                    expected_change_stddev="expected_change_stddev",
                    sigma_multiplier="row_count_change_sigma_multiplier",
                    observation_count="observation_count",
                    min_observations=var(
                        "volume_anomaly_detection.min_historical_observations", 7
                    ),
                )
            }} as is_spike_high_statistical,

            {{
                is_drop_low(
                    metric_change="row_count_change",
                    expected_change_mean="expected_change_mean",
                    expected_change_stddev="expected_change_stddev",
                    sigma_multiplier="row_count_change_sigma_multiplier",
                    observation_count="observation_count",
                    min_observations=var(
                        "volume_anomaly_detection.min_historical_observations", 7
                    ),
                )
            }} as is_drop_low_statistical,

            (
                observation_count
                >= {{ var("volume_anomaly_detection.min_historical_observations", 7) }}
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

            -- Overall anomaly flag: Statistical anomaly AND passes materiality filter
            -- Disabled is_growth_stalled to reduce false positives
            (
                {#
                {{
                    is_growth_stalled(
                        metric_change="row_count_change",
                        expected_change_mean="expected_change_mean",
                        observation_count="observation_count",
                        min_observations=var(
                            "volume_anomaly_detection.min_historical_observations", 7
                        ),
                        positive_change_threshold=100,
                    )
                }}
                or
                #}
                {{
                    is_spike_high(
                        metric_change="row_count_change",
                        expected_change_mean="expected_change_mean",
                        expected_change_stddev="expected_change_stddev",
                        sigma_multiplier="row_count_change_sigma_multiplier",
                        observation_count="observation_count",
                        min_observations=var(
                            "volume_anomaly_detection.min_historical_observations", 7
                        ),
                    )
                }}
                or {{
                    is_drop_low(
                        metric_change="row_count_change",
                        expected_change_mean="expected_change_mean",
                        expected_change_stddev="expected_change_stddev",
                        sigma_multiplier="row_count_change_sigma_multiplier",
                        observation_count="observation_count",
                        min_observations=var(
                            "volume_anomaly_detection.min_historical_observations", 7
                        ),
                    )
                }}
                or (
                    observation_count
                    >= {{
                        var(
                            "volume_anomaly_detection.min_historical_observations",
                            7,
                        )
                    }}
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
                    and (
                        abs(row_count - expected_row_count_mean) >= 100
                        or case
                            when expected_row_count_mean = 0
                            then abs(row_count) >= 100
                            else
                                abs(
                                    (row_count - expected_row_count_mean)
                                    / nullif(expected_row_count_mean, 0)
                                )
                                >= 0.05
                        end
                    )
                )
            ) as is_anomaly,

            -- Separate anomaly reasons for change-based detection
            case
                when
                    {{
                        is_spike_high(
                            metric_change="row_count_change",
                            expected_change_mean="expected_change_mean",
                            expected_change_stddev="expected_change_stddev",
                            sigma_multiplier="row_count_change_sigma_multiplier",
                            observation_count="observation_count",
                            min_observations=var(
                                "volume_anomaly_detection.min_historical_observations",
                                7,
                            ),
                        )
                    }}
                then 'Row Count Change - Spike'
                when
                    {{
                        is_drop_low(
                            metric_change="row_count_change",
                            expected_change_mean="expected_change_mean",
                            expected_change_stddev="expected_change_stddev",
                            sigma_multiplier="row_count_change_sigma_multiplier",
                            observation_count="observation_count",
                            min_observations=var(
                                "volume_anomaly_detection.min_historical_observations",
                                7,
                            ),
                        )
                    }}
                then 'Row Count Change - Dip'
                else null
            end as row_count_change_anomaly_reason,

            -- Separate anomaly reasons for size-based detection
            case
                when
                    (
                        observation_count
                        >= {{
                            var(
                                "volume_anomaly_detection.min_historical_observations",
                                7,
                            )
                        }}
                        and expected_row_count_stddev > 0
                        and expected_row_count_stddev is not null
                        and row_count < (
                            expected_row_count_mean
                            - (row_count_sigma_multiplier * expected_row_count_stddev)
                        )
                    )
                then 'Row Count - Dip'
                when
                    (
                        observation_count
                        >= {{
                            var(
                                "volume_anomaly_detection.min_historical_observations",
                                7,
                            )
                        }}
                        and expected_row_count_stddev > 0
                        and expected_row_count_stddev is not null
                        and row_count > (
                            expected_row_count_mean
                            + (row_count_sigma_multiplier * expected_row_count_stddev)
                        )
                    )
                then 'Row Count - Spike'
                else null
            end as row_count_anomaly_reason,

            -- Combined anomaly reason showing all detected issues
            case
                when
                    row_count_change_anomaly_reason is not null
                    and row_count_anomaly_reason is not null
                then row_count_change_anomaly_reason || ', ' || row_count_anomaly_reason
                when row_count_change_anomaly_reason is not null
                then row_count_change_anomaly_reason
                when row_count_anomaly_reason is not null
                then row_count_anomaly_reason
                else null
            end as anomaly_reason,

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

        from change_stats
    )

select *
from final

{% if is_incremental() %}
    -- On incremental runs, only return new snapshots (filter out historical data used
    -- for window calculations)
    where snapshot_timestamp > (select max(snapshot_timestamp) from {{ this }})
{% endif %}
