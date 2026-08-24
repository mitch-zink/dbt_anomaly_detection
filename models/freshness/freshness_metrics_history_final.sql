{{ config(materialized="table", tags=["anomaly_detection", "freshness"]) }}
-- Sensitivity percentiles. 0.95 means that it will flag if time since the last update
-- is under 95% the update range in the last 30 days
{% set freshness_sensitivity_map = {
    "very_low": 0.95,
    "low": 0.92,
    "medium": 0.90,
    "high": 0.85,
    "very_high": 0.80,
} %}
{% set min_obs = var("freshness_anomaly_detection.min_historical_observation_days", 7) %}
{% set min_staleness_minutes = var("freshness_anomaly_detection.min_staleness_minutes", 120) %}

with
    source as (
        select *
        from {{ ref("freshness_metrics_history_prep") }}
        -- Keep only rows within each object's configured training window. We read the
        -- training_period_days from the object's most recent snapshot (last_value over
        -- the full partition) so a config change takes effect immediately, then filter
        -- to snapshots newer than that many days ago.
        qualify
            snapshot_timestamp > dateadd(
                'day',
                -1 * last_value(training_period_days) ignore nulls over (
                    partition by object_id
                    order by snapshot_timestamp
                    rows between unbounded preceding and unbounded following
                ),
                current_timestamp()
            )
    ),
    prepa as (
        select
            *,
            datediff(
                minute, table_last_altered_at, snapshot_timestamp
            ) as minutes_since_last_update,
            datediff(
                minute,
                -- Order by snapshot_timestamp so the gap lands on the first snapshot
                -- that
                -- observed this update (the update_observation_seq = 1 row below).
                -- Every
                -- later snapshot that re-observes the same table_last_altered_at
                -- yields a
                -- 0-gap, which is dropped downstream.
                lag(table_last_altered_at) over (
                    partition by object_id order by snapshot_timestamp
                ),
                table_last_altered_at
            ) as minutes_between_updates,
            datediff(
                minute,
                lag(snapshot_timestamp) over (
                    partition by object_id order by snapshot_timestamp
                ),
                snapshot_timestamp
            ) as minutes_between_snapshot,
            -- Ordinal of this snapshot among all snapshots that observed the SAME
            -- table_last_altered_at for this object (1 = first snapshot to see this
            -- update). Snapshots with seq > 1 are just re-observations of an already
            -- counted update; they must not feed their duplicate gap into the baseline.
            row_number() over (
                partition by object_id, table_last_altered_at
                order by snapshot_timestamp
            ) as update_observation_seq
        from source
    ),
    adjusted as (
        select
            *,
            iff(
                minutes_between_snapshot > (24 * 60)
                or minutes_between_updates < 30
                or update_observation_seq > 1,
                null,
                minutes_between_updates
            ) as minutes_between_updates_adjusted
        -- adjusted because for two snapshots that are taken far apart, the
        -- minutes_between_update value is not realizable. Also, if the
        -- minutes_between_updates is too short, it's likely a different step in one
        -- update activity, which should also be ignored. Finally, only the first
        -- observation of each distinct table_last_altered_at (update_observation_seq
        -- = 1)
        -- contributes a gap, so a single update re-seen across many snapshots is
        -- counted
        -- once instead of inflating the baseline with duplicate samples.
        from prepa
    ),
    scored as (
        select
            *,
            -- Days of history backing the percentile: distinct calendar days that
            -- have at least one non-null normal-gap sample for this object. We count
            -- DAYS (not raw samples) because snapshots are sub-daily -- 7 samples in
            -- one afternoon is not 7 days of pattern.
            count(
                distinct case
                    when minutes_between_updates_adjusted is not null
                    then date_trunc('day', snapshot_timestamp)
                end
            ) over (partition by object_id) as observation_day_count,
            -- Percentile is looked up per row from freshness_sensitivity_map using the
            -- sensitivity column. Snowflake requires percentile_cont()'s argument to
            -- be a
            -- literal constant, so we emit one branch (with a literal percentile) per
            -- sensitivity level and let CASE pick the matching one. lower()
            -- normalizes the
            -- mixed-case sensitivity values (UPPER from custom-table branch, lower from
            -- metadata branch) to the lowercase map keys.
            case
                lower(sensitivity)
                {%- for level, pct in freshness_sensitivity_map.items() %}
                    when '{{ level }}'
                    then
                        percentile_cont({{ pct }}) within group (
                            order by minutes_between_updates_adjusted
                        ) over (partition by object_id)
                {%- endfor %}
            end as normal_gap_upper,
            row_number() over (
                partition by object_id order by snapshot_timestamp desc
            ) as snapshot_order
        from adjusted
    ),
    final as (
        select
            *,
            (normal_gap_upper / 60)::decimal(5, 1) as normal_gap_upper_hour,
            -- Freshness issue: table is overdue relative to its own normal update gap.
            -- Guarded to avoid false positives on sparse / low-signal tables:
            -- * no baseline yet -> not stale
            -- * fewer than N days of history backing the percentile -> not stale
            -- * below an absolute staleness floor -> not stale (ignore chatty tables)
            case
                when normal_gap_upper is null
                then false
                when observation_day_count < {{ min_obs }}
                then false
                when minutes_since_last_update < {{ min_staleness_minutes }}
                then false
                when minutes_since_last_update > normal_gap_upper
                then true
                else false
            end as is_stale,
            -- Object is still "in training": not yet enough history to trust a
            -- staleness verdict (no baseline, or fewer than the required days of
            -- pattern). Mutually exclusive with is_stale.
            (
                normal_gap_upper is null
                or observation_day_count < {{ min_obs }}
            ) as is_training
        from scored
    )
select *
from final
