{{
    config(
        materialized="table",
        tags=["anomaly_detection", "volume"],
    )
}}
/*
    ============================================================================
    Volume anomaly detection - FINAL scoring layer
    ============================================================================

    WHAT THIS MODEL DOES
    --------------------
    Reads one row per (table, snapshot) from volume_metrics_history_prep and
    decides whether that snapshot's row_count_change (new rows since the previous
    snapshot) is anomalous. For each row we build a baseline from the same table's
    earlier snapshots, then flag the row if its change falls outside the expected
    band around that baseline.

    THE BASELINE: robust (median-based) and PRIOR-ONLY
    --------------------------------------------------
    "Robust"     -> we use the MEDIAN and MAD (median absolute deviation) instead
                    of mean/stddev, so a single huge spike does not drag the
                    baseline toward itself and mask the very anomaly we want to
                    catch.
    "Prior-only" -> a row is never part of its own baseline. The self-join keeps
                    only earlier snapshots (prev.snapshot_timestamp <
                    cur.snapshot_timestamp), otherwise the current value would
                    help define the band it is tested against.

    K-NEAREST SAMPLING (not all history)
    ------------------------------------
    Rather than using every prior snapshot, we keep the med_sample_size prior
    values whose row_count_change is CLOSEST to the current row's change (nearest
    by absolute difference). This makes the baseline reflect "what a normal load
    of roughly this size looks like" and stops rare, very different regimes from
    distorting the median/MAD.

    THE BAND
    --------
        lower = MED - mad_multiplier * MAD
        upper = MED + mad_multiplier * MAD

      MED = median of the nearest prior pool.
      MAD = median(|value - MED|) over that pool -- the typical distance from the
            median. We use MAD directly as the spread unit (no 1.4826
            normal-scaling), so mad_multiplier is literally "how many MADs wide"
            the band is. Larger multiplier = wider band = less sensitive. The
            multiplier comes from prep (driven by each table's sensitivity).

    MAD FLOOR: guarding the degenerate (zero-width) band
    ----------------------------------------------------
    If a table barely changes across snapshots (e.g. an hourly snapshot of a
    table that only loads once a day), most row_count_change values are equal and
    MAD collapses to 0, making the band [MED, MED] so every real load reads as an
    anomaly. To keep the band usable we floor MAD:
      1. median |dev|  (the true MAD); if that is 0 fall back to
      2. p75 of |dev|; if that is 0 fall back to
      3. p90 of |dev|; and regardless, never let MAD drop below
      4. MED * mad_floor  (a fraction of the typical value).
    The final MAD is the greatest of the first non-zero fallback and the
    proportional floor.

    WHY TWO PASSES + A SELF-JOIN (Snowflake limitation)
    ---------------------------------------------------
    Snowflake's MEDIAN / PERCENTILE_CONT cannot run over a moving window frame,
    so "median of prior rows" cannot be a window function. Instead we materialise
    the prior-rows pool with a self-join, then:
      Pass 1 (med): median of the nearest prior pool.
      Pass 2 (mad): median of |prior value - MED|, which needs MED from pass 1.
*/
{% set min_obs = var("volume_anomaly_detection.min_historical_observations", 7) %}
{% set mad_floor = var("volume_anomaly_detection.mad_floor", 0.2) %}
{% set med_sample_size = var("volume_anomaly_detection.med_sample_size", 20) %}
with
    prep as (select * from {{ ref("volume_metrics_history_prep") }}),
    -- For each current row, pair it with its prior snapshots (same object,
    -- strictly earlier), then keep only the med_sample_size prior values whose
    -- row_count_change is nearest (by absolute difference) to the current row's
    -- change. This nearest-neighbour pool is the baseline sample for med + mad.
    med_mad_sample_pool as (
        select
            cur.metric_id,
            cur.row_count_change as row_count_change_cur,
            prev.row_count_change as row_count_change_prev
        from prep cur
        join
            prep prev
            on prev.object_id = cur.object_id
            and prev.snapshot_timestamp < cur.snapshot_timestamp
        where cur.row_count_change is not null
        qualify
            row_number() over (
                partition by cur.metric_id
                order by abs(cur.row_count_change - prev.row_count_change)
            )
            <= {{ med_sample_size }}
    ),

    -- Pass 1: MED = median of the nearest prior pool, one value per current row.
    med as (
        select metric_id, median(row_count_change_prev) as row_count_change_med
        from med_mad_sample_pool
        group by all
    ),

    -- Pass 2: MAD over the same nearest prior pool, with the zero-width floor.
    -- *_mad_prep_1 = median |dev|  (the standard MAD)
    -- *_mad_prep_2 = p75    |dev|  (fallback if the median deviation is 0)
    -- *_mad_prep_3 = p90    |dev|  (fallback if p75 is also 0)
    -- row_count_change_mad = greatest(first non-zero fallback, MED * mad_floor),
    -- so the spread is never 0 for a table that legitimately loads in bursts.
    mad as (
        select
            med_mad_sample_pool.metric_id,
            med.row_count_change_med,
            count(med_mad_sample_pool.row_count_change_prev) as prior_obs_count,
            median(
                abs(
                    med_mad_sample_pool.row_count_change_prev - med.row_count_change_med
                )
            ) as row_count_change_mad_prep_1,
            percentile_cont(0.75) within group (
                order by
                    abs(
                        med_mad_sample_pool.row_count_change_prev
                        - med.row_count_change_med
                    )
            ) as row_count_change_mad_prep_2,
            percentile_cont(0.90) within group (
                order by
                    abs(
                        med_mad_sample_pool.row_count_change_prev
                        - med.row_count_change_med
                    )
            ) as row_count_change_mad_prep_3,
            greatest(
                coalesce(
                    nullif(row_count_change_mad_prep_1, 0),
                    nullif(row_count_change_mad_prep_2, 0),
                    nullif(row_count_change_mad_prep_3, 0),
                    0
                ),
                med.row_count_change_med *{{ mad_floor }}
            ) as row_count_change_mad
        from med_mad_sample_pool
        left join med using (metric_id)
        group by all
    ),

    -- Attach the baseline to every prep row and derive the band. The modified
    -- z-score (distance from the median in MAD units) is kept as a diagnostic of
    -- how far outside the band a value sits.
    bounds as (
        select
            prep.*,
            mad.prior_obs_count,
            mad.row_count_change_med,
            mad.row_count_change_mad,
            mad.row_count_change_med - (
                prep.row_count_change_mad_multiplier * mad.row_count_change_mad
            ) as row_count_change_lower_limit,
            mad.row_count_change_med + (
                prep.row_count_change_mad_multiplier * mad.row_count_change_mad
            ) as row_count_change_upper_limit,
            abs(prep.row_count_change - mad.row_count_change_med)
            / nullif(mad.row_count_change_mad, 0) as row_count_change_modified_z_score
        from prep
        left join mad on mad.metric_id = prep.metric_id
    ),

    -- For each row, count how many PRIOR snapshots of the SAME object had a
    -- row_count_change_modified_z_score "similar" to this row's -- similar being
    -- within +/-20% of this row's z-score, i.e. in
    -- [z - 20%z, z + 20%z]. This is a diagnostic of how routine (vs. unprecedented)
    -- the current deviation is: a high count means the object has drifted this
    -- far from its median many times before, a count near 0 means a deviation of
    -- this magnitude is novel for the object. Prior-only
    -- (prev.snapshot_timestamp < cur.snapshot_timestamp) to stay consistent with
    -- the rest of the model's baseline philosophy (a row never counts itself).
    similar_z_score as (
        select
            cur.metric_id,
            count_if(
                prev.row_count_change_modified_z_score
                between (cur.row_count_change_modified_z_score * 0.8) and (
                    cur.row_count_change_modified_z_score * 1.2
                )
            ) as row_count_change_similar_z_score_count
        from bounds cur
        join
            bounds prev
            on prev.object_id = cur.object_id
            and prev.snapshot_timestamp < cur.snapshot_timestamp
        where cur.row_count_change_modified_z_score is not null
        group by cur.metric_id
    ),

    final as (
        select
            bounds.*,
            -- 0 when the row has a z-score but no qualifying prior snapshots;
            -- NULL only when the row itself has no z-score (no baseline).
            case
                when bounds.row_count_change_modified_z_score is not null
                then coalesce(similar_z_score.row_count_change_similar_z_score_count, 0)
            end as row_count_change_similar_z_score_count,
            -- Anomaly = current change outside [lower, upper]. Guarded so we only
            -- score rows with a trustworthy baseline: MAD must be defined, the
            -- current change must be present, and there must be more than min_obs
            -- prior observations behind the median/MAD.
            (
                row_count_change_mad is not null
                and row_count_change is not null
                and prior_obs_count > {{ min_obs }}
                and (
                    row_count_change < row_count_change_lower_limit
                    or row_count_change > row_count_change_upper_limit
                )
            ) as is_row_count_change_anomaly
        from bounds
        left join similar_z_score using (metric_id)
    )
select *
from final
