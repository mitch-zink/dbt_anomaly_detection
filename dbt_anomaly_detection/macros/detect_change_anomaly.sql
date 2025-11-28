{% macro is_spike_high(
    metric_change,
    expected_change_mean,
    expected_change_stddev,
    sigma_multiplier,
    observation_count,
    min_observations=7
) %}
    {#
    Detects if growth rate exceeded upper threshold (unusual spike)

    Returns false if:
    - Insufficient history (observation_count < min_observations)
    - No variance in baseline (stddev = 0 means no pattern to compare against)

    Returns true if:
    - Change exceeds upper bound: metric_change > (mean + σ × stddev)
    #}
    case
        when {{ observation_count }} < {{ min_observations }}
        then false
        when {{ expected_change_stddev }} = 0 or {{ expected_change_stddev }} is null
        then false
        when
            {{ metric_change }} > (
                {{ expected_change_mean }}
                + ({{ sigma_multiplier }} * {{ expected_change_stddev }})
            )
        then true
        else false
    end
{% endmacro %}

{% macro is_drop_low(
    metric_change,
    expected_change_mean,
    expected_change_stddev,
    sigma_multiplier,
    observation_count,
    min_observations=7
) %}
    {#
    Detects if growth rate fell below lower threshold (unusual drop)

    Returns false if:
    - Insufficient history (observation_count < min_observations)
    - No variance in baseline (stddev = 0 means no pattern to compare against)

    Returns true if:
    - Change falls below lower bound: metric_change < (mean - σ × stddev)
    #}
    case
        when {{ observation_count }} < {{ min_observations }}
        then false
        when {{ expected_change_stddev }} = 0 or {{ expected_change_stddev }} is null
        then false
        when
            {{ metric_change }} < (
                {{ expected_change_mean }}
                - ({{ sigma_multiplier }} * {{ expected_change_stddev }})
            )
        then true
        else false
    end
{% endmacro %}

{% macro is_growth_stalled(
    metric_change,
    expected_change_mean,
    observation_count,
    min_observations=7,
    positive_change_threshold=100
) %}
    {#
    Detects if growth has stalled for tables that normally grow

    Returns true if:
    - Table historically has positive growth (expected_change_mean > positive_change_threshold)
    - Current change is below threshold (metric_change < positive_change_threshold)
    - Sufficient historical data (observation_count >= min_observations)

    Use case: Catches when a table that normally grows +1000 rows/hour suddenly grows by only +50 rows
    #}
    case
        when {{ observation_count }} < {{ min_observations }}
        then false
        when {{ expected_change_mean }} is null
        then false
        when
            {{ expected_change_mean }} > {{ positive_change_threshold }}
            and {{ metric_change }} < {{ positive_change_threshold }}
        then true
        else false
    end
{% endmacro %}
