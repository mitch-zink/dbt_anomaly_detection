{%- test freshness_anomaly(model, sensitivity="very_low", timestamp_column=none) -%}
    {#
    Generic test for freshness anomaly detection.

    This test is scanned by dbt_anomaly_detection.freshness_metrics_history
    to determine which tables to monitor and with what configuration.

    The actual anomaly detection logic is implemented in the freshness_metrics_history
    incremental model, not in this test itself.

    Parameters:
        - model: The dbt model to monitor (automatically passed by dbt)
        - sensitivity: Sensitivity level (very_low, low, medium, high, very_high)
        - timestamp_column: Optional timestamp column for timestamp-based monitoring
#}
    -- This test always passes - it's just a configuration marker
    -- The actual anomaly detection happens in freshness_metrics_history
    select 1 where false

{%- endtest -%}
