{%- test freshness_anomaly(
    model,
    sensitivity="very_low",
    timestamp_column=none,
    training_period_days=30,
    timezone="UTC"
) -%}
    {#
    Generic test for freshness anomaly detection.

    This test is scanned by dbt_anomaly_detection.freshness_metrics_history
    to determine which tables to monitor and with what configuration.

    The actual anomaly detection logic is implemented in the freshness_metrics_history
    incremental model, not in this test itself. These parameters are configuration
    markers only: they are read from the test's kwargs (via get_enrolled_tables) by the
    freshness models, so every arg used there must be declared here or dbt raises an
    "unexpected keyword argument" error when the test runs.

    Parameters:
        - model: The dbt model to monitor (automatically passed by dbt)
        - sensitivity: Sensitivity level (very_low, low, medium, high, very_high)
        - timestamp_column: Optional timestamp column for timestamp-based monitoring
        - training_period_days: Days of history to train the freshness baseline on
          (default 30). Consumed by the staging/prep/final freshness models.
        - timezone: Timezone of timestamp_column, converted to UTC before measuring
          staleness (default "UTC"). Only relevant for custom-timestamp tables.
#}
    -- This test always passes - it's just a configuration marker
    -- The actual anomaly detection happens in freshness_metrics_history
    select 1 where false

{%- endtest -%}
