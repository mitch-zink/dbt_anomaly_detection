{% snapshot snap_monitored_table_metadata %}

    {{
        config(
            target_schema="dbt_anomaly_detection",
            unique_key="table_id",
            strategy="check",
            check_cols=["created", "last_altered", "snapshot_timestamp"],
            invalidate_hard_deletes=True,
            tags=["anomaly_detection", "snapshot", "metadata"],
            full_refresh=(
                false
                if not var("allow_full_refresh_anomaly_detection", false)
                else none
            ),
        )
    }}

    /*
    Snapshot: Monitored Table Metadata

    Captures point-in-time metadata for tables and sources enrolled in anomaly detection.
    Only tracks tables with volume_anomaly or freshness_anomaly tests (via stg_monitored_tables).
    Supports cross-database monitoring of both dbt models and external source tables.

    This serves as the source-of-truth for:
    - Volume metrics (row_count changes over time)
    - Freshness metrics (last_altered tracking)
    - Table lifecycle (creation, modifications)

    Architecture: Dynamically queries INFORMATION_SCHEMA.TABLES from each database (not account_usage)
    Cross-Database: Queries multiple databases for source tables (e.g., FIVETRAN_SALESFORCE_DB, FIVETRAN_HEAP_DB, ANALYTICS)
    Benefits: Real-time data, standard permissions, decouples data collection from analysis, cross-database support

    **Full Refresh Protection:**
    This snapshot ignores --full-refresh by default to prevent loss of Type 2 SCD history.
    All historical tracking (dbt_valid_from/to) would be lost on full-refresh.
    Override: dbt build --select snap_monitored_table_metadata --vars '{allow_full_refresh_anomaly_detection: true}'
    */
    {#
    ENROLLMENT-BASED FILTERING

    Get the list of monitored tables and sources from stg_monitored_tables.
    This model contains only tables enrolled in anomaly detection via
    volume_anomaly or freshness_anomaly tests (extracted from graph.nodes and graph.sources).

    This approach ensures:
    - Only explicitly enrolled tables are tracked (no accidental monitoring)
    - Automatic discovery of new enrollments on each dbt run
    - Centralized enrollment registry for visibility
    - Cross-database support for external source tables

    Note: stg_monitored_tables must be built before this snapshot.
    #}
    {%- set monitored_tables_query -%}
        select distinct
            database_name,
            schema_name,
            table_name,
            full_table_name
        from {{ ref('stg_monitored_tables') }}
    {%- endset -%}

    {%- set monitored_tables_result = run_query(monitored_tables_query) -%}

    {%- set monitored_tables = [] -%}
    {%- set unique_databases = [] -%}

    {%- if execute and monitored_tables_result -%}
        {%- for row in monitored_tables_result.rows -%}
            {%- do monitored_tables.append(
                {
                    "database": row[0],
                    "schema": row[1],
                    "name": row[2],
                    "full_name": row[3],
                }
            ) -%}
            {%- if row[0] not in unique_databases -%}
                {%- do unique_databases.append(row[0]) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {%- if unique_databases | length > 0 -%}
        {%- for db in unique_databases %}
            {#
    HARDCODED DATABASE REFERENCE EXCEPTION

    This snapshot uses {{ db }}.INFORMATION_SCHEMA.TABLES which violates dbt's "no hard-coded
    references" principle. This is a necessary exception for the following reasons:

    1. INFORMATION_SCHEMA is a Snowflake system view that cannot be referenced via dbt's ref() or source()
    2. We must query across multiple databases dynamically (determined at compile time from graph.nodes and graph.sources)
    3. The database names come from dbt's graph (node.database for models, source.database for sources), ensuring environment consistency
    4. Alternative approaches (account_usage) have unacceptable latency (45min-3hr lag)

    Environment Consistency:
    - Database names are extracted from dbt configs:
      * Models: node.database from dbt_project.yml
      * Sources: source.database from _sources.yml files
    - Each environment (dev/prod) has its own dbt_project.yml with appropriate database mappings
    - No manual database names are hardcoded - all come from graph.nodes and graph.sources
    - Enables cross-database monitoring of external source tables (e.g., FIVETRAN_SALESFORCE_DB)
#}
            -- Query {{ db }}.INFORMATION_SCHEMA.TABLES
            select
                -- Unique identifier for snapshot tracking (case-insensitive)
                {{
                    dbt_utils.generate_surrogate_key(
                        [
                            "upper(nullif(table_catalog, ''))",
                            "upper(nullif(table_schema, ''))",
                            "upper(nullif(table_name, ''))",
                        ]
                    )
                }} as table_id,

                -- Snapshot timestamp (hourly grain)
                date_trunc('hour', current_timestamp())::timestamp_ntz
                as snapshot_timestamp,

                -- Convert empty strings to NULL for data quality
                nullif(table_catalog, '') as table_catalog,
                nullif(table_schema, '') as table_schema,
                nullif(table_name, '') as table_name,

                -- Add explicit aliases so volume_metrics_history can reference
                -- database_name/schema_name/full_table_name
                nullif(table_catalog, '') as database_name,
                nullif(table_schema, '') as schema_name,
                nullif(table_catalog, '')
                || '.'
                || nullif(table_schema, '')
                || '.'
                || nullif(table_name, '') as full_table_name,

                -- All other columns from INFORMATION_SCHEMA.TABLES
                * exclude (
                    table_catalog, table_schema, table_name, created, last_altered
                ),

                -- Cast timestamp columns to TIMESTAMP_NTZ for snapshot compatibility
                created::timestamp_ntz as created,
                last_altered::timestamp_ntz as last_altered

            from {{ db }}.information_schema.tables

            where
                -- Only BASE TABLEs (exclude VIEWs - they don't have row_count or
                -- meaningful LAST_ALTERED)
                table_type = 'BASE TABLE'
                -- Exclude records with NULL or empty metadata (data quality
                -- filter) Use NULLIF to convert empty strings to NULL before checking
                and nullif(table_catalog, '') is not null
                and nullif(table_schema, '') is not null
                and nullif(table_name, '') is not null
                -- Only snapshot tables enrolled in anomaly detection via
                -- stg_monitored_tables
                -- Use COALESCE to prevent empty strings from creating false matches
                and upper(
                    coalesce(nullif(table_catalog, ''), 'INVALID_DB')
                    || '.'
                    || coalesce(nullif(table_schema, ''), 'INVALID_SCHEMA')
                    || '.'
                    || coalesce(nullif(table_name, ''), 'INVALID_TABLE')
                ) in (
                    {%- set matching_tables = [] %}
                    {%- for table in monitored_tables %}
                        {%- if table.database == db %}
                            {%- do matching_tables.append(
                                "'" ~ table.full_name ~ "'"
                            ) %}
                        {%- endif %}
                    {%- endfor %}
                    {{ matching_tables | join(", ") }}
                )
            {%- if not loop.last %}

                union all

            {%- endif %}
        {%- endfor %}
    {%- else %}
        -- No enrolled tables found, return empty result matching
        -- INFORMATION_SCHEMA.TABLES schema
        select
            cast(null as varchar) as table_id,
            cast(null as timestamp_ntz) as snapshot_timestamp,
            cast(null as varchar) as table_catalog,
            cast(null as varchar) as table_schema,
            cast(null as varchar) as table_name,
            cast(null as varchar) as table_owner,
            cast(null as varchar) as table_type,
            cast(null as varchar) as is_transient,
            cast(null as varchar) as clustering_key,
            cast(null as number) as row_count,
            cast(null as number) as bytes,
            cast(null as number) as retention_time,
            cast(null as varchar) as self_referencing_column_name,
            cast(null as varchar) as reference_generation,
            cast(null as varchar) as user_defined_type_catalog,
            cast(null as varchar) as user_defined_type_schema,
            cast(null as varchar) as user_defined_type_name,
            cast(null as varchar) as is_insertable_into,
            cast(null as varchar) as is_typed,
            cast(null as varchar) as commit_action,
            cast(null as timestamp_ntz) as created,
            cast(null as timestamp_ntz) as last_altered,
            cast(null as timestamp_ltz) as last_ddl,
            cast(null as varchar) as last_ddl_by,
            cast(null as varchar) as auto_clustering_on,
            cast(null as varchar) as comment,
            cast(null as varchar) as is_temporary,
            cast(null as varchar) as is_iceberg,
            cast(null as varchar) as is_dynamic,
            cast(null as varchar) as is_immutable,
            cast(null as varchar) as is_hybrid
        where false
    {%- endif %}

{% endsnapshot %}
