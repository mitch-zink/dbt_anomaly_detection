{{
    config(
        materialized="table",
        tags=["anomaly_detection", "metadata"],
        full_refresh=(
            false if not var("allow_full_refresh_anomaly_detection", false) else none
        ),
    )
}}

/*
    Staging: Monitored Tables

    Extracts the list of tables and sources enrolled in anomaly detection by scanning:
    - graph.nodes (dbt models)
    - graph.sources (source tables from external databases)

    Finds all tables with volume_anomaly or freshness_anomaly tests.

    **Cross-Database Support:**
    Fully supports source tables from external databases (e.g., FIVETRAN_SALESFORCE_DB,
    FIVETRAN_HEAP_DB). Each source's database property is captured and used to query
    the correct INFORMATION_SCHEMA.

    **Full Refresh Protection:**
    This model ignores --full-refresh flag by default to prevent accidental rebuilds.
    Override: dbt run --select stg_monitored_tables --vars '{allow_full_refresh_anomaly_detection: true}'

    This model serves as the enrollment registry - only tables appearing here will be
    tracked by the snap_monitored_table_metadata snapshot.
*/
{%- set monitored_tables = [] -%}
{%- if execute -%}
    {%- for node in graph.nodes.values() -%}
        {%- if node.resource_type == "test" -%}
            {%- if node.test_metadata and (
                node.test_metadata.name == "volume_anomaly"
                or node.test_metadata.name == "freshness_anomaly"
            ) -%}
                {%- if node.depends_on.nodes -%}
                    {%- for dep_node_id in node.depends_on.nodes -%}
                        {%- if dep_node_id.startswith(
                            "model."
                        ) or dep_node_id.startswith("source.") -%}
                            {#
                            CROSS-DATABASE SOURCE TABLE SUPPORT:

                            Source tables (external databases) exist in graph.sources, NOT graph.nodes.
                            This conditional enables monitoring of:
                            - dbt models: accessed via graph.nodes.get()
                            - Source tables: accessed via graph.sources.get()

                            This allows anomaly detection on external databases like:
                            FIVETRAN_SALESFORCE_DB, FIVETRAN_HEAP_DB, etc.
                            #}
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
                                {%- if table_info not in monitored_tables -%}
                                    {%- do monitored_tables.append(table_info) -%}
                                {%- endif -%}
                            {%- endif -%}
                        {%- endif -%}
                    {%- endfor -%}
                {%- endif -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

{%- if monitored_tables | length > 0 %}
    {%- for table in monitored_tables %}
        {% if loop.first %} select
        {% else %}
            union all
            select
        {% endif %}
            '{{ table.database | upper }}' as database_name,
            '{{ table.schema | upper }}' as schema_name,
            '{{ table.name | upper }}' as table_name,
            upper('{{ table.full_name }}') as full_table_name,
            current_timestamp() as _loaded_at
    {%- endfor %}
{%- else %}
    -- No monitored tables found, return empty result
    select
        cast(null as varchar) as database_name,
        cast(null as varchar) as schema_name,
        cast(null as varchar) as table_name,
        cast(null as varchar) as full_table_name,
        cast(null as timestamp_ntz) as _loaded_at
    where false
{%- endif %}
