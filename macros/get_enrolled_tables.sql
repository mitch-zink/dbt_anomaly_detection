{% macro get_enrolled_tables(test_names) %}
    {#
    Shared macro to extract enrolled tables from dbt graph.

    Scans graph.nodes for test nodes matching the given test_names, then resolves
    the dependent model/source from graph.nodes or graph.sources.

    Parameters:
        test_names: list of test names to scan for (e.g., ["volume_anomaly", "freshness_anomaly"])

    Returns:
        List of dicts with keys:
        - database, schema, name, full_name: table identification
        - test_name: which test enrolled this table
        - kwargs: test configuration kwargs (sensitivity, timestamp_column, etc.)
    #}
    {%- set enrolled = [] -%}
    {%- if execute -%}
        {%- for node in graph.nodes.values() -%}
            {%- if node.resource_type == "test" -%}
                {%- if node.test_metadata and node.test_metadata.name in test_names -%}
                    {%- set test_kwargs = node.test_metadata.kwargs -%}
                    {%- if node.depends_on.nodes -%}
                        {%- for dep_node_id in node.depends_on.nodes -%}
                            {%- if dep_node_id.startswith(
                                "model."
                            ) or dep_node_id.startswith("source.") -%}
                                {%- if dep_node_id.startswith("source.") -%}
                                    {%- set ref_node = graph.sources.get(
                                        dep_node_id
                                    ) -%}
                                {%- else -%}
                                    {%- set ref_node = graph.nodes.get(dep_node_id) -%}
                                {%- endif -%}
                                {%- if ref_node -%}
                                    {%- set table_name = (
                                        ref_node.name
                                        if ref_node.resource_type == "model"
                                        else ref_node.identifier
                                    ) -%}
                                    {%- set table_info = {
                                        "database": ref_node.database,
                                        "schema": ref_node.schema,
                                        "name": table_name,
                                        "full_name": ref_node.database
                                        ~ "."
                                        ~ ref_node.schema
                                        ~ "."
                                        ~ table_name,
                                        "test_name": node.test_metadata.name,
                                        "kwargs": test_kwargs,
                                    } -%}
                                    {%- if table_name not in [
                                        "volume_metrics_history",
                                        "freshness_metrics_history",
                                    ] -%}
                                        {%- do enrolled.append(table_info) -%}
                                    {%- endif -%}
                                {%- endif -%}
                            {%- endif -%}
                        {%- endfor -%}
                    {%- endif -%}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {{ return(enrolled) }}
{% endmacro %}
