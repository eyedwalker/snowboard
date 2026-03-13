"""Schema discovery tools for exploring Snowflake tables and views."""

from src.agent.tools.registry import tool_definition
from src.connectors.snowflake_connector import execute_query


@tool_definition(
    name="list_schemas",
    description="List all available schemas in the DEV_ANALYTICS database.",
    parameters={"properties": {}, "required": []},
)
def list_schemas():
    df = execute_query("SHOW SCHEMAS IN DATABASE DEV_ANALYTICS")
    if df.empty:
        return []
    return df["name"].tolist()


@tool_definition(
    name="list_tables",
    description="List tables and views in a schema with row counts. Use schema='DATAWAREHOUSE' for clean business views or 'DATAMART' for pre-built dim/fact tables.",
    parameters={
        "properties": {
            "schema": {
                "type": "string",
                "description": "Schema name (DATAWAREHOUSE or DATAMART)",
                "default": "DATAWAREHOUSE",
            },
            "filter": {
                "type": "string",
                "description": "Optional keyword to filter table names (case-insensitive)",
            },
        },
        "required": [],
    },
)
def list_tables(schema="DATAWAREHOUSE", filter=None):
    tables_df = execute_query(f"SHOW TABLES IN SCHEMA DEV_ANALYTICS.{schema}", limit=500)
    views_df = execute_query(f"SHOW VIEWS IN SCHEMA DEV_ANALYTICS.{schema}", limit=500)

    results = []
    for df, kind in [(tables_df, "TABLE"), (views_df, "VIEW")]:
        if df.empty:
            continue
        for _, row in df.iterrows():
            name = row.get("name", "")
            if filter and filter.lower() not in name.lower():
                continue
            results.append({
                "name": name,
                "kind": kind,
                "rows": row.get("rows", None),
            })

    return sorted(results, key=lambda x: x["name"])


@tool_definition(
    name="describe_table",
    description="Get column names, types, and sample values for a table or view. Use this to understand what data is in a table before writing a query.",
    parameters={
        "properties": {
            "table": {"type": "string", "description": "Table or view name"},
            "schema": {
                "type": "string",
                "description": "Schema name",
                "default": "DATAWAREHOUSE",
            },
        },
        "required": ["table"],
    },
)
def describe_table(table, schema="DATAWAREHOUSE"):
    cols_df = execute_query(
        f'SHOW COLUMNS IN DEV_ANALYTICS.{schema}."{table}"', limit=200
    )
    if cols_df.empty:
        return {"error": f"Table {schema}.{table} not found"}

    columns = []
    for _, row in cols_df.iterrows():
        columns.append({
            "name": row.get("column_name", ""),
            "type": row.get("data_type", ""),
            "nullable": row.get("null?", ""),
        })

    sample_df = execute_query(
        f'SELECT * FROM DEV_ANALYTICS.{schema}."{table}" LIMIT 3'
    )
    sample = sample_df.to_dict(orient="records") if not sample_df.empty else []

    return {"table": f"{schema}.{table}", "columns": columns, "sample_rows": sample}


@tool_definition(
    name="search_columns",
    description="Search for columns across all tables/views matching a keyword (e.g. 'patient', 'revenue', 'office'). Useful for finding where specific data lives.",
    parameters={
        "properties": {
            "keyword": {
                "type": "string",
                "description": "Keyword to search for in column names",
            },
            "schema": {
                "type": "string",
                "description": "Schema to search in",
                "default": "DATAWAREHOUSE",
            },
        },
        "required": ["keyword"],
    },
)
def search_columns(keyword, schema="DATAWAREHOUSE"):
    df = execute_query(
        f"""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM DEV_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
          AND LOWER(COLUMN_NAME) LIKE '%{keyword.lower()}%'
        ORDER BY TABLE_NAME, COLUMN_NAME
        """,
        limit=200,
    )
    if df.empty:
        return []
    return df.to_dict(orient="records")
