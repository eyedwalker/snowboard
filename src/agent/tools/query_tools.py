"""Safe query execution tools with read-only validation."""

import re
from src.agent.tools.registry import tool_definition
from src.connectors.snowflake_connector import execute_query


FORBIDDEN_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|MERGE|GRANT|REVOKE|CALL)\b",
    re.IGNORECASE,
)


def _validate_read_only(sql: str):
    """Ensure the query is read-only."""
    stripped = sql.strip().rstrip(";")
    if FORBIDDEN_KEYWORDS.search(stripped):
        raise ValueError(
            f"Only SELECT/WITH/SHOW/DESCRIBE queries are allowed. "
            f"Found forbidden keyword in: {stripped[:100]}"
        )


@tool_definition(
    name="run_query",
    description="Execute a read-only SQL query against Snowflake and return results. Only SELECT, WITH, SHOW, and DESCRIBE statements are allowed. Results are limited to prevent oversized responses.",
    parameters={
        "properties": {
            "sql": {"type": "string", "description": "The SQL query to execute (SELECT only)"},
            "limit": {
                "type": "integer",
                "description": "Max rows to return (default 1000)",
                "default": 1000,
            },
            "schema": {
                "type": "string",
                "description": "Schema context (DATAWAREHOUSE or DATAMART)",
                "default": "DATAWAREHOUSE",
            },
        },
        "required": ["sql"],
    },
)
def run_query(sql, limit=1000, schema="DATAWAREHOUSE"):
    _validate_read_only(sql)
    df = execute_query(sql, schema=schema, limit=limit)
    if df.empty:
        return {"columns": [], "rows": [], "row_count": 0}
    return {
        "columns": list(df.columns),
        "rows": df.head(limit).to_dict(orient="records"),
        "row_count": len(df),
    }


@tool_definition(
    name="explain_query",
    description="Run EXPLAIN on a SQL query to see the execution plan. Useful for understanding query performance before running expensive queries.",
    parameters={
        "properties": {
            "sql": {"type": "string", "description": "The SQL query to explain"},
        },
        "required": ["sql"],
    },
)
def explain_query(sql):
    _validate_read_only(sql)
    df = execute_query(f"EXPLAIN {sql}", limit=100)
    if df.empty:
        return {"plan": "No plan returned"}
    return {"plan": df.to_dict(orient="records")}
