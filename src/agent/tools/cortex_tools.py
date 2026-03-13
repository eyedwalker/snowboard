"""Snowflake Cortex AI tools — runs LLM workloads server-side on Snowflake."""

from src.agent.tools.registry import tool_definition
from src.connectors.snowflake_connector import execute_query


@tool_definition(
    name="cortex_complete",
    description="Generate text using Snowflake Cortex LLM (runs inside Snowflake). Useful for analysis that must stay within the Snowflake security perimeter.",
    parameters={
        "properties": {
            "prompt": {"type": "string", "description": "The prompt to send to Cortex"},
            "model": {
                "type": "string",
                "description": "Cortex model name",
                "default": "mixtral-8x7b",
            },
        },
        "required": ["prompt"],
    },
)
def cortex_complete(prompt, model="mixtral-8x7b"):
    safe_prompt = prompt.replace("'", "''")
    sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{safe_prompt}') AS result"
    df = execute_query(sql, limit=1)
    if df.empty:
        return {"error": "Cortex did not return a result"}
    return {"result": str(df.iloc[0]["RESULT"])}


@tool_definition(
    name="cortex_summarize",
    description="Summarize text using Snowflake Cortex SUMMARIZE function.",
    parameters={
        "properties": {
            "text": {"type": "string", "description": "Text to summarize"},
        },
        "required": ["text"],
    },
)
def cortex_summarize(text):
    safe_text = text.replace("'", "''")
    sql = f"SELECT SNOWFLAKE.CORTEX.SUMMARIZE('{safe_text}') AS result"
    df = execute_query(sql, limit=1)
    if df.empty:
        return {"error": "Cortex did not return a result"}
    return {"result": str(df.iloc[0]["RESULT"])}
