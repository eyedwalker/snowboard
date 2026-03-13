"""Agent tool library with shared registry for MCP and Claude API."""

from src.agent.tools.registry import TOOL_REGISTRY, tool_definition
from src.agent.tools.schema_tools import list_schemas, list_tables, describe_table, search_columns
from src.agent.tools.query_tools import run_query, explain_query
from src.agent.tools.analytics_tools import revenue_summary, patient_summary, top_products, appointment_utilization
from src.agent.tools.cortex_tools import cortex_complete, cortex_summarize
# visualization_tools excluded — requires plotly (too heavy for Lambda)

__all__ = ["TOOL_REGISTRY", "tool_definition"]
