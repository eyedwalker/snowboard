"""
MCP Server for Snowflake Eyecare Analytics.
Exposes agent tools over stdio so Claude Desktop/Code can query Snowflake directly.

Usage:
    python -m src.mcp.server
"""

import sys
import os
import json
import asyncio

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dotenv import load_dotenv
load_dotenv()

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent, Resource

from src.agent.tools.registry import TOOL_REGISTRY, dispatch_tool

# Import tools to trigger registration
import src.agent.tools.schema_tools
import src.agent.tools.query_tools
import src.agent.tools.analytics_tools
import src.agent.tools.cortex_tools

app = Server("snowflake-eyecare")


@app.list_tools()
async def list_tools() -> list[Tool]:
    """List all registered agent tools as MCP tools."""
    tools = []
    for t in TOOL_REGISTRY:
        tools.append(
            Tool(
                name=t["name"],
                description=t["description"],
                inputSchema=t["input_schema"],
            )
        )
    return tools


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Dispatch a tool call and return the result."""
    result = dispatch_tool(name, arguments)
    return [TextContent(type="text", text=result)]


@app.list_resources()
async def list_resources() -> list[Resource]:
    """Expose schema overviews as MCP resources for context."""
    return [
        Resource(
            uri="snowflake://dev_analytics/datawarehouse/overview",
            name="DATAWAREHOUSE Schema Overview",
            description="118 clean business views (Patient, Order, InvoiceSummary, BillingTransaction, etc.) built on top of 130 staging/hub tables.",
            mimeType="text/plain",
        ),
        Resource(
            uri="snowflake://dev_analytics/datamart/overview",
            name="DATAMART Schema Overview",
            description="74 dimensional model tables: Dim/Fact for Billing, AR, POS, Sales, Dashboard analytics.",
            mimeType="text/plain",
        ),
    ]


@app.read_resource()
async def read_resource(uri: str) -> str:
    """Return schema details for a resource."""
    if "datawarehouse" in uri:
        from src.agent.tools.schema_tools import list_tables
        tables = list_tables(schema="DATAWAREHOUSE")
        views = [t for t in tables if t["kind"] == "VIEW"]
        lines = ["DATAWAREHOUSE Views (clean business layer):", ""]
        for v in views:
            lines.append(f"  {v['name']}")
        return "\n".join(lines)
    elif "datamart" in uri:
        from src.agent.tools.schema_tools import list_tables
        tables = list_tables(schema="DATAMART")
        lines = ["DATAMART Tables (dimensional model):", ""]
        for t in tables:
            lines.append(f"  {t['name']}")
        return "\n".join(lines)
    return "Unknown resource"


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
