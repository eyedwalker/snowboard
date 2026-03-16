"""
AWS Lambda handler for Bedrock Agent Action Group.
Receives action group invocations and dispatches to Snowflake tools.

Bedrock Agent sends events in this format:
{
    "messageVersion": "1.0",
    "agent": {"name": "...", "id": "...", "alias": "...", "version": "..."},
    "inputText": "original user message",
    "sessionId": "...",
    "actionGroup": "snowflake-analytics",
    "apiPath": "/list_tables",
    "httpMethod": "POST",
    "parameters": [...],
    "requestBody": {"content": {"application/json": {"properties": [...]}}}
}
"""

import json
import os
import sys
import logging

# Add package root to path for imports
# In Lambda, handler.py is at /var/task/handler.py with src/ at /var/task/src/
# Locally, handler.py is in lambda/ so we also add the parent (project root)
_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _here)
sys.path.insert(0, os.path.dirname(_here))

from src.agent.tools.registry import dispatch_tool

# Import only Lambda-safe tools (no plotly/streamlit dependencies)
import src.agent.tools.schema_tools
import src.agent.tools.query_tools
import src.agent.tools.analytics_tools
import src.agent.tools.cortex_tools
import src.agent.tools.report_tools
# NOTE: visualization_tools excluded - requires plotly which is heavy for Lambda

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _extract_parameters(event: dict) -> dict:
    """Extract parameters from Bedrock action group event."""
    params = {}

    # Parameters from path/query params
    if "parameters" in event and event["parameters"]:
        for param in event["parameters"]:
            name = param.get("name", "")
            value = param.get("value", "")
            # Try to parse as JSON for complex types
            try:
                value = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                pass
            params[name] = value

    # Parameters from request body
    if "requestBody" in event and event["requestBody"]:
        content = event["requestBody"].get("content", {})
        json_content = content.get("application/json", {})
        properties = json_content.get("properties", [])
        for prop in properties:
            name = prop.get("name", "")
            value = prop.get("value", "")
            try:
                value = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                pass
            params[name] = value

    return params


def _build_response(event: dict, status_code: int, body: str) -> dict:
    """Build Bedrock action group response format."""
    return {
        "messageVersion": event.get("messageVersion", "1.0"),
        "response": {
            "actionGroup": event.get("actionGroup", "snowflake-analytics"),
            "apiPath": event.get("apiPath", ""),
            "httpMethod": event.get("httpMethod", "POST"),
            "httpStatusCode": status_code,
            "responseBody": {
                "application/json": {
                    "body": body
                }
            }
        }
    }


# Map API paths to tool names
API_PATH_TO_TOOL = {
    "/list_schemas": "list_schemas",
    "/list_tables": "list_tables",
    "/describe_table": "describe_table",
    "/search_columns": "search_columns",
    "/run_query": "run_query",
    "/explain_query": "explain_query",
    "/revenue_summary": "revenue_summary",
    "/patient_summary": "patient_summary",
    "/top_products": "top_products",
    "/appointment_utilization": "appointment_utilization",
    "/cortex_complete": "cortex_complete",
    "/cortex_summarize": "cortex_summarize",
    "/generate_report": "generate_report",
    "/generate_chart": "generate_chart",
    "/multi_report": "multi_report",
}


def handler(event, context):
    """Lambda handler for Bedrock Agent Action Group invocations."""
    logger.info(f"Action group event: {json.dumps(event, default=str)}")

    api_path = event.get("apiPath", "")

    # Forward /schedule_report to the Chat Agent API Lambda (front-office-actions)
    if api_path == "/schedule_report":
        return _forward_to_api_lambda(event)

    tool_name = API_PATH_TO_TOOL.get(api_path)

    if not tool_name:
        return _build_response(event, 400, json.dumps({
            "error": f"Unknown API path: {api_path}",
            "available_paths": list(API_PATH_TO_TOOL.keys()),
        }))

    params = _extract_parameters(event)
    logger.info(f"Dispatching tool: {tool_name} with params: {params}")

    try:
        result = dispatch_tool(tool_name, params)
        logger.info(f"Tool result length: {len(result)}")

        # Truncate if too large for Bedrock response (25KB limit)
        if len(result) > 24000:
            try:
                parsed = json.loads(result)
                if isinstance(parsed, dict) and "rows" in parsed:
                    parsed["rows"] = parsed["rows"][:50]
                    parsed["truncated"] = True
                    result = json.dumps(parsed, default=str)
                elif isinstance(parsed, list) and len(parsed) > 50:
                    result = json.dumps(parsed[:50], default=str)
            except (json.JSONDecodeError, TypeError):
                result = result[:24000] + "\n...[truncated]"

        return _build_response(event, 200, result)

    except Exception as e:
        logger.error(f"Tool execution failed: {e}", exc_info=True)
        return _build_response(event, 500, json.dumps({
            "error": str(e),
            "tool": tool_name,
        }))


def _forward_to_api_lambda(event):
    """Forward action group events to the Chat Agent API Lambda for front-office actions."""
    import boto3

    original_api_path = event.get("apiPath", "")
    api_lambda_name = os.environ.get(
        "API_LAMBDA_NAME", "chat-agent-api-dev"
    )

    # Remap apiPath from /schedule_report to /scheduleReport for front-office-actions handler
    forwarded_event = dict(event)
    forwarded_event["apiPath"] = "/scheduleReport"

    lambda_client = boto3.client("lambda", region_name=os.environ.get("AWS_REGION", "us-west-2"))
    response = lambda_client.invoke(
        FunctionName=api_lambda_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(forwarded_event).encode(),
    )

    result = json.loads(response["Payload"].read())
    logger.info(f"API Lambda response: {json.dumps(result, default=str)[:500]}")

    # Fix apiPath in response to match original request (Bedrock validates this)
    if "response" in result and "apiPath" in result.get("response", {}):
        result["response"]["apiPath"] = original_api_path

    return result
