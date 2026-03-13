"""Tool registry shared by MCP server and Claude API agent."""

TOOL_REGISTRY: list[dict] = []


def tool_definition(name: str, description: str, parameters: dict):
    """Decorator that registers a tool with its name, description, and JSON Schema."""
    def decorator(func):
        meta = {
            "name": name,
            "description": description,
            "input_schema": {
                "type": "object",
                "properties": parameters.get("properties", {}),
                "required": parameters.get("required", []),
            },
            "handler": func,
        }
        func._tool_meta = meta
        TOOL_REGISTRY.append(meta)
        return func
    return decorator


def get_tool_definitions():
    """Return tool definitions formatted for Claude API tools parameter."""
    return [
        {
            "name": t["name"],
            "description": t["description"],
            "input_schema": t["input_schema"],
        }
        for t in TOOL_REGISTRY
    ]


def dispatch_tool(name: str, arguments: dict):
    """Look up and call a tool by name. Returns the result as a string."""
    for t in TOOL_REGISTRY:
        if t["name"] == name:
            try:
                result = t["handler"](**arguments)
                if isinstance(result, dict) or isinstance(result, list):
                    import json
                    return json.dumps(result, default=str)
                return str(result)
            except Exception as e:
                return f"Error calling {name}: {e}"
    return f"Unknown tool: {name}"
