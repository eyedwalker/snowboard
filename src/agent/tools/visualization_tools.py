"""Visualization tools that generate Plotly chart JSON."""

import json
import plotly.express as px
import plotly.io as pio
import pandas as pd
from src.agent.tools.registry import tool_definition


def _data_to_df(data):
    """Convert dict data (rows list or column-oriented) to DataFrame."""
    if isinstance(data, list):
        return pd.DataFrame(data)
    if isinstance(data, dict):
        if "rows" in data:
            return pd.DataFrame(data["rows"])
        if "data" in data:
            return pd.DataFrame(data["data"])
        return pd.DataFrame(data)
    return pd.DataFrame()


@tool_definition(
    name="create_bar_chart",
    description="Create a bar chart from data. Returns Plotly chart as JSON.",
    parameters={
        "properties": {
            "data": {"description": "List of row objects or query result"},
            "x": {"type": "string", "description": "Column for x-axis"},
            "y": {"type": "string", "description": "Column for y-axis"},
            "title": {"type": "string", "description": "Chart title"},
            "color": {"type": "string", "description": "Column for color grouping (optional)"},
        },
        "required": ["data", "x", "y", "title"],
    },
)
def create_bar_chart(data, x, y, title, color=None):
    df = _data_to_df(data)
    if df.empty:
        return {"error": "No data to chart"}
    fig = px.bar(df, x=x, y=y, title=title, color=color)
    return json.loads(pio.to_json(fig))


@tool_definition(
    name="create_time_series",
    description="Create a time series line chart. Returns Plotly chart as JSON.",
    parameters={
        "properties": {
            "data": {"description": "List of row objects or query result"},
            "date_col": {"type": "string", "description": "Column for dates (x-axis)"},
            "value_col": {"type": "string", "description": "Column for values (y-axis)"},
            "title": {"type": "string", "description": "Chart title"},
            "color": {"type": "string", "description": "Column for color grouping (optional)"},
        },
        "required": ["data", "date_col", "value_col", "title"],
    },
)
def create_time_series(data, date_col, value_col, title, color=None):
    df = _data_to_df(data)
    if df.empty:
        return {"error": "No data to chart"}
    fig = px.line(df, x=date_col, y=value_col, title=title, color=color)
    return json.loads(pio.to_json(fig))


@tool_definition(
    name="create_pie_chart",
    description="Create a pie chart. Returns Plotly chart as JSON.",
    parameters={
        "properties": {
            "data": {"description": "List of row objects or query result"},
            "names": {"type": "string", "description": "Column for slice labels"},
            "values": {"type": "string", "description": "Column for slice values"},
            "title": {"type": "string", "description": "Chart title"},
        },
        "required": ["data", "names", "values", "title"],
    },
)
def create_pie_chart(data, names, values, title):
    df = _data_to_df(data)
    if df.empty:
        return {"error": "No data to chart"}
    fig = px.pie(df, names=names, values=values, title=title)
    return json.loads(pio.to_json(fig))
