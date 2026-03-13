"""
Streamlit Chat UI for the Eyecare Analytics Agent.

Usage:
    streamlit run src/agent/streamlit_ui.py
"""

import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import plotly.io as pio
import pandas as pd

from src.agent.chat_agent import ChatAgent

st.set_page_config(page_title="Eyecare Analytics Agent", page_icon="🏥", layout="wide")
st.title("Eyecare Analytics Agent")
st.caption("Ask questions about your VSP Snowflake data in natural language")

# Initialize agent in session state
if "agent" not in st.session_state:
    st.session_state.agent = ChatAgent()
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Sidebar
with st.sidebar:
    st.header("Settings")
    if st.button("New Conversation"):
        st.session_state.agent.reset()
        st.session_state.chat_history = []
        st.rerun()

    st.divider()
    st.markdown("**Available tools:**")
    st.markdown("""
    - `list_tables` — Browse schemas
    - `describe_table` — Column details
    - `search_columns` — Find data
    - `run_query` — Execute SQL
    - `revenue_summary` — Revenue KPIs
    - `patient_summary` — Patient stats
    - `top_products` — Product rankings
    - `appointment_utilization` — Scheduling
    - `create_bar_chart` — Bar charts
    - `create_time_series` — Line charts
    - `create_pie_chart` — Pie charts
    - `cortex_complete` — Snowflake AI
    """)

    st.divider()
    st.markdown("**Example questions:**")
    st.markdown("""
    - What tables are available?
    - Show me revenue by month
    - What are the top 10 products?
    - How many patients per office?
    - Describe the InvoiceSummary table
    """)

# Render chat history
for msg in st.session_state.chat_history:
    with st.chat_message(msg["role"]):
        if msg["role"] == "assistant":
            # Render tool calls in expanders
            if "tool_calls" in msg and msg["tool_calls"]:
                for tc in msg["tool_calls"]:
                    with st.expander(f"🔧 {tc['tool']}({', '.join(f'{k}={v!r}' for k, v in tc['input'].items())})"):
                        output = tc["output"]
                        try:
                            parsed = json.loads(output) if isinstance(output, str) else output
                            # Check if it's a plotly chart
                            if isinstance(parsed, dict) and "data" in parsed and "layout" in parsed:
                                fig = pio.from_json(json.dumps(parsed))
                                st.plotly_chart(fig, use_container_width=True)
                            # Check if it's query results with rows
                            elif isinstance(parsed, dict) and "rows" in parsed and parsed["rows"]:
                                df = pd.DataFrame(parsed["rows"])
                                st.dataframe(df, use_container_width=True)
                            elif isinstance(parsed, dict) and "data" in parsed and isinstance(parsed["data"], list):
                                df = pd.DataFrame(parsed["data"])
                                st.dataframe(df, use_container_width=True)
                            else:
                                st.json(parsed)
                        except (json.JSONDecodeError, TypeError):
                            st.code(str(output))

            st.markdown(msg["text"])
        else:
            st.markdown(msg["text"])

# Chat input
if prompt := st.chat_input("Ask about your eyecare data..."):
    # Show user message
    st.session_state.chat_history.append({"role": "user", "text": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get agent response
    with st.chat_message("assistant"):
        with st.spinner("Analyzing..."):
            result = st.session_state.agent.ask(prompt)

        # Render tool calls
        if result["tool_calls"]:
            for tc in result["tool_calls"]:
                with st.expander(f"🔧 {tc['tool']}({', '.join(f'{k}={v!r}' for k, v in tc['input'].items())})"):
                    output = tc["output"]
                    try:
                        parsed = json.loads(output) if isinstance(output, str) else output
                        if isinstance(parsed, dict) and "data" in parsed and "layout" in parsed:
                            fig = pio.from_json(json.dumps(parsed))
                            st.plotly_chart(fig, use_container_width=True)
                        elif isinstance(parsed, dict) and "rows" in parsed and parsed["rows"]:
                            df = pd.DataFrame(parsed["rows"])
                            st.dataframe(df, use_container_width=True)
                        elif isinstance(parsed, dict) and "data" in parsed and isinstance(parsed["data"], list):
                            df = pd.DataFrame(parsed["data"])
                            st.dataframe(df, use_container_width=True)
                        else:
                            st.json(parsed)
                    except (json.JSONDecodeError, TypeError):
                        st.code(str(output))

        st.markdown(result["text"])

    st.session_state.chat_history.append({
        "role": "assistant",
        "text": result["text"],
        "tool_calls": result["tool_calls"],
    })
