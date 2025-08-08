
import streamlit as st
import json
import pandas as pd

st.set_page_config(page_title="Advanced Procedure Analysis", layout="wide")
st.title("🔍 Advanced Stored Procedure Analysis")

@st.cache_data
def load_insights():
    try:
        with open('docs/advanced_procedure_insights.json', 'r') as f:
            return json.load(f)
    except:
        return {}

insights = load_insights()

if insights:
    tab1, tab2, tab3, tab4 = st.tabs(["Parameters", "Dependencies", "Calculations", "Workflows"])
    
    with tab1:
        st.header("Parameter Patterns")
        if 'parameter_patterns' in insights:
            for pattern, procedures in insights['parameter_patterns'].items():
                st.subheader(f"{pattern.replace('_', ' ').title()}")
                st.write(f"**{len(procedures)} procedures**")
                with st.expander("View Procedures"):
                    for proc in procedures[:20]:
                        st.write(f"• {proc}")
    
    with tab2:
        st.header("Table Dependencies")
        if 'table_dependencies' in insights:
            st.write(f"**{len(insights['table_dependencies'])} procedures** have table dependencies")
            
            # Most referenced tables
            all_tables = []
            for tables in insights['table_dependencies'].values():
                all_tables.extend(tables)
            
            table_counts = pd.Series(all_tables).value_counts().head(20)
            st.subheader("Most Referenced Tables")
            st.bar_chart(table_counts)
    
    with tab3:
        st.header("Business Calculations")
        if 'business_calculations' in insights:
            calc_types = {}
            for proc, calcs in insights['business_calculations'].items():
                for calc in calcs:
                    calc_types[calc] = calc_types.get(calc, 0) + 1
            
            st.subheader("Calculation Types")
            df = pd.DataFrame(list(calc_types.items()), columns=['Type', 'Count'])
            st.bar_chart(df.set_index('Type'))
    
    with tab4:
        st.header("Workflow Chains")
        if 'workflow_chains' in insights:
            st.write(f"**{len(insights['workflow_chains'])} procedures** call other procedures")
            
            for proc, called_procs in list(insights['workflow_chains'].items())[:10]:
                st.subheader(f"{proc}")
                st.write("Calls:")
                for called in called_procs:
                    st.write(f"• {called}")

else:
    st.error("No insights found. Run the analyzer first.")
        