
import streamlit as st
import json
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Procedure Intelligence", layout="wide", page_icon="🧠")
st.title("🧠 Stored Procedure Business Intelligence")
st.markdown("*Deep insights from 610+ stored procedures in the eyecare database*")

@st.cache_data
def load_insights():
    try:
        with open('docs/procedure_intelligence.json', 'r') as f:
            return json.load(f)
    except:
        return {}

insights = load_insights()

if insights:
    # Business Intelligence Opportunities
    st.header("💡 Business Intelligence Opportunities")
    
    if 'business_intelligence' in insights:
        bi_ops = insights['business_intelligence']
        
        # Create tabs for each BI area
        tab_names = list(bi_ops.keys())
        tabs = st.tabs([name.replace('_', ' ').title() for name in tab_names])
        
        for i, (area_name, area_data) in enumerate(bi_ops.items()):
            with tabs[i]:
                st.subheader(area_data.get('description', ''))
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Key Performance Indicators:**")
                    for kpi in area_data.get('kpis', []):
                        st.write(f"• {kpi}")
                
                with col2:
                    st.write("**Recommended Dashboards:**")
                    for dashboard in area_data.get('dashboards', []):
                        st.write(f"• {dashboard}")
                
                if 'key_procedures' in area_data and area_data['key_procedures']:
                    st.write(f"**Related Procedures ({len(area_data['key_procedures'])}):**")
                    for proc in area_data['key_procedures'][:10]:
                        st.write(f"• {proc}")
    
    # Workflow Patterns
    st.header("🔄 Business Workflow Patterns")
    
    if 'workflow_patterns' in insights:
        workflows = insights['workflow_patterns']
        
        for workflow_name, workflow_data in workflows.items():
            with st.expander(f"{workflow_name.replace('_', ' ').title()}"):
                st.write(f"**Description:** {workflow_data.get('description', '')}")
                
                if 'steps' in workflow_data:
                    st.write("**Workflow Steps:**")
                    for step in workflow_data['steps']:
                        st.write(f"• {step}")
                
                if 'integration_points' in workflow_data:
                    st.write("**Integration Points:**")
                    for point in workflow_data['integration_points']:
                        st.write(f"• {point}")
                
                if 'optimization_opportunities' in workflow_data:
                    st.write("**Optimization Opportunities:**")
                    for opp in workflow_data['optimization_opportunities']:
                        st.write(f"• {opp}")
    
    # Datamart Implications
    st.header("🏗️ Datamart Design Implications")
    
    if 'datamart_implications' in insights:
        dm_data = insights['datamart_implications']
        
        if 'fact_table_opportunities' in dm_data:
            st.subheader("Fact Table Opportunities")
            
            for fact_name, fact_data in dm_data['fact_table_opportunities'].items():
                with st.expander(f"{fact_name.replace('_', ' ').title()}"):
                    st.write(f"**Grain:** {fact_data.get('grain', '')}")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Measures:**")
                        for measure in fact_data.get('measures', []):
                            st.write(f"• {measure}")
                    
                    with col2:
                        st.write("**Dimensions:**")
                        for dim in fact_data.get('dimensions', []):
                            st.write(f"• {dim}")
        
        if 'aggregation_opportunities' in dm_data:
            st.subheader("Aggregation Opportunities")
            
            for agg_name, agg_desc in dm_data['aggregation_opportunities'].items():
                st.write(f"**{agg_name.replace('_', ' ').title()}:** {agg_desc}")

else:
    st.error("No insights found. Run the intelligence extractor first.")
        