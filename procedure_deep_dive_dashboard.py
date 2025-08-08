
import streamlit as st
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Procedure Deep Dive", layout="wide", page_icon="🔍")
st.title("🔍 Stored Procedure Deep Dive Analysis")

@st.cache_data
def load_insights():
    try:
        with open('docs/procedure_deep_insights.json', 'r') as f:
            return json.load(f)
    except:
        return {}

insights = load_insights()

if insights:
    # Overview metrics
    st.header("📊 Overview")
    
    if 'procedure_inventory' in insights:
        inv = insights['procedure_inventory']
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Procedures", inv.get('total_procedures', 0))
        with col2:
            st.metric("With Source Code", inv.get('with_source_code', 0))
        with col3:
            st.metric("Schemas", len(inv.get('schemas', {})))
        with col4:
            st.metric("Avg Definition Length", f"{inv.get('avg_definition_length', 0):.0f}")
        
        # Complexity distribution
        if 'complexity_distribution' in inv:
            st.subheader("Complexity Distribution")
            complexity_data = inv['complexity_distribution']
            fig = px.pie(
                values=list(complexity_data.values()),
                names=list(complexity_data.keys()),
                title="Procedure Complexity"
            )
            st.plotly_chart(fig)
    
    # Business domains
    st.header("🏢 Business Domain Analysis")
    
    if 'naming_patterns' in insights:
        patterns = insights['naming_patterns']
        
        if 'business_domains' in patterns:
            domains_df = pd.DataFrame(
                list(patterns['business_domains'].items()),
                columns=['Domain', 'Count']
            )
            
            fig = px.bar(
                domains_df,
                x='Domain',
                y='Count',
                title="Procedures by Business Domain"
            )
            st.plotly_chart(fig)
            
            # Show example procedures for each domain
            if 'domain_procedures' in patterns:
                st.subheader("Example Procedures by Domain")
                for domain, procedures in patterns['domain_procedures'].items():
                    if procedures:
                        with st.expander(f"{domain.title()} ({len(procedures)} examples)"):
                            for proc in procedures:
                                st.write(f"• {proc}")
    
    # Action verb analysis
    if 'naming_patterns' in insights and 'action_verbs' in insights['naming_patterns']:
        st.header("⚡ Action Verb Analysis")
        
        actions = insights['naming_patterns']['action_verbs']
        actions_df = pd.DataFrame(
            list(actions.items()),
            columns=['Action', 'Count']
        )
        
        fig = px.bar(
            actions_df,
            x='Action',
            y='Count',
            title="Procedures by Action Type"
        )
        st.plotly_chart(fig)
    
    # Parameter analysis
    if 'parameter_analysis' in insights:
        st.header("🔧 Parameter Analysis")
        
        params = insights['parameter_analysis']
        
        if 'status' not in params:  # If we have actual parameter data
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total Parameters", params.get('total_parameters', 0))
                st.metric("Procedures with Params", params.get('procedures_with_params', 0))
            
            with col2:
                st.metric("Avg Params per Procedure", f"{params.get('avg_params_per_procedure', 0):.1f}")
                st.metric("Output Parameters", params.get('output_parameters', 0))
            
            # Data type distribution
            if 'data_type_distribution' in params:
                st.subheader("Parameter Data Types")
                types_df = pd.DataFrame(
                    list(params['data_type_distribution'].items()),
                    columns=['Data Type', 'Count']
                )
                fig = px.bar(types_df, x='Data Type', y='Count')
                st.plotly_chart(fig)
        else:
            st.warning("Parameter analysis unavailable - access may be restricted")
    
    # Business Intelligence Opportunities
    if 'business_intelligence' in insights:
        st.header("💡 Business Intelligence Opportunities")
        
        bi_ops = insights['business_intelligence']
        
        for opportunity, details in bi_ops.items():
            with st.expander(f"{opportunity.replace('_', ' ').title()}"):
                st.write(f"**Description:** {details.get('description', 'N/A')}")
                
                if 'potential_kpis' in details:
                    st.write("**Potential KPIs:**")
                    for kpi in details['potential_kpis']:
                        st.write(f"• {kpi}")
                
                if 'procedures' in details and details['procedures']:
                    st.write(f"**Related Procedures ({len(details['procedures'])}):**")
                    for proc in details['procedures'][:5]:  # Show first 5
                        st.write(f"• {proc}")

else:
    st.error("No insights found. Run the deep dive analysis first.")
        