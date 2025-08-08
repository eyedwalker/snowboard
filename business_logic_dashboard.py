
import streamlit as st
import json
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Business Logic Analysis", layout="wide", page_icon="🧠")
st.title("🧠 Stored Procedure Business Logic Analysis")
st.markdown("*Deep analysis of actual business logic from 610+ stored procedures*")

@st.cache_data
def load_business_insights():
    try:
        with open('docs/business_logic_insights.json', 'r') as f:
            return json.load(f)
    except:
        return {}

insights = load_business_insights()

if insights:
    # Overview metrics
    st.header("📊 Business Logic Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        financial_count = insights.get('financial_calculations', {}).get('total_financial_procedures', 0)
        st.metric("Financial Procedures", financial_count)
    
    with col2:
        clinical_count = insights.get('clinical_workflows', {}).get('total_clinical_procedures', 0)
        st.metric("Clinical Procedures", clinical_count)
    
    with col3:
        integration_count = insights.get('integration_patterns', {}).get('total_integration_procedures', 0)
        st.metric("Integration Procedures", integration_count)
    
    with col4:
        rules_count = insights.get('business_rules', {}).get('total_rule_procedures', 0)
        st.metric("Business Rule Procedures", rules_count)
    
    # Key Procedures
    st.header("⭐ Most Important Procedures")
    
    if 'key_procedures' in insights:
        key_procs = insights['key_procedures'].get('top_20_procedures', [])
        
        if key_procs:
            # Create DataFrame for visualization
            df = pd.DataFrame(key_procs)
            
            # Business area distribution
            if 'business_area' in df.columns:
                area_counts = df['business_area'].value_counts()
                fig = px.pie(
                    values=area_counts.values,
                    names=area_counts.index,
                    title="Key Procedures by Business Area"
                )
                st.plotly_chart(fig)
            
            # Top procedures table
            st.subheader("Top 10 Most Important Procedures")
            display_df = df[['name', 'business_area', 'importance_score', 'definition_length']].head(10)
            st.dataframe(display_df, use_container_width=True)
            
            # Detailed view
            st.subheader("Procedure Details")
            selected_proc = st.selectbox("Select a procedure to view details:", 
                                       [proc['name'] for proc in key_procs[:10]])
            
            if selected_proc:
                proc_details = next((p for p in key_procs if p['name'] == selected_proc), None)
                if proc_details:
                    st.write(f"**Business Area:** {proc_details.get('business_area', 'N/A')}")
                    st.write(f"**Importance Score:** {proc_details.get('importance_score', 0)}")
                    st.write(f"**Definition Length:** {proc_details.get('definition_length', 0):,} characters")
                    st.write(f"**Last Modified:** {proc_details.get('last_modified', 'N/A')}")
                    
                    if proc_details.get('preview'):
                        st.subheader("Code Preview")
                        st.code(proc_details['preview'], language='sql')
    
    # Financial Analysis
    st.header("💰 Financial Calculation Analysis")
    
    if 'financial_calculations' in insights:
        fin_data = insights['financial_calculations']
        
        st.write(f"**Total Financial Procedures:** {fin_data.get('total_financial_procedures', 0)}")
        st.write(f"**Business Value:** {fin_data.get('business_value', 'N/A')}")
        
        if 'calculation_patterns' in fin_data:
            patterns = fin_data['calculation_patterns']
            if patterns:
                st.subheader("Financial Calculation Patterns")
                for pattern, procedures in patterns.items():
                    st.write(f"**{pattern.replace('_', ' ').title()}:** {len(procedures)} procedures")
                    with st.expander(f"View {pattern} procedures"):
                        for proc in procedures[:10]:
                            st.write(f"• {proc}")
        
        if 'key_procedures' in fin_data:
            st.subheader("Key Financial Procedures")
            for proc in fin_data['key_procedures'][:5]:
                with st.expander(proc['name']):
                    st.write(f"**Length:** {proc['definition_length']:,} characters")
                    if proc.get('preview'):
                        st.code(proc['preview'], language='sql')
    
    # Clinical Analysis
    st.header("🏥 Clinical Workflow Analysis")
    
    if 'clinical_workflows' in insights:
        clinical_data = insights['clinical_workflows']
        
        st.write(f"**Total Clinical Procedures:** {clinical_data.get('total_clinical_procedures', 0)}")
        st.write(f"**Business Value:** {clinical_data.get('business_value', 'N/A')}")
        
        if 'workflow_patterns' in clinical_data:
            patterns = clinical_data['workflow_patterns']
            if patterns:
                st.subheader("Clinical Workflow Patterns")
                for pattern, procedures in patterns.items():
                    st.write(f"**{pattern.replace('_', ' ').title()}:** {len(procedures)} procedures")

else:
    st.error("No business logic insights found. Run the analyzer first.")
        