
import streamlit as st
import json
import pandas as pd
from datetime import datetime

st.set_page_config(
    page_title="Eyecare Database Knowledge System",
    page_icon="🏥",
    layout="wide"
)

st.title("🏥 Eyecare Database Knowledge Management System")
st.markdown("*Comprehensive analysis of database structure, business logic, and workflows*")

# Load knowledge base
@st.cache_data
def load_knowledge_base():
    try:
        with open('docs/eyecare_knowledge_base.json', 'r') as f:
            return json.load(f)
    except:
        return {}

kb = load_knowledge_base()

if kb:
    # Sidebar navigation
    st.sidebar.title("📚 Knowledge Areas")
    section = st.sidebar.selectbox("Select Section", [
        "📊 Overview",
        "🔗 Foreign Key Relationships", 
        "⚙️ Stored Procedures",
        "🧮 Functions & Calculations",
        "👁️ Views & Patterns",
        "🔄 Business Workflows",
        "🔌 Integration Points",
        "💡 Recommendations"
    ])
    
    if section == "📊 Overview":
        st.header("System Overview")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Stored Procedures", 
                     sum(kb.get('stored_procedures', {}).get(cat, {}).get('count', 0) 
                         for cat in kb.get('stored_procedures', {})))
        
        with col2:
            st.metric("Functions", kb.get('functions', {}).get('total_count', 0))
        
        with col3:
            st.metric("Views", kb.get('views', {}).get('total_count', 0))
        
        with col4:
            st.metric("Workflows", len(kb.get('workflows', {})))
        
        # Business logic categories
        st.subheader("Business Logic Distribution")
        if 'stored_procedures' in kb:
            categories = []
            counts = []
            for cat, data in kb['stored_procedures'].items():
                categories.append(cat.title())
                counts.append(data.get('count', 0))
            
            df = pd.DataFrame({'Category': categories, 'Count': counts})
            st.bar_chart(df.set_index('Category'))
    
    elif section == "🔗 Foreign Key Relationships":
        st.header("Foreign Key Relationships")
        
        if 'foreign_keys' in kb:
            for relationship_type, data in kb['foreign_keys'].items():
                st.subheader(f"{relationship_type.replace('_', ' ').title()}")
                st.write(data)
    
    elif section == "⚙️ Stored Procedures":
        st.header("Stored Procedures Analysis")
        
        if 'stored_procedures' in kb:
            for category, data in kb['stored_procedures'].items():
                with st.expander(f"{category.title()} ({data.get('count', 0)} procedures)"):
                    st.write(f"**Business Impact:** {data.get('business_impact', 'Not analyzed')}")
                    st.write(f"**Complexity:** {data.get('complexity_analysis', 'Not analyzed')}")
                    
                    if 'procedures' in data:
                        st.subheader("Key Procedures")
                        for proc in data['procedures'][:10]:  # Show top 10
                            st.write(f"• {proc}")
    
    elif section == "💡 Recommendations":
        st.header("Recommendations")
        
        if 'recommendations' in kb:
            for rec_type, recommendations in kb['recommendations'].items():
                st.subheader(f"{rec_type.replace('_', ' ').title()}")
                for rec in recommendations:
                    st.write(f"• {rec}")

else:
    st.error("Knowledge base not found. Please run the analysis first.")
        