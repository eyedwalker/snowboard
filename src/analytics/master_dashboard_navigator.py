#!/usr/bin/env python3
"""
Master Dashboard Navigator
Central hub with dropdown access to all specialized eyecare analytics dashboards
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
import os
from dotenv import load_dotenv
from pathlib import Path
import subprocess
import time

# Load environment variables
project_root = Path(__file__).parent.parent.parent
load_dotenv(project_root / ".env")

class MasterDashboardNavigator:
    """Master dashboard navigator with access to all specialized dashboards"""
    
    def __init__(self):
        """Initialize master dashboard navigator"""
        self.dashboards = {
            "🤖 Main Cortex AI Platform": {
                "description": "Complete AI-powered analytics with natural language queries and strategic insights",
                "file": "cortex_ai_analytics_platform.py",
                "port": 8501,
                "url": "http://localhost:8501",
                "features": [
                    "Executive Dashboard with KPIs",
                    "AI Insights & Recommendations", 
                    "Revenue Analytics",
                    "Patient Analytics",
                    "Office Performance",
                    "Data Explorer",
                    "AI Chat Assistant"
                ]
            },
            "🏥 Insurance Carrier Analytics": {
                "description": "Detailed insurance carrier analysis using exact SQL relationships",
                "file": "insurance_carrier_analytics.py", 
                "port": 8503,
                "url": "http://localhost:8503",
                "features": [
                    "Carrier Overview Metrics",
                    "Geographic Distribution",
                    "Carrier Type Analysis (VSP vs Others)",
                    "Contact Information Analysis",
                    "AI-Powered Carrier Insights"
                ]
            },
            "🏥 Comprehensive Insurance Analytics": {
                "description": "Complete insurance ecosystem analytics (Carriers + Plans + Patient Insurance)",
                "file": "comprehensive_insurance_analytics.py",
                "port": 8504, 
                "url": "http://localhost:8504",
                "features": [
                    "Complete Insurance Ecosystem Overview",
                    "Carrier-Plan Relationships",
                    "VSP vs Other Plans Analysis",
                    "Patient Insurance Coverage",
                    "AI-Powered Strategic Insights"
                ]
            },
            "💰 Ultimate Revenue Cycle Analytics": {
                "description": "Complete revenue cycle management using invoice intersection model",
                "file": "ultimate_revenue_cycle_analytics.py",
                "port": 8505,
                "url": "http://localhost:8505", 
                "features": [
                    "Invoice Metrics & KPIs",
                    "Insurance Processing Analytics",
                    "Eligibility Analysis",
                    "Revenue by Service Type",
                    "Insurance vs Cash Analysis",
                    "Complete Revenue Cycle Data"
                ]
            },
            "💳 Ultimate Financial Transaction Analytics": {
                "description": "Complete financial transaction management using billing transaction layer",
                "file": "ultimate_financial_transaction_analytics.py",
                "port": 8506,
                "url": "http://localhost:8506",
                "features": [
                    "Billing Transaction Metrics",
                    "Accounts Receivable Analytics", 
                    "Claims Financial Data",
                    "Transaction Type Analysis",
                    "AR Trends Over Time",
                    "Claim State Analysis",
                    "Office Financial Performance"
                ]
            },
            "🎯 Ultimate Sales Attribution Analytics": {
                "description": "Complete sales attribution using organizational hierarchy model",
                "file": "ultimate_sales_attribution_analytics.py",
                "port": 8507,
                "url": "http://localhost:8507",
                "features": [
                    "Organizational Structure Overview",
                    "Sales Attribution Chain",
                    "Company Performance Analysis",
                    "Office Performance Analysis", 
                    "Employee Performance Analysis",
                    "Sales Trends by Hierarchy"
                ]
            }
        }
        
        self.sf_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': 'EYECARE_ANALYTICS',
            'schema': 'RAW'
        }
    
    def get_connection(self):
        """Get Snowflake connection"""
        return snowflake.connector.connect(**self.sf_params)
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute query safely with error handling"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch data
            data = cursor.fetchall()
            
            # Create DataFrame
            df = pd.DataFrame(data, columns=columns)
            
            cursor.close()
            conn.close()
            
            return df
            
        except Exception as e:
            st.error(f"Query execution error: {str(e)}")
            return pd.DataFrame()
    
    def check_dashboard_status(self, port: int) -> bool:
        """Check if a dashboard is running on the specified port"""
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', port))
            sock.close()
            return result == 0
        except:
            return False
    
    def launch_dashboard(self, dashboard_name: str):
        """Launch a specific dashboard"""
        dashboard = self.dashboards[dashboard_name]
        file_path = f"src/analytics/{dashboard['file']}"
        port = dashboard['port']
        
        if self.check_dashboard_status(port):
            st.success(f"✅ {dashboard_name} is already running on port {port}")
            st.markdown(f"**Access it here:** [Open {dashboard_name}]({dashboard['url']})")
        else:
            try:
                # Launch the dashboard
                cmd = f"streamlit run {file_path} --server.port {port}"
                subprocess.Popen(cmd.split(), cwd=project_root)
                st.success(f"🚀 Launching {dashboard_name} on port {port}...")
                st.info("Please wait a few seconds for the dashboard to start, then click the link below.")
                time.sleep(2)
                st.markdown(f"**Access it here:** [Open {dashboard_name}]({dashboard['url']})")
            except Exception as e:
                st.error(f"Error launching {dashboard_name}: {str(e)}")
    
    def get_system_overview(self):
        """Get overview of the complete analytics system"""
        overview = {}
        
        # Get basic data counts
        try:
            tables_query = """
            SELECT 
                COUNT(*) as total_migrated_tables
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'RAW'
            AND TABLE_NAME LIKE 'DBO_%'
            """
            
            df = self.execute_query(tables_query)
            if not df.empty:
                overview['migrated_tables'] = df.iloc[0]['TOTAL_MIGRATED_TABLES']
        except:
            overview['migrated_tables'] = 0
        
        # Get sample data counts from key tables
        key_tables = [
            'DBO_INVOICEDET',
            'DBO_BILLINGTRANSACTION', 
            'DBO_INSCARRIER',
            'DBO_INSPLAN',
            'DBO_EMPLOYEE',
            'DBO_POSTRANSACTION'
        ]
        
        overview['table_counts'] = {}
        for table in key_tables:
            try:
                count_query = f"SELECT COUNT(*) as row_count FROM EYECARE_ANALYTICS.RAW.{table}"
                df = self.execute_query(count_query)
                if not df.empty:
                    overview['table_counts'][table] = df.iloc[0]['ROW_COUNT']
            except:
                overview['table_counts'][table] = 0
        
        return overview

def main():
    """Main Streamlit application"""
    st.set_page_config(
        page_title="Master Dashboard Navigator",
        page_icon="🎛️",
        layout="wide"
    )
    
    st.title("🎛️ Master Dashboard Navigator")
    st.markdown("### Central Hub for All Eyecare Analytics Dashboards")
    
    # Initialize navigator
    navigator = MasterDashboardNavigator()
    
    # Sidebar with dashboard selection
    st.sidebar.title("📊 Dashboard Selection")
    
    # Dashboard dropdown
    dashboard_names = list(navigator.dashboards.keys())
    selected_dashboard = st.sidebar.selectbox(
        "Choose a Dashboard to Launch:",
        ["Select a Dashboard..."] + dashboard_names
    )
    
    # Launch button
    if selected_dashboard != "Select a Dashboard...":
        if st.sidebar.button(f"🚀 Launch {selected_dashboard}"):
            navigator.launch_dashboard(selected_dashboard)
    
    # Quick access buttons
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🚀 Quick Access")
    
    for dashboard_name, dashboard_info in navigator.dashboards.items():
        if navigator.check_dashboard_status(dashboard_info['port']):
            st.sidebar.success(f"✅ {dashboard_name}")
            st.sidebar.markdown(f"[Open {dashboard_name}]({dashboard_info['url']})")
        else:
            st.sidebar.error(f"❌ {dashboard_name}")
    
    # Main content area
    st.header("📊 Available Analytics Dashboards")
    
    # Dashboard cards
    for dashboard_name, dashboard_info in navigator.dashboards.items():
        with st.expander(f"{dashboard_name} - {dashboard_info['description']}", expanded=False):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown("**Key Features:**")
                for feature in dashboard_info['features']:
                    st.markdown(f"• {feature}")
            
            with col2:
                status = "🟢 Running" if navigator.check_dashboard_status(dashboard_info['port']) else "🔴 Stopped"
                st.markdown(f"**Status:** {status}")
                st.markdown(f"**Port:** {dashboard_info['port']}")
                
                if navigator.check_dashboard_status(dashboard_info['port']):
                    st.markdown(f"**[Open Dashboard]({dashboard_info['url']})**")
                else:
                    if st.button(f"Launch {dashboard_name}", key=f"launch_{dashboard_name}"):
                        navigator.launch_dashboard(dashboard_name)
    
    # System overview
    st.header("🔍 System Overview")
    
    overview = navigator.get_system_overview()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Dashboards",
            value=len(navigator.dashboards)
        )
    
    with col2:
        running_count = sum(1 for d in navigator.dashboards.values() if navigator.check_dashboard_status(d['port']))
        st.metric(
            label="Running Dashboards", 
            value=running_count
        )
    
    with col3:
        st.metric(
            label="Migrated Tables",
            value=f"{overview.get('migrated_tables', 0):,}"
        )
    
    with col4:
        total_rows = sum(overview.get('table_counts', {}).values())
        st.metric(
            label="Total Data Rows",
            value=f"{total_rows:,}"
        )
    
    # Data availability table
    st.subheader("📋 Key Data Tables Availability")
    
    table_counts = overview.get('table_counts', {})
    if table_counts:
        df_tables = pd.DataFrame([
            {"Table": table, "Row Count": f"{count:,}", "Status": "✅ Available" if count > 0 else "❌ Empty"}
            for table, count in table_counts.items()
        ])
        st.dataframe(df_tables, use_container_width=True)
    
    # Architecture diagram
    st.header("🏗️ Analytics Platform Architecture")
    
    st.markdown("""
    ### 📊 Complete Eyecare Analytics Ecosystem
    
    **Data Flow:**
    ```
    SQL Server Dev Database 
           ↓
    Snowflake RAW Layer (555+ Tables, 480K+ Rows)
           ↓
    Specialized Analytics Dashboards
           ↓
    AI-Powered Insights & Recommendations
    ```
    
    **Dashboard Specializations:**
    - **🤖 Main Platform**: Central AI hub with executive dashboard
    - **🏥 Insurance Analytics**: Carrier and plan management
    - **💰 Revenue Cycle**: Complete revenue workflow
    - **💳 Financial Transactions**: Billing and payment processing  
    - **🎯 Sales Attribution**: Organizational hierarchy and sales tracking
    
    **Key Technologies:**
    - **Snowflake**: Data warehouse and Cortex AI
    - **Streamlit**: Interactive dashboard framework
    - **Python**: Analytics and data processing
    - **Plotly**: Advanced visualizations
    - **SQL**: Complex business logic and relationships
    """)
    
    # Footer
    st.markdown("---")
    st.markdown("🚀 **Master Dashboard Navigator** | 🎛️ **Complete Eyecare Analytics Platform**")
    st.markdown("*Built with your complete SQL knowledge and business understanding*")

if __name__ == "__main__":
    main()
