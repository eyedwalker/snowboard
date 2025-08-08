#!/usr/bin/env python3
"""
Cortex AI Analytics Platform
Advanced AI-powered analytics dashboard using Snowflake Cortex with complete eyecare dataset
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
import os
from dotenv import load_dotenv
from pathlib import Path
import json
from datetime import datetime, timedelta
import numpy as np

# Load environment variables
project_root = Path(__file__).parent.parent.parent
load_dotenv(project_root / ".env")

class CortexAIAnalyticsPlatform:
    """Advanced AI-powered analytics platform using Snowflake Cortex"""
    
    def __init__(self):
        """Initialize Cortex AI analytics platform"""
        self.sf_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': 'EYECARE_ANALYTICS',
            'schema': 'RAW'
        }
        
        # Cache for table discovery
        if 'table_cache' not in st.session_state:
            st.session_state.table_cache = None
    
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
    
    def discover_available_tables(self):
        """Discover all available tables in the RAW schema"""
        if st.session_state.table_cache is not None:
            return st.session_state.table_cache
        
        query = """
        SHOW TABLES IN EYECARE_ANALYTICS.RAW
        """
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            
            tables = []
            for row in cursor.fetchall():
                table_name = row[1]  # Table name is in second column
                tables.append(table_name)
            
            cursor.close()
            conn.close()
            
            # Cache the results
            st.session_state.table_cache = sorted(tables)
            return st.session_state.table_cache
            
        except Exception as e:
            st.error(f"Error discovering tables: {str(e)}")
            return []
    
    def get_table_sample(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """Get sample data from a table"""
        query = f"""
        SELECT * FROM EYECARE_ANALYTICS.RAW.{table_name}
        LIMIT {limit}
        """
        return self.execute_query(query)
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get row count for a table"""
        query = f"""
        SELECT COUNT(*) as row_count FROM EYECARE_ANALYTICS.RAW.{table_name}
        """
        df = self.execute_query(query)
        return df.iloc[0]['ROW_COUNT'] if not df.empty else 0
    
    def cortex_analyze_text(self, text: str, analysis_type: str = "sentiment") -> str:
        """Use Snowflake Cortex for text analysis"""
        try:
            if analysis_type == "sentiment":
                query = f"""
                SELECT SNOWFLAKE.CORTEX.SENTIMENT('{text}') as analysis_result
                """
            elif analysis_type == "summarize":
                query = f"""
                SELECT SNOWFLAKE.CORTEX.SUMMARIZE('{text}') as analysis_result
                """
            elif analysis_type == "translate":
                query = f"""
                SELECT SNOWFLAKE.CORTEX.TRANSLATE('{text}', 'en', 'es') as analysis_result
                """
            else:
                return "Analysis type not supported"
            
            df = self.execute_query(query)
            return df.iloc[0]['ANALYSIS_RESULT'] if not df.empty else "Analysis failed"
            
        except Exception as e:
            return f"Cortex analysis error: {str(e)}"
    
    def cortex_complete_text(self, prompt: str) -> str:
        """Use Snowflake Cortex for text completion/generation"""
        try:
            query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', '{prompt}') as completion_result
            """
            
            df = self.execute_query(query)
            return df.iloc[0]['COMPLETION_RESULT'] if not df.empty else "Completion failed"
            
        except Exception as e:
            return f"Cortex completion error: {str(e)}"
    
    def analyze_business_metrics(self):
        """Analyze key business metrics across all tables"""
        metrics = {}
        
        # Patient metrics
        try:
            patient_query = """
            SELECT 
                COUNT(*) as total_patients,
                COUNT(DISTINCT "PatientId") as unique_patients
            FROM EYECARE_ANALYTICS.RAW.DBO_PATIENT
            WHERE "PatientId" IS NOT NULL AND "PatientId" != ''
            """
            df = self.execute_query(patient_query)
            if not df.empty:
                metrics['patients'] = {
                    'total': df.iloc[0]['TOTAL_PATIENTS'],
                    'unique': df.iloc[0]['UNIQUE_PATIENTS']
                }
        except:
            metrics['patients'] = {'total': 0, 'unique': 0}
        
        # Orders metrics
        try:
            orders_query = """
            SELECT 
                COUNT(*) as total_orders,
                SUM(CAST("OrderTotal" AS FLOAT)) as total_revenue
            FROM EYECARE_ANALYTICS.RAW.DBO_ORDERS
            WHERE "OrderTotal" IS NOT NULL AND "OrderTotal" != ''
            """
            df = self.execute_query(orders_query)
            if not df.empty:
                metrics['orders'] = {
                    'total': df.iloc[0]['TOTAL_ORDERS'],
                    'revenue': df.iloc[0]['TOTAL_REVENUE'] or 0
                }
        except:
            metrics['orders'] = {'total': 0, 'revenue': 0}
        
        # Billing metrics
        try:
            billing_query = """
            SELECT 
                COUNT(*) as total_claims,
                COUNT(DISTINCT "PatientId") as patients_with_claims
            FROM EYECARE_ANALYTICS.RAW.DBO_BILLINGCLAIM
            WHERE "PatientId" IS NOT NULL AND "PatientId" != ''
            """
            df = self.execute_query(billing_query)
            if not df.empty:
                metrics['billing'] = {
                    'total_claims': df.iloc[0]['TOTAL_CLAIMS'],
                    'patients_with_claims': df.iloc[0]['PATIENTS_WITH_CLAIMS']
                }
        except:
            metrics['billing'] = {'total_claims': 0, 'patients_with_claims': 0}
        
        # Office metrics
        try:
            office_query = """
            SELECT 
                COUNT(*) as total_offices,
                COUNT(DISTINCT "OfficeId") as unique_offices
            FROM EYECARE_ANALYTICS.RAW.DBO_OFFICE
            WHERE "OfficeId" IS NOT NULL AND "OfficeId" != ''
            """
            df = self.execute_query(office_query)
            if not df.empty:
                metrics['offices'] = {
                    'total': df.iloc[0]['TOTAL_OFFICES'],
                    'unique': df.iloc[0]['UNIQUE_OFFICES']
                }
        except:
            metrics['offices'] = {'total': 0, 'unique': 0}
        
        return metrics
    
    def generate_ai_insights(self, metrics: dict) -> str:
        """Generate AI insights using Cortex"""
        prompt = f"""
        Analyze the following eyecare business metrics and provide strategic insights:
        
        Patients: {metrics.get('patients', {}).get('total', 0)} total patients
        Orders: {metrics.get('orders', {}).get('total', 0)} total orders, ${metrics.get('orders', {}).get('revenue', 0):,.2f} revenue
        Billing: {metrics.get('billing', {}).get('total_claims', 0)} claims from {metrics.get('billing', {}).get('patients_with_claims', 0)} patients
        Offices: {metrics.get('offices', {}).get('total', 0)} office locations
        
        Provide 3-5 key business insights and recommendations for this eyecare practice.
        """
        
        return self.cortex_complete_text(prompt)
    
    def create_revenue_analysis_chart(self):
        """Create revenue analysis visualization"""
        try:
            query = """
            SELECT 
                "OrderDate",
                CAST("OrderTotal" AS FLOAT) as order_total,
                "OfficeId"
            FROM EYECARE_ANALYTICS.RAW.DBO_ORDERS
            WHERE "OrderTotal" IS NOT NULL 
            AND "OrderTotal" != ''
            AND "OrderDate" IS NOT NULL
            AND "OrderDate" != ''
            LIMIT 1000
            """
            
            df = self.execute_query(query)
            
            if not df.empty and len(df) > 0:
                # Clean and convert data
                df['ORDER_TOTAL'] = pd.to_numeric(df['ORDER_TOTAL'], errors='coerce')
                df = df.dropna(subset=['ORDER_TOTAL'])
                
                if len(df) > 0:
                    # Create revenue trend chart
                    fig = px.histogram(df, x='ORDER_TOTAL', nbins=30, 
                                     title='Revenue Distribution Analysis',
                                     labels={'ORDER_TOTAL': 'Order Total ($)', 'count': 'Number of Orders'})
                    
                    fig.update_layout(
                        xaxis_title="Order Total ($)",
                        yaxis_title="Number of Orders",
                        showlegend=False
                    )
                    
                    return fig
            
            # Fallback chart if no data
            fig = go.Figure()
            fig.add_annotation(text="No revenue data available for visualization", 
                             xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
            fig.update_layout(title="Revenue Analysis")
            return fig
            
        except Exception as e:
            fig = go.Figure()
            fig.add_annotation(text=f"Error creating revenue chart: {str(e)}", 
                             xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
            fig.update_layout(title="Revenue Analysis")
            return fig
    
    def create_patient_demographics_chart(self):
        """Create patient demographics visualization"""
        try:
            query = """
            SELECT 
                "Gender",
                COUNT(*) as patient_count
            FROM EYECARE_ANALYTICS.RAW.DBO_PATIENT
            WHERE "Gender" IS NOT NULL AND "Gender" != ''
            GROUP BY "Gender"
            """
            
            df = self.execute_query(query)
            
            if not df.empty and len(df) > 0:
                fig = px.pie(df, values='PATIENT_COUNT', names='GENDER', 
                           title='Patient Demographics by Gender')
                return fig
            
            # Fallback chart
            fig = go.Figure()
            fig.add_annotation(text="No patient demographic data available", 
                             xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
            fig.update_layout(title="Patient Demographics")
            return fig
            
        except Exception as e:
            fig = go.Figure()
            fig.add_annotation(text=f"Error creating demographics chart: {str(e)}", 
                             xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
            fig.update_layout(title="Patient Demographics")
            return fig
    
    def create_office_performance_chart(self):
        """Create office performance visualization"""
        try:
            query = """
            SELECT 
                o."OfficeId",
                o."OfficeName",
                COUNT(ord."OrderId") as total_orders,
                SUM(CAST(ord."OrderTotal" AS FLOAT)) as total_revenue
            FROM EYECARE_ANALYTICS.RAW.DBO_OFFICE o
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_ORDERS ord ON o."OfficeId" = ord."OfficeId"
            WHERE o."OfficeId" IS NOT NULL AND o."OfficeId" != ''
            GROUP BY o."OfficeId", o."OfficeName"
            HAVING COUNT(ord."OrderId") > 0
            ORDER BY total_revenue DESC
            LIMIT 10
            """
            
            df = self.execute_query(query)
            
            if not df.empty and len(df) > 0:
                fig = px.bar(df, x='OFFICENAME', y='TOTAL_REVENUE', 
                           title='Top 10 Offices by Revenue',
                           labels={'TOTAL_REVENUE': 'Total Revenue ($)', 'OFFICENAME': 'Office Name'})
                
                fig.update_layout(xaxis_tickangle=-45)
                return fig
            
            # Fallback chart
            fig = go.Figure()
            fig.add_annotation(text="No office performance data available", 
                             xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
            fig.update_layout(title="Office Performance")
            return fig
            
        except Exception as e:
            fig = go.Figure()
            fig.add_annotation(text=f"Error creating office performance chart: {str(e)}", 
                             xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
            fig.update_layout(title="Office Performance")
            return fig

def main():
    """Main Streamlit application"""
    st.set_page_config(
        page_title="Cortex AI Eyecare Analytics Platform",
        page_icon="👁️",
        layout="wide"
    )
    
    st.title("🤖 Cortex AI Eyecare Analytics Platform")
    st.markdown("### Advanced AI-Powered Business Intelligence with Complete Dataset")
    
    # Initialize platform
    platform = CortexAIAnalyticsPlatform()
    
    # Sidebar navigation
    st.sidebar.title("🎯 Analytics Navigation")
    
    analysis_type = st.sidebar.selectbox(
        "Choose Analysis Type",
        [
            "📊 Executive Dashboard",
            "🤖 AI Insights & Recommendations", 
            "📈 Revenue Analytics",
            "👥 Patient Analytics",
            "🏢 Office Performance",
            "🔍 Data Explorer",
            "💬 AI Chat Assistant"
        ]
    )
    
    if analysis_type == "📊 Executive Dashboard":
        st.header("📊 Executive Dashboard")
        
        # Get business metrics
        with st.spinner("Analyzing business metrics..."):
            metrics = platform.analyze_business_metrics()
        
        # Display KPIs
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="Total Patients",
                value=f"{metrics.get('patients', {}).get('total', 0):,}"
            )
        
        with col2:
            st.metric(
                label="Total Orders",
                value=f"{metrics.get('orders', {}).get('total', 0):,}"
            )
        
        with col3:
            st.metric(
                label="Total Revenue",
                value=f"${metrics.get('orders', {}).get('revenue', 0):,.2f}"
            )
        
        with col4:
            st.metric(
                label="Office Locations",
                value=f"{metrics.get('offices', {}).get('total', 0):,}"
            )
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            revenue_chart = platform.create_revenue_analysis_chart()
            st.plotly_chart(revenue_chart, use_container_width=True)
        
        with col2:
            demographics_chart = platform.create_patient_demographics_chart()
            st.plotly_chart(demographics_chart, use_container_width=True)
        
        # Office performance
        office_chart = platform.create_office_performance_chart()
        st.plotly_chart(office_chart, use_container_width=True)
    
    elif analysis_type == "🤖 AI Insights & Recommendations":
        st.header("🤖 AI Insights & Recommendations")
        
        # Get business metrics
        with st.spinner("Generating AI insights..."):
            metrics = platform.analyze_business_metrics()
            ai_insights = platform.generate_ai_insights(metrics)
        
        st.subheader("📊 Business Metrics Summary")
        st.json(metrics)
        
        st.subheader("🧠 AI-Generated Strategic Insights")
        st.markdown(ai_insights)
        
        # Custom AI analysis
        st.subheader("💬 Custom AI Analysis")
        user_question = st.text_area(
            "Ask AI to analyze your eyecare business:",
            placeholder="e.g., What are the key growth opportunities for our practice?"
        )
        
        if st.button("🤖 Generate AI Analysis") and user_question:
            with st.spinner("AI is analyzing your question..."):
                custom_prompt = f"""
                Based on an eyecare practice with the following metrics:
                - {metrics.get('patients', {}).get('total', 0)} patients
                - {metrics.get('orders', {}).get('total', 0)} orders
                - ${metrics.get('orders', {}).get('revenue', 0):,.2f} revenue
                - {metrics.get('offices', {}).get('total', 0)} offices
                
                Question: {user_question}
                
                Provide a detailed, actionable response for this eyecare business.
                """
                
                ai_response = platform.cortex_complete_text(custom_prompt)
                st.markdown("### 🤖 AI Response:")
                st.markdown(ai_response)
    
    elif analysis_type == "🔍 Data Explorer":
        st.header("🔍 Data Explorer")
        
        # Discover available tables
        with st.spinner("Discovering available tables..."):
            tables = platform.discover_available_tables()
        
        st.subheader(f"📋 Available Tables ({len(tables)} total)")
        
        # Table selection
        selected_table = st.selectbox("Select a table to explore:", tables)
        
        if selected_table:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.subheader(f"📊 Sample Data from {selected_table}")
                sample_data = platform.get_table_sample(selected_table, 10)
                st.dataframe(sample_data)
            
            with col2:
                st.subheader("📈 Table Statistics")
                row_count = platform.get_table_row_count(selected_table)
                st.metric("Total Rows", f"{row_count:,}")
                
                if not sample_data.empty:
                    st.metric("Columns", len(sample_data.columns))
                    
                    # Show column info
                    st.subheader("📋 Column Information")
                    for col in sample_data.columns:
                        st.text(f"• {col}")
    
    elif analysis_type == "💬 AI Chat Assistant":
        st.header("💬 AI Chat Assistant")
        
        st.markdown("Ask questions about your eyecare data and get AI-powered insights!")
        
        # Chat interface
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = []
        
        # Display chat history
        for i, (question, answer) in enumerate(st.session_state.chat_history):
            st.markdown(f"**You:** {question}")
            st.markdown(f"**AI:** {answer}")
            st.markdown("---")
        
        # New question input
        user_question = st.text_input("Ask your question:", key="chat_input")
        
        if st.button("🤖 Ask AI") and user_question:
            with st.spinner("AI is thinking..."):
                # Get current metrics for context
                metrics = platform.analyze_business_metrics()
                
                context_prompt = f"""
                You are an AI assistant for an eyecare practice analytics platform. 
                Current business metrics:
                - Patients: {metrics.get('patients', {}).get('total', 0)}
                - Orders: {metrics.get('orders', {}).get('total', 0)}
                - Revenue: ${metrics.get('orders', {}).get('revenue', 0):,.2f}
                - Offices: {metrics.get('offices', {}).get('total', 0)}
                
                User question: {user_question}
                
                Provide a helpful, specific answer related to eyecare business analytics.
                """
                
                ai_answer = platform.cortex_complete_text(context_prompt)
                
                # Add to chat history
                st.session_state.chat_history.append((user_question, ai_answer))
                
                # Rerun to show updated chat
                st.rerun()
    
    # Footer
    st.markdown("---")
    st.markdown("🚀 **Powered by Snowflake Cortex AI** | 👁️ **Complete Eyecare Analytics Platform**")

if __name__ == "__main__":
    main()
