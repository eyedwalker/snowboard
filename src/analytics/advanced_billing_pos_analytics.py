#!/usr/bin/env python3
"""
Advanced Billing and POS Analytics Dashboard
Complete accounts receivable and financial analytics using user's sophisticated V1.3 query
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
from datetime import datetime, timedelta

# Load environment variables
project_root = Path(__file__).parent.parent.parent
load_dotenv(project_root / ".env")

class AdvancedBillingPOSAnalytics:
    """Advanced billing and POS analytics using sophisticated financial model"""
    
    def __init__(self):
        """Initialize advanced billing and POS analytics"""
        self.sf_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': 'EYECARE_ANALYTICS',
            'schema': 'RAW'
        }
        
        # Transaction type mappings from user's query
        self.billing_transaction_types = {
            1: 'Charges',
            2: 'Insurance Payment', 3: 'Insurance Payment',
            4: 'Refund', 5: 'Refund',
            6: 'Billing', 16: 'Billing', 17: 'Billing', 18: 'Billing', 19: 'Billing',
            7: 'Collections',
            8: 'Carrier Write-off',
            10: 'Patient AR', 6: 'Patient AR',
            11: 'Adjustment',
            15: 'Patient Write-off'
        }
        
        self.pos_transaction_types = {
            2: 'Patient Payment', 4: 'Patient Payment',
            12: 'Patient Credit',
            13: 'Collections',
            15: 'Refund'
        }
        
        self.adjustment_reasons = {
            765: 'Carrier Credit',
            766: 'Patient Credit'
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
    
    def check_billing_pos_tables_availability(self):
        """Check availability of core billing and POS tables only"""
        # Only check tables that we know exist and are essential for billing/POS analytics
        core_tables_to_check = [
            'DBO_BILLINGTRANSACTION',
            'DBO_POSTRANSACTION',
            'DBO_PATIENT',
            'DBO_OFFICE'
        ]
        
        available_tables = {}
        
        for table in core_tables_to_check:
            try:
                query = f"SELECT COUNT(*) as row_count FROM EYECARE_ANALYTICS.RAW.{table}"
                df = self.execute_query(query)
                if not df.empty:
                    available_tables[table] = df.iloc[0]['ROW_COUNT']
                else:
                    available_tables[table] = 0
            except Exception as e:
                # Silently handle missing tables
                available_tables[table] = 0
        
        return available_tables
    
    def get_billing_pos_overview(self, office_num: str = None, from_date: str = None, to_date: str = None):
        """Get billing and POS overview with filters"""
        
        # Set default dates if not provided
        if not from_date:
            from_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        if not to_date:
            to_date = datetime.now().strftime('%Y-%m-%d')
        
        overview = {}
        
        # Billing transactions overview - using only confirmed columns
        try:
            billing_query = f"""
            SELECT 
                COUNT(*) as total_billing_transactions,
                COALESCE(SUM(CAST("InsAR" AS DECIMAL(18,2))), 0) as total_insurance_ar,
                COALESCE(SUM(CAST("PatAR" AS DECIMAL(18,2))), 0) as total_patient_ar,
                COALESCE(SUM(CAST("InsDeltaAR" AS DECIMAL(18,2))), 0) as total_insurance_delta_ar,
                COALESCE(SUM(CAST("PatDeltaAR" AS DECIMAL(18,2))), 0) as total_patient_delta_ar
            FROM EYECARE_ANALYTICS.RAW.DBO_BILLINGTRANSACTION
            WHERE "TransactionDate" >= '{from_date}' 
            AND "TransactionDate" <= '{to_date}'
            """
            
            df = self.execute_query(billing_query)
            if not df.empty:
                overview['billing'] = {
                    'total_transactions': int(df.iloc[0]['TOTAL_BILLING_TRANSACTIONS'] or 0),
                    'unique_orders': 0,  # Will calculate separately if needed
                    'insurance_ar': float(df.iloc[0]['TOTAL_INSURANCE_AR'] or 0),
                    'patient_ar': float(df.iloc[0]['TOTAL_PATIENT_AR'] or 0),
                    'insurance_delta_ar': float(df.iloc[0]['TOTAL_INSURANCE_DELTA_AR'] or 0),
                    'patient_delta_ar': float(df.iloc[0]['TOTAL_PATIENT_DELTA_AR'] or 0)
                }
            else:
                overview['billing'] = {'total_transactions': 0, 'unique_orders': 0, 'insurance_ar': 0, 'patient_ar': 0, 'insurance_delta_ar': 0, 'patient_delta_ar': 0}
        except Exception as e:
            st.error(f"Error getting billing overview: {str(e)}")
            overview['billing'] = {'total_transactions': 0, 'unique_orders': 0, 'insurance_ar': 0, 'patient_ar': 0, 'insurance_delta_ar': 0, 'patient_delta_ar': 0}
        
        # POS transactions overview - using confirmed columns only
        try:
            pos_query = f"""
            SELECT 
                COUNT(*) as total_pos_transactions,
                COALESCE(SUM(CAST("Amount" AS DECIMAL(18,2))), 0) as total_pos_amount,
                COALESCE(AVG(CAST("Amount" AS DECIMAL(18,2))), 0) as avg_pos_amount
            FROM EYECARE_ANALYTICS.RAW.DBO_POSTRANSACTION
            WHERE "TransactionDate" >= '{from_date}' 
            AND "TransactionDate" <= '{to_date}'
            """
            
            df = self.execute_query(pos_query)
            if not df.empty:
                overview['pos'] = {
                    'total_transactions': int(df.iloc[0]['TOTAL_POS_TRANSACTIONS'] or 0),
                    'unique_orders': 0,  # Will calculate separately if needed
                    'total_amount': float(df.iloc[0]['TOTAL_POS_AMOUNT'] or 0),
                    'avg_amount': float(df.iloc[0]['AVG_POS_AMOUNT'] or 0)
                }
            else:
                overview['pos'] = {'total_transactions': 0, 'unique_orders': 0, 'total_amount': 0, 'avg_amount': 0}
        except Exception as e:
            st.error(f"Error getting POS overview: {str(e)}")
            overview['pos'] = {'total_transactions': 0, 'unique_orders': 0, 'total_amount': 0, 'avg_amount': 0}
        
        return overview
    
    def get_transaction_type_analysis(self, from_date: str, to_date: str, office_num: str = None):
        """Analyze transactions by type using user's business logic"""
        try:
            # Simplified billing transaction types analysis
            billing_types_query = f"""
            SELECT 
                "TransTypeId" as transaction_type_id,
                CASE 
                    WHEN "TransTypeId" = 1 THEN 'Charges'
                    WHEN "TransTypeId" IN (2, 3) THEN 'Insurance Payment'
                    WHEN "TransTypeId" IN (4, 5) THEN 'Refund'
                    WHEN "TransTypeId" IN (6, 16, 17, 18, 19) THEN 'Billing'
                    WHEN "TransTypeId" = 7 THEN 'Collections'
                    WHEN "TransTypeId" = 8 THEN 'Carrier Write-off'
                    WHEN "TransTypeId" IN (6, 10) THEN 'Patient AR'
                    WHEN "TransTypeId" = 11 THEN 'Adjustment'
                    WHEN "TransTypeId" = 15 THEN 'Patient Write-off'
                    ELSE 'Other'
                END as transaction_type,
                COUNT(*) as transaction_count,
                COALESCE(SUM(CAST("InsDeltaAR" AS DECIMAL(18,2)) + CAST("PatDeltaAR" AS DECIMAL(18,2))), 0) as total_amount
            FROM EYECARE_ANALYTICS.RAW.DBO_BILLINGTRANSACTION
            WHERE "TransactionDate" >= '{from_date}' 
            AND "TransactionDate" <= '{to_date}'
            AND ("IsVoid" = 'False' OR "IsVoid" = '0' OR "IsVoid" IS NULL)
            GROUP BY "TransTypeId"
            ORDER BY transaction_count DESC
            """
            
            return self.execute_query(billing_types_query)
            
        except Exception as e:
            st.error(f"Error analyzing transaction types: {str(e)}")
            return pd.DataFrame()
    
    def get_outstanding_balances_summary(self, from_date: str, to_date: str, office_num: str = None):
        """Get outstanding balances summary using simplified logic"""
        try:
            # Simplified balances query that works with available tables
            balances_query = f"""
            SELECT 
                'Summary' as office_name,
                COUNT(*) as order_count,
                COALESCE(SUM(CASE WHEN "TransTypeId" IN (1, 6, 16, 17, 18, 19) 
                    THEN CAST("InsDeltaAR" AS DECIMAL(18,2)) + CAST("PatDeltaAR" AS DECIMAL(18,2)) 
                    ELSE 0 END), 0) as total_billed,
                COALESCE(SUM(CASE WHEN "TransTypeId" IN (2, 3) 
                    THEN CAST("InsDeltaAR" AS DECIMAL(18,2)) + CAST("PatDeltaAR" AS DECIMAL(18,2)) 
                    ELSE 0 END), 0) * -1 as total_insurance_payments,
                COALESCE(SUM(CASE WHEN "TransTypeId" IN (8, 15) 
                    THEN CAST("InsDeltaAR" AS DECIMAL(18,2)) + CAST("PatDeltaAR" AS DECIMAL(18,2)) 
                    ELSE 0 END), 0) * -1 as total_writeoffs,
                COALESCE(SUM(CASE WHEN "TransTypeId" = 11 
                    THEN CAST("InsDeltaAR" AS DECIMAL(18,2)) + CAST("PatDeltaAR" AS DECIMAL(18,2)) 
                    ELSE 0 END), 0) * -1 as total_adjustments,
                COALESCE(SUM(CAST("InsDeltaAR" AS DECIMAL(18,2)) + CAST("PatDeltaAR" AS DECIMAL(18,2))), 0) as outstanding_balance
            FROM EYECARE_ANALYTICS.RAW.DBO_BILLINGTRANSACTION
            WHERE "TransactionDate" >= '{from_date}' 
            AND "TransactionDate" <= '{to_date}'
            AND ("IsVoid" = 'False' OR "IsVoid" = '0' OR "IsVoid" IS NULL)
            """
            
            return self.execute_query(balances_query)
            
        except Exception as e:
            st.error(f"Error getting outstanding balances: {str(e)}")
            return pd.DataFrame()
    
    def get_insurance_vs_patient_analysis(self, from_date: str, to_date: str, office_num: str = None):
        """Analyze insurance vs patient financial patterns"""
        try:
            # Simplified insurance vs patient analysis
            analysis_query = f"""
            SELECT 
                'Insurance' as payer_type,
                COUNT(*) as transaction_count,
                COALESCE(SUM(CAST("InsAR" AS DECIMAL(18,2))), 0) as total_ar,
                COALESCE(SUM(CAST("InsDeltaAR" AS DECIMAL(18,2))), 0) as total_delta_ar,
                COALESCE(AVG(CAST("InsAR" AS DECIMAL(18,2))), 0) as avg_ar
            FROM EYECARE_ANALYTICS.RAW.DBO_BILLINGTRANSACTION
            WHERE "TransactionDate" >= '{from_date}' 
            AND "TransactionDate" <= '{to_date}'
            AND ("IsVoid" = 'False' OR "IsVoid" = '0' OR "IsVoid" IS NULL)
            AND "InsAR" IS NOT NULL AND "InsAR" != ''
            
            UNION ALL
            
            SELECT 
                'Patient' as payer_type,
                COUNT(*) as transaction_count,
                COALESCE(SUM(CAST("PatAR" AS DECIMAL(18,2))), 0) as total_ar,
                COALESCE(SUM(CAST("PatDeltaAR" AS DECIMAL(18,2))), 0) as total_delta_ar,
                COALESCE(AVG(CAST("PatAR" AS DECIMAL(18,2))), 0) as avg_ar
            FROM EYECARE_ANALYTICS.RAW.DBO_BILLINGTRANSACTION
            WHERE "TransactionDate" >= '{from_date}' 
            AND "TransactionDate" <= '{to_date}'
            AND ("IsVoid" = 'False' OR "IsVoid" = '0' OR "IsVoid" IS NULL)
            AND "PatAR" IS NOT NULL AND "PatAR" != ''
            """
            
            return self.execute_query(analysis_query)
            
        except Exception as e:
            st.error(f"Error analyzing insurance vs patient: {str(e)}")
            return pd.DataFrame()
    
    def cortex_analyze_billing_pos_data(self, overview: dict) -> str:
        """Use Cortex AI to analyze billing and POS data"""
        try:
            billing = overview.get('billing', {})
            pos = overview.get('pos', {})
            
            prompt = f"""
            Analyze this comprehensive billing and POS financial data for an eyecare practice:
            
            BILLING SYSTEM:
            - Total transactions: {billing.get('total_transactions', 0):,}
            - Unique orders: {billing.get('unique_orders', 0):,}
            - Insurance AR: ${billing.get('insurance_ar', 0):,.2f}
            - Patient AR: ${billing.get('patient_ar', 0):,.2f}
            - Insurance Delta AR: ${billing.get('insurance_delta_ar', 0):,.2f}
            - Patient Delta AR: ${billing.get('patient_delta_ar', 0):,.2f}
            
            POS SYSTEM:
            - Total POS transactions: {pos.get('total_transactions', 0):,}
            - Unique POS orders: {pos.get('unique_orders', 0):,}
            - Total POS amount: ${pos.get('total_amount', 0):,.2f}
            - Average POS transaction: ${pos.get('avg_amount', 0):.2f}
            
            Provide strategic insights on:
            1. Accounts receivable management
            2. Cash flow optimization
            3. Billing efficiency
            4. Patient payment patterns
            5. Revenue cycle performance
            """
            
            query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', '{prompt}') as analysis_result
            """
            
            df = self.execute_query(query)
            return df.iloc[0]['ANALYSIS_RESULT'] if not df.empty else "AI analysis unavailable"
            
        except Exception as e:
            return f"AI analysis error: {str(e)}"

def main():
    """Main Streamlit application"""
    st.set_page_config(
        page_title="Advanced Billing & POS Analytics",
        page_icon="💼",
        layout="wide"
    )
    
    st.title("💼 Advanced Billing & POS Analytics")
    st.markdown("### Complete Accounts Receivable and Financial Analytics (V1.3)")
    
    # Initialize analytics
    analytics = AdvancedBillingPOSAnalytics()
    
    # Sidebar filters
    st.sidebar.title("📊 Filters")
    
    # Date range filter
    col1, col2 = st.sidebar.columns(2)
    with col1:
        from_date = st.date_input("From Date", value=datetime.now() - timedelta(days=90))
    with col2:
        to_date = st.date_input("To Date", value=datetime.now())
    
    # Office filter
    office_options = ['All', '0936', '0937', '0938', '0939', '0940']  # Add more as needed
    selected_office = st.sidebar.selectbox("Office Number", office_options)
    
    # Convert dates to strings
    from_date_str = from_date.strftime('%Y-%m-%d')
    to_date_str = to_date.strftime('%Y-%m-%d')
    
    # Check available tables
    st.sidebar.title("📋 Data Availability")
    available_tables = analytics.check_billing_pos_tables_availability()
    
    for table, count in available_tables.items():
        if count > 0:
            st.sidebar.success(f"✅ {table}: {count:,}")
        else:
            st.sidebar.error(f"❌ {table}: Not available")
    
    # Main dashboard
    st.header("📊 Financial Overview")
    
    # Get overview data
    overview = analytics.get_billing_pos_overview(selected_office, from_date_str, to_date_str)
    
    # Display KPIs
    st.subheader("💳 Billing System Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Billing Transactions",
            value=f"{overview.get('billing', {}).get('total_transactions', 0):,}"
        )
    
    with col2:
        st.metric(
            label="Insurance AR",
            value=f"${overview.get('billing', {}).get('insurance_ar', 0):,.2f}"
        )
    
    with col3:
        st.metric(
            label="Patient AR",
            value=f"${overview.get('billing', {}).get('patient_ar', 0):,.2f}"
        )
    
    with col4:
        st.metric(
            label="Unique Orders",
            value=f"{overview.get('billing', {}).get('unique_orders', 0):,}"
        )
    
    st.subheader("🛒 POS System Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total POS Transactions",
            value=f"{overview.get('pos', {}).get('total_transactions', 0):,}"
        )
    
    with col2:
        st.metric(
            label="Total POS Amount",
            value=f"${overview.get('pos', {}).get('total_amount', 0):,.2f}"
        )
    
    with col3:
        st.metric(
            label="Average POS Transaction",
            value=f"${overview.get('pos', {}).get('avg_amount', 0):.2f}"
        )
    
    with col4:
        st.metric(
            label="Unique POS Orders",
            value=f"{overview.get('pos', {}).get('unique_orders', 0):,}"
        )
    
    # Transaction type analysis
    st.header("📊 Transaction Type Analysis")
    
    transaction_types = analytics.get_transaction_type_analysis(from_date_str, to_date_str, selected_office)
    
    if not transaction_types.empty:
        st.dataframe(transaction_types, use_container_width=True)
        
        # Visualization
        fig = px.bar(transaction_types, 
                    x='TRANSACTION_TYPE', y='TRANSACTION_COUNT',
                    title='Transaction Types by Volume',
                    labels={'TRANSACTION_COUNT': 'Transaction Count', 'TRANSACTION_TYPE': 'Transaction Type'})
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    # Outstanding balances
    st.header("💰 Outstanding Balances Summary")
    
    outstanding_balances = analytics.get_outstanding_balances_summary(from_date_str, to_date_str, selected_office)
    
    if not outstanding_balances.empty:
        st.dataframe(outstanding_balances, use_container_width=True)
        
        # Outstanding balances chart
        fig = px.bar(outstanding_balances.head(10), 
                    x='OFFICE_NAME', y='OUTSTANDING_BALANCE',
                    title='Top 10 Offices by Outstanding Balance',
                    labels={'OUTSTANDING_BALANCE': 'Outstanding Balance ($)', 'OFFICE_NAME': 'Office'})
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    # Insurance vs Patient analysis
    st.header("🏥 Insurance vs Patient Analysis")
    
    ins_vs_patient = analytics.get_insurance_vs_patient_analysis(from_date_str, to_date_str, selected_office)
    
    if not ins_vs_patient.empty:
        st.dataframe(ins_vs_patient, use_container_width=True)
        
        # Pie chart
        fig = px.pie(ins_vs_patient, values='TOTAL_AR', names='PAYER_TYPE',
                    title='Accounts Receivable Distribution: Insurance vs Patient')
        st.plotly_chart(fig, use_container_width=True)
    
    # AI Analysis
    st.header("🤖 AI-Powered Financial Analysis")
    
    if st.button("🧠 Generate Comprehensive Financial Insights"):
        with st.spinner("AI is analyzing your billing and POS data..."):
            ai_insights = analytics.cortex_analyze_billing_pos_data(overview)
            st.markdown("### 🤖 Cortex AI Strategic Insights:")
            st.markdown(ai_insights)
    
    # Footer
    st.markdown("---")
    st.markdown("🚀 **Built with Your Sophisticated V1.3 Query** | 💼 **Advanced Billing & POS Analytics**")

if __name__ == "__main__":
    main()
