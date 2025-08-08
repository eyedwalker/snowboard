#!/usr/bin/env python3
"""
Ultimate Financial Transaction Analytics Dashboard
Complete financial transaction analytics using the billing transaction layer
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

class UltimateFinancialTransactionAnalytics:
    """Complete financial transaction analytics using billing transaction layer"""
    
    def __init__(self):
        """Initialize ultimate financial transaction analytics"""
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
    
    def check_financial_tables_availability(self):
        """Check availability of all financial transaction tables"""
        tables_to_check = [
            'DBO_BILLINGTRANSACTION',        # Central financial hub
            'DBO_BILLINGTRANSACTIONTYPE',    # Transaction type descriptions
            'DBO_BILLINGCLAIMLINEITEM',      # Claim line items
            'DBO_BILLINGCLAIMDATA',          # Claim header data
            'DBO_BILLINGADJUSTMENTTYPE',     # Adjustment types
            'DBO_ITEMTYPE',                  # Item types
            'DBO_ITEM',                      # Items/products
            'DBO_BILLINGCLAIM',              # Billing claims
            'DBO_BILLINGCLAIMORDER',         # Claim orders
            'DBO_BILLINGCLAIMDETAIL'         # Claim details
        ]
        
        available_tables = {}
        
        for table in tables_to_check:
            try:
                query = f"SELECT COUNT(*) as row_count FROM EYECARE_ANALYTICS.RAW.{table}"
                df = self.execute_query(query)
                if not df.empty:
                    available_tables[table] = df.iloc[0]['ROW_COUNT']
            except:
                available_tables[table] = 0
        
        return available_tables
    
    def get_financial_transaction_overview(self):
        """Get comprehensive financial transaction overview"""
        overview = {}
        
        # Billing transactions overview
        try:
            transaction_query = """
            SELECT 
                COUNT(*) as total_transactions,
                COUNT(DISTINCT "OrderId") as unique_orders,
                SUM(CAST("InsAR" AS DECIMAL(18,2))) as total_insurance_ar,
                SUM(CAST("PatAR" AS DECIMAL(18,2))) as total_patient_ar,
                SUM(CAST("InsDeltaAR" AS DECIMAL(18,2))) as total_insurance_delta_ar,
                SUM(CAST("PatDeltaAR" AS DECIMAL(18,2))) as total_patient_delta_ar,
                COUNT(CASE WHEN "IsVoid" = 'True' OR "IsVoid" = '1' THEN 1 END) as voided_transactions
            FROM EYECARE_ANALYTICS.RAW.DBO_BILLINGTRANSACTION
            WHERE "TransactionDate" IS NOT NULL
            """
            
            df = self.execute_query(transaction_query)
            if not df.empty:
                overview['transactions'] = {
                    'total': df.iloc[0]['TOTAL_TRANSACTIONS'],
                    'unique_orders': df.iloc[0]['UNIQUE_ORDERS'],
                    'insurance_ar': df.iloc[0]['TOTAL_INSURANCE_AR'],
                    'patient_ar': df.iloc[0]['TOTAL_PATIENT_AR'],
                    'insurance_delta_ar': df.iloc[0]['TOTAL_INSURANCE_DELTA_AR'],
                    'patient_delta_ar': df.iloc[0]['TOTAL_PATIENT_DELTA_AR'],
                    'voided': df.iloc[0]['VOIDED_TRANSACTIONS']
                }
        except:
            overview['transactions'] = {'total': 0, 'unique_orders': 0, 'insurance_ar': 0, 'patient_ar': 0, 'insurance_delta_ar': 0, 'patient_delta_ar': 0, 'voided': 0}
        
        # Claim line items overview
        try:
            claim_query = """
            SELECT 
                COUNT(*) as total_claim_line_items,
                SUM(CAST("Allowance" AS DECIMAL(18,2))) as total_allowances,
                SUM(CAST("Copay" AS DECIMAL(18,2))) as total_copays,
                SUM(CAST("InsuranceDiscount" AS DECIMAL(18,2))) as total_insurance_discounts,
                SUM(CAST("Retail" AS DECIMAL(18,2))) as total_retail,
                SUM(CAST("PaidAmount" AS DECIMAL(18,2))) as total_paid_amount,
                SUM(CAST("RetailDiscount" AS DECIMAL(18,2))) as total_retail_discounts,
                COUNT(DISTINCT "CurrentState") as unique_states
            FROM EYECARE_ANALYTICS.RAW.DBO_BILLINGCLAIMLINEITEM
            WHERE "Allowance" IS NOT NULL AND "Allowance" != ''
            """
            
            df = self.execute_query(claim_query)
            if not df.empty:
                overview['claims'] = {
                    'total_line_items': df.iloc[0]['TOTAL_CLAIM_LINE_ITEMS'],
                    'total_allowances': df.iloc[0]['TOTAL_ALLOWANCES'],
                    'total_copays': df.iloc[0]['TOTAL_COPAYS'],
                    'total_insurance_discounts': df.iloc[0]['TOTAL_INSURANCE_DISCOUNTS'],
                    'total_retail': df.iloc[0]['TOTAL_RETAIL'],
                    'total_paid_amount': df.iloc[0]['TOTAL_PAID_AMOUNT'],
                    'total_retail_discounts': df.iloc[0]['TOTAL_RETAIL_DISCOUNTS'],
                    'unique_states': df.iloc[0]['UNIQUE_STATES']
                }
        except:
            overview['claims'] = {'total_line_items': 0, 'total_allowances': 0, 'total_copays': 0, 'total_insurance_discounts': 0, 'total_retail': 0, 'total_paid_amount': 0, 'total_retail_discounts': 0, 'unique_states': 0}
        
        return overview
    
    def get_complete_financial_transaction_data(self):
        """Get complete financial transaction data using the exact SQL structure"""
        try:
            # Adapt the user's complete billing transaction SQL to our Snowflake structure
            query = """
            SELECT 
                bt."TransId" as trans_id,
                bt."LineItemId" as billing_claim_line_item_id,
                bt."OrderId" as order_id,
                bt."TransactionDate" as transaction_date,
                btt."Description" as billing_transaction_type,
                bt."InsAR" as ins_ar,
                bt."PatAR" as pat_ar,
                bt."InsDeltaAR" as ins_delta_ar,
                bt."PatDeltaAR" as pat_delta_ar,
                bcd."OfficeNum" as office_num,
                bcli."ItemID" as company_info_id,
                bcli."ItemDescription" as item_description,
                it."Description" as item_type,
                bt."TransTypeId" as trans_type_id,
                bcli."CurrentState" as current_state,
                bcli."Allowance" as allowance,
                bcli."Copay" as copay,
                bcli."InsuranceDiscount" as insurance_discount,
                bcli."Retail" as retail,
                bcli."BilledDate" as billed_date,
                bcli."PaidAmount" as paid_amount,
                bcli."RetailDiscount" as retail_discount
            FROM EYECARE_ANALYTICS.RAW.DBO_BILLINGTRANSACTION bt
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_BILLINGTRANSACTIONTYPE btt ON btt."ID" = bt."TransTypeId"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_BILLINGCLAIMLINEITEM bcli ON bcli."LineItemId" = bt."LineItemId"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_ITEMTYPE it ON it."ItemType" = bcli."InsItemTypeId"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_BILLINGCLAIMDATA bcd ON bcli."ClaimId" = bcd."ClaimId" AND (bcd."IsCurrent" = 'True' OR bcd."IsCurrent" = '1')
            WHERE bt."TransactionDate" > '2023-05-01' 
            AND (bt."IsVoid" = 'False' OR bt."IsVoid" = '0' OR bt."IsVoid" IS NULL)
            ORDER BY bt."TransactionDate" DESC
            LIMIT 1000
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error executing complete financial transaction query: {str(e)}")
            
            # Fallback to simple transaction data
            try:
                fallback_query = """
                SELECT 
                    "TransId" as trans_id,
                    "OrderId" as order_id,
                    "TransactionDate" as transaction_date,
                    "InsAR" as ins_ar,
                    "PatAR" as pat_ar,
                    "TransTypeId" as trans_type_id
                FROM EYECARE_ANALYTICS.RAW.DBO_BILLINGTRANSACTION
                WHERE "TransactionDate" > '2023-05-01'
                AND ("IsVoid" = 'False' OR "IsVoid" = '0' OR "IsVoid" IS NULL)
                ORDER BY "TransactionDate" DESC
                LIMIT 1000
                """
                
                return self.execute_query(fallback_query)
            except:
                return pd.DataFrame()
    
    def get_transaction_type_analysis(self):
        """Analyze transactions by type"""
        try:
            query = """
            SELECT 
                btt."Description" as transaction_type,
                COUNT(*) as transaction_count,
                SUM(CAST(bt."InsAR" AS DECIMAL(18,2))) as total_insurance_ar,
                SUM(CAST(bt."PatAR" AS DECIMAL(18,2))) as total_patient_ar,
                AVG(CAST(bt."InsAR" AS DECIMAL(18,2))) as avg_insurance_ar,
                AVG(CAST(bt."PatAR" AS DECIMAL(18,2))) as avg_patient_ar
            FROM EYECARE_ANALYTICS.RAW.DBO_BILLINGTRANSACTION bt
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_BILLINGTRANSACTIONTYPE btt ON btt."ID" = bt."TransTypeId"
            WHERE bt."TransactionDate" > '2023-05-01'
            AND (bt."IsVoid" = 'False' OR bt."IsVoid" = '0' OR bt."IsVoid" IS NULL)
            AND btt."Description" IS NOT NULL AND btt."Description" != ''
            GROUP BY btt."Description"
            ORDER BY transaction_count DESC
            LIMIT 20
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error analyzing transaction types: {str(e)}")
            return pd.DataFrame()
    
    def get_accounts_receivable_trends(self):
        """Analyze accounts receivable trends over time"""
        try:
            query = """
            SELECT 
                DATE_TRUNC('month', TO_DATE(bt."TransactionDate")) as transaction_month,
                SUM(CAST(bt."InsAR" AS DECIMAL(18,2))) as monthly_insurance_ar,
                SUM(CAST(bt."PatAR" AS DECIMAL(18,2))) as monthly_patient_ar,
                SUM(CAST(bt."InsDeltaAR" AS DECIMAL(18,2))) as monthly_insurance_delta_ar,
                SUM(CAST(bt."PatDeltaAR" AS DECIMAL(18,2))) as monthly_patient_delta_ar,
                COUNT(*) as monthly_transaction_count
            FROM EYECARE_ANALYTICS.RAW.DBO_BILLINGTRANSACTION bt
            WHERE bt."TransactionDate" > '2023-01-01'
            AND (bt."IsVoid" = 'False' OR bt."IsVoid" = '0' OR bt."IsVoid" IS NULL)
            AND bt."TransactionDate" IS NOT NULL
            GROUP BY DATE_TRUNC('month', TO_DATE(bt."TransactionDate"))
            ORDER BY transaction_month DESC
            LIMIT 24
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error analyzing AR trends: {str(e)}")
            return pd.DataFrame()
    
    def get_claim_state_analysis(self):
        """Analyze claim states and their financial impact"""
        try:
            query = """
            SELECT 
                bcli."CurrentState" as claim_state,
                COUNT(*) as claim_count,
                SUM(CAST(bcli."Allowance" AS DECIMAL(18,2))) as total_allowance,
                SUM(CAST(bcli."Copay" AS DECIMAL(18,2))) as total_copay,
                SUM(CAST(bcli."PaidAmount" AS DECIMAL(18,2))) as total_paid_amount,
                SUM(CAST(bcli."Retail" AS DECIMAL(18,2))) as total_retail,
                AVG(CAST(bcli."PaidAmount" AS DECIMAL(18,2))) as avg_paid_amount
            FROM EYECARE_ANALYTICS.RAW.DBO_BILLINGCLAIMLINEITEM bcli
            WHERE bcli."CurrentState" IS NOT NULL AND bcli."CurrentState" != ''
            AND bcli."Allowance" IS NOT NULL AND bcli."Allowance" != ''
            GROUP BY bcli."CurrentState"
            ORDER BY claim_count DESC
            LIMIT 20
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error analyzing claim states: {str(e)}")
            return pd.DataFrame()
    
    def get_office_financial_performance(self):
        """Analyze financial performance by office"""
        try:
            query = """
            SELECT 
                bcd."OfficeNum" as office_num,
                COUNT(DISTINCT bt."TransId") as transaction_count,
                SUM(CAST(bt."InsAR" AS DECIMAL(18,2))) as total_insurance_ar,
                SUM(CAST(bt."PatAR" AS DECIMAL(18,2))) as total_patient_ar,
                SUM(CAST(bcli."PaidAmount" AS DECIMAL(18,2))) as total_paid_amount,
                AVG(CAST(bcli."PaidAmount" AS DECIMAL(18,2))) as avg_paid_amount
            FROM EYECARE_ANALYTICS.RAW.DBO_BILLINGTRANSACTION bt
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_BILLINGCLAIMLINEITEM bcli ON bcli."LineItemId" = bt."LineItemId"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_BILLINGCLAIMDATA bcd ON bcli."ClaimId" = bcd."ClaimId" AND (bcd."IsCurrent" = 'True' OR bcd."IsCurrent" = '1')
            WHERE bt."TransactionDate" > '2023-05-01'
            AND (bt."IsVoid" = 'False' OR bt."IsVoid" = '0' OR bt."IsVoid" IS NULL)
            AND bcd."OfficeNum" IS NOT NULL AND bcd."OfficeNum" != ''
            GROUP BY bcd."OfficeNum"
            ORDER BY total_paid_amount DESC
            LIMIT 20
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error analyzing office financial performance: {str(e)}")
            return pd.DataFrame()
    
    def cortex_analyze_financial_transactions(self, overview: dict) -> str:
        """Use Cortex AI to analyze the complete financial transaction data"""
        try:
            transactions = overview.get('transactions', {})
            claims = overview.get('claims', {})
            
            prompt = f"""
            Analyze this comprehensive financial transaction data for an eyecare practice:
            
            BILLING TRANSACTIONS:
            - Total transactions: {transactions.get('total', 0):,}
            - Unique orders: {transactions.get('unique_orders', 0):,}
            - Insurance AR: ${transactions.get('insurance_ar', 0):,.2f}
            - Patient AR: ${transactions.get('patient_ar', 0):,.2f}
            - Insurance Delta AR: ${transactions.get('insurance_delta_ar', 0):,.2f}
            - Patient Delta AR: ${transactions.get('patient_delta_ar', 0):,.2f}
            - Voided transactions: {transactions.get('voided', 0):,}
            
            CLAIM LINE ITEMS:
            - Total line items: {claims.get('total_line_items', 0):,}
            - Total allowances: ${claims.get('total_allowances', 0):,.2f}
            - Total copays: ${claims.get('total_copays', 0):,.2f}
            - Total insurance discounts: ${claims.get('total_insurance_discounts', 0):,.2f}
            - Total retail: ${claims.get('total_retail', 0):,.2f}
            - Total paid amount: ${claims.get('total_paid_amount', 0):,.2f}
            - Total retail discounts: ${claims.get('total_retail_discounts', 0):,.2f}
            
            Provide strategic insights on:
            1. Financial transaction efficiency
            2. Accounts receivable optimization
            3. Claims processing performance
            4. Cash flow management
            5. Revenue cycle optimization opportunities
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
        page_title="Ultimate Financial Transaction Analytics",
        page_icon="💳",
        layout="wide"
    )
    
    st.title("💳 Ultimate Financial Transaction Analytics")
    st.markdown("### Complete Financial Transaction Management Using Billing Transaction Layer")
    
    # Initialize analytics
    analytics = UltimateFinancialTransactionAnalytics()
    
    # Check available tables
    st.sidebar.title("📊 Financial Data Availability")
    available_tables = analytics.check_financial_tables_availability()
    
    for table, count in available_tables.items():
        if count > 0:
            st.sidebar.success(f"✅ {table}: {count:,} rows")
        else:
            st.sidebar.error(f"❌ {table}: Not available")
    
    # Main dashboard
    st.header("📊 Financial Transaction Overview")
    
    # Get comprehensive overview
    overview = analytics.get_financial_transaction_overview()
    
    # Display KPIs in organized sections
    st.subheader("💳 Billing Transactions")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Transactions",
            value=f"{overview.get('transactions', {}).get('total', 0):,}"
        )
    
    with col2:
        st.metric(
            label="Unique Orders",
            value=f"{overview.get('transactions', {}).get('unique_orders', 0):,}"
        )
    
    with col3:
        st.metric(
            label="Voided Transactions",
            value=f"{overview.get('transactions', {}).get('voided', 0):,}"
        )
    
    with col4:
        st.metric(
            label="Transaction Rate",
            value=f"{(overview.get('transactions', {}).get('total', 0) - overview.get('transactions', {}).get('voided', 0)) / max(overview.get('transactions', {}).get('total', 1), 1) * 100:.1f}%"
        )
    
    st.subheader("💰 Accounts Receivable")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Insurance AR",
            value=f"${overview.get('transactions', {}).get('insurance_ar', 0):,.2f}"
        )
    
    with col2:
        st.metric(
            label="Patient AR",
            value=f"${overview.get('transactions', {}).get('patient_ar', 0):,.2f}"
        )
    
    with col3:
        st.metric(
            label="Insurance Delta AR",
            value=f"${overview.get('transactions', {}).get('insurance_delta_ar', 0):,.2f}"
        )
    
    with col4:
        st.metric(
            label="Patient Delta AR",
            value=f"${overview.get('transactions', {}).get('patient_delta_ar', 0):,.2f}"
        )
    
    st.subheader("🏥 Claims Financial Data")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Allowances",
            value=f"${overview.get('claims', {}).get('total_allowances', 0):,.2f}"
        )
    
    with col2:
        st.metric(
            label="Total Copays",
            value=f"${overview.get('claims', {}).get('total_copays', 0):,.2f}"
        )
    
    with col3:
        st.metric(
            label="Total Paid Amount",
            value=f"${overview.get('claims', {}).get('total_paid_amount', 0):,.2f}"
        )
    
    with col4:
        st.metric(
            label="Total Retail",
            value=f"${overview.get('claims', {}).get('total_retail', 0):,.2f}"
        )
    
    # Transaction type analysis
    st.header("📊 Transaction Type Analysis")
    
    transaction_types = analytics.get_transaction_type_analysis()
    
    if not transaction_types.empty:
        st.dataframe(transaction_types, use_container_width=True)
        
        # Visualization
        fig = px.bar(transaction_types.head(10), 
                    x='TRANSACTION_TYPE', y='TRANSACTION_COUNT',
                    title='Top 10 Transaction Types by Volume',
                    labels={'TRANSACTION_COUNT': 'Transaction Count', 'TRANSACTION_TYPE': 'Transaction Type'})
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    # AR trends analysis
    st.header("📈 Accounts Receivable Trends")
    
    ar_trends = analytics.get_accounts_receivable_trends()
    
    if not ar_trends.empty:
        st.dataframe(ar_trends, use_container_width=True)
        
        # Time series chart
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=ar_trends['TRANSACTION_MONTH'], y=ar_trends['MONTHLY_INSURANCE_AR'],
                                mode='lines+markers', name='Insurance AR'))
        fig.add_trace(go.Scatter(x=ar_trends['TRANSACTION_MONTH'], y=ar_trends['MONTHLY_PATIENT_AR'],
                                mode='lines+markers', name='Patient AR'))
        fig.update_layout(title='Monthly Accounts Receivable Trends',
                         xaxis_title='Month', yaxis_title='Amount ($)')
        st.plotly_chart(fig, use_container_width=True)
    
    # Claim state analysis
    st.header("📋 Claim State Analysis")
    
    claim_states = analytics.get_claim_state_analysis()
    
    if not claim_states.empty:
        st.dataframe(claim_states, use_container_width=True)
        
        # Pie chart
        fig = px.pie(claim_states, values='CLAIM_COUNT', names='CLAIM_STATE',
                    title='Distribution of Claim States')
        st.plotly_chart(fig, use_container_width=True)
    
    # Office financial performance
    st.header("🏢 Office Financial Performance")
    
    office_performance = analytics.get_office_financial_performance()
    
    if not office_performance.empty:
        st.dataframe(office_performance, use_container_width=True)
        
        # Bar chart
        fig = px.bar(office_performance.head(10), 
                    x='OFFICE_NUM', y='TOTAL_PAID_AMOUNT',
                    title='Top 10 Offices by Total Paid Amount',
                    labels={'TOTAL_PAID_AMOUNT': 'Total Paid Amount ($)', 'OFFICE_NUM': 'Office Number'})
        st.plotly_chart(fig, use_container_width=True)
    
    # Complete financial transaction data
    st.header("🔗 Complete Financial Transaction Data (Using Your SQL Model)")
    
    complete_data = analytics.get_complete_financial_transaction_data()
    
    if not complete_data.empty:
        st.dataframe(complete_data.head(100), use_container_width=True)
        st.info(f"Showing first 100 rows of {len(complete_data):,} total records")
    
    # AI Analysis
    st.header("🤖 AI-Powered Financial Transaction Analysis")
    
    if st.button("🧠 Generate Comprehensive Financial Transaction Insights"):
        with st.spinner("AI is analyzing your complete financial transaction data..."):
            ai_insights = analytics.cortex_analyze_financial_transactions(overview)
            st.markdown("### 🤖 Cortex AI Strategic Insights:")
            st.markdown(ai_insights)
    
    # Footer
    st.markdown("---")
    st.markdown("🚀 **Built with Your Complete Billing Transaction Model** | 💳 **Ultimate Financial Transaction Analytics**")

if __name__ == "__main__":
    main()
