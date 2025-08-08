#!/usr/bin/env python3
"""
Ultimate Revenue Cycle Analytics Dashboard
Complete eyecare revenue cycle analytics using the central invoice intersection model
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

# Load environment variables
project_root = Path(__file__).parent.parent.parent
load_dotenv(project_root / ".env")

class UltimateRevenueCycleAnalytics:
    """Complete revenue cycle analytics using invoice intersection model"""
    
    def __init__(self):
        """Initialize ultimate revenue cycle analytics"""
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
    
    def check_revenue_cycle_tables(self):
        """Check availability of all revenue cycle tables"""
        tables_to_check = [
            'DBO_INVOICEDET',           # Central hub - invoice details
            'DBO_INVOICEINSURANCEDET',  # Insurance details per invoice line
            'DBO_INSELIGIBILITY',       # Insurance eligibility
            'DBO_PATIENTINSURANCE',     # Patient insurance relationships
            'DBO_INSPLAN',              # Insurance plans
            'DBO_INSSCHEDULE',          # Insurance schedules/pricing
            'DBO_INSSCHEDULEMETHOD',    # Schedule methods
            'DBO_ITEM',                 # Products/services
            'DBO_ITEMTYPE',             # Item types
            'DBO_INVOICE',              # Invoice headers
            'DBO_PATIENT',              # Patients
            'DBO_PROMOTION'             # Promotions/discounts
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
    
    def get_revenue_cycle_overview(self):
        """Get comprehensive revenue cycle overview"""
        overview = {}
        
        # Invoice overview
        try:
            invoice_query = """
            SELECT 
                COUNT(*) as total_invoice_lines,
                COUNT(DISTINCT "InvoiceID") as unique_invoices,
                SUM(CAST("Amount" AS DECIMAL(18,2))) as total_revenue,
                AVG(CAST("Amount" AS DECIMAL(18,2))) as avg_line_amount,
                COUNT(CASE WHEN "BillToInsurance" = 'True' OR "BillToInsurance" = '1' THEN 1 END) as insurance_billed_lines
            FROM EYECARE_ANALYTICS.RAW.DBO_INVOICEDET
            WHERE "Amount" IS NOT NULL AND "Amount" != ''
            """
            
            df = self.execute_query(invoice_query)
            if not df.empty:
                overview['invoices'] = {
                    'total_lines': df.iloc[0]['TOTAL_INVOICE_LINES'],
                    'unique_invoices': df.iloc[0]['UNIQUE_INVOICES'],
                    'total_revenue': df.iloc[0]['TOTAL_REVENUE'],
                    'avg_line_amount': df.iloc[0]['AVG_LINE_AMOUNT'],
                    'insurance_billed': df.iloc[0]['INSURANCE_BILLED_LINES']
                }
        except:
            overview['invoices'] = {'total_lines': 0, 'unique_invoices': 0, 'total_revenue': 0, 'avg_line_amount': 0, 'insurance_billed': 0}
        
        # Insurance processing overview
        try:
            insurance_query = """
            SELECT 
                COUNT(*) as total_insurance_lines,
                SUM(CAST("Allowance" AS DECIMAL(18,2))) as total_allowances,
                SUM(CAST("Copay" AS DECIMAL(18,2))) as total_copays,
                SUM(CAST("Receivable" AS DECIMAL(18,2))) as total_receivables,
                SUM(CAST("InsuranceDiscount" AS DECIMAL(18,2))) as total_insurance_discounts,
                COUNT(CASE WHEN "IsCovered" = 'True' OR "IsCovered" = '1' THEN 1 END) as covered_lines,
                COUNT(CASE WHEN "IsPrimary" = 'True' OR "IsPrimary" = '1' THEN 1 END) as primary_insurance_lines
            FROM EYECARE_ANALYTICS.RAW.DBO_INVOICEINSURANCEDET
            WHERE "Allowance" IS NOT NULL AND "Allowance" != ''
            """
            
            df = self.execute_query(insurance_query)
            if not df.empty:
                overview['insurance'] = {
                    'total_lines': df.iloc[0]['TOTAL_INSURANCE_LINES'],
                    'total_allowances': df.iloc[0]['TOTAL_ALLOWANCES'],
                    'total_copays': df.iloc[0]['TOTAL_COPAYS'],
                    'total_receivables': df.iloc[0]['TOTAL_RECEIVABLES'],
                    'total_discounts': df.iloc[0]['TOTAL_INSURANCE_DISCOUNTS'],
                    'covered_lines': df.iloc[0]['COVERED_LINES'],
                    'primary_lines': df.iloc[0]['PRIMARY_INSURANCE_LINES']
                }
        except:
            overview['insurance'] = {'total_lines': 0, 'total_allowances': 0, 'total_copays': 0, 'total_receivables': 0, 'total_discounts': 0, 'covered_lines': 0, 'primary_lines': 0}
        
        # Eligibility overview
        try:
            eligibility_query = """
            SELECT 
                COUNT(*) as total_eligibility_records,
                COUNT(CASE WHEN "IsExamElig" = 'True' OR "IsExamElig" = '1' THEN 1 END) as exam_eligible,
                COUNT(CASE WHEN "IsFrameElig" = 'True' OR "IsFrameElig" = '1' THEN 1 END) as frame_eligible,
                COUNT(CASE WHEN "IsLensElig" = 'True' OR "IsLensElig" = '1' THEN 1 END) as lens_eligible,
                COUNT(CASE WHEN "IsCLElig" = 'True' OR "IsCLElig" = '1' THEN 1 END) as cl_eligible,
                COUNT(CASE WHEN "IsMedicalExamElig" = 'True' OR "IsMedicalExamElig" = '1' THEN 1 END) as medical_eligible
            FROM EYECARE_ANALYTICS.RAW.DBO_INSELIGIBILITY
            """
            
            df = self.execute_query(eligibility_query)
            if not df.empty:
                overview['eligibility'] = {
                    'total_records': df.iloc[0]['TOTAL_ELIGIBILITY_RECORDS'],
                    'exam_eligible': df.iloc[0]['EXAM_ELIGIBLE'],
                    'frame_eligible': df.iloc[0]['FRAME_ELIGIBLE'],
                    'lens_eligible': df.iloc[0]['LENS_ELIGIBLE'],
                    'cl_eligible': df.iloc[0]['CL_ELIGIBLE'],
                    'medical_eligible': df.iloc[0]['MEDICAL_ELIGIBLE']
                }
        except:
            overview['eligibility'] = {'total_records': 0, 'exam_eligible': 0, 'frame_eligible': 0, 'lens_eligible': 0, 'cl_eligible': 0, 'medical_eligible': 0}
        
        return overview
    
    def get_complete_revenue_cycle_data(self):
        """Get complete revenue cycle data using the exact intersection model"""
        try:
            # Adapt the user's complete SQL to our Snowflake structure
            query = """
            SELECT 
                -- Eligibility data
                ie."IsExamElig" as is_exam_eligible,
                ie."IsFrameElig" as is_frame_eligible,
                ie."IsLensElig" as is_lens_eligible,
                ie."IsCLElig" as is_cl_eligible,
                ie."IsMedicalExamElig" as is_medical_eligible,
                
                -- Insurance detail data
                iid."ItemID" as item_id,
                it."Description" as item_type_description,
                i."ItemName" as item_name,
                iid."Allowance" as allowance,
                iid."Copay" as copay,
                iid."Receivable" as receivable,
                iid."InsuranceDiscount" as insurance_discount,
                iid."Quantity" as quantity,
                iid."Retail" as retail,
                iid."IsCovered" as is_covered,
                iid."IsPrimary" as is_primary,
                
                -- Invoice detail data
                id."InvoiceID" as invoice_id,
                id."Price" as price,
                id."Tax" as tax,
                id."Discount" as discount,
                id."Amount" as amount,
                id."BillToInsurance" as bill_to_insurance,
                id."PromotionDiscount" as promotion_discount,
                
                -- Insurance plan data
                ip."CarrierCode" as carrier_code,
                ip."PlanName" as plan_name,
                ip."CoverageType" as coverage_type,
                
                -- Authorization data
                ie."InsCarrierCode" as ins_carrier_code,
                ie."AuthNumber" as auth_number
                
            FROM EYECARE_ANALYTICS.RAW.DBO_INVOICEDET id
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_ITEMTYPE it ON id."ItemType" = it."ItemType"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_ITEM i ON id."ItemID" = i."ID"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_INVOICEINSURANCEDET iid ON iid."OrderInsuranceId" = id."InvoiceID"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_INSELIGIBILITY ie ON ie."ID" = iid."InsuranceEligibilityId"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_PATIENTINSURANCE pi ON pi."ID" = iid."PatientInsuranceID"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_INSPLAN ip ON ip."ID" = pi."InsurancePlanID"
            WHERE id."Amount" IS NOT NULL AND id."Amount" != ''
            LIMIT 1000
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error executing complete revenue cycle query: {str(e)}")
            
            # Fallback to simple invoice data
            try:
                fallback_query = """
                SELECT 
                    "InvoiceID" as invoice_id,
                    "ItemType" as item_type,
                    "ItemID" as item_id,
                    "Price" as price,
                    "Amount" as amount,
                    "BillToInsurance" as bill_to_insurance
                FROM EYECARE_ANALYTICS.RAW.DBO_INVOICEDET
                WHERE "Amount" IS NOT NULL AND "Amount" != ''
                LIMIT 1000
                """
                
                return self.execute_query(fallback_query)
            except:
                return pd.DataFrame()
    
    def get_revenue_by_service_type(self):
        """Analyze revenue by service/product type"""
        try:
            query = """
            SELECT 
                it."Description" as service_type,
                COUNT(*) as transaction_count,
                SUM(CAST(id."Amount" AS DECIMAL(18,2))) as total_revenue,
                AVG(CAST(id."Amount" AS DECIMAL(18,2))) as avg_revenue_per_transaction,
                COUNT(CASE WHEN id."BillToInsurance" = 'True' OR id."BillToInsurance" = '1' THEN 1 END) as insurance_billed_count
            FROM EYECARE_ANALYTICS.RAW.DBO_INVOICEDET id
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_ITEMTYPE it ON id."ItemType" = it."ItemType"
            WHERE id."Amount" IS NOT NULL AND id."Amount" != ''
            AND it."Description" IS NOT NULL AND it."Description" != ''
            GROUP BY it."Description"
            ORDER BY total_revenue DESC
            LIMIT 20
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error analyzing revenue by service type: {str(e)}")
            return pd.DataFrame()
    
    def get_insurance_vs_cash_analysis(self):
        """Analyze insurance vs cash revenue patterns"""
        try:
            query = """
            SELECT 
                CASE 
                    WHEN "BillToInsurance" = 'True' OR "BillToInsurance" = '1' THEN 'Insurance Billed'
                    ELSE 'Cash/Self-Pay'
                END as payment_type,
                COUNT(*) as transaction_count,
                SUM(CAST("Amount" AS DECIMAL(18,2))) as total_revenue,
                AVG(CAST("Amount" AS DECIMAL(18,2))) as avg_transaction_amount
            FROM EYECARE_ANALYTICS.RAW.DBO_INVOICEDET
            WHERE "Amount" IS NOT NULL AND "Amount" != ''
            GROUP BY payment_type
            ORDER BY total_revenue DESC
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error analyzing insurance vs cash: {str(e)}")
            return pd.DataFrame()
    
    def get_eligibility_utilization_analysis(self):
        """Analyze eligibility utilization patterns"""
        try:
            query = """
            SELECT 
                'Exam Eligible' as eligibility_type,
                COUNT(CASE WHEN "IsExamElig" = 'True' OR "IsExamElig" = '1' THEN 1 END) as eligible_count,
                COUNT(*) as total_records
            FROM EYECARE_ANALYTICS.RAW.DBO_INSELIGIBILITY
            WHERE "IsExamElig" IS NOT NULL
            
            UNION ALL
            
            SELECT 
                'Frame Eligible' as eligibility_type,
                COUNT(CASE WHEN "IsFrameElig" = 'True' OR "IsFrameElig" = '1' THEN 1 END) as eligible_count,
                COUNT(*) as total_records
            FROM EYECARE_ANALYTICS.RAW.DBO_INSELIGIBILITY
            WHERE "IsFrameElig" IS NOT NULL
            
            UNION ALL
            
            SELECT 
                'Lens Eligible' as eligibility_type,
                COUNT(CASE WHEN "IsLensElig" = 'True' OR "IsLensElig" = '1' THEN 1 END) as eligible_count,
                COUNT(*) as total_records
            FROM EYECARE_ANALYTICS.RAW.DBO_INSELIGIBILITY
            WHERE "IsLensElig" IS NOT NULL
            
            UNION ALL
            
            SELECT 
                'Contact Lens Eligible' as eligibility_type,
                COUNT(CASE WHEN "IsCLElig" = 'True' OR "IsCLElig" = '1' THEN 1 END) as eligible_count,
                COUNT(*) as total_records
            FROM EYECARE_ANALYTICS.RAW.DBO_INSELIGIBILITY
            WHERE "IsCLElig" IS NOT NULL
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error analyzing eligibility utilization: {str(e)}")
            return pd.DataFrame()
    
    def cortex_analyze_revenue_cycle(self, overview: dict) -> str:
        """Use Cortex AI to analyze the complete revenue cycle"""
        try:
            invoices = overview.get('invoices', {})
            insurance = overview.get('insurance', {})
            eligibility = overview.get('eligibility', {})
            
            prompt = f"""
            Analyze this comprehensive eyecare revenue cycle data:
            
            INVOICE METRICS:
            - Total invoice lines: {invoices.get('total_lines', 0):,}
            - Unique invoices: {invoices.get('unique_invoices', 0):,}
            - Total revenue: ${invoices.get('total_revenue', 0):,.2f}
            - Average line amount: ${invoices.get('avg_line_amount', 0):.2f}
            - Insurance billed lines: {invoices.get('insurance_billed', 0):,}
            
            INSURANCE PROCESSING:
            - Insurance lines: {insurance.get('total_lines', 0):,}
            - Total allowances: ${insurance.get('total_allowances', 0):,.2f}
            - Total copays: ${insurance.get('total_copays', 0):,.2f}
            - Total receivables: ${insurance.get('total_receivables', 0):,.2f}
            - Insurance discounts: ${insurance.get('total_discounts', 0):,.2f}
            - Covered lines: {insurance.get('covered_lines', 0):,}
            
            ELIGIBILITY DATA:
            - Total eligibility records: {eligibility.get('total_records', 0):,}
            - Exam eligible: {eligibility.get('exam_eligible', 0):,}
            - Frame eligible: {eligibility.get('frame_eligible', 0):,}
            - Lens eligible: {eligibility.get('lens_eligible', 0):,}
            - Contact lens eligible: {eligibility.get('cl_eligible', 0):,}
            
            Provide strategic insights on:
            1. Revenue cycle efficiency
            2. Insurance processing optimization
            3. Eligibility utilization opportunities
            4. Cash flow optimization
            5. Operational recommendations
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
        page_title="Ultimate Revenue Cycle Analytics",
        page_icon="💰",
        layout="wide"
    )
    
    st.title("💰 Ultimate Revenue Cycle Analytics")
    st.markdown("### Complete Eyecare Revenue Cycle Management Using Invoice Intersection Model")
    
    # Initialize analytics
    analytics = UltimateRevenueCycleAnalytics()
    
    # Check available tables
    st.sidebar.title("📊 Revenue Cycle Data Availability")
    available_tables = analytics.check_revenue_cycle_tables()
    
    for table, count in available_tables.items():
        if count > 0:
            st.sidebar.success(f"✅ {table}: {count:,} rows")
        else:
            st.sidebar.error(f"❌ {table}: Not available")
    
    # Main dashboard
    st.header("📊 Revenue Cycle Overview")
    
    # Get comprehensive overview
    overview = analytics.get_revenue_cycle_overview()
    
    # Display KPIs in organized sections
    st.subheader("💰 Invoice Metrics")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label="Total Invoice Lines",
            value=f"{overview.get('invoices', {}).get('total_lines', 0):,}"
        )
    
    with col2:
        st.metric(
            label="Unique Invoices",
            value=f"{overview.get('invoices', {}).get('unique_invoices', 0):,}"
        )
    
    with col3:
        st.metric(
            label="Total Revenue",
            value=f"${overview.get('invoices', {}).get('total_revenue', 0):,.2f}"
        )
    
    with col4:
        st.metric(
            label="Avg Line Amount",
            value=f"${overview.get('invoices', {}).get('avg_line_amount', 0):.2f}"
        )
    
    with col5:
        st.metric(
            label="Insurance Billed Lines",
            value=f"{overview.get('invoices', {}).get('insurance_billed', 0):,}"
        )
    
    st.subheader("🏥 Insurance Processing")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Allowances",
            value=f"${overview.get('insurance', {}).get('total_allowances', 0):,.2f}"
        )
    
    with col2:
        st.metric(
            label="Total Copays",
            value=f"${overview.get('insurance', {}).get('total_copays', 0):,.2f}"
        )
    
    with col3:
        st.metric(
            label="Total Receivables",
            value=f"${overview.get('insurance', {}).get('total_receivables', 0):,.2f}"
        )
    
    with col4:
        st.metric(
            label="Insurance Discounts",
            value=f"${overview.get('insurance', {}).get('total_discounts', 0):,.2f}"
        )
    
    st.subheader("✅ Eligibility Overview")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label="Exam Eligible",
            value=f"{overview.get('eligibility', {}).get('exam_eligible', 0):,}"
        )
    
    with col2:
        st.metric(
            label="Frame Eligible",
            value=f"{overview.get('eligibility', {}).get('frame_eligible', 0):,}"
        )
    
    with col3:
        st.metric(
            label="Lens Eligible",
            value=f"{overview.get('eligibility', {}).get('lens_eligible', 0):,}"
        )
    
    with col4:
        st.metric(
            label="Contact Lens Eligible",
            value=f"{overview.get('eligibility', {}).get('cl_eligible', 0):,}"
        )
    
    with col5:
        st.metric(
            label="Medical Eligible",
            value=f"{overview.get('eligibility', {}).get('medical_eligible', 0):,}"
        )
    
    # Revenue by service type analysis
    st.header("📊 Revenue by Service Type")
    
    service_revenue = analytics.get_revenue_by_service_type()
    
    if not service_revenue.empty:
        st.dataframe(service_revenue, use_container_width=True)
        
        # Visualization
        fig = px.bar(service_revenue.head(10), 
                    x='SERVICE_TYPE', y='TOTAL_REVENUE',
                    title='Top 10 Service Types by Revenue',
                    labels={'TOTAL_REVENUE': 'Total Revenue ($)', 'SERVICE_TYPE': 'Service Type'})
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    # Insurance vs Cash analysis
    st.header("💳 Insurance vs Cash Analysis")
    
    payment_analysis = analytics.get_insurance_vs_cash_analysis()
    
    if not payment_analysis.empty:
        st.dataframe(payment_analysis, use_container_width=True)
        
        # Pie chart
        fig = px.pie(payment_analysis, values='TOTAL_REVENUE', names='PAYMENT_TYPE',
                    title='Revenue Distribution: Insurance vs Cash')
        st.plotly_chart(fig, use_container_width=True)
    
    # Eligibility utilization
    st.header("✅ Eligibility Utilization Analysis")
    
    eligibility_data = analytics.get_eligibility_utilization_analysis()
    
    if not eligibility_data.empty:
        # Calculate utilization rates
        eligibility_data['UTILIZATION_RATE'] = (eligibility_data['ELIGIBLE_COUNT'] / eligibility_data['TOTAL_RECORDS'] * 100).round(2)
        
        st.dataframe(eligibility_data, use_container_width=True)
        
        # Bar chart
        fig = px.bar(eligibility_data, 
                    x='ELIGIBILITY_TYPE', y='UTILIZATION_RATE',
                    title='Eligibility Utilization Rates by Service Type',
                    labels={'UTILIZATION_RATE': 'Utilization Rate (%)', 'ELIGIBILITY_TYPE': 'Eligibility Type'})
        st.plotly_chart(fig, use_container_width=True)
    
    # Complete revenue cycle data
    st.header("🔗 Complete Revenue Cycle Data (Using Your SQL Model)")
    
    complete_data = analytics.get_complete_revenue_cycle_data()
    
    if not complete_data.empty:
        st.dataframe(complete_data.head(100), use_container_width=True)
        st.info(f"Showing first 100 rows of {len(complete_data):,} total records")
    
    # AI Analysis
    st.header("🤖 AI-Powered Revenue Cycle Analysis")
    
    if st.button("🧠 Generate Comprehensive Revenue Cycle Insights"):
        with st.spinner("AI is analyzing your complete revenue cycle..."):
            ai_insights = analytics.cortex_analyze_revenue_cycle(overview)
            st.markdown("### 🤖 Cortex AI Strategic Insights:")
            st.markdown(ai_insights)
    
    # Footer
    st.markdown("---")
    st.markdown("🚀 **Built with Your Complete Invoice Intersection Model** | 💰 **Ultimate Revenue Cycle Analytics**")

if __name__ == "__main__":
    main()
