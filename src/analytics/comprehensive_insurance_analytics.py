#!/usr/bin/env python3
"""
Comprehensive Insurance Analytics Dashboard
Complete insurance ecosystem analytics using Carrier + Plan relationships from user's SQL knowledge
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

class ComprehensiveInsuranceAnalytics:
    """Complete insurance ecosystem analytics using proper table relationships"""
    
    def __init__(self):
        """Initialize comprehensive insurance analytics"""
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
    
    def check_insurance_tables_availability(self):
        """Check which insurance-related tables are available"""
        tables_to_check = [
            'DBO_INSCARRIER',
            'DBO_INSPLAN', 
            'DBO_ADDRESS', 
            'DBO_PHONE',
            'DBO_INSHCFA',
            'DBO_PATIENTINSURANCE',
            'DBO_INSURANCESUBSCRIBER',
            'DBO_INSURANCECARRIERCUSTOMATTRIBUTES',
            'DBO_INSPLANPRODUCTS',
            'DBO_INSBILLINGRULES'
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
    
    def get_insurance_ecosystem_overview(self):
        """Get comprehensive insurance ecosystem overview"""
        overview = {}
        
        # Carriers overview
        try:
            carrier_query = """
            SELECT 
                COUNT(*) as total_carriers,
                COUNT(CASE WHEN "Active" = 'True' OR "Active" = '1' THEN 1 END) as active_carriers,
                COUNT(CASE WHEN "IsVspCarrier" = 'True' OR "IsVspCarrier" = '1' THEN 1 END) as vsp_carriers
            FROM EYECARE_ANALYTICS.RAW.DBO_INSCARRIER
            WHERE "CarrierName" IS NOT NULL AND "CarrierName" != ''
            """
            
            df = self.execute_query(carrier_query)
            if not df.empty:
                overview['carriers'] = {
                    'total': df.iloc[0]['TOTAL_CARRIERS'],
                    'active': df.iloc[0]['ACTIVE_CARRIERS'],
                    'vsp': df.iloc[0]['VSP_CARRIERS']
                }
        except:
            overview['carriers'] = {'total': 0, 'active': 0, 'vsp': 0}
        
        # Plans overview
        try:
            plan_query = """
            SELECT 
                COUNT(*) as total_plans,
                COUNT(CASE WHEN "Active" = 'True' OR "Active" = '1' THEN 1 END) as active_plans,
                COUNT(DISTINCT "CarrierCode") as unique_carrier_codes,
                COUNT(CASE WHEN "OverrideCarrierHcfa" = 'True' OR "OverrideCarrierHcfa" = '1' THEN 1 END) as override_hcfa_plans
            FROM EYECARE_ANALYTICS.RAW.DBO_INSPLAN
            WHERE "PlanName" IS NOT NULL AND "PlanName" != ''
            """
            
            df = self.execute_query(plan_query)
            if not df.empty:
                overview['plans'] = {
                    'total': df.iloc[0]['TOTAL_PLANS'],
                    'active': df.iloc[0]['ACTIVE_PLANS'],
                    'carrier_codes': df.iloc[0]['UNIQUE_CARRIER_CODES'],
                    'override_hcfa': df.iloc[0]['OVERRIDE_HCFA_PLANS']
                }
        except:
            overview['plans'] = {'total': 0, 'active': 0, 'carrier_codes': 0, 'override_hcfa': 0}
        
        # Patient insurance overview
        try:
            patient_ins_query = """
            SELECT 
                COUNT(*) as total_patient_insurance_records,
                COUNT(DISTINCT "PatientId") as patients_with_insurance
            FROM EYECARE_ANALYTICS.RAW.DBO_PATIENTINSURANCE
            WHERE "PatientId" IS NOT NULL AND "PatientId" != ''
            """
            
            df = self.execute_query(patient_ins_query)
            if not df.empty:
                overview['patient_insurance'] = {
                    'total_records': df.iloc[0]['TOTAL_PATIENT_INSURANCE_RECORDS'],
                    'patients_with_insurance': df.iloc[0]['PATIENTS_WITH_INSURANCE']
                }
        except:
            overview['patient_insurance'] = {'total_records': 0, 'patients_with_insurance': 0}
        
        return overview
    
    def get_carrier_plan_relationship_analysis(self):
        """Analyze carrier-plan relationships using the exact SQL structure"""
        try:
            # This query attempts to join carriers and plans based on the user's SQL knowledge
            query = """
            SELECT 
                ic."CarrierName" as carrier_name,
                COUNT(ip."ID") as total_plans,
                COUNT(CASE WHEN ip."Active" = 'True' OR ip."Active" = '1' THEN 1 END) as active_plans,
                COUNT(CASE WHEN ip."OverrideCarrierHcfa" = 'True' OR ip."OverrideCarrierHcfa" = '1' THEN 1 END) as override_hcfa_plans,
                STRING_AGG(ip."PlanName", ', ') as sample_plan_names
            FROM EYECARE_ANALYTICS.RAW.DBO_INSCARRIER ic
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_INSPLAN ip ON ic."CarrierCode" = ip."CarrierCode"
            WHERE ic."CarrierName" IS NOT NULL AND ic."CarrierName" != ''
            GROUP BY ic."CarrierName"
            ORDER BY total_plans DESC
            LIMIT 20
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error analyzing carrier-plan relationships: {str(e)}")
            return pd.DataFrame()
    
    def get_plan_details_with_full_joins(self):
        """Get plan details using the exact joins from user's SQL"""
        try:
            # Adapt the user's InsPlan SQL to our Snowflake table structure
            query = """
            SELECT 
                ip."ID" as plan_id,
                ip."CarrierCode" as carrier_code,
                ip."PlanName" as plan_name,
                ip."Active" as is_active,
                ip."StartDate" as start_date,
                ip."RenewalDate" as renewal_date,
                ip."VspProductPackage" as vsp_product_package,
                ip."OverrideCarrierHcfa" as override_carrier_hcfa,
                addr."Address1" as address1,
                addr."City" as city,
                addr."State" as state,
                phone."PhoneNumber" as phone_number,
                phone2."PhoneNumber" as fax_number,
                hcfa."BillingChargeAmount" as billing_charge_amount,
                hcfa."PlaceOfService" as place_of_service
            FROM EYECARE_ANALYTICS.RAW.DBO_INSPLAN ip
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_ADDRESS addr ON ip."AddressId" = addr."ID"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_PHONE phone ON ip."PhoneId" = phone."ID"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_PHONE phone2 ON ip."FaxId" = phone2."ID"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_INSHCFA hcfa ON ip."HcfaID" = hcfa."ID"
            WHERE ip."PlanName" IS NOT NULL AND ip."PlanName" != ''
            AND (ip."OverrideCarrierHcfa" = 'True' OR ip."OverrideCarrierHcfa" = '1')
            LIMIT 50
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error executing plan joins query: {str(e)}")
            
            # Fallback to simple plan data
            try:
                fallback_query = """
                SELECT 
                    "ID" as plan_id,
                    "CarrierCode" as carrier_code,
                    "PlanName" as plan_name,
                    "Active" as is_active,
                    "VspProductPackage" as vsp_product_package,
                    "OverrideCarrierHcfa" as override_carrier_hcfa
                FROM EYECARE_ANALYTICS.RAW.DBO_INSPLAN
                WHERE "PlanName" IS NOT NULL AND "PlanName" != ''
                LIMIT 50
                """
                
                return self.execute_query(fallback_query)
            except:
                return pd.DataFrame()
    
    def get_vsp_vs_other_analysis(self):
        """Analyze VSP vs other insurance patterns"""
        try:
            query = """
            SELECT 
                CASE 
                    WHEN ip."VspProductPackage" IS NOT NULL AND ip."VspProductPackage" != '' THEN 'VSP Plans'
                    WHEN ic."IsVspCarrier" = 'True' OR ic."IsVspCarrier" = '1' THEN 'VSP Carrier Plans'
                    ELSE 'Other Plans'
                END as plan_category,
                COUNT(*) as plan_count,
                COUNT(CASE WHEN ip."Active" = 'True' OR ip."Active" = '1' THEN 1 END) as active_count
            FROM EYECARE_ANALYTICS.RAW.DBO_INSPLAN ip
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_INSCARRIER ic ON ip."CarrierCode" = ic."CarrierCode"
            WHERE ip."PlanName" IS NOT NULL AND ip."PlanName" != ''
            GROUP BY plan_category
            ORDER BY plan_count DESC
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error analyzing VSP vs other plans: {str(e)}")
            return pd.DataFrame()
    
    def cortex_analyze_insurance_ecosystem(self, overview: dict) -> str:
        """Use Cortex AI to analyze the complete insurance ecosystem"""
        try:
            carriers = overview.get('carriers', {})
            plans = overview.get('plans', {})
            patient_ins = overview.get('patient_insurance', {})
            
            prompt = f"""
            Analyze this comprehensive insurance ecosystem for an eyecare practice:
            
            CARRIERS:
            - Total carriers: {carriers.get('total', 0)}
            - Active carriers: {carriers.get('active', 0)}
            - VSP carriers: {carriers.get('vsp', 0)}
            
            PLANS:
            - Total plans: {plans.get('total', 0)}
            - Active plans: {plans.get('active', 0)}
            - Unique carrier codes: {plans.get('carrier_codes', 0)}
            - Plans with HCFA overrides: {plans.get('override_hcfa', 0)}
            
            PATIENT INSURANCE:
            - Total insurance records: {patient_ins.get('total_records', 0)}
            - Patients with insurance: {patient_ins.get('patients_with_insurance', 0)}
            
            Provide strategic insights on:
            1. Insurance portfolio optimization
            2. VSP vs other carrier balance
            3. Plan management efficiency
            4. Revenue optimization opportunities
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
        page_title="Comprehensive Insurance Analytics",
        page_icon="🏥",
        layout="wide"
    )
    
    st.title("🏥 Comprehensive Insurance Ecosystem Analytics")
    st.markdown("### Complete Carrier + Plan Analytics Using Your SQL Knowledge")
    
    # Initialize analytics
    analytics = ComprehensiveInsuranceAnalytics()
    
    # Check available tables
    st.sidebar.title("📊 Insurance Data Availability")
    available_tables = analytics.check_insurance_tables_availability()
    
    for table, count in available_tables.items():
        if count > 0:
            st.sidebar.success(f"✅ {table}: {count:,} rows")
        else:
            st.sidebar.error(f"❌ {table}: Not available")
    
    # Main dashboard
    st.header("📊 Insurance Ecosystem Overview")
    
    # Get comprehensive overview
    overview = analytics.get_insurance_ecosystem_overview()
    
    # Display KPIs in organized sections
    st.subheader("🏢 Insurance Carriers")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="Total Carriers",
            value=f"{overview.get('carriers', {}).get('total', 0):,}"
        )
    
    with col2:
        st.metric(
            label="Active Carriers",
            value=f"{overview.get('carriers', {}).get('active', 0):,}"
        )
    
    with col3:
        st.metric(
            label="VSP Carriers",
            value=f"{overview.get('carriers', {}).get('vsp', 0):,}"
        )
    
    st.subheader("📋 Insurance Plans")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Plans",
            value=f"{overview.get('plans', {}).get('total', 0):,}"
        )
    
    with col2:
        st.metric(
            label="Active Plans",
            value=f"{overview.get('plans', {}).get('active', 0):,}"
        )
    
    with col3:
        st.metric(
            label="Carrier Codes",
            value=f"{overview.get('plans', {}).get('carrier_codes', 0):,}"
        )
    
    with col4:
        st.metric(
            label="HCFA Overrides",
            value=f"{overview.get('plans', {}).get('override_hcfa', 0):,}"
        )
    
    st.subheader("👥 Patient Insurance")
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric(
            label="Insurance Records",
            value=f"{overview.get('patient_insurance', {}).get('total_records', 0):,}"
        )
    
    with col2:
        st.metric(
            label="Patients with Insurance",
            value=f"{overview.get('patient_insurance', {}).get('patients_with_insurance', 0):,}"
        )
    
    # Carrier-Plan relationship analysis
    st.header("🔗 Carrier-Plan Relationships")
    
    carrier_plan_data = analytics.get_carrier_plan_relationship_analysis()
    
    if not carrier_plan_data.empty:
        st.dataframe(carrier_plan_data, use_container_width=True)
        
        # Visualization
        fig = px.bar(carrier_plan_data.head(10), 
                    x='CARRIER_NAME', y='TOTAL_PLANS',
                    title='Top 10 Carriers by Number of Plans',
                    labels={'TOTAL_PLANS': 'Number of Plans', 'CARRIER_NAME': 'Carrier Name'})
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    # Plan details with full joins
    st.header("📋 Plan Details (Using Your SQL Joins)")
    
    plan_details = analytics.get_plan_details_with_full_joins()
    
    if not plan_details.empty:
        st.dataframe(plan_details, use_container_width=True)
        
        # VSP vs Other analysis
        st.header("🎯 VSP vs Other Plans Analysis")
        vsp_analysis = analytics.get_vsp_vs_other_analysis()
        
        if not vsp_analysis.empty:
            fig = px.pie(vsp_analysis, values='PLAN_COUNT', names='PLAN_CATEGORY',
                        title='Distribution of Plan Categories')
            st.plotly_chart(fig, use_container_width=True)
    
    # AI Analysis
    st.header("🤖 AI-Powered Insurance Ecosystem Analysis")
    
    if st.button("🧠 Generate Comprehensive AI Insights"):
        with st.spinner("AI is analyzing your complete insurance ecosystem..."):
            ai_insights = analytics.cortex_analyze_insurance_ecosystem(overview)
            st.markdown("### 🤖 Cortex AI Strategic Insights:")
            st.markdown(ai_insights)
    
    # Footer
    st.markdown("---")
    st.markdown("🚀 **Enhanced with Your Complete SQL Knowledge** | 🏥 **Comprehensive Insurance Analytics**")

if __name__ == "__main__":
    main()
