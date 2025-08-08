#!/usr/bin/env python3
"""
Insurance Carrier Analytics Dashboard
Enhanced analytics using the exact table relationships provided by the user
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

class InsuranceCarrierAnalytics:
    """Insurance carrier analytics using proper table relationships"""
    
    def __init__(self):
        """Initialize insurance carrier analytics"""
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
    
    def check_available_tables(self):
        """Check which insurance-related tables are available"""
        tables_to_check = [
            'DBO_INSCARRIER',
            'DBO_ADDRESS', 
            'DBO_PHONE',
            'DBO_INSHCFA',
            'DBO_PATIENTADDRESS',
            'DBO_PATIENTPHONE',
            'DBO_INSURANCECARRIER',
            'DBO_INSURANCECARRIERCUSTOMATTRIBUTES'
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
    
    def get_insurance_carrier_overview(self):
        """Get insurance carrier overview using available tables"""
        # Try the exact relationship from user's SQL first
        try:
            # Check if we have the exact tables from the user's query
            query = """
            SELECT 
                COUNT(*) as total_carriers,
                COUNT(DISTINCT "CarrierName") as unique_carriers,
                COUNT(CASE WHEN "Active" = 'True' OR "Active" = '1' THEN 1 END) as active_carriers
            FROM EYECARE_ANALYTICS.RAW.DBO_INSCARRIER
            WHERE "CarrierName" IS NOT NULL AND "CarrierName" != ''
            """
            
            df = self.execute_query(query)
            if not df.empty:
                return {
                    'total_carriers': df.iloc[0]['TOTAL_CARRIERS'],
                    'unique_carriers': df.iloc[0]['UNIQUE_CARRIERS'],
                    'active_carriers': df.iloc[0]['ACTIVE_CARRIERS']
                }
        except:
            pass
        
        # Fallback to other insurance-related tables
        try:
            query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT "CarrierName") as unique_carriers
            FROM EYECARE_ANALYTICS.RAW.DBO_INSURANCECARRIERCUSTOMATTRIBUTES
            WHERE "CarrierName" IS NOT NULL AND "CarrierName" != ''
            """
            
            df = self.execute_query(query)
            if not df.empty:
                return {
                    'total_carriers': df.iloc[0]['TOTAL_RECORDS'],
                    'unique_carriers': df.iloc[0]['UNIQUE_CARRIERS'],
                    'active_carriers': 'N/A'
                }
        except:
            pass
        
        return {'total_carriers': 0, 'unique_carriers': 0, 'active_carriers': 0}
    
    def get_insurance_carrier_details_with_joins(self):
        """Get insurance carrier details using the exact joins from user's SQL"""
        try:
            # Adapt the user's SQL to our Snowflake table structure
            query = """
            SELECT 
                ic."ID" as carrier_id,
                ic."CarrierName" as carrier_name,
                ic."Contact" as contact,
                addr."Address1" as address1,
                addr."Address2" as address2,
                addr."City" as city,
                addr."State" as state,
                addr."ZipCode" as zipcode,
                phone."PhoneNumber" as phone_number,
                phone2."PhoneNumber" as fax_number,
                ic."Active" as is_active,
                ic."IsVspCarrier" as is_vsp_carrier,
                ic."WebsiteAddress" as website
            FROM EYECARE_ANALYTICS.RAW.DBO_INSCARRIER ic
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_ADDRESS addr ON ic."AddressID" = addr."ID"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_PHONE phone ON ic."PhoneID" = phone."ID"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_PHONE phone2 ON ic."FaxID" = phone2."ID"
            WHERE ic."CarrierName" IS NOT NULL AND ic."CarrierName" != ''
            LIMIT 100
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error executing joined query: {str(e)}")
            
            # Fallback to simple carrier data
            try:
                fallback_query = """
                SELECT 
                    "ID" as carrier_id,
                    "CarrierName" as carrier_name,
                    "Contact" as contact,
                    "Active" as is_active,
                    "WebsiteAddress" as website
                FROM EYECARE_ANALYTICS.RAW.DBO_INSCARRIER
                WHERE "CarrierName" IS NOT NULL AND "CarrierName" != ''
                LIMIT 50
                """
                
                return self.execute_query(fallback_query)
            except:
                return pd.DataFrame()
    
    def get_carrier_geographic_distribution(self):
        """Get geographic distribution of carriers"""
        try:
            query = """
            SELECT 
                addr."State" as state,
                COUNT(*) as carrier_count
            FROM EYECARE_ANALYTICS.RAW.DBO_INSCARRIER ic
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_ADDRESS addr ON ic."AddressID" = addr."ID"
            WHERE addr."State" IS NOT NULL AND addr."State" != ''
            GROUP BY addr."State"
            ORDER BY carrier_count DESC
            LIMIT 20
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error getting geographic distribution: {str(e)}")
            return pd.DataFrame()
    
    def get_carrier_types_analysis(self):
        """Analyze carrier types (VSP vs others)"""
        try:
            query = """
            SELECT 
                CASE 
                    WHEN "IsVspCarrier" = 'True' OR "IsVspCarrier" = '1' THEN 'VSP Carrier'
                    ELSE 'Other Carrier'
                END as carrier_type,
                COUNT(*) as count,
                COUNT(CASE WHEN "Active" = 'True' OR "Active" = '1' THEN 1 END) as active_count
            FROM EYECARE_ANALYTICS.RAW.DBO_INSCARRIER
            WHERE "CarrierName" IS NOT NULL AND "CarrierName" != ''
            GROUP BY carrier_type
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error analyzing carrier types: {str(e)}")
            return pd.DataFrame()
    
    def cortex_analyze_carrier_data(self, carrier_data: pd.DataFrame) -> str:
        """Use Cortex AI to analyze carrier data"""
        if carrier_data.empty:
            return "No carrier data available for analysis."
        
        try:
            # Create summary for AI analysis
            total_carriers = len(carrier_data)
            active_carriers = len(carrier_data[carrier_data.get('IS_ACTIVE', '') == 'True'])
            
            prompt = f"""
            Analyze this insurance carrier dataset for an eyecare practice:
            
            - Total carriers: {total_carriers}
            - Active carriers: {active_carriers}
            - Sample carrier names: {', '.join(carrier_data['CARRIER_NAME'].head(5).tolist()) if 'CARRIER_NAME' in carrier_data.columns else 'N/A'}
            
            Provide insights on:
            1. Carrier portfolio diversity
            2. Geographic coverage
            3. Recommendations for carrier management
            4. Potential optimization opportunities
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
        page_title="Insurance Carrier Analytics",
        page_icon="🏥",
        layout="wide"
    )
    
    st.title("🏥 Insurance Carrier Analytics Dashboard")
    st.markdown("### Using Proper Table Relationships from Your SQL Knowledge")
    
    # Initialize analytics
    analytics = InsuranceCarrierAnalytics()
    
    # Check available tables
    st.sidebar.title("📊 Data Availability")
    available_tables = analytics.check_available_tables()
    
    for table, count in available_tables.items():
        if count > 0:
            st.sidebar.success(f"✅ {table}: {count:,} rows")
        else:
            st.sidebar.error(f"❌ {table}: Not available")
    
    # Main dashboard
    st.header("📊 Insurance Carrier Overview")
    
    # Get overview metrics
    overview = analytics.get_insurance_carrier_overview()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="Total Carriers",
            value=f"{overview['total_carriers']:,}"
        )
    
    with col2:
        st.metric(
            label="Unique Carriers",
            value=f"{overview['unique_carriers']:,}"
        )
    
    with col3:
        st.metric(
            label="Active Carriers",
            value=f"{overview['active_carriers']}"
        )
    
    # Detailed carrier data using proper joins
    st.header("🔗 Carrier Details (Using Your SQL Relationships)")
    
    carrier_details = analytics.get_insurance_carrier_details_with_joins()
    
    if not carrier_details.empty:
        st.dataframe(carrier_details, use_container_width=True)
        
        # Geographic distribution
        st.header("🗺️ Geographic Distribution")
        geo_data = analytics.get_carrier_geographic_distribution()
        
        if not geo_data.empty:
            fig = px.bar(geo_data, x='STATE', y='CARRIER_COUNT',
                        title='Insurance Carriers by State',
                        labels={'CARRIER_COUNT': 'Number of Carriers', 'STATE': 'State'})
            st.plotly_chart(fig, use_container_width=True)
        
        # Carrier types analysis
        st.header("📈 Carrier Types Analysis")
        types_data = analytics.get_carrier_types_analysis()
        
        if not types_data.empty:
            fig = px.pie(types_data, values='COUNT', names='CARRIER_TYPE',
                        title='Distribution of Carrier Types')
            st.plotly_chart(fig, use_container_width=True)
        
        # AI Analysis
        st.header("🤖 AI-Powered Carrier Analysis")
        
        if st.button("🧠 Generate AI Insights"):
            with st.spinner("AI is analyzing your carrier data..."):
                ai_insights = analytics.cortex_analyze_carrier_data(carrier_details)
                st.markdown("### 🤖 Cortex AI Insights:")
                st.markdown(ai_insights)
    
    else:
        st.warning("⚠️ No carrier data available with the expected table structure. The tables may have different column names or relationships.")
        
        # Show what tables are available for debugging
        st.subheader("🔍 Available Tables for Debugging")
        for table, count in available_tables.items():
            if count > 0:
                st.write(f"**{table}** ({count:,} rows)")
                
                # Show sample data
                try:
                    sample_query = f"SELECT * FROM EYECARE_ANALYTICS.RAW.{table} LIMIT 3"
                    sample_data = analytics.execute_query(sample_query)
                    if not sample_data.empty:
                        st.dataframe(sample_data)
                except:
                    st.write("Could not retrieve sample data")
    
    # Footer
    st.markdown("---")
    st.markdown("🚀 **Enhanced with Your SQL Knowledge** | 🏥 **Insurance Carrier Analytics**")

if __name__ == "__main__":
    main()
