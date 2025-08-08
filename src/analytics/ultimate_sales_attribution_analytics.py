#!/usr/bin/env python3
"""
Ultimate Sales Attribution Analytics Dashboard
Complete sales attribution analytics using the organizational hierarchy model
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

class UltimateSalesAttributionAnalytics:
    """Complete sales attribution analytics using organizational hierarchy"""
    
    def __init__(self):
        """Initialize ultimate sales attribution analytics"""
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
    
    def check_organizational_tables_availability(self):
        """Check availability of all organizational hierarchy tables"""
        tables_to_check = [
            'DBO_COMPANYINFO',          # Top level - company/corporate
            'DBO_OFFICE',               # Office locations
            'DBO_EMPLOYEE',             # Employees
            'DBO_POSTRANSACTION',       # POS transactions with employee attribution
            'DBO_EMPLOYEEROLE',         # Employee roles
            'DBO_EMPLOYEETYPE',         # Employee types
            'DBO_OFFICEEMPLOYEE',       # Office-employee relationships
            'DBO_EMPLOYEECOMMISSION',   # Employee commissions
            'DBO_EMPLOYEESCHEDULE',     # Employee schedules
            'DBO_OFFICEHOURS'           # Office hours
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
    
    def get_organizational_hierarchy_overview(self):
        """Get comprehensive organizational hierarchy overview"""
        overview = {}
        
        # Company overview
        try:
            company_query = """
            SELECT 
                COUNT(*) as total_companies,
                COUNT(CASE WHEN "Active" = 'True' OR "Active" = '1' THEN 1 END) as active_companies
            FROM EYECARE_ANALYTICS.RAW.DBO_COMPANYINFO
            WHERE "CompanyName" IS NOT NULL AND "CompanyName" != ''
            """
            
            df = self.execute_query(company_query)
            if not df.empty:
                overview['companies'] = {
                    'total': df.iloc[0]['TOTAL_COMPANIES'],
                    'active': df.iloc[0]['ACTIVE_COMPANIES']
                }
        except:
            overview['companies'] = {'total': 0, 'active': 0}
        
        # Office overview
        try:
            office_query = """
            SELECT 
                COUNT(*) as total_offices,
                COUNT(CASE WHEN "Active" = 'True' OR "Active" = '1' THEN 1 END) as active_offices,
                COUNT(DISTINCT "CompanyID") as companies_with_offices
            FROM EYECARE_ANALYTICS.RAW.DBO_OFFICE
            WHERE "OfficeName" IS NOT NULL AND "OfficeName" != ''
            """
            
            df = self.execute_query(office_query)
            if not df.empty:
                overview['offices'] = {
                    'total': df.iloc[0]['TOTAL_OFFICES'],
                    'active': df.iloc[0]['ACTIVE_OFFICES'],
                    'companies_with_offices': df.iloc[0]['COMPANIES_WITH_OFFICES']
                }
        except:
            overview['offices'] = {'total': 0, 'active': 0, 'companies_with_offices': 0}
        
        # Employee overview
        try:
            employee_query = """
            SELECT 
                COUNT(*) as total_employees,
                COUNT(CASE WHEN "Active" = 'True' OR "Active" = '1' THEN 1 END) as active_employees,
                COUNT(DISTINCT "OfficeNum") as offices_with_employees
            FROM EYECARE_ANALYTICS.RAW.DBO_EMPLOYEE
            WHERE "FirstName" IS NOT NULL AND "FirstName" != ''
            """
            
            df = self.execute_query(employee_query)
            if not df.empty:
                overview['employees'] = {
                    'total': df.iloc[0]['TOTAL_EMPLOYEES'],
                    'active': df.iloc[0]['ACTIVE_EMPLOYEES'],
                    'offices_with_employees': df.iloc[0]['OFFICES_WITH_EMPLOYEES']
                }
        except:
            overview['employees'] = {'total': 0, 'active': 0, 'offices_with_employees': 0}
        
        # POS Transaction overview
        try:
            pos_query = """
            SELECT 
                COUNT(*) as total_pos_transactions,
                COUNT(DISTINCT "EmployeeID") as employees_with_sales,
                SUM(CAST("Amount" AS DECIMAL(18,2))) as total_pos_amount,
                AVG(CAST("Amount" AS DECIMAL(18,2))) as avg_pos_amount
            FROM EYECARE_ANALYTICS.RAW.DBO_POSTRANSACTION
            WHERE "Amount" IS NOT NULL AND "Amount" != ''
            """
            
            df = self.execute_query(query)
            if not df.empty:
                overview['pos_transactions'] = {
                    'total': df.iloc[0]['TOTAL_POS_TRANSACTIONS'],
                    'employees_with_sales': df.iloc[0]['EMPLOYEES_WITH_SALES'],
                    'total_amount': df.iloc[0]['TOTAL_POS_AMOUNT'],
                    'avg_amount': df.iloc[0]['AVG_POS_AMOUNT']
                }
        except:
            overview['pos_transactions'] = {'total': 0, 'employees_with_sales': 0, 'total_amount': 0, 'avg_amount': 0}
        
        return overview
    
    def get_complete_sales_attribution_hierarchy(self):
        """Get complete sales attribution using the organizational hierarchy"""
        try:
            # Build the complete hierarchy: CompanyInfo → Office → Employee → POSTransaction
            query = """
            SELECT 
                ci."CompanyName" as company_name,
                ci."ID" as company_id,
                o."OfficeName" as office_name,
                o."OfficeNum" as office_num,
                o."ID" as office_id,
                e."FirstName" as employee_first_name,
                e."LastName" as employee_last_name,
                e."ID" as employee_id,
                e."EmployeeNum" as employee_num,
                pos."Amount" as pos_amount,
                pos."TransactionDate" as transaction_date,
                pos."ID" as pos_transaction_id
            FROM EYECARE_ANALYTICS.RAW.DBO_COMPANYINFO ci
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_OFFICE o ON o."CompanyID" = ci."ID"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_EMPLOYEE e ON e."OfficeNum" = o."OfficeNum"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_POSTRANSACTION pos ON pos."EmployeeID" = e."ID"
            WHERE ci."CompanyName" IS NOT NULL AND ci."CompanyName" != ''
            AND pos."Amount" IS NOT NULL AND pos."Amount" != ''
            ORDER BY ci."CompanyName", o."OfficeName", e."LastName", pos."TransactionDate" DESC
            LIMIT 1000
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error executing complete sales attribution query: {str(e)}")
            
            # Fallback to simpler hierarchy
            try:
                fallback_query = """
                SELECT 
                    o."OfficeName" as office_name,
                    e."FirstName" as employee_first_name,
                    e."LastName" as employee_last_name,
                    COUNT(pos."ID") as transaction_count,
                    SUM(CAST(pos."Amount" AS DECIMAL(18,2))) as total_sales
                FROM EYECARE_ANALYTICS.RAW.DBO_OFFICE o
                LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_EMPLOYEE e ON e."OfficeNum" = o."OfficeNum"
                LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_POSTRANSACTION pos ON pos."EmployeeID" = e."ID"
                WHERE o."OfficeName" IS NOT NULL AND o."OfficeName" != ''
                AND e."FirstName" IS NOT NULL AND e."FirstName" != ''
                AND pos."Amount" IS NOT NULL AND pos."Amount" != ''
                GROUP BY o."OfficeName", e."FirstName", e."LastName"
                ORDER BY total_sales DESC
                LIMIT 100
                """
                
                return self.execute_query(fallback_query)
            except:
                return pd.DataFrame()
    
    def get_company_performance_analysis(self):
        """Analyze performance by company"""
        try:
            query = """
            SELECT 
                ci."CompanyName" as company_name,
                COUNT(DISTINCT o."ID") as office_count,
                COUNT(DISTINCT e."ID") as employee_count,
                COUNT(pos."ID") as transaction_count,
                SUM(CAST(pos."Amount" AS DECIMAL(18,2))) as total_sales,
                AVG(CAST(pos."Amount" AS DECIMAL(18,2))) as avg_transaction_amount
            FROM EYECARE_ANALYTICS.RAW.DBO_COMPANYINFO ci
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_OFFICE o ON o."CompanyID" = ci."ID"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_EMPLOYEE e ON e."OfficeNum" = o."OfficeNum"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_POSTRANSACTION pos ON pos."EmployeeID" = e."ID"
            WHERE ci."CompanyName" IS NOT NULL AND ci."CompanyName" != ''
            AND pos."Amount" IS NOT NULL AND pos."Amount" != ''
            GROUP BY ci."CompanyName"
            ORDER BY total_sales DESC
            LIMIT 20
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error analyzing company performance: {str(e)}")
            return pd.DataFrame()
    
    def get_office_performance_analysis(self):
        """Analyze performance by office"""
        try:
            query = """
            SELECT 
                o."OfficeName" as office_name,
                o."OfficeNum" as office_num,
                COUNT(DISTINCT e."ID") as employee_count,
                COUNT(pos."ID") as transaction_count,
                SUM(CAST(pos."Amount" AS DECIMAL(18,2))) as total_sales,
                AVG(CAST(pos."Amount" AS DECIMAL(18,2))) as avg_transaction_amount,
                SUM(CAST(pos."Amount" AS DECIMAL(18,2))) / NULLIF(COUNT(DISTINCT e."ID"), 0) as sales_per_employee
            FROM EYECARE_ANALYTICS.RAW.DBO_OFFICE o
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_EMPLOYEE e ON e."OfficeNum" = o."OfficeNum"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_POSTRANSACTION pos ON pos."EmployeeID" = e."ID"
            WHERE o."OfficeName" IS NOT NULL AND o."OfficeName" != ''
            AND pos."Amount" IS NOT NULL AND pos."Amount" != ''
            GROUP BY o."OfficeName", o."OfficeNum"
            ORDER BY total_sales DESC
            LIMIT 20
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error analyzing office performance: {str(e)}")
            return pd.DataFrame()
    
    def get_employee_performance_analysis(self):
        """Analyze performance by employee"""
        try:
            query = """
            SELECT 
                e."FirstName" as first_name,
                e."LastName" as last_name,
                e."EmployeeNum" as employee_num,
                o."OfficeName" as office_name,
                COUNT(pos."ID") as transaction_count,
                SUM(CAST(pos."Amount" AS DECIMAL(18,2))) as total_sales,
                AVG(CAST(pos."Amount" AS DECIMAL(18,2))) as avg_transaction_amount,
                MAX(pos."TransactionDate") as last_sale_date
            FROM EYECARE_ANALYTICS.RAW.DBO_EMPLOYEE e
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_OFFICE o ON o."OfficeNum" = e."OfficeNum"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_POSTRANSACTION pos ON pos."EmployeeID" = e."ID"
            WHERE e."FirstName" IS NOT NULL AND e."FirstName" != ''
            AND pos."Amount" IS NOT NULL AND pos."Amount" != ''
            GROUP BY e."FirstName", e."LastName", e."EmployeeNum", o."OfficeName"
            ORDER BY total_sales DESC
            LIMIT 50
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error analyzing employee performance: {str(e)}")
            return pd.DataFrame()
    
    def get_sales_trends_by_hierarchy(self):
        """Analyze sales trends across the organizational hierarchy"""
        try:
            query = """
            SELECT 
                DATE_TRUNC('month', TO_DATE(pos."TransactionDate")) as sale_month,
                ci."CompanyName" as company_name,
                COUNT(pos."ID") as monthly_transactions,
                SUM(CAST(pos."Amount" AS DECIMAL(18,2))) as monthly_sales,
                COUNT(DISTINCT e."ID") as active_employees
            FROM EYECARE_ANALYTICS.RAW.DBO_POSTRANSACTION pos
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_EMPLOYEE e ON e."ID" = pos."EmployeeID"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_OFFICE o ON o."OfficeNum" = e."OfficeNum"
            LEFT JOIN EYECARE_ANALYTICS.RAW.DBO_COMPANYINFO ci ON ci."ID" = o."CompanyID"
            WHERE pos."TransactionDate" IS NOT NULL
            AND pos."Amount" IS NOT NULL AND pos."Amount" != ''
            AND ci."CompanyName" IS NOT NULL AND ci."CompanyName" != ''
            GROUP BY DATE_TRUNC('month', TO_DATE(pos."TransactionDate")), ci."CompanyName"
            ORDER BY sale_month DESC, monthly_sales DESC
            LIMIT 100
            """
            
            return self.execute_query(query)
            
        except Exception as e:
            st.error(f"Error analyzing sales trends: {str(e)}")
            return pd.DataFrame()
    
    def cortex_analyze_sales_attribution(self, overview: dict) -> str:
        """Use Cortex AI to analyze the complete sales attribution data"""
        try:
            companies = overview.get('companies', {})
            offices = overview.get('offices', {})
            employees = overview.get('employees', {})
            pos = overview.get('pos_transactions', {})
            
            prompt = f"""
            Analyze this comprehensive sales attribution data for an eyecare organization:
            
            ORGANIZATIONAL STRUCTURE:
            - Total companies: {companies.get('total', 0)}
            - Active companies: {companies.get('active', 0)}
            - Total offices: {offices.get('total', 0)}
            - Active offices: {offices.get('active', 0)}
            - Total employees: {employees.get('total', 0)}
            - Active employees: {employees.get('active', 0)}
            
            SALES ATTRIBUTION:
            - Total POS transactions: {pos.get('total', 0):,}
            - Employees with sales: {pos.get('employees_with_sales', 0)}
            - Total POS amount: ${pos.get('total_amount', 0):,.2f}
            - Average transaction: ${pos.get('avg_amount', 0):.2f}
            
            Provide strategic insights on:
            1. Sales performance optimization
            2. Employee productivity analysis
            3. Office performance benchmarking
            4. Organizational efficiency opportunities
            5. Sales attribution accuracy and completeness
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
        page_title="Ultimate Sales Attribution Analytics",
        page_icon="🎯",
        layout="wide"
    )
    
    st.title("🎯 Ultimate Sales Attribution Analytics")
    st.markdown("### Complete Sales Attribution Using Organizational Hierarchy Model")
    
    # Initialize analytics
    analytics = UltimateSalesAttributionAnalytics()
    
    # Check available tables
    st.sidebar.title("📊 Organizational Data Availability")
    available_tables = analytics.check_organizational_tables_availability()
    
    for table, count in available_tables.items():
        if count > 0:
            st.sidebar.success(f"✅ {table}: {count:,} rows")
        else:
            st.sidebar.error(f"❌ {table}: Not available")
    
    # Main dashboard
    st.header("📊 Organizational Hierarchy Overview")
    
    # Get comprehensive overview
    overview = analytics.get_organizational_hierarchy_overview()
    
    # Display KPIs in organized sections
    st.subheader("🏢 Organizational Structure")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Companies",
            value=f"{overview.get('companies', {}).get('total', 0):,}"
        )
    
    with col2:
        st.metric(
            label="Total Offices",
            value=f"{overview.get('offices', {}).get('total', 0):,}"
        )
    
    with col3:
        st.metric(
            label="Total Employees",
            value=f"{overview.get('employees', {}).get('total', 0):,}"
        )
    
    with col4:
        st.metric(
            label="Active Employees",
            value=f"{overview.get('employees', {}).get('active', 0):,}"
        )
    
    st.subheader("🎯 Sales Attribution")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total POS Transactions",
            value=f"{overview.get('pos_transactions', {}).get('total', 0):,}"
        )
    
    with col2:
        st.metric(
            label="Employees with Sales",
            value=f"{overview.get('pos_transactions', {}).get('employees_with_sales', 0):,}"
        )
    
    with col3:
        st.metric(
            label="Total POS Amount",
            value=f"${overview.get('pos_transactions', {}).get('total_amount', 0):,.2f}"
        )
    
    with col4:
        st.metric(
            label="Avg Transaction",
            value=f"${overview.get('pos_transactions', {}).get('avg_amount', 0):.2f}"
        )
    
    # Company performance analysis
    st.header("🏢 Company Performance Analysis")
    
    company_performance = analytics.get_company_performance_analysis()
    
    if not company_performance.empty:
        st.dataframe(company_performance, use_container_width=True)
        
        # Visualization
        fig = px.bar(company_performance.head(10), 
                    x='COMPANY_NAME', y='TOTAL_SALES',
                    title='Top 10 Companies by Total Sales',
                    labels={'TOTAL_SALES': 'Total Sales ($)', 'COMPANY_NAME': 'Company Name'})
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    # Office performance analysis
    st.header("🏪 Office Performance Analysis")
    
    office_performance = analytics.get_office_performance_analysis()
    
    if not office_performance.empty:
        st.dataframe(office_performance, use_container_width=True)
        
        # Scatter plot: Employee count vs Total sales
        fig = px.scatter(office_performance, 
                        x='EMPLOYEE_COUNT', y='TOTAL_SALES',
                        hover_data=['OFFICE_NAME'],
                        title='Office Performance: Employee Count vs Total Sales',
                        labels={'EMPLOYEE_COUNT': 'Employee Count', 'TOTAL_SALES': 'Total Sales ($)'})
        st.plotly_chart(fig, use_container_width=True)
    
    # Employee performance analysis
    st.header("👥 Employee Performance Analysis")
    
    employee_performance = analytics.get_employee_performance_analysis()
    
    if not employee_performance.empty:
        st.dataframe(employee_performance, use_container_width=True)
        
        # Top performers chart
        fig = px.bar(employee_performance.head(15), 
                    x='TOTAL_SALES', y='LAST_NAME',
                    orientation='h',
                    title='Top 15 Employees by Total Sales',
                    labels={'TOTAL_SALES': 'Total Sales ($)', 'LAST_NAME': 'Employee Last Name'})
        st.plotly_chart(fig, use_container_width=True)
    
    # Sales trends by hierarchy
    st.header("📈 Sales Trends by Organizational Hierarchy")
    
    sales_trends = analytics.get_sales_trends_by_hierarchy()
    
    if not sales_trends.empty:
        st.dataframe(sales_trends, use_container_width=True)
        
        # Time series by company
        fig = px.line(sales_trends, 
                     x='SALE_MONTH', y='MONTHLY_SALES', 
                     color='COMPANY_NAME',
                     title='Monthly Sales Trends by Company',
                     labels={'MONTHLY_SALES': 'Monthly Sales ($)', 'SALE_MONTH': 'Month'})
        st.plotly_chart(fig, use_container_width=True)
    
    # Complete sales attribution hierarchy
    st.header("🔗 Complete Sales Attribution Hierarchy")
    
    complete_attribution = analytics.get_complete_sales_attribution_hierarchy()
    
    if not complete_attribution.empty:
        st.dataframe(complete_attribution.head(100), use_container_width=True)
        st.info(f"Showing first 100 rows of {len(complete_attribution):,} total records")
    
    # AI Analysis
    st.header("🤖 AI-Powered Sales Attribution Analysis")
    
    if st.button("🧠 Generate Comprehensive Sales Attribution Insights"):
        with st.spinner("AI is analyzing your complete sales attribution data..."):
            ai_insights = analytics.cortex_analyze_sales_attribution(overview)
            st.markdown("### 🤖 Cortex AI Strategic Insights:")
            st.markdown(ai_insights)
    
    # Footer
    st.markdown("---")
    st.markdown("🚀 **Built with Your Complete Organizational Hierarchy Model** | 🎯 **Ultimate Sales Attribution Analytics**")

if __name__ == "__main__":
    main()
