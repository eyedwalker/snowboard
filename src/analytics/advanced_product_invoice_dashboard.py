import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os

# Add the parent directory to the path to import the connector
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.robust_snowfall_connector import RobustSnowfallConnector

# Page configuration
st.set_page_config(
    page_title="Advanced Product & Invoice Analytics",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        color: #1f77b4;
        margin-bottom: 2rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
        box-shadow: 0 8px 32px rgba(31, 38, 135, 0.37);
        backdrop-filter: blur(4px);
        border: 1px solid rgba(255, 255, 255, 0.18);
    }
    .metric-card h2 {
        margin: 0;
        font-size: 2.5rem;
        font-weight: bold;
    }
    .metric-card p {
        margin: 0.5rem 0 0 0;
        font-size: 1.1rem;
        opacity: 0.9;
    }
    .insight-box {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        margin: 1rem 0;
        box-shadow: 0 8px 32px rgba(31, 38, 135, 0.37);
    }
    .section-header {
        font-size: 2rem;
        font-weight: bold;
        color: #2c3e50;
        margin: 2rem 0 1rem 0;
        border-bottom: 3px solid #3498db;
        padding-bottom: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_snowflake_connector():
    """Get cached Snowflake connector"""
    return RobustSnowfallConnector()

def main():
    st.markdown('<h1 class="main-header">🛍️ Advanced Product & Invoice Analytics</h1>', unsafe_allow_html=True)
    
    # Initialize connector
    connector = get_snowflake_connector()
    
    # Sidebar for filters
    st.sidebar.markdown("## 🎛️ Analytics Filters")
    
    # Date range filter
    date_range = st.sidebar.selectbox(
        "📅 Time Period",
        ["Last 30 Days", "Last 90 Days", "Last 6 Months", "Last Year", "All Time"],
        index=4  # Default to "All Time" since we have data from 2018-2025
    )
    
    # Convert date range to SQL condition
    date_conditions = {
        "Last 30 Days": "AND p.\"TransactionDate\" >= CURRENT_DATE - 30",
        "Last 90 Days": "AND p.\"TransactionDate\" >= CURRENT_DATE - 90", 
        "Last 6 Months": "AND p.\"TransactionDate\" >= CURRENT_DATE - 180",
        "Last Year": "AND p.\"TransactionDate\" >= CURRENT_DATE - 365",
        "All Time": ""
    }
    date_filter = date_conditions[date_range]
    
    # Main analytics sections
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Product Sales Overview", 
        "💰 Invoice Analytics", 
        "🏆 Top Products", 
        "📈 Revenue Trends",
        "🔍 Advanced Insights"
    ])
    
    with tab1:
        st.markdown('<div class="section-header">📊 Product Sales Overview</div>', unsafe_allow_html=True)
        
        # Get product sales data
        product_query = f"""
        SELECT 
            p."TransactionDate",
            p."Amount",
            p."EmployeeId",
            p."OfficeNum",
            o."OfficeName",
            o."CompanyID"
        FROM RAW.DBO_POSTRANSACTION p
        LEFT JOIN RAW.DBO_OFFICE o ON p."OfficeNum" = o."OfficeNum"
        WHERE p."Amount" IS NOT NULL 
        {date_filter}
        ORDER BY p."TransactionDate" DESC
        """
        
        product_data = connector.execute_safe_query(product_query)
        
        if not product_data.empty:
            # Convert Amount to numeric
            product_data['Amount'] = pd.to_numeric(product_data['Amount'], errors='coerce').fillna(0)
            
            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_transactions = len(product_data)
                st.markdown(f"""<div class="metric-card"><h2>{total_transactions:,}</h2><p>Total Transactions</p></div>""", unsafe_allow_html=True)
            
            with col2:
                total_revenue = product_data['Amount'].sum()
                st.markdown(f"""<div class="metric-card"><h2>${total_revenue:,.0f}</h2><p>Total Revenue</p></div>""", unsafe_allow_html=True)
            
            with col3:
                avg_transaction = product_data['Amount'].mean()
                st.markdown(f"""<div class="metric-card"><h2>${avg_transaction:.2f}</h2><p>Avg Transaction</p></div>""", unsafe_allow_html=True)
            
            with col4:
                unique_offices = product_data['OfficeNum'].nunique()
                st.markdown(f"""<div class="metric-card"><h2>{unique_offices}</h2><p>Active Offices</p></div>""", unsafe_allow_html=True)
            
            # Revenue by office
            office_revenue = product_data.groupby('OfficeName')['Amount'].agg(['sum', 'count', 'mean']).reset_index()
            office_revenue.columns = ['Office', 'Total_Revenue', 'Transaction_Count', 'Avg_Transaction']
            office_revenue = office_revenue.sort_values('Total_Revenue', ascending=False).head(15)
            
            fig_office = px.bar(
                office_revenue, 
                x='Office', 
                y='Total_Revenue',
                title="Top 15 Offices by Revenue",
                labels={'Total_Revenue': 'Revenue ($)', 'Office': 'Office Name'}
            )
            fig_office.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_office, use_container_width=True)
            
            # Transaction distribution
            col1, col2 = st.columns(2)
            
            with col1:
                # Transaction amount distribution
                fig_dist = px.histogram(
                    product_data[product_data['Amount'] > 0], 
                    x='Amount', 
                    nbins=30,
                    title="Transaction Amount Distribution"
                )
                st.plotly_chart(fig_dist, use_container_width=True)
            
            with col2:
                # Daily transaction count
                product_data['TransactionDate'] = pd.to_datetime(product_data['TransactionDate'], errors='coerce')
                daily_counts = product_data.groupby(product_data['TransactionDate'].dt.date).size().reset_index()
                daily_counts.columns = ['Date', 'Count']
                
                fig_daily = px.line(
                    daily_counts.tail(30), 
                    x='Date', 
                    y='Count',
                    title="Daily Transaction Volume (Last 30 Days)",
                    markers=True
                )
                st.plotly_chart(fig_daily, use_container_width=True)
        
        else:
            st.warning("No product transaction data available for the selected period.")
    
    with tab2:
        st.markdown('<div class="section-header">💰 Invoice Analytics</div>', unsafe_allow_html=True)
        
        # Get billing transaction data for invoice analysis
        invoice_query = f"""
        SELECT 
            b."TransId",
            b."TransactionDate",
            b."InsAR",
            b."PatAR",
            b."Amount"
        FROM RAW.DBO_BILLINGTRANSACTION b
        WHERE (b."InsAR" IS NOT NULL OR b."PatAR" IS NOT NULL)
        {date_filter.replace('p.', 'b.')}
        ORDER BY b."TransactionDate" DESC
        """
        
        invoice_data = connector.execute_safe_query(invoice_query)
        
        if not invoice_data.empty:
            # Convert to numeric
            invoice_data['InsAR'] = pd.to_numeric(invoice_data['InsAR'], errors='coerce').fillna(0)
            invoice_data['PatAR'] = pd.to_numeric(invoice_data['PatAR'], errors='coerce').fillna(0)
            invoice_data['Total_AR'] = invoice_data['InsAR'] + invoice_data['PatAR']
            
            # Invoice metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_invoices = len(invoice_data)
                st.markdown(f"""<div class="metric-card"><h2>{total_invoices:,}</h2><p>Total Invoices</p></div>""", unsafe_allow_html=True)
            
            with col2:
                avg_invoice = invoice_data['Total_AR'].mean()
                st.markdown(f"""<div class="metric-card"><h2>${avg_invoice:.2f}</h2><p>Avg Invoice</p></div>""", unsafe_allow_html=True)
            
            with col3:
                total_ar = invoice_data['Total_AR'].sum()
                st.markdown(f"""<div class="metric-card"><h2>${total_ar:,.0f}</h2><p>Total A/R</p></div>""", unsafe_allow_html=True)
            
            with col4:
                ins_vs_pat_ratio = invoice_data['InsAR'].sum() / (invoice_data['PatAR'].sum() + 1)
                st.markdown(f"""<div class="metric-card"><h2>{ins_vs_pat_ratio:.1f}x</h2><p>Ins/Pat Ratio</p></div>""", unsafe_allow_html=True)
            
            # Invoice size distribution
            col1, col2 = st.columns(2)
            
            with col1:
                # Invoice amount ranges
                invoice_data['Invoice_Range'] = pd.cut(
                    invoice_data['Total_AR'], 
                    bins=[0, 100, 250, 500, 1000, float('inf')],
                    labels=['$0-100', '$100-250', '$250-500', '$500-1000', '$1000+']
                )
                range_counts = invoice_data['Invoice_Range'].value_counts().reset_index()
                range_counts.columns = ['Range', 'Count']
                
                fig_ranges = px.pie(
                    range_counts, 
                    values='Count', 
                    names='Range',
                    title="Invoice Amount Distribution"
                )
                st.plotly_chart(fig_ranges, use_container_width=True)
            
            with col2:
                # Insurance vs Patient AR
                ar_breakdown = pd.DataFrame({
                    'Type': ['Insurance AR', 'Patient AR'],
                    'Amount': [invoice_data['InsAR'].sum(), invoice_data['PatAR'].sum()]
                })
                
                fig_ar = px.bar(
                    ar_breakdown, 
                    x='Type', 
                    y='Amount',
                    title="A/R Breakdown: Insurance vs Patient",
                    color='Type'
                )
                st.plotly_chart(fig_ar, use_container_width=True)
            
            # Monthly A/R trends
            invoice_data['TransactionDate'] = pd.to_datetime(invoice_data['TransactionDate'], errors='coerce')
            monthly_ar = invoice_data.groupby(invoice_data['TransactionDate'].dt.to_period('M'))['Total_AR'].sum().reset_index()
            monthly_ar['Month'] = monthly_ar['TransactionDate'].astype(str)
            
            fig_monthly_ar = px.line(
                monthly_ar, 
                x='Month', 
                y='Total_AR',
                title="Monthly A/R Trends",
                markers=True,
                labels={'Total_AR': 'Total A/R ($)'}
            )
            st.plotly_chart(fig_monthly_ar, use_container_width=True)
        
        else:
            st.warning("No billing transaction data available for the selected period.")
    
    with tab3:
        st.markdown('<div class="section-header">🏆 Top Products & Performance</div>', unsafe_allow_html=True)
        
        # Since we don't have specific product tables, let's analyze transaction patterns
        if not product_data.empty:
            # Employee performance (as proxy for product/service performance)
            employee_query = f"""
            SELECT 
                e."EmployeeNum",
                e."FirstName",
                e."LastName",
                e."Active",
                p."Amount",
                p."TransactionDate",
                o."OfficeName"
            FROM RAW.DBO_POSTRANSACTION p
            LEFT JOIN RAW.DBO_EMPLOYEE e ON p."EmployeeId" = e."EmployeeNum"
            LEFT JOIN RAW.DBO_OFFICE o ON p."OfficeNum" = o."OfficeNum"
            WHERE p."Amount" IS NOT NULL AND e."EmployeeNum" IS NOT NULL
            {date_filter}
            """
            
            emp_performance = connector.execute_safe_query(employee_query)
            
            if not emp_performance.empty:
                emp_performance['Amount'] = pd.to_numeric(emp_performance['Amount'], errors='coerce').fillna(0)
                
                # Top performing employees (service providers)
                top_employees = emp_performance.groupby(['FirstName', 'LastName']).agg({
                    'Amount': ['sum', 'count', 'mean']
                }).reset_index()
                top_employees.columns = ['FirstName', 'LastName', 'Total_Sales', 'Transaction_Count', 'Avg_Sale']
                top_employees['Employee'] = top_employees['FirstName'] + ' ' + top_employees['LastName']
                top_employees = top_employees.sort_values('Total_Sales', ascending=False).head(15)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    fig_top_emp = px.bar(
                        top_employees, 
                        x='Employee', 
                        y='Total_Sales',
                        title="Top 15 Employees by Sales",
                        labels={'Total_Sales': 'Total Sales ($)'}
                    )
                    fig_top_emp.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig_top_emp, use_container_width=True)
                
                with col2:
                    fig_avg_sale = px.bar(
                        top_employees.head(10), 
                        x='Employee', 
                        y='Avg_Sale',
                        title="Top 10 Employees by Average Sale",
                        labels={'Avg_Sale': 'Average Sale ($)'}
                    )
                    fig_avg_sale.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig_avg_sale, use_container_width=True)
                
                # Performance metrics table
                st.markdown("### 📊 Employee Performance Summary")
                display_df = top_employees[['Employee', 'Total_Sales', 'Transaction_Count', 'Avg_Sale']].copy()
                display_df['Total_Sales'] = display_df['Total_Sales'].apply(lambda x: f"${x:,.2f}")
                display_df['Avg_Sale'] = display_df['Avg_Sale'].apply(lambda x: f"${x:.2f}")
                st.dataframe(display_df, use_container_width=True)
    
    with tab4:
        st.markdown('<div class="section-header">📈 Revenue Trends & Patterns</div>', unsafe_allow_html=True)
        
        if not product_data.empty:
            product_data['TransactionDate'] = pd.to_datetime(product_data['TransactionDate'], errors='coerce')
            
            # Monthly revenue trends
            monthly_revenue = product_data.groupby(product_data['TransactionDate'].dt.to_period('M'))['Amount'].sum().reset_index()
            monthly_revenue['Month'] = monthly_revenue['TransactionDate'].astype(str)
            
            fig_monthly = px.line(
                monthly_revenue, 
                x='Month', 
                y='Amount',
                title="Monthly Revenue Trends",
                markers=True,
                labels={'Amount': 'Revenue ($)'}
            )
            st.plotly_chart(fig_monthly, use_container_width=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Day of week analysis
                product_data['DayOfWeek'] = product_data['TransactionDate'].dt.day_name()
                dow_revenue = product_data.groupby('DayOfWeek')['Amount'].agg(['sum', 'count']).reset_index()
                dow_revenue.columns = ['Day', 'Revenue', 'Count']
                
                # Order days properly
                day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                dow_revenue['Day'] = pd.Categorical(dow_revenue['Day'], categories=day_order, ordered=True)
                dow_revenue = dow_revenue.sort_values('Day')
                
                fig_dow = px.bar(
                    dow_revenue, 
                    x='Day', 
                    y='Revenue',
                    title="Revenue by Day of Week"
                )
                st.plotly_chart(fig_dow, use_container_width=True)
            
            with col2:
                # Hour of day analysis (if time data available)
                if 'TransactionDate' in product_data.columns:
                    product_data['Hour'] = product_data['TransactionDate'].dt.hour
                    hourly_revenue = product_data.groupby('Hour')['Amount'].sum().reset_index()
                    
                    fig_hourly = px.line(
                        hourly_revenue, 
                        x='Hour', 
                        y='Amount',
                        title="Revenue by Hour of Day",
                        markers=True
                    )
                    st.plotly_chart(fig_hourly, use_container_width=True)
    
    with tab5:
        st.markdown('<div class="section-header">🔍 Advanced Business Insights</div>', unsafe_allow_html=True)
        
        if not product_data.empty and not invoice_data.empty:
            # Advanced analytics and insights
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 💡 Key Business Insights")
                
                # Calculate key insights
                total_pos_revenue = product_data['Amount'].sum()
                total_ar = invoice_data['Total_AR'].sum()
                avg_transaction = product_data['Amount'].mean()
                avg_invoice = invoice_data['Total_AR'].mean()
                
                st.markdown(f"""
                <div class="insight-box">
                <h3>📊 Revenue Analysis</h3>
                <p>• POS Revenue: ${total_pos_revenue:,.0f}</p>
                <p>• Total A/R: ${total_ar:,.0f}</p>
                <p>• Revenue to A/R Ratio: {total_pos_revenue/total_ar:.2f}x</p>
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown(f"""
                <div class="insight-box">
                <h3>💰 Transaction Insights</h3>
                <p>• Average POS Transaction: ${avg_transaction:.2f}</p>
                <p>• Average Invoice: ${avg_invoice:.2f}</p>
                <p>• Invoice Premium: {(avg_invoice/avg_transaction-1)*100:.1f}%</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                # Office performance comparison
                office_comparison = product_data.groupby('OfficeName').agg({
                    'Amount': ['sum', 'count', 'mean']
                }).reset_index()
                office_comparison.columns = ['Office', 'Revenue', 'Transactions', 'Avg_Transaction']
                
                # Fix negative values and ensure positive sizes for scatter plot
                office_comparison['Revenue_Abs'] = office_comparison['Revenue'].abs()
                office_comparison['Avg_Transaction_Abs'] = office_comparison['Avg_Transaction'].abs()
                
                # Filter out offices with zero or negative metrics
                office_comparison = office_comparison[
                    (office_comparison['Revenue_Abs'] > 0) & 
                    (office_comparison['Transactions'] > 0) &
                    (office_comparison['Avg_Transaction_Abs'] > 0)
                ].sort_values('Revenue_Abs', ascending=False).head(10)
                
                if not office_comparison.empty:
                    fig_comparison = px.scatter(
                        office_comparison,
                        x='Transactions',
                        y='Avg_Transaction_Abs',
                        size='Revenue_Abs',
                        hover_name='Office',
                        title="Office Performance: Volume vs Average Transaction",
                        labels={
                            'Transactions': 'Number of Transactions',
                            'Avg_Transaction_Abs': 'Average Transaction ($)'
                        }
                    )
                    st.plotly_chart(fig_comparison, use_container_width=True)
                else:
                    st.warning("No valid office performance data available for comparison.")
            
            # Predictive insights
            st.markdown("### 🔮 Predictive Analytics")
            
            if len(product_data) > 30:
                # Simple trend analysis
                product_data_sorted = product_data.sort_values('TransactionDate')
                recent_30_days = product_data_sorted.tail(int(len(product_data) * 0.3))
                previous_30_days = product_data_sorted.head(int(len(product_data) * 0.3))
                
                recent_avg = recent_30_days['Amount'].mean()
                previous_avg = previous_30_days['Amount'].mean()
                trend = (recent_avg - previous_avg) / previous_avg * 100
                
                trend_color = "green" if trend > 0 else "red"
                trend_direction = "📈" if trend > 0 else "📉"
                
                st.markdown(f"""
                <div class="insight-box">
                <h3>{trend_direction} Trend Analysis</h3>
                <p>Recent average transaction: ${recent_avg:.2f}</p>
                <p>Previous average transaction: ${previous_avg:.2f}</p>
                <p style="color: {trend_color}; font-weight: bold;">
                Trend: {trend:+.1f}% change
                </p>
                </div>
                """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
