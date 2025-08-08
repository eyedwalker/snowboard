#!/usr/bin/env python3
"""
Product Sales Analytics Dashboard
=================================
Real product sales analytics: counts, dollars, discounts, top products sold
Using actual transaction data from POS, invoices, and stock orders
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from connectors.robust_snowfall_connector import RobustSnowfallConnector

# Page configuration (only when run as main script)
if __name__ == "__main__":
    st.set_page_config(
        page_title="Product Sales Analytics",
        page_icon="💰",
        layout="wide",
        initial_sidebar_state="expanded"
    )

# Initialize connector
@st.cache_resource
def get_connector():
    return RobustSnowfallConnector()

connector = get_connector()

# Header
st.title("💰 Product Sales Analytics Dashboard")
st.markdown("**Real Sales Performance** | *26,523 POS Transactions • 14,503 Invoices • 2,571 Stock Orders*")

# Sidebar
st.sidebar.header("🎛️ Sales Analytics Controls")

# Date range selector
date_range = st.sidebar.selectbox(
    "📅 Analysis Period",
    ["All Time", "Last 12 Months", "Last 6 Months", "Last 3 Months", "Current Year"]
)

# Analytics sections
analytics_section = st.sidebar.selectbox(
    "📊 Analytics Section",
    ["Sales Overview", "Top Products", "Product Performance", "Discount Analysis", "Transaction Insights"]
)

# Refresh data button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_resource.clear()
    st.rerun()

# Main content area
if analytics_section == "Sales Overview":
    st.header("📊 Sales Performance Overview")
    
    try:
        # Get overall sales metrics
        col1, col2, col3, col4 = st.columns(4)
        
        # POS Transaction metrics
        pos_metrics = connector.execute_safe_query('''
            SELECT 
                COUNT(*) as TRANSACTION_COUNT,
                COUNT(DISTINCT "OrderID") as UNIQUE_ORDERS,
                COUNT(DISTINCT "PatientID") as UNIQUE_CUSTOMERS,
                COUNT(DISTINCT "OfficeNum") as OFFICES_WITH_SALES
            FROM RAW.DBO_POSTRANSACTION
            WHERE "OrderID" IS NOT NULL
        ''')
        
        if not pos_metrics.empty:
            with col1:
                st.metric("🛒 POS Transactions", f"{pos_metrics.iloc[0]['TRANSACTION_COUNT']:,}")
            
            with col2:
                st.metric("📋 Unique Orders", f"{pos_metrics.iloc[0]['UNIQUE_ORDERS']:,}")
            
            with col3:
                st.metric("👥 Unique Customers", f"{pos_metrics.iloc[0]['UNIQUE_CUSTOMERS']:,}")
            
            with col4:
                st.metric("🏢 Active Offices", f"{pos_metrics.iloc[0]['OFFICES_WITH_SALES']:,}")
        
        # Invoice summary metrics
        st.subheader("💳 Invoice Summary")
        
        invoice_metrics = connector.execute_safe_query('''
            SELECT 
                COUNT(*) as INVOICE_COUNT,
                COUNT(DISTINCT "OrderNum") as UNIQUE_ORDERS,
                COUNT(DISTINCT "DiscountTypeID") as DISCOUNT_TYPES_USED
            FROM RAW.DBO_INVOICESUM
            WHERE "OrderNum" IS NOT NULL
        ''')
        
        if not invoice_metrics.empty:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("🧾 Total Invoices", f"{invoice_metrics.iloc[0]['INVOICE_COUNT']:,}")
            
            with col2:
                st.metric("📦 Invoiced Orders", f"{invoice_metrics.iloc[0]['UNIQUE_ORDERS']:,}")
            
            with col3:
                discount_types = invoice_metrics.iloc[0]['DISCOUNT_TYPES_USED'] or 0
                st.metric("💸 Discount Types", f"{discount_types}")
        
        # Stock order activity
        st.subheader("📦 Stock Order Activity")
        
        stock_metrics = connector.execute_safe_query('''
            SELECT 
                COUNT(*) as ORDER_LINES,
                COUNT(DISTINCT "StockOrderNum") as UNIQUE_STOCK_ORDERS,
                COUNT(DISTINCT "ItemID") as UNIQUE_ITEMS_ORDERED,
                SUM(CAST("Quantity" AS INTEGER)) as TOTAL_QUANTITY
            FROM RAW.DBO_STOCKORDERDETAIL
            WHERE "ItemID" IS NOT NULL AND "Quantity" IS NOT NULL
        ''')
        
        if not stock_metrics.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("📋 Order Lines", f"{stock_metrics.iloc[0]['ORDER_LINES']:,}")
            
            with col2:
                st.metric("📦 Stock Orders", f"{stock_metrics.iloc[0]['UNIQUE_STOCK_ORDERS']:,}")
            
            with col3:
                st.metric("🛍️ Items Ordered", f"{stock_metrics.iloc[0]['UNIQUE_ITEMS_ORDERED']:,}")
            
            with col4:
                total_qty = stock_metrics.iloc[0]['TOTAL_QUANTITY'] or 0
                st.metric("📊 Total Quantity", f"{total_qty:,}")
        
        # Sales activity trends
        st.subheader("📈 Sales Activity Trends")
        
        # POS transaction trends by office
        office_activity = connector.execute_safe_query('''
            SELECT 
                "OfficeNum",
                COUNT(*) as TRANSACTION_COUNT,
                COUNT(DISTINCT "OrderID") as UNIQUE_ORDERS,
                COUNT(DISTINCT "PatientID") as UNIQUE_CUSTOMERS
            FROM RAW.DBO_POSTRANSACTION
            WHERE "OfficeNum" IS NOT NULL
            GROUP BY "OfficeNum"
            ORDER BY COUNT(*) DESC
            LIMIT 10
        ''')
        
        if not office_activity.empty:
            fig_office = px.bar(
                office_activity,
                x='OfficeNum',
                y='TRANSACTION_COUNT',
                title="Top 10 Offices by Transaction Volume",
                labels={'TRANSACTION_COUNT': 'Number of Transactions', 'OfficeNum': 'Office Number'},
                color='TRANSACTION_COUNT',
                color_continuous_scale='viridis'
            )
            
            st.plotly_chart(fig_office, use_container_width=True)
        
    except Exception as e:
        st.error(f"❌ Error loading sales overview: {e}")

elif analytics_section == "Top Products":
    st.header("🏆 Top Products Analysis")
    
    try:
        # Top products from stock orders (actual product movement)
        st.subheader("📦 Most Ordered Products")
        
        top_products = connector.execute_safe_query('''
            SELECT 
                SOD."ItemID",
                I."Description" as PRODUCT_NAME,
                I."ItemType" as CATEGORY,
                COUNT(*) as ORDER_COUNT,
                SUM(CAST(SOD."Quantity" AS INTEGER)) as TOTAL_QUANTITY,
                AVG(CAST(SOD."Quantity" AS INTEGER)) as AVG_QUANTITY_PER_ORDER
            FROM RAW.DBO_STOCKORDERDETAIL SOD
            LEFT JOIN RAW.DBO_ITEM I ON SOD."ItemID" = I."ID"
            WHERE SOD."ItemID" IS NOT NULL AND SOD."Quantity" IS NOT NULL
            GROUP BY SOD."ItemID", I."Description", I."ItemType"
            ORDER BY SUM(CAST(SOD."Quantity" AS INTEGER)) DESC
            LIMIT 20
        ''')
        
        if not top_products.empty:
            # Top products metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                top_product = top_products.iloc[0]
                product_name = str(top_product['PRODUCT_NAME']) if pd.notna(top_product['PRODUCT_NAME']) else f"ID: {top_product['ItemID']}"
                # Truncate long names for metric display
                display_name = (product_name[:25] + '...') if len(product_name) > 25 else product_name
                st.metric("🥇 #1 Product", 
                         display_name, 
                         f"{top_product['TOTAL_QUANTITY']:,} units")
            
            with col2:
                total_products_ordered = len(top_products)
                st.metric("🛍️ Top Products", f"{total_products_ordered}")
            
            with col3:
                total_quantity_top20 = top_products['TOTAL_QUANTITY'].sum()
                st.metric("📊 Top 20 Total Qty", f"{total_quantity_top20:,}")
            
            # Top products chart - use product names for better readability
            chart_data = top_products.head(10).copy()
            # Truncate long product names for better display
            chart_data['DISPLAY_NAME'] = chart_data['PRODUCT_NAME'].apply(
                lambda x: (str(x)[:30] + '...') if pd.notna(x) and len(str(x)) > 30 else str(x)
            )
            
            fig_top_products = px.bar(
                chart_data,
                x='DISPLAY_NAME',
                y='TOTAL_QUANTITY',
                title="Top 10 Products by Total Quantity Ordered",
                labels={'TOTAL_QUANTITY': 'Total Quantity', 'DISPLAY_NAME': 'Product Name'},
                color='TOTAL_QUANTITY',
                color_continuous_scale='blues',
                hover_data={'ItemID': True, 'PRODUCT_NAME': True, 'CATEGORY': True, 'ORDER_COUNT': True, 'DISPLAY_NAME': False}
            )
            
            fig_top_products.update_layout(xaxis_tickangle=45, height=600)
            st.plotly_chart(fig_top_products, use_container_width=True)
            
            # Top products table
            st.subheader("📋 Top Products Details")
            
            display_products = top_products.copy()
            display_products.columns = ['Product ID', 'Product Name', 'Category', 'Order Count', 'Total Quantity', 'Avg Qty/Order']
            display_products['Avg Qty/Order'] = display_products['Avg Qty/Order'].round(1)
            
            st.dataframe(display_products, use_container_width=True)
        
        # Product category performance
        st.subheader("📂 Category Performance")
        
        category_performance = connector.execute_safe_query('''
            SELECT 
                I."ItemType" as CATEGORY,
                COUNT(DISTINCT SOD."ItemID") as UNIQUE_PRODUCTS,
                COUNT(*) as ORDER_LINES,
                SUM(CAST(SOD."Quantity" AS INTEGER)) as TOTAL_QUANTITY
            FROM RAW.DBO_STOCKORDERDETAIL SOD
            LEFT JOIN RAW.DBO_ITEM I ON SOD."ItemID" = I."ID"
            WHERE SOD."ItemID" IS NOT NULL AND SOD."Quantity" IS NOT NULL AND I."ItemType" IS NOT NULL
            GROUP BY I."ItemType"
            ORDER BY SUM(CAST(SOD."Quantity" AS INTEGER)) DESC
            LIMIT 15
        ''')
        
        if not category_performance.empty:
            fig_category = px.pie(
                category_performance,
                values='TOTAL_QUANTITY',
                names='CATEGORY',
                title="Product Sales by Category (Total Quantity)",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            
            fig_category.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_category, use_container_width=True)
            
            # Category details
            st.dataframe(category_performance, use_container_width=True)
        
    except Exception as e:
        st.error(f"❌ Error loading top products: {e}")

elif analytics_section == "Product Performance":
    st.header("📈 Product Performance Analysis")
    
    try:
        # Product performance metrics
        st.subheader("🎯 Product Performance Metrics")
        
        # Products with multiple orders (repeat purchases)
        repeat_products = connector.execute_safe_query('''
            SELECT 
                SOD."ItemID",
                I."Description" as PRODUCT_NAME,
                I."ItemType" as CATEGORY,
                COUNT(DISTINCT SOD."StockOrderNum") as UNIQUE_ORDERS,
                COUNT(*) as TOTAL_ORDER_LINES,
                SUM(CAST(SOD."Quantity" AS INTEGER)) as TOTAL_QUANTITY,
                AVG(CAST(SOD."Quantity" AS INTEGER)) as AVG_QUANTITY
            FROM RAW.DBO_STOCKORDERDETAIL SOD
            LEFT JOIN RAW.DBO_ITEM I ON SOD."ItemID" = I."ID"
            WHERE SOD."ItemID" IS NOT NULL AND SOD."Quantity" IS NOT NULL
            GROUP BY SOD."ItemID", I."Description", I."ItemType"
            HAVING COUNT(DISTINCT SOD."StockOrderNum") > 1
            ORDER BY COUNT(DISTINCT SOD."StockOrderNum") DESC
            LIMIT 15
        ''')
        
        if not repeat_products.empty:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                repeat_count = len(repeat_products)
                st.metric("🔄 Repeat Products", f"{repeat_count}")
            
            with col2:
                top_repeat = repeat_products.iloc[0]
                st.metric("🏆 Most Reordered", 
                         f"ID: {top_repeat['ItemID']}", 
                         f"{top_repeat['UNIQUE_ORDERS']} orders")
            
            with col3:
                avg_reorders = repeat_products['UNIQUE_ORDERS'].mean()
                st.metric("📊 Avg Reorders", f"{avg_reorders:.1f}")
            
            # Repeat products chart
            fig_repeat = px.scatter(
                repeat_products,
                x='UNIQUE_ORDERS',
                y='TOTAL_QUANTITY',
                size='TOTAL_ORDER_LINES',
                color='CATEGORY',
                title="Product Reorder Patterns",
                labels={'UNIQUE_ORDERS': 'Number of Different Orders', 'TOTAL_QUANTITY': 'Total Quantity'},
                hover_data=['ItemID', 'PRODUCT_NAME']
            )
            
            st.plotly_chart(fig_repeat, use_container_width=True)
            
            # Repeat products table
            st.subheader("🔄 Most Reordered Products")
            
            display_repeat = repeat_products[['ItemID', 'PRODUCT_NAME', 'CATEGORY', 'UNIQUE_ORDERS', 'TOTAL_QUANTITY', 'AVG_QUANTITY']].copy()
            display_repeat.columns = ['Product ID', 'Product Name', 'Category', 'Unique Orders', 'Total Quantity', 'Avg Quantity']
            display_repeat['Avg Quantity'] = display_repeat['Avg Quantity'].round(1)
            
            st.dataframe(display_repeat, use_container_width=True)
        
        # Single-order products (potential discontinued or slow-moving)
        st.subheader("⚠️ Single-Order Products")
        
        single_order_products = connector.execute_safe_query('''
            SELECT 
                I."ItemType" as CATEGORY,
                COUNT(*) as SINGLE_ORDER_PRODUCTS,
                SUM(CAST(SOD."Quantity" AS INTEGER)) as TOTAL_QUANTITY
            FROM RAW.DBO_STOCKORDERDETAIL SOD
            LEFT JOIN RAW.DBO_ITEM I ON SOD."ItemID" = I."ID"
            WHERE SOD."ItemID" IS NOT NULL AND SOD."Quantity" IS NOT NULL AND I."ItemType" IS NOT NULL
            GROUP BY SOD."ItemID", I."ItemType"
            HAVING COUNT(DISTINCT SOD."StockOrderNum") = 1
        ''')
        
        single_summary = connector.execute_safe_query('''
            SELECT 
                I."ItemType" as CATEGORY,
                COUNT(DISTINCT SOD."ItemID") as SINGLE_ORDER_PRODUCTS
            FROM RAW.DBO_STOCKORDERDETAIL SOD
            LEFT JOIN RAW.DBO_ITEM I ON SOD."ItemID" = I."ID"
            WHERE SOD."ItemID" IS NOT NULL AND I."ItemType" IS NOT NULL
            GROUP BY SOD."ItemID", I."ItemType"
            HAVING COUNT(DISTINCT SOD."StockOrderNum") = 1
        ''')
        
        if not single_summary.empty:
            category_singles = single_summary.groupby('CATEGORY')['SINGLE_ORDER_PRODUCTS'].count().reset_index()
            category_singles.columns = ['CATEGORY', 'SINGLE_ORDER_COUNT']
            
            fig_singles = px.bar(
                category_singles,
                x='CATEGORY',
                y='SINGLE_ORDER_COUNT',
                title="Single-Order Products by Category",
                labels={'SINGLE_ORDER_COUNT': 'Number of Single-Order Products', 'CATEGORY': 'Product Category'},
                color='SINGLE_ORDER_COUNT',
                color_continuous_scale='reds'
            )
            
            fig_singles.update_layout(xaxis_tickangle=45)
            st.plotly_chart(fig_singles, use_container_width=True)
        
    except Exception as e:
        st.error(f"❌ Error loading product performance: {e}")

elif analytics_section == "Discount Analysis":
    st.header("💸 Discount Analysis")
    
    try:
        # Discount usage from invoices
        st.subheader("🎯 Discount Usage Patterns")
        
        discount_usage = connector.execute_safe_query('''
            SELECT 
                "DiscountTypeID",
                COUNT(*) as USAGE_COUNT,
                COUNT(DISTINCT "OrderNum") as UNIQUE_ORDERS
            FROM RAW.DBO_INVOICESUM
            WHERE "DiscountTypeID" IS NOT NULL AND "DiscountTypeID" != ''
            GROUP BY "DiscountTypeID"
            ORDER BY COUNT(*) DESC
            LIMIT 15
        ''')
        
        if not discount_usage.empty:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                total_discounted_invoices = discount_usage['USAGE_COUNT'].sum()
                st.metric("💳 Discounted Invoices", f"{total_discounted_invoices:,}")
            
            with col2:
                unique_discount_types = len(discount_usage)
                st.metric("🏷️ Discount Types Used", f"{unique_discount_types}")
            
            with col3:
                most_used_discount = discount_usage.iloc[0]
                st.metric("🏆 Most Used Discount", 
                         f"Type {most_used_discount['DiscountTypeID']}", 
                         f"{most_used_discount['USAGE_COUNT']} uses")
            
            # Discount usage chart
            fig_discounts = px.bar(
                discount_usage.head(10),
                x='DiscountTypeID',
                y='USAGE_COUNT',
                title="Top 10 Discount Types by Usage",
                labels={'USAGE_COUNT': 'Number of Uses', 'DiscountTypeID': 'Discount Type ID'},
                color='USAGE_COUNT',
                color_continuous_scale='greens'
            )
            
            st.plotly_chart(fig_discounts, use_container_width=True)
            
            # Discount details table
            st.subheader("📋 Discount Usage Details")
            
            display_discounts = discount_usage.copy()
            display_discounts.columns = ['Discount Type ID', 'Usage Count', 'Unique Orders']
            
            st.dataframe(display_discounts, use_container_width=True)
        
        # Refund analysis
        st.subheader("🔄 Refund Analysis")
        
        refund_analysis = connector.execute_safe_query('''
            SELECT 
                "RefundTypeID",
                COUNT(*) as REFUND_COUNT,
                COUNT(DISTINCT "OrderNum") as UNIQUE_ORDERS_WITH_REFUNDS
            FROM RAW.DBO_INVOICESUM
            WHERE "RefundTypeID" IS NOT NULL AND "RefundTypeID" != ''
            GROUP BY "RefundTypeID"
            ORDER BY COUNT(*) DESC
        ''')
        
        if not refund_analysis.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                total_refunds = refund_analysis['REFUND_COUNT'].sum()
                st.metric("🔄 Total Refunds", f"{total_refunds:,}")
            
            with col2:
                refund_types = len(refund_analysis)
                st.metric("🏷️ Refund Types", f"{refund_types}")
            
            # Refund types chart
            fig_refunds = px.pie(
                refund_analysis,
                values='REFUND_COUNT',
                names='RefundTypeID',
                title="Refunds by Type",
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            
            st.plotly_chart(fig_refunds, use_container_width=True)
        
    except Exception as e:
        st.error(f"❌ Error loading discount analysis: {e}")

elif analytics_section == "Transaction Insights":
    st.header("🔍 Transaction Insights")
    
    try:
        # Transaction patterns
        st.subheader("📊 Transaction Patterns")
        
        # Employee transaction activity
        employee_activity = connector.execute_safe_query('''
            SELECT 
                "EmployeeId",
                COUNT(*) as TRANSACTION_COUNT,
                COUNT(DISTINCT "OrderID") as UNIQUE_ORDERS,
                COUNT(DISTINCT "PatientID") as UNIQUE_CUSTOMERS
            FROM RAW.DBO_POSTRANSACTION
            WHERE "EmployeeId" IS NOT NULL AND "EmployeeId" != ''
            GROUP BY "EmployeeId"
            ORDER BY COUNT(*) DESC
            LIMIT 15
        ''')
        
        if not employee_activity.empty:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                active_employees = len(employee_activity)
                st.metric("👥 Active Employees", f"{active_employees}")
            
            with col2:
                top_employee = employee_activity.iloc[0]
                st.metric("🏆 Top Employee", 
                         f"ID: {top_employee['EmployeeId']}", 
                         f"{top_employee['TRANSACTION_COUNT']} transactions")
            
            with col3:
                avg_transactions = employee_activity['TRANSACTION_COUNT'].mean()
                st.metric("📊 Avg Transactions", f"{avg_transactions:.0f}")
            
            # Employee activity chart
            fig_employees = px.bar(
                employee_activity.head(10),
                x='EmployeeId',
                y='TRANSACTION_COUNT',
                title="Top 10 Employees by Transaction Volume",
                labels={'TRANSACTION_COUNT': 'Number of Transactions', 'EmployeeId': 'Employee ID'},
                color='TRANSACTION_COUNT',
                color_continuous_scale='plasma'
            )
            
            st.plotly_chart(fig_employees, use_container_width=True)
        
        # Customer transaction patterns
        st.subheader("👥 Customer Transaction Patterns")
        
        customer_patterns = connector.execute_safe_query('''
            SELECT 
                "PatientID",
                COUNT(*) as TRANSACTION_COUNT,
                COUNT(DISTINCT "OrderID") as UNIQUE_ORDERS,
                COUNT(DISTINCT "OfficeNum") as OFFICES_VISITED
            FROM RAW.DBO_POSTRANSACTION
            WHERE "PatientID" IS NOT NULL AND "PatientID" != ''
            GROUP BY "PatientID"
            ORDER BY COUNT(*) DESC
            LIMIT 20
        ''')
        
        if not customer_patterns.empty:
            # Customer metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                repeat_customers = len(customer_patterns[customer_patterns['TRANSACTION_COUNT'] > 1])
                st.metric("🔄 Repeat Customers", f"{repeat_customers}")
            
            with col2:
                top_customer = customer_patterns.iloc[0]
                st.metric("🏆 Top Customer", 
                         f"ID: {top_customer['PatientID']}", 
                         f"{top_customer['TRANSACTION_COUNT']} transactions")
            
            with col3:
                multi_office_customers = len(customer_patterns[customer_patterns['OFFICES_VISITED'] > 1])
                st.metric("🏢 Multi-Office Customers", f"{multi_office_customers}")
            
            # Customer transaction distribution
            transaction_distribution = customer_patterns['TRANSACTION_COUNT'].value_counts().sort_index()
            
            fig_distribution = px.bar(
                x=transaction_distribution.index,
                y=transaction_distribution.values,
                title="Customer Transaction Count Distribution",
                labels={'x': 'Number of Transactions per Customer', 'y': 'Number of Customers'}
            )
            
            st.plotly_chart(fig_distribution, use_container_width=True)
        
        # Office transaction patterns
        st.subheader("🏢 Office Transaction Patterns")
        
        office_patterns = connector.execute_safe_query('''
            SELECT 
                "OfficeNum",
                COUNT(*) as TRANSACTION_COUNT,
                COUNT(DISTINCT "PatientID") as UNIQUE_CUSTOMERS,
                COUNT(DISTINCT "EmployeeId") as ACTIVE_EMPLOYEES,
                COUNT(DISTINCT "OrderID") as UNIQUE_ORDERS
            FROM RAW.DBO_POSTRANSACTION
            WHERE "OfficeNum" IS NOT NULL
            GROUP BY "OfficeNum"
            ORDER BY COUNT(*) DESC
        ''')
        
        if not office_patterns.empty:
            # Office performance table
            st.subheader("📋 Office Performance Summary")
            
            display_offices = office_patterns.copy()
            display_offices.columns = ['Office Number', 'Transactions', 'Unique Customers', 'Active Employees', 'Unique Orders']
            
            st.dataframe(display_offices, use_container_width=True)
        
    except Exception as e:
        st.error(f"❌ Error loading transaction insights: {e}")

# Footer
st.markdown("---")
st.markdown("💰 **Product Sales Analytics Dashboard** | *Real Transaction Data • Sales Performance • Business Intelligence*")

if __name__ == "__main__":
    pass
