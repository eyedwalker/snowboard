#!/usr/bin/env python3
"""
Quick Product KPIs Dashboard
============================
Immediate product insights using available ITEM and ITEMTYPE data
978,822 products across 20 categories - instant analytics!
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from connectors.robust_snowfall_connector import RobustSnowfallConnector

# Page configuration (only when run as main script)
if __name__ == "__main__":
    st.set_page_config(
        page_title="Quick Product KPIs",
        page_icon="🛍️",
        layout="wide",
        initial_sidebar_state="expanded"
    )

# Initialize connector
@st.cache_resource
def get_connector():
    return RobustSnowfallConnector()

connector = get_connector()

# Header
st.title("🛍️ Quick Product KPIs Dashboard")
st.markdown("**Immediate Product Insights** | *978,822 Products • 20 Categories • Real-Time Analytics*")

# Sidebar
st.sidebar.header("🎛️ Product Analytics Controls")

# Analytics sections
analytics_section = st.sidebar.selectbox(
    "📊 Analytics Section",
    ["Product Overview", "Category Analysis", "Product Catalog Insights", "Specialized Products", "Product Search & Discovery"]
)

# Refresh data button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_resource.clear()
    st.rerun()

# Main content area
if analytics_section == "Product Overview":
    st.header("📋 Product Portfolio Overview")
    
    try:
        # Get basic product metrics
        product_count = connector.execute_safe_query("SELECT COUNT(*) as count FROM RAW.DBO_ITEM")
        category_count = connector.execute_safe_query("SELECT COUNT(*) as count FROM RAW.DBO_ITEMTYPE")
        
        # Get active vs inactive products
        product_status = connector.execute_safe_query('''
            SELECT 
                "Active",
                COUNT(*) as PRODUCT_COUNT
            FROM RAW.DBO_ITEM
            GROUP BY "Active"
            ORDER BY "Active" DESC
        ''')
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_products = product_count.iloc[0]['COUNT'] if not product_count.empty else 0
            st.metric("🛍️ Total Products", f"{total_products:,}")
        
        with col2:
            total_categories = category_count.iloc[0]['COUNT'] if not category_count.empty else 0
            st.metric("📂 Product Categories", f"{total_categories}")
        
        with col3:
            if not product_status.empty:
                active_products = product_status[product_status['Active'] == True]['PRODUCT_COUNT'].sum() if any(product_status['Active'] == True) else 0
                st.metric("✅ Active Products", f"{active_products:,}")
            else:
                st.metric("✅ Active Products", "N/A")
        
        with col4:
            if not product_status.empty and len(product_status) > 1:
                inactive_products = product_status[product_status['Active'] == False]['PRODUCT_COUNT'].sum() if any(product_status['Active'] == False) else 0
                st.metric("❌ Inactive Products", f"{inactive_products:,}")
            else:
                st.metric("❌ Inactive Products", "0")
        
        # Product status distribution
        if not product_status.empty:
            st.subheader("📊 Product Status Distribution")
            
            fig_status = px.pie(
                product_status,
                values='PRODUCT_COUNT',
                names='Active',
                title="Active vs Inactive Products",
                color_discrete_map={True: '#2ecc71', False: '#e74c3c'}
            )
            
            fig_status.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_status, use_container_width=True)
        
        # Product catalog insights
        st.subheader("🔍 Product Catalog Insights")
        
        catalog_insights = f"""
        ### 📈 Key Insights from Your Product Catalog:
        
        - **Massive Scale**: {total_products:,} products across {total_categories} categories
        - **Product Diversity**: Average of {total_products//total_categories if total_categories > 0 else 0:,} products per category
        - **Catalog Health**: {"Strong active product ratio" if active_products > inactive_products else "Review inactive products for optimization"}
        - **Analytics Ready**: Product data is clean and ready for advanced analytics
        
        ### 🎯 Immediate Opportunities:
        - **Category Performance**: Analyze which categories drive the most value
        - **Product Lifecycle**: Optimize active/inactive product ratios
        - **Inventory Intelligence**: Leverage product attributes for better insights
        - **Sales Integration**: Connect with transaction data for revenue analytics
        """
        
        st.markdown(catalog_insights)
        
    except Exception as e:
        st.error(f"❌ Error loading product overview: {e}")

elif analytics_section == "Category Analysis":
    st.header("📂 Product Category Analysis")
    
    try:
        # Get category breakdown
        category_analysis = connector.execute_safe_query('''
            SELECT 
                IT."ItemType" as CATEGORY,
                IT."Description" as CATEGORY_DESCRIPTION,
                COUNT(I."ID") as PRODUCT_COUNT,
                SUM(CASE WHEN I."Active" = TRUE THEN 1 ELSE 0 END) as ACTIVE_PRODUCTS,
                SUM(CASE WHEN I."Active" = FALSE THEN 1 ELSE 0 END) as INACTIVE_PRODUCTS
            FROM RAW.DBO_ITEMTYPE IT
            LEFT JOIN RAW.DBO_ITEM I ON IT."ItemType" = I."ItemType"
            GROUP BY IT."ItemType", IT."Description"
            ORDER BY COUNT(I."ID") DESC
        ''')
        
        if not category_analysis.empty:
            # Category metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                top_category = category_analysis.iloc[0]
                st.metric("🏆 Largest Category", 
                         f"{top_category['CATEGORY']}", 
                         f"{top_category['PRODUCT_COUNT']:,} products")
            
            with col2:
                total_active = category_analysis['ACTIVE_PRODUCTS'].sum()
                st.metric("✅ Total Active", f"{total_active:,}")
            
            with col3:
                avg_per_category = category_analysis['PRODUCT_COUNT'].mean()
                st.metric("📊 Avg per Category", f"{avg_per_category:,.0f}")
            
            # Category distribution chart
            st.subheader("📊 Products by Category")
            
            # Top 10 categories for better visualization
            top_categories = category_analysis.head(10)
            
            fig_categories = px.bar(
                top_categories,
                x='CATEGORY',
                y='PRODUCT_COUNT',
                title="Top 10 Product Categories by Count",
                labels={'PRODUCT_COUNT': 'Number of Products', 'CATEGORY': 'Product Category'},
                color='PRODUCT_COUNT',
                color_continuous_scale='viridis'
            )
            
            fig_categories.update_layout(xaxis_tickangle=45, height=500)
            st.plotly_chart(fig_categories, use_container_width=True)
            
            # Active vs Inactive by category
            st.subheader("🔄 Active vs Inactive Products by Category")
            
            # Create stacked bar chart
            fig_status = go.Figure()
            
            fig_status.add_trace(go.Bar(
                x=top_categories['CATEGORY'],
                y=top_categories['ACTIVE_PRODUCTS'],
                name='Active Products',
                marker_color='#2ecc71'
            ))
            
            fig_status.add_trace(go.Bar(
                x=top_categories['CATEGORY'],
                y=top_categories['INACTIVE_PRODUCTS'],
                name='Inactive Products',
                marker_color='#e74c3c'
            ))
            
            fig_status.update_layout(
                title="Active vs Inactive Products by Category",
                xaxis_title="Product Category",
                yaxis_title="Number of Products",
                barmode='stack',
                xaxis_tickangle=45,
                height=500
            )
            
            st.plotly_chart(fig_status, use_container_width=True)
            
            # Category details table
            st.subheader("📋 Category Details")
            
            # Format the data for display
            display_data = category_analysis.copy()
            display_data['ACTIVE_RATE'] = (display_data['ACTIVE_PRODUCTS'] / display_data['PRODUCT_COUNT'] * 100).round(1)
            display_data = display_data[['CATEGORY', 'CATEGORY_DESCRIPTION', 'PRODUCT_COUNT', 'ACTIVE_PRODUCTS', 'INACTIVE_PRODUCTS', 'ACTIVE_RATE']]
            display_data.columns = ['Category', 'Description', 'Total Products', 'Active', 'Inactive', 'Active Rate (%)']
            
            st.dataframe(display_data, use_container_width=True)
            
        else:
            st.warning("⚠️ No category data available")
            
    except Exception as e:
        st.error(f"❌ Error loading category analysis: {e}")

elif analytics_section == "Product Catalog Insights":
    st.header("🔍 Product Catalog Deep Dive")
    
    try:
        # Get product attribute analysis
        st.subheader("📊 Product Attributes Analysis")
        
        # Sample product data
        product_sample = connector.execute_safe_query('''
            SELECT 
                "ID",
                "ItemType",
                "Description",
                "Active",
                "CreatedDate",
                "ModifiedDate"
            FROM RAW.DBO_ITEM
            ORDER BY "ModifiedDate" DESC
            LIMIT 100
        ''')
        
        if not product_sample.empty:
            # Recent activity metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                recent_products = len(product_sample)
                st.metric("📋 Sample Size", f"{recent_products}")
            
            with col2:
                if 'CreatedDate' in product_sample.columns:
                    product_sample['CreatedDate'] = pd.to_datetime(product_sample['CreatedDate'], errors='coerce')
                    recent_created = product_sample[product_sample['CreatedDate'] > '2023-01-01'].shape[0]
                    st.metric("🆕 Recent Products", f"{recent_created}")
                else:
                    st.metric("🆕 Recent Products", "N/A")
            
            with col3:
                unique_types = product_sample['ItemType'].nunique()
                st.metric("🏷️ Types in Sample", f"{unique_types}")
            
            # Product sample table
            st.subheader("🔍 Recent Product Sample")
            
            display_sample = product_sample[['ID', 'ItemType', 'Description', 'Active']].head(20)
            st.dataframe(display_sample, use_container_width=True)
            
            # Product creation timeline (if dates available)
            if 'CreatedDate' in product_sample.columns:
                st.subheader("📈 Product Creation Timeline")
                
                # Filter out invalid dates
                valid_dates = product_sample.dropna(subset=['CreatedDate'])
                if not valid_dates.empty:
                    valid_dates['Year'] = valid_dates['CreatedDate'].dt.year
                    yearly_creation = valid_dates.groupby('Year').size().reset_index(name='Products_Created')
                    
                    fig_timeline = px.line(
                        yearly_creation,
                        x='Year',
                        y='Products_Created',
                        title="Product Creation Over Time",
                        markers=True
                    )
                    
                    st.plotly_chart(fig_timeline, use_container_width=True)
                else:
                    st.info("📅 Date information not available for timeline analysis")
        
        # Product ID analysis
        st.subheader("🔢 Product ID Insights")
        
        id_analysis = connector.execute_safe_query('''
            SELECT 
                MIN("ID") as MIN_ID,
                MAX("ID") as MAX_ID,
                COUNT(DISTINCT "ID") as UNIQUE_IDS,
                COUNT(*) as TOTAL_RECORDS
            FROM RAW.DBO_ITEM
        ''')
        
        if not id_analysis.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                min_id = id_analysis.iloc[0]['MIN_ID']
                st.metric("🔽 Min Product ID", f"{min_id}")
            
            with col2:
                max_id = id_analysis.iloc[0]['MAX_ID']
                st.metric("🔼 Max Product ID", f"{max_id}")
            
            with col3:
                unique_ids = id_analysis.iloc[0]['UNIQUE_IDS']
                st.metric("🆔 Unique IDs", f"{unique_ids:,}")
            
            with col4:
                total_records = id_analysis.iloc[0]['TOTAL_RECORDS']
                st.metric("📊 Total Records", f"{total_records:,}")
            
            # ID range insights
            id_range = max_id - min_id + 1
            utilization = (unique_ids / id_range * 100) if id_range > 0 else 0
            
            st.info(f"📈 **ID Range Utilization**: {utilization:.1f}% ({unique_ids:,} used out of {id_range:,} possible IDs)")
        
    except Exception as e:
        st.error(f"❌ Error loading catalog insights: {e}")

elif analytics_section == "Specialized Products":
    st.header("🔬 Specialized Product Analysis")
    
    st.markdown("""
    ### 🎯 Eyecare Specialized Product Tables
    
    Your system includes specialized tables for detailed product analytics:
    """)
    
    # Check specialized tables
    specialized_tables = {
        'DBO_ITEMFRAME': '👓 Eyeglass Frames',
        'DBO_ITEMEGLENS': '🔍 Eyeglass Lenses', 
        'DBO_ITEMCOAT': '✨ Lens Coatings',
        'DBO_ITEMEXAM': '👁️ Eye Exams'
    }
    
    for table, description in specialized_tables.items():
        try:
            st.subheader(f"{description}")
            
            # Get count and sample
            count_query = f'SELECT COUNT(*) as count FROM RAW.{table}'
            count_result = connector.execute_safe_query(count_query)
            
            if not count_result.empty:
                count = count_result.iloc[0]['COUNT']
                st.metric(f"📊 {description} Records", f"{count:,}")
                
                if count > 0:
                    # Get sample data
                    sample_query = f'SELECT * FROM RAW.{table} LIMIT 5'
                    sample_data = connector.execute_safe_query(sample_query)
                    
                    if not sample_data.empty:
                        st.dataframe(sample_data, use_container_width=True)
                    else:
                        st.info("No sample data available")
                else:
                    st.info("No records found")
            else:
                st.warning("Unable to access table")
                
        except Exception as e:
            st.warning(f"⚠️ {description}: {str(e)[:50]}...")
    
    # Specialized product insights
    st.subheader("💡 Specialized Product Opportunities")
    
    opportunities = """
    ### 🚀 Advanced Analytics Opportunities:
    
    **👓 Frame Analytics:**
    - Brand performance analysis
    - Style trend identification
    - Price point optimization
    - Inventory turnover rates
    
    **🔍 Lens Analytics:**
    - Prescription type distribution
    - Lens material preferences
    - Progressive vs single vision trends
    - Anti-reflective coating adoption
    
    **✨ Coating Analytics:**
    - Coating type popularity
    - Premium coating upsell rates
    - Durability and warranty analysis
    - Customer satisfaction correlation
    
    **👁️ Exam Analytics:**
    - Exam type frequency
    - Seasonal patterns
    - Follow-up compliance rates
    - Revenue per exam analysis
    """
    
    st.markdown(opportunities)

elif analytics_section == "Product Search & Discovery":
    st.header("🔍 Product Search & Discovery")
    
    # Search interface
    st.subheader("🔎 Product Search")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        search_term = st.text_input("Search products by description:", placeholder="Enter product name or description...")
    
    with col2:
        search_category = st.selectbox("Filter by category:", ["All Categories"] + ["Frame", "Lens", "Coating", "Exam", "Other"])
    
    if search_term:
        try:
            # Build search query
            search_query = f'''
                SELECT 
                    I."ID",
                    I."ItemType",
                    I."Description",
                    I."Active",
                    IT."Description" as CATEGORY_DESCRIPTION
                FROM RAW.DBO_ITEM I
                LEFT JOIN RAW.DBO_ITEMTYPE IT ON I."ItemType" = IT."ItemType"
                WHERE UPPER(I."Description") LIKE UPPER('%{search_term}%')
            '''
            
            if search_category != "All Categories":
                search_query += f" AND UPPER(I.\"ItemType\") LIKE UPPER('%{search_category}%')"
            
            search_query += " ORDER BY I.\"Description\" LIMIT 50"
            
            search_results = connector.execute_safe_query(search_query)
            
            if not search_results.empty:
                st.success(f"🎯 Found {len(search_results)} products matching '{search_term}'")
                
                # Display results
                display_results = search_results[['ID', 'ItemType', 'Description', 'Active', 'CATEGORY_DESCRIPTION']]
                display_results.columns = ['Product ID', 'Type', 'Description', 'Active', 'Category Description']
                
                st.dataframe(display_results, use_container_width=True)
                
                # Search insights
                active_count = search_results['Active'].sum()
                inactive_count = len(search_results) - active_count
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("📋 Total Found", len(search_results))
                with col2:
                    st.metric("✅ Active", active_count)
                with col3:
                    st.metric("❌ Inactive", inactive_count)
                    
            else:
                st.warning(f"No products found matching '{search_term}'")
                
        except Exception as e:
            st.error(f"❌ Search error: {e}")
    
    # Popular search suggestions
    st.subheader("💡 Popular Search Suggestions")
    
    suggestions = [
        "frame", "lens", "coating", "exam", "progressive", 
        "bifocal", "single", "anti-reflective", "transition", "polarized"
    ]
    
    suggestion_cols = st.columns(5)
    for i, suggestion in enumerate(suggestions):
        with suggestion_cols[i % 5]:
            if st.button(f"🔍 {suggestion.title()}", key=f"search_{suggestion}"):
                st.rerun()

# Footer
st.markdown("---")
st.markdown("🛍️ **Quick Product KPIs Dashboard** | *978,822 Products • 20 Categories • Instant Insights*")

if __name__ == "__main__":
    pass
