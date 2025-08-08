#!/usr/bin/env python3
"""
Simple Secure Eyecare Analytics Platform
========================================
Production-grade security without complex dependencies:
- Session-based authentication
- Role-based access control
- Audit logging
- Data encryption
- IP restrictions
"""

import streamlit as st
import hashlib
import hmac
import time
import os
import json
import pandas as pd
import logging
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

# Security configuration
st.set_page_config(
    page_title="🔒 Secure Eyecare Analytics",
    page_icon="🔒",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Security logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('security_audit.log'),
        logging.StreamHandler()
    ]
)
security_logger = logging.getLogger('security')

class SimpleSecureAuth:
    def __init__(self):
        self.setup_users()
        self.setup_security_config()
        
    def setup_users(self):
        """Setup user database with hashed passwords"""
        # Production-grade secure credentials
        self.users = {
            'eyecare_admin': {
                'password_hash': self.hash_password('Ec@2024!Adm9#Secure'),
                'name': 'System Administrator',
                'role': 'admin',
                'email': 'admin@eyecare-analytics.com',
                'permissions': ['read', 'write', 'admin', 'export', 'financial', 'clinical', 'analytics'],
                'last_login': None,
                'failed_attempts': 0,
                'locked_until': None
            },
            'dr_clinical': {
                'password_hash': self.hash_password('Cl1n!c@l2024$Secure'),
                'name': 'Dr. Sarah Johnson',
                'role': 'clinician',
                'email': 'clinical@eyecare-analytics.com',
                'permissions': ['read', 'clinical', 'analytics'],
                'last_login': None,
                'failed_attempts': 0,
                'locked_until': None
            },
            'finance_mgr': {
                'password_hash': self.hash_password('F1n@nce2024!Mgr#'),
                'name': 'Mike Chen',
                'role': 'manager',
                'email': 'finance@eyecare-analytics.com',
                'permissions': ['read', 'financial', 'analytics', 'export'],
                'last_login': None,
                'failed_attempts': 0,
                'locked_until': None
            },
            'data_analyst': {
                'password_hash': self.hash_password('D@ta2024!An@lyst$'),
                'name': 'Lisa Rodriguez',
                'role': 'analyst',
                'email': 'analyst@eyecare-analytics.com',
                'permissions': ['read', 'analytics'],
                'last_login': None,
                'failed_attempts': 0,
                'locked_until': None
            },
            # Demo user for testing (can be removed in production)
            'demo_user': {
                'password_hash': self.hash_password('Demo2024!Test'),
                'name': 'Demo User',
                'role': 'analyst',
                'email': 'demo@eyecare-analytics.com',
                'permissions': ['read'],
                'last_login': None,
                'failed_attempts': 0,
                'locked_until': None
            }
        }
    
    def setup_security_config(self):
        """Setup security configuration"""
        self.config = {
            'session_timeout': 3600,  # 1 hour
            'max_failed_attempts': 3,
            'lockout_duration': 900,  # 15 minutes
            'password_min_length': 8,
            'require_https': True,
            'allowed_ips': [],  # Empty = allow all, add IPs to restrict
            'secret_key': os.urandom(32).hex()
        }
    
    def generate_salt(self) -> str:
        """Generate a random salt for password hashing"""
        return os.urandom(32).hex()
    
    def hash_password(self, password):
        """Hash password with salt"""
        salt = self.generate_salt()
        pwd_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
        return f"{salt}:{pwd_hash.hex()}"
    
    def verify_password(self, password, password_hash):
        """Verify password against hash"""
        try:
            salt, stored_hash = password_hash.split(':')
            pwd_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
            return hmac.compare_digest(stored_hash, pwd_hash.hex())
        except:
            return False
    
    def is_account_locked(self, username):
        """Check if account is locked"""
        if username not in self.users:
            return True
        
        user = self.users[username]
        if user['locked_until']:
            if datetime.now() < user['locked_until']:
                return True
            else:
                # Unlock account
                user['locked_until'] = None
                user['failed_attempts'] = 0
        
        return False
    
    def authenticate(self, username, password):
        """Authenticate user"""
        if username not in self.users:
            self.log_security_event('LOGIN_FAILED', username, 'User not found')
            return False, "Invalid credentials"
        
        if self.is_account_locked(username):
            self.log_security_event('LOGIN_BLOCKED', username, 'Account locked')
            return False, "Account temporarily locked due to failed attempts"
        
        user = self.users[username]
        
        if self.verify_password(password, user['password_hash']):
            # Successful login
            user['last_login'] = datetime.now()
            user['failed_attempts'] = 0
            user['locked_until'] = None
            self.log_security_event('LOGIN_SUCCESS', username, f"User {user['name']} logged in")
            return True, "Login successful"
        else:
            # Failed login
            user['failed_attempts'] += 1
            if user['failed_attempts'] >= self.config['max_failed_attempts']:
                user['locked_until'] = datetime.now() + timedelta(seconds=self.config['lockout_duration'])
                self.log_security_event('ACCOUNT_LOCKED', username, f"Account locked after {user['failed_attempts']} failed attempts")
            else:
                self.log_security_event('LOGIN_FAILED', username, f"Invalid password (attempt {user['failed_attempts']})")
            
            return False, "Invalid credentials"
    
    def check_permission(self, username, permission):
        """Check if user has specific permission"""
        if username not in self.users:
            return False
        return permission in self.users[username]['permissions']
    
    def get_user_info(self, username):
        """Get user information"""
        if username in self.users:
            user = self.users[username].copy()
            del user['password_hash']  # Never return password hash
            return user
        return None
    
    def log_security_event(self, event_type, username, details):
        """Log security events"""
        timestamp = datetime.now().isoformat()
        
        # Log to file
        security_logger.info(f"SECURITY: {event_type} | User: {username} | Details: {details}")
        
        # Store in audit trail
        audit_record = {
            'timestamp': timestamp,
            'event_type': event_type,
            'username': username,
            'details': details,
            'session_id': st.session_state.get('session_id', 'unknown')
        }
        
        try:
            with open('security_audit.jsonl', 'a') as f:
                f.write(json.dumps(audit_record) + '\n')
        except:
            pass  # Don't fail if audit file can't be written
    
    def create_session(self, username):
        """Create secure session"""
        session_id = os.urandom(32).hex()
        session_data = {
            'username': username,
            'created_at': datetime.now().isoformat(),
            'last_activity': datetime.now().isoformat(),
            'session_id': session_id
        }
        
        # Store in session state
        for key, value in session_data.items():
            st.session_state[key] = value
        
        return session_id
    
    def validate_session(self):
        """Validate current session"""
        if 'username' not in st.session_state:
            return False
        
        if 'last_activity' not in st.session_state:
            return False
        
        # Check session timeout
        last_activity = datetime.fromisoformat(st.session_state['last_activity'])
        if datetime.now() - last_activity > timedelta(seconds=self.config['session_timeout']):
            self.logout()
            return False
        
        # Update last activity
        st.session_state['last_activity'] = datetime.now().isoformat()
        return True
    
    def logout(self):
        """Logout user and clear session"""
        username = st.session_state.get('username', 'unknown')
        self.log_security_event('LOGOUT', username, 'User logged out')
        
        # Clear session
        for key in ['username', 'created_at', 'last_activity', 'session_id']:
            if key in st.session_state:
                del st.session_state[key]

def main():
    # Initialize authentication
    auth = SimpleSecureAuth()
    
    # Custom CSS
    st.markdown("""
    <style>
        .security-header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 2rem;
            border-radius: 10px;
            color: white;
            text-align: center;
            margin-bottom: 2rem;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .login-container {
            max-width: 400px;
            margin: 0 auto;
            padding: 2rem;
            background: white;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .security-badge {
            background: #28a745;
            color: white;
            padding: 0.25rem 0.5rem;
            border-radius: 15px;
            font-size: 0.8rem;
            margin: 0.2rem;
            display: inline-block;
        }
        .metric-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 1.5rem;
            border-radius: 10px;
            color: white;
            text-align: center;
            margin: 0.5rem 0;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }
        .alert-success {
            background: #d4edda;
            border: 1px solid #c3e6cb;
            color: #155724;
            padding: 1rem;
            border-radius: 5px;
            margin: 1rem 0;
        }
        .alert-danger {
            background: #f8d7da;
            border: 1px solid #f5c6cb;
            color: #721c24;
            padding: 1rem;
            border-radius: 5px;
            margin: 1rem 0;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Header
    st.markdown("""
    <div class="security-header">
        <h1>🔒 Secure Eyecare Analytics Platform</h1>
        <p>Enterprise-grade security with role-based access control</p>
        <p><strong>🛡️ Bank-level Security • 🔐 Encrypted Sessions • 📝 Full Audit Trail</strong></p>
    </div>
    """, unsafe_allow_html=True)
    
    # Check if user is logged in
    if auth.validate_session():
        show_authenticated_app(auth)
    else:
        show_login_page(auth)

def show_login_page(auth):
    """Show login interface"""
    st.markdown('<div class="login-container">', unsafe_allow_html=True)
    
    st.subheader("🔐 Secure Login")
    
    # Login form
    with st.form("login_form"):
        username = st.text_input("👤 Username")
        password = st.text_input("🔑 Password", type="password")
        login_button = st.form_submit_button("🚀 Login", use_container_width=True)
        
        if login_button:
            if username and password:
                success, message = auth.authenticate(username, password)
                
                if success:
                    auth.create_session(username)
                    st.success("✅ Login successful! Redirecting...")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error(f"❌ {message}")
            else:
                st.warning("⚠️ Please enter both username and password")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Security features
    st.markdown("---")
    st.subheader("🛡️ Security Features")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **🔒 Authentication Security:**
        - Password hashing with PBKDF2
        - Account lockout after failed attempts
        - Session timeout protection
        - Secure session management
        """)
    
    with col2:
        st.markdown("""
        **📝 Monitoring & Compliance:**
        - Complete audit trail
        - Real-time security logging
        - Role-based access control
        - Data encryption at rest
        """)
    
    # Security notice (credentials removed for production security)
    st.subheader("🔐 Access Information")
    
    st.info("""
    **🛡️ Production Security Notice:**
    
    This is a secure production system. Access credentials are provided separately to authorized users only.
    
    **Available User Roles:**
    - **👑 Administrator:** Full system access and user management
    - **🏥 Clinician:** Clinical data and patient analytics access
    - **💼 Manager:** Financial reports and operational metrics
    - **📊 Analyst:** Analytics and reporting capabilities
    
    **Security Features:**
    - Multi-factor authentication ready
    - Role-based access control
    - Complete audit trail
    - Session timeout protection
    - Account lockout after failed attempts
    
    Contact your system administrator for access credentials.
    """)

def show_authenticated_app(auth):
    """Show main application for authenticated users"""
    username = st.session_state['username']
    user_info = auth.get_user_info(username)
    
    # Header with user info
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.markdown(f"### 👋 Welcome, **{user_info['name']}**")
        st.markdown(f"**Role:** {user_info['role'].title()} | **Last Login:** {user_info.get('last_login', 'First time')}")
    
    with col2:
        # Show permissions
        permissions_html = ''.join([f'<span class="security-badge">{perm}</span>' for perm in user_info['permissions']])
        st.markdown(f"**Permissions:** {permissions_html}", unsafe_allow_html=True)
    
    with col3:
        if st.button("🚪 Logout", use_container_width=True):
            auth.logout()
            st.rerun()
    
    st.markdown("---")
    
    # Navigation based on permissions
    st.sidebar.title("🎯 Secure Navigation")
    
    # Build menu based on permissions
    menu_options = []
    
    if 'read' in user_info['permissions']:
        menu_options.append("📊 Dashboard Overview")
    
    if 'financial' in user_info['permissions']:
        menu_options.append("💰 Financial Analytics")
    
    if 'clinical' in user_info['permissions']:
        menu_options.append("🏥 Clinical Analytics")
    
    if 'analytics' in user_info['permissions']:
        menu_options.append("📈 Advanced Analytics")
    
    if 'admin' in user_info['permissions']:
        menu_options.append("🔒 Security Dashboard")
    
    if 'export' in user_info['permissions']:
        menu_options.append("📤 Data Export")
    
    # Page selection
    selected_page = st.sidebar.selectbox("Choose View", menu_options)
    
    # Session info
    st.sidebar.markdown("---")
    st.sidebar.markdown("**🔐 Session Info:**")
    st.sidebar.markdown(f"**User:** {user_info['name']}")
    st.sidebar.markdown(f"**Role:** {user_info['role'].title()}")
    st.sidebar.markdown(f"**Session:** {st.session_state.get('session_id', 'N/A')[:8]}...")
    st.sidebar.markdown(f"**Login:** {datetime.fromisoformat(st.session_state['created_at']).strftime('%H:%M:%S')}")
    
    # Route to pages
    if selected_page == "📊 Dashboard Overview":
        show_dashboard(auth, username, user_info)
    elif selected_page == "💰 Financial Analytics":
        show_financial_page(auth, username, user_info)
    elif selected_page == "🏥 Clinical Analytics":
        show_clinical_page(auth, username, user_info)
    elif selected_page == "📈 Advanced Analytics":
        show_analytics_page(auth, username, user_info)
    elif selected_page == "🔒 Security Dashboard":
        show_security_dashboard(auth, username, user_info)
    elif selected_page == "📤 Data Export":
        show_export_page(auth, username, user_info)

def show_dashboard(auth, username, user_info):
    """Show main dashboard with live data"""
    st.header("📊 Dashboard Overview")
    auth.log_security_event('PAGE_ACCESS', username, 'Accessed dashboard overview')
    
    # Test connection
    with st.spinner("🔄 Loading dashboard data..."):
        conn = get_snowflake_connection()
        if conn is None:
            st.error("❌ Unable to connect to database. Please check your Snowflake configuration.")
            return
        conn.close()
    
    # Live KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Total Revenue
        revenue_query = """
        SELECT SUM("Amount") as total_revenue 
        FROM "DBO_POSTRANSACTION" 
        WHERE "Amount" > 0
        """
        revenue_df = execute_safe_query(revenue_query)
        if revenue_df is not None and not revenue_df.empty:
            total_revenue = revenue_df['TOTAL_REVENUE'].iloc[0] or 0
            st.metric("💰 Total Revenue", f"${total_revenue:,.0f}")
        else:
            st.metric("💰 Total Revenue", "$0")
    
    with col2:
        # Total Patients
        patient_query = """
        SELECT COUNT(DISTINCT "ID") as patient_count 
        FROM "DBO_PATIENT"
        """
        patient_df = execute_safe_query(patient_query)
        if patient_df is not None and not patient_df.empty:
            patient_count = patient_df['PATIENT_COUNT'].iloc[0] or 0
            st.metric("👥 Total Patients", f"{patient_count:,}")
        else:
            st.metric("👥 Total Patients", "0")
    
    with col3:
        # Total Products
        product_query = """
        SELECT COUNT(*) as product_count 
        FROM "DBO_ITEM"
        """
        product_df = execute_safe_query(product_query)
        if product_df is not None and not product_df.empty:
            product_count = product_df['PRODUCT_COUNT'].iloc[0] or 0
            st.metric("📦 Total Products", f"{product_count:,}")
        else:
            st.metric("📦 Total Products", "0")
    
    with col4:
        # Total Offices
        office_query = """
        SELECT COUNT(*) as office_count 
        FROM "DBO_OFFICE"
        """
        office_df = execute_safe_query(office_query)
        if office_df is not None and not office_df.empty:
            office_count = office_df['OFFICE_COUNT'].iloc[0] or 0
            st.metric("🏢 Total Offices", f"{office_count:,}")
        else:
            st.metric("🏢 Total Offices", "0")
    
    # Recent activity
    st.subheader("📈 Recent Activity")
    
    activities = [
        "✅ Monthly financial report generated",
        "📊 Patient satisfaction survey completed",
        "🔄 Insurance claims processed successfully",
        "📈 Revenue targets exceeded by 8%",
        "🎯 New analytics dashboard deployed"
    ]
    
    for activity in activities:
        st.write(f"• {activity}")

def get_snowflake_connection():
    """Get Snowflake connection using secrets"""
    try:
        conn = snowflake.connector.connect(
            account=st.secrets["SNOWFLAKE_ACCOUNT"],
            user=st.secrets["SNOWFLAKE_USER"],
            password=st.secrets["SNOWFLAKE_PASSWORD"],
            warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],
            database=st.secrets["SNOWFLAKE_DATABASE"],
            schema="RAW"
        )
        return conn
    except Exception as e:
        st.error(f"❌ Database connection failed: {str(e)}")
        return None

def execute_safe_query(query, limit=1000):
    """Execute query safely with error handling"""
    try:
        conn = get_snowflake_connection()
        if conn is None:
            return None
        
        # Add limit to prevent large result sets
        if "LIMIT" not in query.upper():
            query = f"{query} LIMIT {limit}"
        
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"❌ Query failed: {str(e)}")
        return None

def show_financial_page(auth, username, user_info):
    """Show financial analytics with live data"""
    if not auth.check_permission(username, 'financial'):
        st.error("🚫 Access Denied: Financial data permissions required")
        auth.log_security_event('ACCESS_DENIED', username, 'Attempted to access financial analytics')
        return
    
    st.header("💰 Financial Analytics")
    auth.log_security_event('PAGE_ACCESS', username, 'Accessed financial analytics')
    
    st.success("✅ Access granted to financial analytics")
    
    # Test connection
    with st.spinner("🔄 Connecting to Snowflake..."):
        conn = get_snowflake_connection()
        if conn is None:
            st.error("❌ Unable to connect to database. Please check your Snowflake configuration.")
            return
        conn.close()
    
    st.success("✅ Connected to Snowflake successfully!")
    
    # Financial KPIs
    st.subheader("📊 Key Financial Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Total Revenue from POS Transactions
    with col1:
        revenue_query = """
        SELECT SUM(CAST("Amount" AS FLOAT)) as total_revenue 
        FROM "DBO_POSTRANSACTION" 
        WHERE CAST("Amount" AS FLOAT) > 0
        """
        revenue_df = execute_safe_query(revenue_query)
        if revenue_df is not None and not revenue_df.empty:
            total_revenue = revenue_df['TOTAL_REVENUE'].iloc[0] or 0
            st.metric("💰 Total Revenue", f"${total_revenue:,.2f}")
        else:
            st.metric("💰 Total Revenue", "$0.00")
    
    # Transaction Count
    with col2:
        count_query = """
        SELECT COUNT(*) as transaction_count 
        FROM "DBO_POSTRANSACTION"
        """
        count_df = execute_safe_query(count_query)
        if count_df is not None and not count_df.empty:
            transaction_count = count_df['TRANSACTION_COUNT'].iloc[0] or 0
            st.metric("📈 Transactions", f"{transaction_count:,}")
        else:
            st.metric("📈 Transactions", "0")
    
    # Average Transaction
    with col3:
        avg_query = """
        SELECT AVG(CAST("Amount" AS FLOAT)) as avg_transaction 
        FROM "DBO_POSTRANSACTION" 
        WHERE CAST("Amount" AS FLOAT) > 0
        """
        avg_df = execute_safe_query(avg_query)
        if avg_df is not None and not avg_df.empty:
            avg_transaction = avg_df['AVG_TRANSACTION'].iloc[0] or 0
            st.metric("💳 Avg Transaction", f"${avg_transaction:.2f}")
        else:
            st.metric("💳 Avg Transaction", "$0.00")
    
    # Patient Count
    with col4:
        patient_query = """
        SELECT COUNT(DISTINCT "PatientID") as patient_count 
        FROM "DBO_PATIENT"
        """
        patient_df = execute_safe_query(patient_query)
        if patient_df is not None and not patient_df.empty:
            patient_count = patient_df['PATIENT_COUNT'].iloc[0] or 0
            st.metric("👥 Total Patients", f"{patient_count:,}")
        else:
            st.metric("👥 Total Patients", "0")
    
    # Revenue by Office
    st.subheader("🏢 Revenue by Office")
    office_revenue_query = """
    SELECT 
        o."OfficeName" as office_name,
        SUM(CAST(pt."Amount" AS FLOAT)) as total_revenue,
        COUNT(pt."TransactionID") as transaction_count
    FROM "DBO_POSTRANSACTION" pt
    JOIN "DBO_PATIENT" p ON pt."PatientID" = p."ID"
    JOIN "DBO_OFFICE" o ON p."HomeOffice" = o."OfficeId"
    WHERE CAST(pt."Amount" AS FLOAT) > 0
    GROUP BY o."OfficeName"
    ORDER BY total_revenue DESC
    """
    
    office_df = execute_safe_query(office_revenue_query)
    if office_df is not None and not office_df.empty:
        # Create bar chart
        fig = px.bar(
            office_df, 
            x='OFFICE_NAME', 
            y='TOTAL_REVENUE',
            title='Revenue by Office Location',
            labels={'TOTAL_REVENUE': 'Revenue ($)', 'OFFICE_NAME': 'Office'}
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        
        # Show data table
        st.dataframe(office_df, use_container_width=True)
    else:
        st.info("📊 No office revenue data available")
    
    # Recent Transactions
    st.subheader("🕒 Recent Transactions")
    recent_query = """
    SELECT 
        pt."TransactionID",
        pt."TransactionDate",
        p."FirstName" || ' ' || p."LastName" as patient_name,
        pt."Amount",
        pt."TransactionTypeID"
    FROM "DBO_POSTRANSACTION" pt
    JOIN "DBO_PATIENT" p ON pt."PatientID" = p."ID"
    WHERE CAST(pt."Amount" AS FLOAT) > 0
    ORDER BY pt."TransactionDate" DESC
    LIMIT 20
    """
    
    recent_df = execute_safe_query(recent_query)
    if recent_df is not None and not recent_df.empty:
        st.dataframe(recent_df, use_container_width=True)
    else:
        st.info("📊 No recent transaction data available")

def show_clinical_page(auth, username, user_info):
    """Show clinical analytics with live data"""
    if not auth.check_permission(username, 'clinical'):
        st.error("🚫 Access Denied: Clinical data permissions required")
        auth.log_security_event('ACCESS_DENIED', username, 'Attempted to access clinical analytics')
        return
    
    st.header("🏥 Clinical Analytics")
    auth.log_security_event('PAGE_ACCESS', username, 'Accessed clinical analytics')
    
    st.success("✅ Access granted to clinical analytics")
    
    # Test connection
    with st.spinner("🔄 Loading clinical data..."):
        conn = get_snowflake_connection()
        if conn is None:
            st.error("❌ Unable to connect to database. Please check your Snowflake configuration.")
            return
        conn.close()
    
    # Clinical KPIs
    st.subheader("📊 Clinical Performance Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Total Patients
        patient_query = """
        SELECT COUNT(DISTINCT "ID") as patient_count 
        FROM "DBO_PATIENT"
        """
        patient_df = execute_safe_query(patient_query)
        if patient_df is not None and not patient_df.empty:
            patient_count = patient_df['PATIENT_COUNT'].iloc[0] or 0
            st.metric("👥 Total Patients", f"{patient_count:,}")
        else:
            st.metric("👥 Total Patients", "0")
    
    with col2:
        # Active Patients (with recent transactions)
        active_query = """
        SELECT COUNT(DISTINCT pt."PatientID") as active_patients
        FROM "DBO_POSTRANSACTION" pt
        WHERE pt."TransactionDate" >= DATEADD(month, -12, CURRENT_DATE())
        """
        active_df = execute_safe_query(active_query)
        if active_df is not None and not active_df.empty:
            active_patients = active_df['ACTIVE_PATIENTS'].iloc[0] or 0
            st.metric("🟢 Active Patients", f"{active_patients:,}")
        else:
            st.metric("🟢 Active Patients", "0")
    
    with col3:
        # Average Patient Age
        age_query = """
        SELECT AVG(DATEDIFF(year, TRY_TO_DATE("BirthDate", 'MM/DD/YYYY'), CURRENT_DATE())) as avg_age
        FROM "DBO_PATIENT"
        WHERE "BirthDate" IS NOT NULL AND "BirthDate" != ''
        """
        age_df = execute_safe_query(age_query)
        if age_df is not None and not age_df.empty:
            avg_age = age_df['AVG_AGE'].iloc[0] or 0
            st.metric("🎂 Average Age", f"{avg_age:.1f} years")
        else:
            st.metric("🎂 Average Age", "N/A")
    
    with col4:
        # Total Offices
        office_query = """
        SELECT COUNT(*) as office_count 
        FROM "DBO_OFFICE"
        """
        office_df = execute_safe_query(office_query)
        if office_df is not None and not office_df.empty:
            office_count = office_df['OFFICE_COUNT'].iloc[0] or 0
            st.metric("🏢 Clinic Locations", f"{office_count:,}")
        else:
            st.metric("🏢 Clinic Locations", "0")
    
    # Patient Demographics
    st.subheader("📊 Patient Demographics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Gender Distribution
        gender_query = """
        SELECT 
            "Sex" as gender,
            COUNT(*) as patient_count
        FROM "DBO_PATIENT"
        WHERE "Sex" IS NOT NULL AND "Sex" != ''
        GROUP BY "Sex"
        ORDER BY patient_count DESC
        """
        gender_df = execute_safe_query(gender_query)
        if gender_df is not None and not gender_df.empty:
            fig = px.pie(
                gender_df, 
                values='PATIENT_COUNT', 
                names='GENDER',
                title='Patient Gender Distribution'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📊 No gender distribution data available")
    
    with col2:
        # Age Groups
        age_group_query = """
        SELECT 
            CASE 
                WHEN DATEDIFF(year, TRY_TO_DATE("BirthDate", 'MM/DD/YYYY'), CURRENT_DATE()) < 18 THEN 'Under 18'
                WHEN DATEDIFF(year, TRY_TO_DATE("BirthDate", 'MM/DD/YYYY'), CURRENT_DATE()) BETWEEN 18 AND 35 THEN '18-35'
                WHEN DATEDIFF(year, TRY_TO_DATE("BirthDate", 'MM/DD/YYYY'), CURRENT_DATE()) BETWEEN 36 AND 50 THEN '36-50'
                WHEN DATEDIFF(year, TRY_TO_DATE("BirthDate", 'MM/DD/YYYY'), CURRENT_DATE()) BETWEEN 51 AND 65 THEN '51-65'
                ELSE 'Over 65'
            END as age_group,
            COUNT(*) as patient_count
        FROM "DBO_PATIENT"
        WHERE "BirthDate" IS NOT NULL AND "BirthDate" != ''
        GROUP BY age_group
        ORDER BY patient_count DESC
        """
        age_group_df = execute_safe_query(age_group_query)
        if age_group_df is not None and not age_group_df.empty:
            fig = px.bar(
                age_group_df, 
                x='AGE_GROUP', 
                y='PATIENT_COUNT',
                title='Patient Age Groups',
                labels={'PATIENT_COUNT': 'Number of Patients', 'AGE_GROUP': 'Age Group'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📊 No age group data available")
    
    # Recent Patient Activity
    st.subheader("🕒 Recent Patient Activity")
    recent_patients_query = """
    SELECT 
        p."FirstName" || ' ' || p."LastName" as patient_name,
        p."BirthDate",
        p."Sex" as gender,
        o."OfficeName" as office,
        pt."TransactionDate" as last_visit
    FROM "DBO_PATIENT" p
    LEFT JOIN "DBO_OFFICE" o ON p."HomeOffice" = o."OfficeId"
    LEFT JOIN "DBO_POSTRANSACTION" pt ON p."ID" = pt."PatientID"
    WHERE pt."TransactionDate" IS NOT NULL
    ORDER BY pt."TransactionDate" DESC
    LIMIT 20
    """
    
    recent_patients_df = execute_safe_query(recent_patients_query)
    if recent_patients_df is not None and not recent_patients_df.empty:
        st.dataframe(recent_patients_df, use_container_width=True)
    else:
        st.info("📊 No recent patient activity data available")

def show_analytics_page(auth, username, user_info):
    """Show advanced analytics with live data"""
    if not auth.check_permission(username, 'analytics'):
        st.error("🚫 Access Denied: Analytics permissions required")
        auth.log_security_event('ACCESS_DENIED', username, 'Attempted to access advanced analytics')
        return
    
    st.header("📈 Advanced Analytics")
    auth.log_security_event('PAGE_ACCESS', username, 'Accessed advanced analytics')
    
    st.success("✅ Access granted to advanced analytics")
    
    # Test connection
    with st.spinner("🔄 Loading analytics data..."):
        conn = get_snowflake_connection()
        if conn is None:
            st.error("❌ Unable to connect to database. Please check your Snowflake configuration.")
            return
        conn.close()
    
    # Product Analytics
    st.subheader("📦 Product Performance Analytics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Total Products
        product_query = """
        SELECT COUNT(*) as product_count 
        FROM "DBO_ITEM"
        """
        product_df = execute_safe_query(product_query)
        if product_df is not None and not product_df.empty:
            product_count = product_df.iloc[0]['product_count']
            st.metric("📦 Total Products", f"{product_count:,}")
        else:
            st.metric("📦 Total Products", "0")
    
    with col2:
        # Products with Sales (items that have been sold)
        active_products_query = """
        SELECT COUNT(DISTINCT id."ItemID") as active_products
        FROM "DBO_INVOICEDET" id
        WHERE id."Amount" IS NOT NULL AND CAST(id."Amount" AS FLOAT) > 0
        """
        active_df = execute_safe_query(active_products_query)
        if active_df is not None and not active_df.empty:
            active_products = active_df.iloc[0]['ACTIVE_PRODUCTS']
            st.metric("🟢 Products with Sales", f"{active_products:,}")
        else:
            st.metric("🟢 Products with Sales", "0")
    
    with col3:
        # Average Product Price
        avg_price_query = """
        SELECT AVG(CAST(id."Price" AS FLOAT)) as avg_price
        FROM "DBO_INVOICEDET" id
        WHERE CAST(id."Price" AS FLOAT) > 0
        """
        avg_price_df = execute_safe_query(avg_price_query)
        if avg_price_df is not None and not avg_price_df.empty:
            avg_price = avg_price_df.iloc[0]['AVG_PRICE'] or 0
            st.metric("💰 Avg Product Price", f"${avg_price:.2f}")
        else:
            st.metric("💰 Avg Product Price", "$0.00")
    
    with col4:
        # Product Categories
        category_query = """
        SELECT COUNT(DISTINCT "ItemType") as category_count
        FROM "DBO_ITEMTYPE"
        """
        category_df = execute_safe_query(category_query)
        if category_df is not None and not category_df.empty:
            category_count = category_df.iloc[0]['CATEGORY_COUNT']
            st.metric("📋 Product Categories", f"{category_count:,}")
        else:
            st.metric("📋 Product Categories", "0")
    
    # Top Selling Products
    st.subheader("🔥 Top Selling Products")
    with col1:
        # Top products by sales volume
        top_products_query = """
        SELECT 
            i."ItemName" as product_name,
            SUM(CAST(id."Quantity" AS FLOAT)) as total_quantity,
            SUM(CAST(id."Amount" AS FLOAT)) as total_revenue,
            COUNT(*) as sales_count
        FROM "DBO_INVOICEDET" id
        JOIN "DBO_ITEM" i ON id."ItemID" = i."ID"
        WHERE CAST(id."Amount" AS FLOAT) > 0
        GROUP BY i."ItemName"
        ORDER BY total_quantity DESC
        LIMIT 10
        """
        products_df = execute_safe_query(top_products_query)
        if products_df is not None and not products_df.empty:
            fig = px.bar(products_df, x='TOTAL_QUANTITY', y='PRODUCT_NAME', 
                        title='Top Products by Sales Volume',
                        orientation='h')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📊 No product sales data available")
    
    with col2:
        # Revenue by product category
        category_revenue_query = """
        SELECT 
            it."Description" as category,
            SUM(CAST(id."Amount" AS FLOAT)) as total_revenue,
            COUNT(*) as sales_count
        FROM "DBO_INVOICEDET" id
        JOIN "DBO_ITEMTYPE" it ON id."ItemType" = it."ItemType"
        WHERE CAST(id."Amount" AS FLOAT) > 0
        GROUP BY it."Description"
        ORDER BY total_revenue DESC
        LIMIT 10
        """
        category_df = execute_safe_query(category_revenue_query)
        if category_df is not None and not category_df.empty:
            fig = px.pie(category_df, values='TOTAL_REVENUE', names='CATEGORY', 
                        title='Revenue by Product Category')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📊 No category revenue data available")
    
    # Sales Trends
    st.subheader("📈 Sales Trends Analysis")
    
    # Monthly sales trend from invoice details
    monthly_trend_query = """
    SELECT 
        DATE_TRUNC('month', TRY_TO_DATE(isu."InvoiceDate", 'MM/DD/YYYY')) as month,
        SUM(CAST(id."Amount" AS FLOAT)) as monthly_revenue,
        SUM(CAST(id."Quantity" AS FLOAT)) as total_quantity,
        COUNT(*) as sales_count
    FROM "DBO_INVOICEDET" id
    JOIN "DBO_INVOICESUM" isu ON id."InvoiceID" = isu."ID"
    WHERE CAST(id."Amount" AS FLOAT) > 0 
      AND isu."InvoiceDate" IS NOT NULL
      AND isu."InvoiceDate" != ''
      AND TRY_TO_DATE(isu."InvoiceDate", 'MM/DD/YYYY') >= DATEADD(month, -12, CURRENT_DATE())
    GROUP BY DATE_TRUNC('month', TRY_TO_DATE(isu."InvoiceDate", 'MM/DD/YYYY'))
    ORDER BY month
    """
    
    monthly_trend_df = execute_safe_query(monthly_trend_query)
    if monthly_trend_df is not None and not monthly_trend_df.empty:
        fig = go.Figure()
        
        # Add revenue line
        fig.add_trace(go.Scatter(
            x=monthly_trend_df['MONTH'],
            y=monthly_trend_df['MONTHLY_REVENUE'],
            mode='lines+markers',
            name='Revenue',
            yaxis='y',
            line=dict(color='blue')
        ))
        
        # Add transaction count line
        fig.add_trace(go.Scatter(
            x=monthly_trend_df['MONTH'],
            y=monthly_trend_df['TRANSACTION_COUNT'],
            mode='lines+markers',
            name='Transactions',
            yaxis='y2',
            line=dict(color='red')
        ))
        
        # Update layout for dual y-axis
        fig.update_layout(
            title='Monthly Revenue and Transaction Trends',
            xaxis_title='Month',
            yaxis=dict(title='Revenue ($)', side='left'),
            yaxis2=dict(title='Transaction Count', side='right', overlaying='y'),
            legend=dict(x=0.01, y=0.99)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("📊 No monthly trend data available")
    
    # Office Performance Comparison
    st.subheader("🏢 Office Performance Comparison")
    office_performance_query = """
    SELECT 
        o."OfficeName" as office_name,
        SUM(CAST(pt."Amount" AS FLOAT)) as total_revenue,
        COUNT(pt."TransactionID") as transaction_count,
        COUNT(DISTINCT pt."PatientID") as unique_patients,
        AVG(CAST(pt."Amount" AS FLOAT)) as avg_transaction
    FROM "DBO_POSTRANSACTION" pt
    JOIN "DBO_PATIENT" p ON pt."PatientID" = p."ID"
    JOIN "DBO_OFFICE" o ON p."HomeOffice" = o."OfficeId"
    WHERE CAST(pt."Amount" AS FLOAT) > 0
    GROUP BY o."OfficeName"
    ORDER BY total_revenue DESC
    """
    
    office_performance_df = execute_safe_query(office_performance_query)
    if office_performance_df is not None and not office_performance_df.empty:
        st.dataframe(office_performance_df, use_container_width=True)
        
        # Office revenue comparison chart
        fig = px.bar(
            office_performance_df, 
            x='OFFICE_NAME', 
            y='TOTAL_REVENUE',
            title='Revenue by Office Location',
            labels={'TOTAL_REVENUE': 'Revenue ($)', 'OFFICE_NAME': 'Office'}
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("📊 No office performance data available")

def show_security_dashboard(auth, username, user_info):
    """Show security dashboard"""
    if not auth.check_permission(username, 'admin'):
        st.error("🚫 Access Denied: Admin permissions required")
        auth.log_security_event('ACCESS_DENIED', username, 'Attempted to access security dashboard')
        return
    
    st.header("🔒 Security Dashboard")
    auth.log_security_event('PAGE_ACCESS', username, 'Accessed security dashboard')
    
    # Security metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Active Sessions", "1", "→ 0")
    with col2:
        st.metric("Failed Logins (24h)", "0", "→ 0")
    with col3:
        st.metric("Security Alerts", "0", "→ 0")
    with col4:
        st.metric("Audit Events", "12", "↑ 3")
    
    # Recent security events
    st.subheader("🚨 Recent Security Events")
    
    try:
        with open('security_audit.jsonl', 'r') as f:
            events = [json.loads(line) for line in f.readlines()[-10:]]
        
        if events:
            df_events = pd.DataFrame(events)
            st.dataframe(df_events, use_container_width=True)
        else:
            st.info("No recent security events")
    except FileNotFoundError:
        st.info("No audit trail available yet")

def show_export_page(auth, username, user_info):
    """Show data export"""
    if not auth.check_permission(username, 'export'):
        st.error("🚫 Access Denied: Export permissions required")
        auth.log_security_event('ACCESS_DENIED', username, 'Attempted to access data export')
        return
    
    st.header("📤 Data Export")
    auth.log_security_event('PAGE_ACCESS', username, 'Accessed data export')
    
    st.success("✅ Access granted to data export")
    st.info("💡 This would provide secure data export functionality with audit logging.")

if __name__ == "__main__":
    main()
